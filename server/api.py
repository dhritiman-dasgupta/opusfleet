"""REST API: the CMS's view of the fleet.

Device state is rebuilt by consuming device.events / device.stat / device.levels
rather than being held in the ingest process. That is the point of putting Kafka
in the middle -- the API can restart, or run on another host, without ingest
knowing or a single device reconnecting.

Routes match the original server so the existing CMS keeps working:
    GET /api/devices     GET /api/timeline?imei=
    GET /api/recordings?imei=&date=      GET /api/dates?imei=
    GET /health
"""

import collections
import json
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

from . import bus, config
from .storage import SegmentStore

STATE = {}
STATE_LOCK = threading.Lock()


def device(imei):
    with STATE_LOCK:
        d = STATE.get(imei)
        if d is None:
            d = STATE[imei] = {
                "imei": imei, "online": False, "frames": 0, "bytes": 0,
                "connected_at": None, "last_seen": None, "addr": None,
                "segments_uploaded": 0, "signal": None,
                "tl": collections.deque(maxlen=config.TIMELINE_DEPTH),
            }
        return d


def consume_state():
    consumer = bus.make_consumer(
        "api-state", [config.TOPIC_EVENTS, config.TOPIC_STAT, config.TOPIC_LEVELS],
        client_id="api")
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            payload = bus.jload(msg.value())
        except (ValueError, UnicodeDecodeError):
            continue
        imei = payload.get("imei") or (msg.key().decode() if msg.key() else "unknown")
        d = device(imei)
        topic = msg.topic()
        with STATE_LOCK:
            if topic == config.TOPIC_EVENTS:
                if payload.get("event") == "segment":
                    d["segments_uploaded"] += 1
                    d["bytes"] += payload.get("bytes", 0)
                elif payload.get("event") == "connect":
                    d["online"] = True
                    d["connected_at"] = payload.get("at")
                    d["addr"] = payload.get("addr")
                else:
                    d["online"] = False
            elif topic == config.TOPIC_STAT:
                d["signal"] = payload
                d["last_seen"] = payload.get("at", time.time())
            elif topic == config.TOPIC_LEVELS:
                d["tl"].append({k: payload[k] for k in ("t", "db", "pk", "f") if k in payload})
                d["last_seen"] = time.time()
                d["frames"] += payload.get("f", 0)


class Handler(BaseHTTPRequestHandler):
    store = None

    def _send(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        imei = (q.get("imei") or [""])[0]

        if u.path == "/health":
            ok, err = self.store.health()
            return self._send({"ok": ok, "storage": "up" if ok else err,
                               "devices": len(STATE)}, 200 if ok else 503)

        if u.path == "/api/devices":
            with STATE_LOCK:
                return self._send([
                    {k: d[k] for k in ("imei", "online", "frames", "bytes",
                                       "segments_uploaded", "last_seen", "signal")}
                    for d in STATE.values()
                ])

        if u.path == "/api/timeline":
            with STATE_LOCK:
                d = STATE.get(imei)
                return self._send(list(d["tl"]) if d else [])

        if u.path == "/api/recordings":
            date = (q.get("date") or [""])[0]
            try:
                return self._send(self.store.list_segments(imei, date or None))
            except Exception as exc:
                return self._send({"error": str(exc)}, 500)

        if u.path == "/api/dates":
            try:
                return self._send(self.store.list_dates(imei))
            except Exception as exc:
                return self._send({"error": str(exc)}, 500)

        self._send({"error": "not found"}, 404)

    def log_message(self, *args):
        pass


class ThreadedHTTP(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def run():
    Handler.store = SegmentStore()
    threading.Thread(target=consume_state, daemon=True).start()
    srv = ThreadedHTTP((config.API_HOST, config.API_PORT), Handler)
    print(f"api on http://{config.API_HOST}:{config.API_PORT} "
          f"(/api/devices /api/timeline /api/recordings /api/dates /health)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\napi stopped")
