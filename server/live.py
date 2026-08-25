"""Live audio: audio.raw -> WebSocket listeners.

Browsers connect to ws://host:8081/?imei=<imei> and receive raw 48 kHz mono
int16 PCM, the same payload the original server broadcast, so the existing CMS
player needs no change.

Decoding is lazy: a device with no listeners is never decoded. That is what
makes 500 devices affordable here -- in practice a handful are being monitored
at any moment, and the rest cost only a dictionary lookup.
"""

import asyncio
import threading
import urllib.parse
from collections import defaultdict

import opuslib
import websockets

from . import bus, config
from . import protocol as P


class Live:
    def __init__(self):
        self.listeners = defaultdict(set)      # imei -> {websocket}
        self.decoders = {}
        self.loop = None
        self.frames_out = 0

    def watched(self, imei):
        return bool(self.listeners.get(imei))

    async def handler(self, ws):
        path = getattr(ws, "request", ws).path if hasattr(ws, "request") else ws.path
        q = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
        imei = (q.get("imei") or ["?"])[0]
        self.listeners[imei].add(ws)
        print(f"[live] listener joined {imei} ({len(self.listeners[imei])} total)")
        try:
            await ws.wait_closed()
        finally:
            self.listeners[imei].discard(ws)
            if not self.listeners[imei]:
                self.listeners.pop(imei, None)
                self.decoders.pop(imei, None)   # free the decoder with the last listener
            print(f"[live] listener left {imei}")

    def consume_forever(self):
        """Runs on a worker thread; hands PCM to the asyncio loop for broadcast."""
        consumer = bus.make_consumer("live", [config.TOPIC_AUDIO], client_id="live")
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                imei = msg.key().decode() if msg.key() else "unknown"
                if not self.watched(imei):
                    continue
                try:
                    _seq, _ts, _ssrc, opus = P.parse_audio(msg.value())
                except P.ProtocolError:
                    continue
                dec = self.decoders.get(imei)
                if dec is None:
                    dec = self.decoders[imei] = opuslib.Decoder(P.SAMPLE_RATE, P.CHANNELS)
                try:
                    pcm = dec.decode(opus, P.FRAME_SAMPLES)
                except Exception:
                    continue
                targets = list(self.listeners.get(imei, ()))
                if targets and self.loop:
                    self.frames_out += 1
                    self.loop.call_soon_threadsafe(websockets.broadcast, targets, pcm)
        finally:
            consumer.close()


async def main():
    live = Live()
    live.loop = asyncio.get_running_loop()
    threading.Thread(target=live.consume_forever, daemon=True).start()
    async with websockets.serve(live.handler, config.WS_HOST, config.WS_PORT, max_size=None):
        print(f"live websocket on ws://{config.WS_HOST}:{config.WS_PORT}/?imei=<imei>")
        await asyncio.Future()


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nlive stopped")
