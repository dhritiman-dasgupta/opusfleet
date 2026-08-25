"""TCP ingest: device sockets in, Kafka out.

This is the only process that must keep up with every device in real time, so it
does as little as possible -- parse the framing, produce, move on. It never
decodes Opus. Decoding costs roughly a full core at 500 devices, and pushing it
into a consumer means it can be scaled or restarted without dropping a single
socket.

Replaces handle_conn() in the original single-process server, which did ingest,
Opus decode, level metering, WAV segmenting and S3 upload inline on the socket
thread -- so a slow S3 PUT applied backpressure all the way to the microphone.
"""

import asyncio
import time

from confluent_kafka import KafkaException

from . import auth, bus, config
from . import protocol as P


class Ingest:
    def __init__(self):
        self.producer = bus.make_producer("ingest")
        self.connections = 0
        self.frames = 0
        self.bytes = 0
        self.dropped = 0
        self.online = {}
        self.mode = auth.mode()
        self.registry = auth.DeviceRegistry()
        self.limiter = auth.ConnectionLimiter()
        self.denied = 0
        self.unauthenticated = 0
        print(f"[auth] mode={self.mode} · {len(self.registry)} device keys loaded")
        if self.mode == auth.MODE_DISABLED:
            print("[auth] WARNING: authentication disabled — any client can claim any device id")
        elif self.mode == auth.MODE_OPTIONAL:
            print("[auth] NOTE: optional mode — unauthenticated devices are still accepted. "
                  "Set AUTH_MODE=required once the fleet is updated.")

    # -- Kafka plumbing ----------------------------------------------------
    def _produce(self, topic, key, value):
        try:
            self.producer.produce(topic, key=key.encode(), value=value)
        except BufferError:
            # Local queue full: the broker is not keeping up. Drop and count
            # rather than block the event loop and stall every other device.
            self.dropped += 1
        except KafkaException as exc:
            self.dropped += 1
            print(f"produce failed on {topic}: {exc}")

    async def _poll_forever(self):
        """librdkafka needs poll() called to fire delivery callbacks and free queue slots."""
        while True:
            self.producer.poll(0)
            await asyncio.sleep(0.05)

    # -- handshake ---------------------------------------------------------
    async def _read_frame(self, reader, buf, timeout):
        """Read until one complete frame is available. Returns (payload, leftover)."""
        while True:
            frames, rest = P.iter_frames(buf)
            if frames:
                return frames[0], rest + b"".join(P.frame(f) for f in frames[1:])
            chunk = await asyncio.wait_for(reader.read(65536), timeout=timeout)
            if not chunk:
                raise auth.AuthError("connection closed during handshake")
            buf += chunk

    async def _handshake(self, reader, writer, peer):
        """Returns (device_id, authenticated, leftover_bytes). Raises AuthError to refuse.

        The challenge is always sent, even in disabled/optional mode: a device
        that does not understand it simply ignores it and sends HELLO, so this
        stays compatible with firmware that predates authentication.
        """
        if self.mode == auth.MODE_DISABLED:
            return None, False, b""

        nonce = auth.make_nonce()
        writer.write(P.frame(P.build_challenge(nonce)))
        await writer.drain()

        payload, leftover = await self._read_frame(reader, b"", auth.HANDSHAKE_TIMEOUT)
        kind, value = P.classify(payload)

        if kind == "auth":
            device_id, mac = value
            key = self.registry.get(device_id)
            if key and auth.verify(key, nonce, mac):
                writer.write(P.frame(P.build_ok(device_id)))
                await writer.drain()
                return device_id, True, leftover
            # One coarse reason for both failures: distinguishing "unknown device"
            # from "bad MAC" tells an attacker which half of the guess was wrong.
            raise auth.AuthError(f"rejected {device_id} from {peer}: bad credentials")

        if self.mode == auth.MODE_REQUIRED:
            raise auth.AuthError(f"rejected {peer}: no AUTH frame (mode=required)")

        # optional mode: a legacy device that answered with HELLO or audio
        self.unauthenticated += 1
        device_id = value if kind == "hello" else None
        print(f"[auth] UNAUTHENTICATED device accepted from {peer} "
              f"(id={device_id or 'unknown'}) — allowed only because AUTH_MODE=optional")
        return device_id, False, P.frame(payload) + leftover

    # -- connection handling ----------------------------------------------
    async def handle(self, reader, writer):
        peer = writer.get_extra_info("peername")
        ip = peer[0] if peer else "?"
        ok, why = self.limiter.allow(ip)
        if not ok:
            print(f"[auth] {why}")
            writer.close()
            return

        self.connections += 1
        imei = "unknown"
        buf = b""
        authed = False
        try:
            try:
                claimed, authed, buf = await self._handshake(reader, writer, peer)
                if claimed:
                    imei = claimed
            except (auth.AuthError, asyncio.TimeoutError, P.ProtocolError) as exc:
                self.denied += 1
                print(f"[auth] DENY {exc}")
                try:
                    writer.write(P.frame(P.build_deny("unauthorized")))
                    await writer.drain()
                except (ConnectionError, OSError):
                    pass
                return
            if authed:
                print(f"device authenticated: {imei} {peer}")
            while True:
                chunk = await asyncio.wait_for(
                    reader.read(65536), timeout=P.SERVER_RECV_TIMEOUT)
                if not chunk:
                    break
                buf += chunk
                frames, buf = P.iter_frames(buf)
                for payload in frames:
                    try:
                        kind, value = P.classify(payload)
                    except P.ProtocolError:
                        continue

                    if kind == "auth":
                        continue          # handshake already done for this socket
                    if kind == "hello":
                        # An authenticated socket keeps the id it proved; a HELLO
                        # arriving later must not be able to reassign it.
                        if authed and value != imei:
                            print(f"[auth] ignoring HELLO:{value} on a socket authenticated as {imei}")
                            continue
                        imei = value
                        self.online[imei] = time.time()
                        self._produce(config.TOPIC_EVENTS, imei, bus.jdump(
                            {"imei": imei, "event": "connect",
                             "addr": str(peer), "at": time.time()}))
                        print(f"device online: {imei} {peer}")
                    elif kind == "stat":
                        value["at"] = time.time()
                        value["imei"] = imei
                        self._produce(config.TOPIC_STAT, imei, bus.jdump(value))
                    else:
                        self.frames += 1
                        self.bytes += len(payload)
                        self._produce(config.TOPIC_AUDIO, imei, payload)
        except (asyncio.TimeoutError, ConnectionError, OSError):
            pass
        finally:
            self.limiter.release(ip)
            self.online.pop(imei, None)
            self._produce(config.TOPIC_EVENTS, imei, bus.jdump(
                {"imei": imei, "event": "disconnect", "at": time.time()}))
            print(f"device offline: {imei}")
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def report(self, every=30):
        last = (0, 0)
        while True:
            await asyncio.sleep(every)
            df = self.frames - last[0]
            db = self.bytes - last[1]
            last = (self.frames, self.bytes)
            print(f"[ingest] {len(self.online)} online | {self.connections} conns | "
                  f"{df/every:.0f} fps | {db*8/every/1e6:.2f} Mbps | dropped {self.dropped} | "
                  f"queue {len(self.producer)} | denied {self.denied} | "
                  f"unauth {self.unauthenticated} | ratelimited {self.limiter.rejected}")


async def main():
    ing = Ingest()
    server = await asyncio.start_server(
        ing.handle, config.INGEST_HOST, config.INGEST_PORT, backlog=config.INGEST_BACKLOG)
    print(f"ingest listening on {config.INGEST_HOST}:{config.INGEST_PORT} "
          f"-> kafka {config.KAFKA_BOOTSTRAP} topic {config.TOPIC_AUDIO}")
    asyncio.ensure_future(ing._poll_forever())
    asyncio.ensure_future(ing.report())
    async with server:
        await server.serve_forever()


def run():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\ningest stopped")
