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

from . import bus, config
from . import protocol as P


class Ingest:
    def __init__(self):
        self.producer = bus.make_producer("ingest")
        self.connections = 0
        self.frames = 0
        self.bytes = 0
        self.dropped = 0
        self.online = {}

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

    # -- connection handling ----------------------------------------------
    async def handle(self, reader, writer):
        peer = writer.get_extra_info("peername")
        self.connections += 1
        imei = "unknown"
        buf = b""
        try:
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

                    if kind == "hello":
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
                  f"queue {len(self.producer)}")


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
