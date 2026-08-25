"""Levels: audio.raw -> per-second dBFS -> device.levels.

The only consumer that decodes Opus, and the reason decoding was kept out of
ingest: it is the expensive stage (~1 core at 500 devices) and the one most
likely to need scaling. Because it is a Kafka consumer group, running more
replicas splits partitions between them with no change to ingest.

Emits the same shape the CMS timeline already expects:
    {"t": epoch_second, "db": rms_dbfs, "pk": peak_dbfs, "f": frames_that_second}
"""

import math
import time

import numpy as np
import opuslib

from . import bus, config
from . import protocol as P


class Meter:
    """Accumulates one device's frames into whole-second buckets."""

    def __init__(self, imei):
        self.imei = imei
        self.sec = None
        self.sum_sq = 0.0
        self.n = 0
        self.peak = 0
        self.frames = 0

    def add(self, pcm, now):
        sec = int(now)
        out = None
        if self.sec is None:
            self.sec = sec
        elif sec != self.sec:
            out = self.emit()
            self.sec = sec

        a = np.frombuffer(pcm, dtype="<i2").astype(np.float64)
        if a.size:
            self.sum_sq += float(np.dot(a, a))
            self.n += a.size
            self.peak = max(self.peak, int(np.abs(a).max()))
        self.frames += 1
        return out

    def emit(self):
        if not self.n:
            return None
        rms = math.sqrt(self.sum_sq / self.n)
        rec = {
            "t": self.sec,
            "db": round(20 * math.log10(max(rms, 1.0) / 32768.0), 1),
            "pk": round(20 * math.log10(max(self.peak, 1) / 32768.0), 1),
            "f": self.frames,
        }
        self.sum_sq = 0.0
        self.n = 0
        self.peak = 0
        self.frames = 0
        return rec


def run():
    consumer = bus.make_consumer("levels", [config.TOPIC_AUDIO], client_id="levels")
    producer = bus.make_producer("levels")
    meters = {}
    decoders = {}
    print(f"levels: {config.TOPIC_AUDIO} -> {config.TOPIC_LEVELS}")
    try:
        while True:
            msg = consumer.poll(1.0)
            producer.poll(0)
            if msg is None or msg.error():
                continue
            imei = msg.key().decode() if msg.key() else "unknown"
            try:
                _seq, _ts, _ssrc, opus = P.parse_audio(msg.value())
            except P.ProtocolError:
                continue

            dec = decoders.get(imei)
            if dec is None:
                dec = decoders[imei] = opuslib.Decoder(P.SAMPLE_RATE, P.CHANNELS)
            try:
                pcm = dec.decode(opus, P.FRAME_SAMPLES)
            except Exception:
                continue

            meter = meters.get(imei)
            if meter is None:
                meter = meters[imei] = Meter(imei)
            rec = meter.add(pcm, time.time())
            if rec:
                rec["imei"] = imei
                producer.produce(config.TOPIC_LEVELS, key=imei.encode(), value=bus.jdump(rec))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush(5)
        print("levels stopped")
