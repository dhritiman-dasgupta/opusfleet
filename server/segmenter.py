"""Segmenter: audio.raw -> Ogg Opus segments -> object storage.

Never decodes. The device already encoded the audio; this re-muxes the same
packets into an Ogg container, which is why a segment costs ~8 KB/s instead of
the 96 KB/s the original WAV pipeline wrote. At 500 devices that is the
difference between 173 GB/day and 14 GB/day.

Rotation is driven by packet count, not wall clock: 20 ms per packet means
SEGMENT_SECONDS * 50 packets is exactly SEGMENT_SECONDS of audio, so segments
stay honest even when a device bursts a store-and-forward backlog after an
outage. A wall-clock rotation would slice that burst into wrongly-labelled files.
"""

import io
import time
import wave

import opuslib

from . import bus, config
from . import protocol as P
from .oggopus import OggOpusWriter
from .storage import SegmentStore

PACKETS_PER_SEGMENT = int(config.SEGMENT_SECONDS * 1000 / P.FRAME_MS)


class DeviceSegment:
    def __init__(self, imei):
        self.imei = imei
        self.reset()

    def reset(self):
        self.started_at = time.time()
        self.packets = []
        # a fresh Ogg serial per segment keeps each file an independent stream
        self.serial = (hash((self.imei, self.started_at)) & 0x7FFFFFFF) or 1

    def add(self, opus_packet):
        self.packets.append(opus_packet)
        return len(self.packets) >= PACKETS_PER_SEGMENT

    def build_opus(self):
        w = OggOpusWriter(self.serial, channels=P.CHANNELS, frame_samples=P.FRAME_SAMPLES)
        for p in self.packets:
            w.add_packet(p)
        return w.finish(), w.duration_s

    def build_wav(self):
        """Legacy path: decode to 48 kHz mono WAV, matching the original server."""
        dec = opuslib.Decoder(P.SAMPLE_RATE, P.CHANNELS)
        pcm = b"".join(dec.decode(p, P.FRAME_SAMPLES) for p in self.packets)
        buf = io.BytesIO()
        w = wave.open(buf, "wb")
        w.setnchannels(P.CHANNELS)
        w.setsampwidth(2)
        w.setframerate(P.SAMPLE_RATE)
        w.writeframes(pcm)
        w.close()
        return buf.getvalue(), len(pcm) / 2 / P.SAMPLE_RATE


def run():
    store = SegmentStore()
    store.ensure_bucket()
    consumer = bus.make_consumer("segmenter", [config.TOPIC_AUDIO], client_id="segmenter")
    producer = bus.make_producer("segmenter")
    segments = {}
    uploaded = 0
    fmt = config.SEGMENT_FORMAT.lower()
    ext, ctype = ("opus", "audio/ogg") if fmt == "opus" else ("wav", "audio/wav")
    print(f"segmenter: {config.SEGMENT_SECONDS}s {ext} segments "
          f"({PACKETS_PER_SEGMENT} packets) -> {config.S3_BUCKET}")

    def flush(seg):
        nonlocal uploaded
        if not seg.packets:
            return
        data, duration = seg.build_opus() if ext == "opus" else seg.build_wav()
        try:
            key = store.put_segment(seg.imei, data, seg.started_at, duration, ext, ctype)
            uploaded += 1
            # tell the API a segment landed -- it holds no state of its own
            producer.produce(config.TOPIC_EVENTS, key=seg.imei.encode(), value=bus.jdump(
                {"imei": seg.imei, "event": "segment", "key": key,
                 "bytes": len(data), "duration": duration, "at": time.time()}))
            producer.poll(0)
            print(f"[segmenter] {key}  {len(data)/1024:.0f} KiB  {duration:.1f}s  (#{uploaded})")
        except Exception as exc:
            print(f"[segmenter] upload failed for {seg.imei}: {exc}")
        seg.reset()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # idle tick: close out any segment that has gone quiet, so a
                # device that disconnects mid-segment still lands its audio
                cutoff = time.time() - config.SEGMENT_SECONDS * 2
                for seg in list(segments.values()):
                    if seg.packets and seg.started_at < cutoff:
                        flush(seg)
                continue
            if msg.error():
                print(f"[segmenter] {msg.error()}")
                continue

            imei = msg.key().decode() if msg.key() else "unknown"
            try:
                _seq, _ts, _ssrc, opus = P.parse_audio(msg.value())
            except P.ProtocolError:
                continue

            seg = segments.get(imei)
            if seg is None:
                seg = segments[imei] = DeviceSegment(imei)
            if seg.add(opus):
                flush(seg)
    except KeyboardInterrupt:
        pass
    finally:
        for seg in segments.values():
            flush(seg)
        consumer.close()
        print("segmenter stopped")
