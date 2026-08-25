"""One simulated opusfleet device.

Speaks the real TCP wire protocol (see server/protocol.py) so the server cannot
tell it from a real device. Encoder settings are copied from
the device firmware's encoder setup, so frame sizes and bitrate match the hardware.

Audio is encoded to Opus ONCE per (file, bitrate) and cached process-wide. That
matters at fleet scale: 500 devices sharing three clips run three encoders, not
five hundred. Encoding is also ~30x faster than real time, so a fleet that
re-encoded per device would spend its whole CPU budget in libopus instead of
exercising the server.

Failure modes worth simulating, all drawn from things the real device does:

    --loss P     drop P%% of frames before they hit the wire. The PSRAM ring
                 buffer drops NEW frames when full (transport.c), so loss is
                 tail-drop, not random gaps in an otherwise healthy stream.
    --stall S    hold frames for S seconds then burst them. This is the 4G
                 outage path: uart_write_bytes blocks, frames pile into the
                 1.7 MB PSRAM ring (~100 s at 128 kbps), then drain at once.
                 The most under-tested path in the whole system.
    --drop-hello skip the HELLO frame. The server then files the stream under
                 "unknown" rather than dropping it -- worth asserting on.
"""

import argparse
import asyncio
import json
import os
import random
import struct
import subprocess
import sys
import tempfile
import time
import wave

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import opuslib
from server import auth
from server import opus_ctl
from server import protocol as P

# (path, bitrate) -> [opus packet, ...]   shared by every device in the process
_ENCODED_CACHE = {}


# --------------------------------------------------------------------------
# audio
# --------------------------------------------------------------------------

def load_pcm(path):
    """Read a WAV as 48 kHz mono 16-bit PCM, converting via ffmpeg if needed."""
    with wave.open(path, "rb") as w:
        ok = (w.getframerate() == P.SAMPLE_RATE
              and w.getnchannels() == P.CHANNELS
              and w.getsampwidth() == 2)
        if ok:
            return w.readframes(w.getnframes())

    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", path,
         "-ac", str(P.CHANNELS), "-ar", str(P.SAMPLE_RATE), "-sample_fmt", "s16", tmp.name],
        check=True,
    )
    try:
        with wave.open(tmp.name, "rb") as w:
            return w.readframes(w.getnframes())
    finally:
        os.unlink(tmp.name)


def encode_opus(path, bitrate=P.BITRATE):
    """Encode a WAV to a list of Opus packets, one per 20 ms. Cached per process."""
    key = (os.path.abspath(path), bitrate)
    if key in _ENCODED_CACHE:
        return _ENCODED_CACHE[key]

    pcm = load_pcm(path)
    enc = opuslib.Encoder(P.SAMPLE_RATE, P.CHANNELS, opuslib.APPLICATION_AUDIO)
    # opuslib's own setters are broken on arm64 -- see server/opus_ctl.py
    opus_ctl.configure_like_firmware(
        enc, bitrate, P.COMPLEXITY, opus_ctl.SIGNAL_MUSIC,
        inband_fec=P.USE_FEC, packet_loss_perc=P.PACKET_LOSS_PERC)

    step = P.FRAME_SAMPLES * 2                       # bytes per 20 ms frame
    packets = [
        enc.encode(pcm[off:off + step], P.FRAME_SAMPLES)
        for off in range(0, len(pcm) - step + 1, step)
    ]
    if not packets:
        raise ValueError(f"{path}: shorter than one 20 ms frame")

    _ENCODED_CACHE[key] = packets
    return packets


# --------------------------------------------------------------------------
# telemetry
# --------------------------------------------------------------------------

def make_stat(rng, fw=7):
    """A plausible cellular modem report, shaped like app.py:collect_stat()."""
    csq = rng.randint(8, 31)
    op, mcc, mnc = rng.choice([("Jio 4G", "404", "870"), ("airtel", "404", "10"),
                               ("Vi India", "404", "11"), ("BSNL", "404", "72")])
    return {
        "csq": csq,
        "dbm": -113 + 2 * csq,
        "op": op, "mcc": mcc, "mnc": mnc,
        "cells": [
            {"cid": rng.randint(1_000_000, 9_999_999), "mcc": mcc, "mnc": mnc,
             "pci": rng.randint(0, 503), "tac": rng.randint(1000, 65000),
             "earfcn": rng.choice([1850, 3050, 9410, 1300]),
             "rssi": -rng.randint(60, 110)}
            for _ in range(rng.randint(1, 4))
        ],
        "fw": fw,
    }


# --------------------------------------------------------------------------
# the device
# --------------------------------------------------------------------------

class SimDevice:
    """One device: connect, HELLO, then paced audio frames + periodic STAT."""

    def __init__(self, imei, packets, host="127.0.0.1", port=6000, *,
                 loss=0.0, stall=0.0, stall_every=0.0, loop=True,
                 send_hello=True, stat_interval=30.0, ssrc=P.RTP_SSRC,
                 seed=None, on_event=None, key=None):
        self.imei = imei
        self.packets = packets
        self.host, self.port = host, port
        self.loss = loss
        self.stall, self.stall_every = stall, stall_every
        self.loop = loop
        self.send_hello = send_hello
        self.stat_interval = stat_interval
        self.ssrc = ssrc
        self.rng = random.Random(seed if seed is not None else imei)
        self.on_event = on_event or (lambda *a: None)
        self.key = key            # shared device key; None = legacy, unauthenticated

        self.seq = 0
        self.ts = 0
        self.sent_frames = 0
        self.sent_bytes = 0
        self.dropped = 0
        self.reconnects = 0
        self.stopped = False

    def _next_audio_frame(self, idx):
        pkt = self.packets[idx]
        wire = P.frame(P.build_audio(pkt, self.seq, self.ts, self.ssrc))
        self.seq = (self.seq + 1) & 0xFFFF
        self.ts = (self.ts + P.FRAME_SAMPLES) & 0xFFFFFFFF
        return wire

    async def _authenticate(self, reader, writer):
        """Answer the server's challenge, if it issues one and we hold a key.

        A server with AUTH_MODE=disabled sends nothing, so this must not block:
        we wait briefly, and a device with no key simply proceeds the old way.
        """
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=3.0)
        except asyncio.TimeoutError:
            return                                  # no challenge — legacy server
        if not data:
            raise ConnectionError("server closed before the handshake")

        frames, _rest = P.iter_frames(data)
        if not frames or not frames[0].startswith(P.CHALLENGE_PREFIX):
            return
        if self.key is None:
            self.on_event("noauth", self.imei, "challenged but no key configured")
            return

        nonce = P.parse_challenge(frames[0])
        writer.write(P.frame(P.build_auth(self.imei, auth.sign(self.key, nonce))))
        await writer.drain()

        reply = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        rframes, _ = P.iter_frames(reply)
        if rframes and rframes[0].startswith(P.DENY_PREFIX):
            raise ConnectionError(f"server denied auth: {rframes[0].decode(errors='ignore')}")
        if rframes and rframes[0].startswith(P.OK_PREFIX):
            self.on_event("auth", self.imei, "accepted")

    async def run(self):
        while not self.stopped:
            try:
                await self._session()
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                self.on_event("disconnect", self.imei, str(exc))
                self.reconnects += 1
                await asyncio.sleep(2)          # app.py sleeps 2 s before retrying
            if not self.loop:
                return

    async def _session(self):
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), timeout=10)
        self.on_event("connect", self.imei, f"{self.host}:{self.port}")
        try:
            await self._authenticate(reader, writer)
            if self.send_hello:
                writer.write(P.frame(P.build_hello(self.imei)))
                await writer.drain()

            idx = 0
            held = []                            # store-and-forward backlog
            now = time.monotonic()
            next_frame = now
            next_stat = now + self.stat_interval
            stall_until = 0.0
            next_stall = now + self.stall_every if self.stall_every else float("inf")

            while not self.stopped:
                next_frame += P.FRAME_MS / 1000.0
                delay = next_frame - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)

                now = time.monotonic()
                if self.stall_every and now >= next_stall:
                    stall_until = now + self.stall
                    next_stall = now + self.stall_every
                    self.on_event("stall", self.imei, f"{self.stall:.0f}s")

                wire = self._next_audio_frame(idx)
                idx += 1
                if idx >= len(self.packets):
                    if not self.loop:
                        break
                    idx = 0

                if self.loss and self.rng.random() < self.loss:
                    self.dropped += 1            # ring-buffer tail drop
                elif now < stall_until:
                    held.append(wire)            # 4G down: pile into PSRAM
                else:
                    if held:
                        self.on_event("burst", self.imei, f"{len(held)} frames")
                        wire = b"".join(held) + wire
                        held.clear()
                    writer.write(wire)
                    self.sent_frames += 1
                    self.sent_bytes += len(wire)

                if now >= next_stat:
                    next_stat = now + self.stat_interval
                    if now >= stall_until:
                        writer.write(P.frame(P.build_stat(make_stat(self.rng))))
                await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description="Simulate one opusfleet device.")
    ap.add_argument("wav", help="audio to stream (any format ffmpeg reads)")
    ap.add_argument("--imei", default="860000000000001")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6000)
    ap.add_argument("--bitrate", type=int, default=P.BITRATE)
    ap.add_argument("--loss", type=float, default=0.0, help="fraction of frames to drop, 0..1")
    ap.add_argument("--stall", type=float, default=0.0, help="outage length in seconds")
    ap.add_argument("--stall-every", type=float, default=0.0, help="seconds between outages")
    ap.add_argument("--once", action="store_true", help="play the clip once and exit")
    ap.add_argument("--drop-hello", action="store_true", help="never send HELLO")
    ap.add_argument("--duration", type=float, default=0.0, help="stop after N seconds")
    ap.add_argument("--key", default=None,
                    help="device key as hex, to answer the server's auth challenge")
    args = ap.parse_args(argv)

    t0 = time.time()
    packets = encode_opus(args.wav, args.bitrate)
    kbps = sum(len(p) for p in packets) * 8 / (len(packets) * P.FRAME_MS)
    print(f"encoded {os.path.basename(args.wav)}: {len(packets)} frames "
          f"({len(packets) * P.FRAME_MS / 1000:.1f}s audio, {kbps:.1f} kbps) "
          f"in {time.time() - t0:.2f}s")

    dev = SimDevice(
        args.imei, packets, args.host, args.port,
        loss=args.loss, stall=args.stall, stall_every=args.stall_every,
        loop=not args.once, send_hello=not args.drop_hello,
        key=bytes.fromhex(args.key) if args.key else None,
        on_event=lambda kind, imei, detail: print(f"[{imei}] {kind}: {detail}"),
    )

    async def drive():
        task = asyncio.ensure_future(dev.run())
        if args.duration:
            await asyncio.sleep(args.duration)
            dev.stopped = True
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    try:
        asyncio.run(drive())
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[{dev.imei}] sent {dev.sent_frames} frames / {dev.sent_bytes/1024:.0f} KiB, "
              f"dropped {dev.dropped}, reconnects {dev.reconnects}")


if __name__ == "__main__":
    main()
