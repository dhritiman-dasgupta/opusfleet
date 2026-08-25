"""Reference receiver -- the server's ingest path with nothing else attached.

Mirrors handle_conn() from the original single-process server exactly, but instead
of writing S3 segments it reports what it saw. Two uses:

  * validate the simulator (does the wire actually parse?)
  * a baseline to diff the Kafka pipeline against -- same input, same numbers

Unlike the deployed server it tracks RTP sequence numbers, so frame loss and
reordering show up as counts instead of silently vanishing into the decoder.
It also uses numpy rather than audioop, which was removed in Python 3.13.
"""

import argparse
import asyncio
import math
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import opuslib
from server import protocol as P


class DeviceStats:
    def __init__(self, imei):
        self.imei = imei
        self.frames = 0
        self.bytes = 0
        self.samples = 0
        self.stats_rx = 0
        self.decode_errors = 0
        self.lost = 0                 # gaps in the RTP sequence
        self.reordered = 0
        self.last_seq = None
        self.sum_sq = 0.0
        self.peak = 0
        self.first_seen = time.time()
        self.last_seen = self.first_seen
        self.last_stat = None

    def note_seq(self, seq):
        if self.last_seq is not None:
            gap = (seq - self.last_seq - 1) & 0xFFFF
            if gap == 0:
                pass
            elif gap < 0x8000:
                self.lost += gap
            else:
                self.reordered += 1
                return
        self.last_seq = seq

    def note_pcm(self, pcm):
        a = np.frombuffer(pcm, dtype="<i2").astype(np.float64)
        self.samples += a.size
        self.sum_sq += float(np.dot(a, a))
        self.peak = max(self.peak, int(np.abs(a).max()) if a.size else 0)

    @property
    def dbfs(self):
        if not self.samples:
            return float("-inf")
        rms = math.sqrt(self.sum_sq / self.samples)
        return 20 * math.log10(max(rms, 1e-9) / 32768.0)

    @property
    def peak_dbfs(self):
        return 20 * math.log10(max(self.peak, 1) / 32768.0)

    def line(self):
        secs = self.samples / P.SAMPLE_RATE
        wall = max(self.last_seen - self.first_seen, 1e-9)
        kbps = self.bytes * 8 / wall / 1000
        return (f"{self.imei:>16}  {self.frames:>7} fr  {secs:>7.1f}s audio  "
                f"{kbps:>6.1f} kbps  rms {self.dbfs:>6.1f} dBFS  pk {self.peak_dbfs:>6.1f}  "
                f"lost {self.lost:>4}  reord {self.reordered:>3}  "
                f"err {self.decode_errors:>3}  stat {self.stats_rx:>3}")


class Sink:
    def __init__(self, verbose=False):
        self.devices = {}
        self.verbose = verbose
        self.connections = 0

    def dev(self, imei):
        if imei not in self.devices:
            self.devices[imei] = DeviceStats(imei)
        return self.devices[imei]

    async def handle(self, reader, writer):
        peer = writer.get_extra_info("peername")
        self.connections += 1
        imei = "unknown"
        d = None
        dec = opuslib.Decoder(P.SAMPLE_RATE, P.CHANNELS)
        buf = b""
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=P.SERVER_RECV_TIMEOUT)
                if not chunk:
                    break
                buf += chunk
                frames, buf = P.iter_frames(buf)
                for payload in frames:
                    kind, value = P.classify(payload)
                    if kind == "hello":
                        imei = value
                        d = self.dev(imei)
                        if self.verbose:
                            print(f"  + {imei} online from {peer}")
                        continue
                    if kind == "stat":
                        d = d or self.dev(imei)
                        d.stats_rx += 1
                        d.last_stat = value
                        continue
                    d = d or self.dev(imei)
                    d.frames += 1
                    d.bytes += len(payload)
                    d.last_seen = time.time()
                    try:
                        seq, _ts, _ssrc, opus = P.parse_audio(payload)
                    except P.ProtocolError:
                        d.decode_errors += 1
                        continue
                    d.note_seq(seq)
                    try:
                        d.note_pcm(dec.decode(opus, P.FRAME_SAMPLES))
                    except Exception:
                        d.decode_errors += 1
        except (asyncio.TimeoutError, ConnectionError, OSError):
            pass
        finally:
            if self.verbose:
                print(f"  - {imei} offline")
            writer.close()

    async def report(self, every):
        while True:
            await asyncio.sleep(every)
            if not self.devices:
                print("(no devices yet)")
                continue
            tot_fr = sum(d.frames for d in self.devices.values())
            tot_by = sum(d.bytes for d in self.devices.values())
            tot_lost = sum(d.lost for d in self.devices.values())
            print(f"\n--- {len(self.devices)} devices | {self.connections} conns | "
                  f"{tot_fr} frames | {tot_by/1e6:.1f} MB | {tot_lost} lost ---")
            for d in sorted(self.devices.values(), key=lambda x: x.imei)[:20]:
                print(d.line())
            if len(self.devices) > 20:
                print(f"  ... and {len(self.devices) - 20} more")


async def serve(host, port, report_every, verbose):
    sink = Sink(verbose)
    server = await asyncio.start_server(sink.handle, host, port, backlog=1024)
    print(f"sink listening on {host}:{port}  (reporting every {report_every}s)")
    asyncio.ensure_future(sink.report(report_every))
    async with server:
        await server.serve_forever()


def main(argv=None):
    ap = argparse.ArgumentParser(description="Reference opusfleet receiver.")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=6000)
    ap.add_argument("--report-every", type=float, default=10.0)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    try:
        asyncio.run(serve(args.host, args.port, args.report_every, args.verbose))
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
