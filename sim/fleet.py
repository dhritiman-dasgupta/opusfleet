"""Fleet driver -- run hundreds of simulated devices against the ingest server.

Shape of the load at 500 devices:
    500 devices x 50 frames/s          = 25,000 frames/s
    ~174 bytes/frame (RTP + Opus + len) = ~4.3 MB/s = ~35 Mbps
    Kafka sees 25,000 messages/s; the segmenter writes 500 segments/minute

Two levels of parallelism, because neither alone gets there:

  * processes  -- one Python process saturates a core well before 500 devices,
                  and the GIL means threads would not help. Workers are separate
                  processes, each owning a slice of the fleet.
  * asyncio    -- inside a worker, devices are coroutines. 60+ sockets per worker
                  is cheap when each one does 20 ms of nothing between writes.

Audio is encoded once per worker and shared by reference across its devices, so
adding devices costs sockets, not CPU. Without that, 500 encoders would burn the
whole machine encoding instead of exercising the server -- the load generator
would become the bottleneck under test.
"""

import argparse
import asyncio
import itertools
import multiprocessing as mp
import os
import random
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sim.sim_device import SimDevice, encode_opus
from server import protocol as P


def imei_for(index, base=860000000000000):
    """Deterministic, checksum-free IMEI-shaped id, unique per device index."""
    return str(base + index + 1)


def worker(worker_id, device_indices, args, stats_q, stop_evt):
    """One process: encode the sample pool once, then drive its share of devices."""
    random.seed(worker_id)
    pool = [encode_opus(path, args.bitrate) for path in args.wavs]
    total_frames = sum(len(p) for p in pool)
    print(f"[worker {worker_id}] {len(device_indices)} devices, "
          f"{len(pool)} clips ({total_frames * P.FRAME_MS / 1000:.0f}s audio) encoded")

    devices = []
    for n, idx in enumerate(device_indices):
        devices.append(SimDevice(
            imei_for(idx, args.imei_base),
            pool[n % len(pool)],
            args.host, args.port,
            loss=args.loss,
            stall=args.stall,
            stall_every=args.stall_every,
            loop=True,
            stat_interval=args.stat_interval,
            seed=idx,
        ))

    async def drive():
        tasks = []
        for i, dev in enumerate(devices):
            # stagger connects: 500 simultaneous SYNs is a different test than
            # a fleet coming online, and it is not the one we usually want
            await asyncio.sleep(args.ramp / max(len(devices), 1))
            tasks.append(asyncio.ensure_future(dev.run()))

        last = time.monotonic()
        prev = 0
        while not stop_evt.is_set():
            await asyncio.sleep(args.report_every)
            now = time.monotonic()
            frames = sum(d.sent_frames for d in devices)
            stats_q.put({
                "worker": worker_id,
                "devices": len(devices),
                "frames": frames,
                "fps": (frames - prev) / max(now - last, 1e-9),
                "bytes": sum(d.sent_bytes for d in devices),
                "dropped": sum(d.dropped for d in devices),
                "reconnects": sum(d.reconnects for d in devices),
            })
            prev, last = frames, now

        for dev in devices:
            dev.stopped = True
        for t in tasks:
            t.cancel()

    try:
        asyncio.run(drive())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


def main(argv=None):
    ap = argparse.ArgumentParser(description="Drive a fleet of simulated opusfleet devices.")
    ap.add_argument("-n", "--devices", type=int, default=50)
    ap.add_argument("-w", "--workers", type=int, default=0, help="0 = one per CPU core")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6000)
    ap.add_argument("--wavs", nargs="*", default=None, help="clips to stream (default: all samples)")
    ap.add_argument("--bitrate", type=int, default=P.BITRATE)
    ap.add_argument("--loss", type=float, default=0.0)
    ap.add_argument("--stall", type=float, default=0.0)
    ap.add_argument("--stall-every", type=float, default=0.0)
    ap.add_argument("--stat-interval", type=float, default=30.0)
    ap.add_argument("--imei-base", type=int, default=860000000000000)
    ap.add_argument("--ramp", type=float, default=5.0, help="seconds to bring a worker's devices online")
    ap.add_argument("--duration", type=float, default=0.0, help="0 = run until Ctrl-C")
    ap.add_argument("--report-every", type=float, default=10.0)
    args = ap.parse_args(argv)

    if not args.wavs:
        here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")
        args.wavs = sorted(os.path.join(here, f) for f in os.listdir(here) if f.endswith(".wav"))
        if not args.wavs:
            ap.error(f"no samples in {here} -- run sim/make_samples.sh first")

    workers = args.workers or min(os.cpu_count() or 4, max(1, args.devices // 25) or 1)
    workers = max(1, min(workers, args.devices))
    buckets = [list(itertools.islice(range(args.devices), w, args.devices, workers))
               for w in range(workers)]

    est_mbps = args.devices * 174 * 8 * 50 / 1e6
    print(f"fleet: {args.devices} devices across {workers} workers -> "
          f"{args.host}:{args.port}  (~{args.devices * 50:,} frames/s, ~{est_mbps:.0f} Mbps)")

    ctx = mp.get_context("spawn")
    stats_q = ctx.Queue()
    stop_evt = ctx.Event()
    procs = [ctx.Process(target=worker, args=(w, buckets[w], args, stats_q, stop_evt), daemon=True)
             for w in range(workers)]
    for p in procs:
        p.start()

    t0 = time.time()
    latest = {}
    try:
        while True:
            if args.duration and time.time() - t0 > args.duration:
                break
            # drain whatever the workers have queued, then roll it up
            drained = False
            deadline = time.time() + 1.0
            while time.time() < deadline:
                try:
                    rec = stats_q.get(timeout=max(deadline - time.time(), 0.01))
                except Exception:
                    break
                latest[rec["worker"]] = rec
                drained = True
            if drained and latest:
                vals = [v for v in latest.values() if v]
                dev = sum(v["devices"] for v in vals)
                fps = sum(v["fps"] for v in vals)
                by = sum(v["bytes"] for v in vals)
                dr = sum(v["dropped"] for v in vals)
                rc = sum(v["reconnects"] for v in vals)
                print(f"[fleet {time.time()-t0:6.0f}s] {dev} devices | {fps:8,.0f} fps | "
                      f"{by*8/max(time.time()-t0,1)/1e6:6.1f} Mbps avg | dropped {dr} | reconnects {rc}")
    except KeyboardInterrupt:
        print("\nstopping fleet...")
    finally:
        stop_evt.set()
        for p in procs:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        print("fleet stopped")


if __name__ == "__main__":
    mp.freeze_support()
    main()
