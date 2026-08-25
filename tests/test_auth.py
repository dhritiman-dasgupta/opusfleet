"""Handshake tests. Run: python -m tests.test_auth

Exercises the real Ingest._handshake against real sockets — no mocks of the
protocol layer, because the bugs worth catching here live in the framing and the
comparison, not in the business logic.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

KEY_A = "11" * 32
KEY_B = "22" * 32
DEV = "860000000000001"

os.environ.setdefault("KAFKA_BOOTSTRAP", "127.0.0.1:1")   # never connects; unused here
os.environ["DEVICE_KEYS"] = f"{DEV}:{KEY_A}"

from server import auth              # noqa: E402
from server import protocol as P     # noqa: E402


async def run_case(mode, client, port_holder):
    os.environ["AUTH_MODE"] = mode
    from server.ingest import Ingest
    ing = Ingest()
    result = {}

    async def serve(reader, writer):
        try:
            dev, authed, _left = await ing._handshake(reader, writer, ("test", 0))
            result["outcome"] = ("accepted", dev, authed)
        except (auth.AuthError, asyncio.TimeoutError, P.ProtocolError) as exc:
            result["outcome"] = ("denied", str(exc), False)
            try:
                writer.write(P.frame(P.build_deny("unauthorized")))
                await writer.drain()
            except (ConnectionError, OSError):
                pass
        finally:
            writer.close()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    port_holder.append(port)
    async with server:
        await client(port)
        for _ in range(50):
            if "outcome" in result:
                break
            await asyncio.sleep(0.02)
    return result.get("outcome", ("timeout", "", False))


async def client_with_key(port, key_hex, device=DEV):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    data = await asyncio.wait_for(r.read(4096), timeout=3)
    frames, _ = P.iter_frames(data)
    nonce = P.parse_challenge(frames[0])
    w.write(P.frame(P.build_auth(device, auth.sign(bytes.fromhex(key_hex), nonce))))
    await w.drain()
    await asyncio.sleep(0.1)
    w.close()
    return nonce


async def client_legacy(port):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.wait_for(r.read(4096), timeout=3)      # ignore the challenge
    w.write(P.frame(P.build_hello(DEV)))                 # firmware that predates auth
    await w.drain()
    await asyncio.sleep(0.1)
    w.close()


async def client_replay(port, stolen_mac):
    """A MAC captured from an earlier session, replayed verbatim."""
    r, w = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.wait_for(r.read(4096), timeout=3)      # a NEW nonce is issued
    w.write(P.frame(P.build_auth(DEV, stolen_mac)))
    await w.drain()
    await asyncio.sleep(0.1)
    w.close()


async def main():
    failures = []

    def check(name, got, want_kind):
        ok = got[0] == want_kind
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {got[0]}" + ("" if ok else f"  (wanted {want_kind})"))
        if not ok:
            failures.append(name)

    print("AUTH_MODE=required")
    check("correct key accepted",
          await run_case("required", lambda p: client_with_key(p, KEY_A), []), "accepted")
    check("wrong key denied",
          await run_case("required", lambda p: client_with_key(p, KEY_B), []), "denied")
    check("unknown device denied",
          await run_case("required", lambda p: client_with_key(p, KEY_A, "999999999999999"), []), "denied")
    check("legacy HELLO denied",
          await run_case("required", client_legacy, []), "denied")

    # replay: sign a real nonce, then present that MAC on a fresh connection
    stolen = {}
    async def capture(p):
        nonce = await client_with_key(p, KEY_A)
        stolen["mac"] = auth.sign(bytes.fromhex(KEY_A), nonce)
    await run_case("required", capture, [])
    check("replayed MAC denied",
          await run_case("required", lambda p: client_replay(p, stolen["mac"]), []), "denied")

    print("AUTH_MODE=optional")
    check("legacy HELLO accepted",
          await run_case("optional", client_legacy, []), "accepted")
    check("correct key still accepted",
          await run_case("optional", lambda p: client_with_key(p, KEY_A), []), "accepted")

    print("AUTH_MODE=disabled")
    check("no handshake at all",
          await run_case("disabled", client_legacy, []), "accepted")

    print()
    if failures:
        print(f"{len(failures)} FAILED: {', '.join(failures)}")
        return 1
    print("all handshake tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
