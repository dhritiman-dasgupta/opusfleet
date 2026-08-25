"""Device authentication for the ingest path.

Without this, the protocol trusts whatever a socket claims: send `HELLO:<id>`
and you are that device. Anything that can reach the port can inject audio into
another unit's recordings, or fill the disk.

The handshake is a server-issued challenge, so it is replay-proof without the
server having to remember anything:

    server -> device   CHALLENGE:<32 hex>          16 random bytes, per connection
    device -> server   AUTH:<id>:<64 hex>          HMAC-SHA256(device_key, nonce)
    server -> device   OK:<id>   |   DENY:<reason>

Why HMAC and not TLS: an MCU with a cellular modem often has no room for a TLS
stack on the audio path, but HMAC-SHA256 is already in mbedTLS on ESP32 and is
~20 lines over `uhashlib` on QuecPython. This is the security you can add
without re-architecting the link. It authenticates the device; it does NOT make
the audio confidential — for that the link still needs TLS, and the AWS
reference architecture shows where that goes.

Rollout is staged with AUTH_MODE, because a fleet cannot be updated atomically:

    disabled  no handshake at all — the original behaviour
    optional  authenticate if the device offers it, accept it if not (DEFAULT)
              every unauthenticated connection is logged as a warning
    required  refuse anything that cannot prove it holds a device key

Run `optional` while the fleet updates, watch the warning count fall to zero,
then flip to `required`. That last flip is the one that actually closes the hole.
"""

import hashlib
import hmac
import json
import os
import secrets
import time
from collections import defaultdict, deque

MODE_DISABLED = "disabled"
MODE_OPTIONAL = "optional"
MODE_REQUIRED = "required"
MODES = (MODE_DISABLED, MODE_OPTIONAL, MODE_REQUIRED)

NONCE_BYTES = 16
MAC_HEX_LEN = 64
HANDSHAKE_TIMEOUT = 10.0


class AuthError(Exception):
    """Handshake failed. The message is safe to log, not to send to the client."""


class DeviceRegistry:
    """Maps a device id to its shared key.

    Keys come from a JSON file (`DEVICE_KEYS_FILE`) so they can be mounted as a
    secret, or from `DEVICE_KEYS` as `id:hexkey,id:hexkey` for quick tests.
    A file is re-read when its mtime changes, so adding a device does not
    require restarting ingest and dropping every live socket.
    """

    def __init__(self, path=None, inline=None):
        self.path = path or os.environ.get("DEVICE_KEYS_FILE") or ""
        self.inline = inline if inline is not None else os.environ.get("DEVICE_KEYS", "")
        self._keys = {}
        self._mtime = None
        self.reload()

    def _parse_inline(self, raw):
        out = {}
        for pair in raw.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if ":" not in pair:
                raise AuthError(f"malformed DEVICE_KEYS entry {pair!r}, expected id:hexkey")
            dev, key = pair.split(":", 1)
            out[dev.strip()] = bytes.fromhex(key.strip())
        return out

    def reload(self):
        keys = {}
        if self.inline:
            keys.update(self._parse_inline(self.inline))
        if self.path and os.path.exists(self.path):
            try:
                self._mtime = os.path.getmtime(self.path)
                with open(self.path) as fh:
                    for dev, key in json.load(fh).items():
                        keys[str(dev)] = bytes.fromhex(key)
            except (OSError, ValueError) as exc:
                # Keep serving the keys already loaded: a malformed edit should
                # not lock out a whole fleet.
                print(f"[auth] could not reload {self.path}: {exc}")
                return self._keys
        self._keys = keys
        return keys

    def maybe_reload(self):
        if not self.path or not os.path.exists(self.path):
            return
        try:
            if os.path.getmtime(self.path) != self._mtime:
                self.reload()
        except OSError:
            pass

    def get(self, device_id):
        self.maybe_reload()
        return self._keys.get(str(device_id))

    def __len__(self):
        return len(self._keys)


def make_nonce():
    """A fresh challenge. Server-issued, so a captured response cannot be replayed."""
    return secrets.token_bytes(NONCE_BYTES)


def sign(key: bytes, nonce: bytes) -> str:
    """What a device holding `key` must return for `nonce`."""
    return hmac.new(key, nonce, hashlib.sha256).hexdigest()


def verify(key: bytes, nonce: bytes, response: str) -> bool:
    """Constant-time comparison — a timing-variable compare leaks the MAC byte by byte."""
    if not key or not response or len(response) != MAC_HEX_LEN:
        return False
    return hmac.compare_digest(sign(key, nonce), response.strip().lower())


class ConnectionLimiter:
    """Caps concurrent connections and connection rate per source address.

    Authentication stops impersonation but not exhaustion: an attacker who never
    completes a handshake can still tie up sockets. This bounds that.
    """

    def __init__(self, max_concurrent=None, max_per_minute=None):
        self.max_concurrent = int(
            max_concurrent if max_concurrent is not None
            else os.environ.get("MAX_CONNS_PER_IP", 64))
        self.max_per_minute = int(
            max_per_minute if max_per_minute is not None
            else os.environ.get("MAX_CONNS_PER_IP_PER_MIN", 120))
        self._live = defaultdict(int)
        self._recent = defaultdict(deque)
        self.rejected = 0

    def allow(self, ip, now=None):
        now = now or time.time()
        window = self._recent[ip]
        while window and now - window[0] > 60:
            window.popleft()
        if self._live[ip] >= self.max_concurrent:
            self.rejected += 1
            return False, f"too many concurrent connections from {ip}"
        if len(window) >= self.max_per_minute:
            self.rejected += 1
            return False, f"connection rate exceeded from {ip}"
        window.append(now)
        self._live[ip] += 1
        return True, ""

    def release(self, ip):
        if self._live.get(ip):
            self._live[ip] -= 1
            if self._live[ip] <= 0:
                self._live.pop(ip, None)


def mode():
    m = os.environ.get("AUTH_MODE", MODE_OPTIONAL).strip().lower()
    if m not in MODES:
        raise AuthError(f"AUTH_MODE must be one of {MODES}, got {m!r}")
    return m
