# Device authentication

Without this, the protocol trusts whatever a socket claims. Send `HELLO:<id>` and
you *are* that device — anything that can reach the port can inject audio into
another unit's recordings.

This adds a challenge–response handshake that a constrained MCU can actually
implement: HMAC-SHA256 over a server-issued nonce. No TLS stack required on the
audio path.

## The handshake

```
   device                                          server
     |  ── TCP connect ──────────────────────────────▶ |
     |                                                 |
     | ◀───────────── CHALLENGE:<32 hex>  (16 bytes)  |   fresh per connection
     |                                                 |
     |  ── AUTH:<device-id>:<64 hex> ─────────────────▶ |   HMAC-SHA256(key, nonce)
     |                                                 |
     | ◀───────────── OK:<device-id>                   |   or DENY:unauthorized
     |                                                 |
     |  ── audio frames as before ────────────────────▶ |
```

Every frame uses the normal `[BE16 length][payload]` framing.

**The nonce is issued by the server**, which is what makes this replay-proof: a
MAC captured off the wire is worthless on the next connection, because the nonce
is different. There is no clock involved, so a device with no RTC is fine.

`DENY` is deliberately vague. Distinguishing "unknown device" from "bad MAC"
would tell an attacker which half of a guess was wrong.

## What this does and does not do

**Does:** proves the device holds a per-device secret, so ids cannot be spoofed
and a single stolen key burns exactly one device, which you then revoke by
deleting one line.

**Does not:** make the audio confidential. The Opus frames still cross the
network in the clear. Only TLS fixes that — see the security row in
[`aws-architecture.svg`](aws-architecture.svg). Treat this as the step you can
ship without re-architecting the link, not as the finish line.

## Rolling it out

A fleet cannot be updated atomically, so the server has three modes:

| `AUTH_MODE` | Behaviour |
|---|---|
| `disabled` | no handshake — the original behaviour |
| `optional` | **default.** Authenticate if the device offers it; accept it if not, and log a warning each time |
| `required` | refuse anything that cannot prove it holds a key |

Run `optional`, watch `unauth` in the ingest stats fall to zero as firmware rolls
out, then set `required`. **That last flip is the step that actually closes the
hole** — `optional` only tells you how far along you are.

```bash
# server: one key per device, mounted as a secret
DEVICE_KEYS_FILE=/run/secrets/device-keys.json   # {"860000000000001": "<64 hex>"}
DEVICE_KEYS="860000000000001:<64 hex>"           # or inline, for testing
AUTH_MODE=optional
```

The key file is re-read when its mtime changes, so adding a device does not
require restarting ingest and dropping every live socket.

Generate a key:

```bash
openssl rand -hex 32
```

## Device side — QuecPython (EC800K)

`uhashlib` has SHA-256 but no HMAC, so it is about fifteen lines:

```python
import uhashlib, usocket, ubinascii

BLOCK = 64

def hmac_sha256(key, msg):
    if len(key) > BLOCK:
        key = uhashlib.sha256(key).digest()
    key = key + b"\x00" * (BLOCK - len(key))
    inner = uhashlib.sha256(bytes(b ^ 0x36 for b in key) + msg).digest()
    return uhashlib.sha256(bytes(b ^ 0x5C for b in key) + inner).digest()

def frame(payload):
    return bytes([len(payload) >> 8, len(payload) & 0xFF]) + payload

def authenticate(sock, device_id, key):
    """Call once, right after connect, before any audio."""
    hdr = sock.recv(2)
    if len(hdr) < 2:
        return False
    n = (hdr[0] << 8) | hdr[1]
    payload = sock.recv(n)
    if not payload.startswith(b"CHALLENGE:"):
        return False                       # server has auth disabled

    nonce = ubinascii.unhexlify(payload[10:])
    mac = ubinascii.hexlify(hmac_sha256(key, nonce)).decode()
    sock.send(frame(b"AUTH:" + device_id.encode() + b":" + mac.encode()))

    hdr = sock.recv(2)
    n = (hdr[0] << 8) | hdr[1]
    return sock.recv(n).startswith(b"OK:")
```

Store the key in the filesystem alongside the config the supervisor already
manages, or better, have it written at manufacture so it never travels.

## Device side — ESP32-S3 (ESP-IDF)

mbedTLS ships with the toolchain, so the MAC is one call:

```c
#include "mbedtls/md.h"

// out must be 32 bytes
static int hmac_sha256(const uint8_t *key, size_t key_len,
                       const uint8_t *msg, size_t msg_len, uint8_t *out)
{
    const mbedtls_md_info_t *info = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
    return mbedtls_md_hmac(info, key, key_len, msg, msg_len, out);
}

// Respond to CHALLENGE:<hex>. Returns 0 on success.
int device_authenticate(int sock, const char *device_id,
                        const uint8_t *key, size_t key_len)
{
    uint8_t hdr[2], buf[256];
    if (recv(sock, hdr, 2, MSG_WAITALL) != 2) return -1;
    int n = (hdr[0] << 8) | hdr[1];
    if (n <= 0 || n > (int)sizeof(buf)) return -1;
    if (recv(sock, buf, n, MSG_WAITALL) != n) return -1;
    if (strncmp((char *)buf, "CHALLENGE:", 10) != 0) return 0;  // auth disabled

    uint8_t nonce[16];
    for (int i = 0; i < 16; i++)
        sscanf((char *)buf + 10 + i * 2, "%2hhx", &nonce[i]);

    uint8_t mac[32];
    if (hmac_sha256(key, key_len, nonce, sizeof(nonce), mac) != 0) return -1;

    char out[128];
    int len = snprintf(out, sizeof(out), "AUTH:%s:", device_id);
    for (int i = 0; i < 32; i++) len += sprintf(out + len, "%02x", mac[i]);

    uint8_t framed[160];
    framed[0] = len >> 8; framed[1] = len & 0xFF;
    memcpy(framed + 2, out, len);
    if (send(sock, framed, len + 2, 0) != len + 2) return -1;

    if (recv(sock, hdr, 2, MSG_WAITALL) != 2) return -1;
    n = (hdr[0] << 8) | hdr[1];
    if (recv(sock, buf, n, MSG_WAITALL) != n) return -1;
    return strncmp((char *)buf, "OK:", 3) == 0 ? 0 : -1;
}
```

**Where the key lives matters more than the algorithm.** In rough order of how
much they are worth:

1. **A secure element** (ATECC608A). The key is generated on-chip and cannot be
   read back — extracting it means attacking the element, not dumping flash.
2. **NVS with flash encryption enabled.** Good, and nearly free if you are
   enabling Secure Boot anyway. Reading the flash off the board yields ciphertext.
3. **Plain NVS.** Better than nothing, but anyone with the board and a programmer
   has the key.

Never ship the same key to every unit. One key per device is what makes
revocation a one-line change instead of a fleet-wide reflash.

## Other hardening in the same path

Authentication stops impersonation but not exhaustion, so ingest also caps
connections per source address:

```bash
MAX_CONNS_PER_IP=64            # concurrent
MAX_CONNS_PER_IP_PER_MIN=120   # new connections per minute
```

An attacker who never completes a handshake can otherwise still tie up sockets.
Both counters, plus `denied` and `unauth`, appear in the ingest stats line so
you can alarm on them.
