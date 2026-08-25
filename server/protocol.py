"""opusfleet wire protocol.

One framing, shared by the device simulator and the server, so the two can never
drift apart. This mirrors the real device exactly:

    the device firmware's transport layer : build_rtp()          -> the 12-byte RTP header
    the device firmware's transport layer : transport_send_frame -> AA 55 + LE16 len (UART hop)
    the modem forwarder : run()              -> BE16 len + payload (TCP hop)
    the original single-process server : handle_conn()    -> the parser on the far end

The UART hop only exists between the device MCU and the modem. A simulator speaks TCP
directly, so it emits the modem's output format: BE16 length + payload.

    TCP :6000   [2-byte big-endian length][payload]  ... repeated forever

Payload is discriminated by prefix:

    b"HELLO:<imei>"     first frame of a connection; registers the device
    b"STAT:{json}"      telemetry, every 30 s
    <anything else>     audio: 12-byte RTP header + one Opus packet

The server decodes audio as `decoder.decode(frame[12:], 960)` and skips any frame
of 12 bytes or fewer, so the RTP header is mandatory on audio frames.
"""

import json
import struct

# ---- audio parameters (must match the device firmware's encoder setup) ----
SAMPLE_RATE = 48000
FRAME_SAMPLES = 960          # 20 ms at 48 kHz
FRAME_MS = 20
CHANNELS = 1
BITRATE = 64000              # opus_encoder_ctl(OPUS_SET_BITRATE(64000))
COMPLEXITY = 3               # must encode in < 20 ms/frame on the MCU
PACKET_LOSS_PERC = 10
USE_FEC = True

# ---- RTP (transport.c:build_rtp) ----
RTP_HEADER_LEN = 12
RTP_VERSION_BYTE = 0x80      # v2, no padding/extension/CSRC
RTP_PAYLOAD_TYPE = 111       # dynamic PT for Opus
RTP_SSRC = 0xDEADBEEF        # hardcoded in firmware; server ignores it

# ---- frame prefixes ----
HELLO_PREFIX = b"HELLO:"
STAT_PREFIX = b"STAT:"

# ---- authentication handshake (see server/auth.py) ----
# server -> device   CHALLENGE:<32 hex>
# device -> server   AUTH:<id>:<64 hex>
# server -> device   OK:<id>  |  DENY:<reason>
CHALLENGE_PREFIX = b"CHALLENGE:"
AUTH_PREFIX = b"AUTH:"
OK_PREFIX = b"OK:"
DENY_PREFIX = b"DENY:"

# ---- limits ----
MAX_FRAME = 0xFFFF           # the BE16 length prefix caps a frame at 64 KiB
SERVER_RECV_TIMEOUT = 90     # conn.settimeout(90) on the server side


class ProtocolError(ValueError):
    """Malformed frame — a length prefix or RTP header that cannot be parsed."""


# --------------------------------------------------------------------------
# framing
# --------------------------------------------------------------------------

def frame(payload: bytes) -> bytes:
    """Wrap a payload in the 2-byte big-endian length prefix used on the TCP hop."""
    if len(payload) > MAX_FRAME:
        raise ProtocolError(f"payload {len(payload)} exceeds {MAX_FRAME}-byte frame limit")
    return struct.pack(">H", len(payload)) + payload


def iter_frames(buf: bytes):
    """Split a receive buffer into complete frames.

    Returns (frames, remainder). The remainder is the partial tail that has not
    arrived in full yet and must be prepended to the next read — the same
    incremental parse the server does.
    """
    frames = []
    off = 0
    n = len(buf)
    while n - off >= 2:
        ln = (buf[off] << 8) | buf[off + 1]
        if n - off < 2 + ln:
            break
        frames.append(buf[off + 2:off + 2 + ln])
        off += 2 + ln
    return frames, buf[off:]


def uart_frame(payload: bytes) -> bytes:
    """The device -> modem framing: AA 55 + little-endian length + payload.

    Only needed to exercise the modem's reframing logic; a TCP simulator does
    not use this. Note the endianness flip against the TCP hop — that asymmetry
    is real and lives in app.py's parser.
    """
    if len(payload) > MAX_FRAME:
        raise ProtocolError(f"payload {len(payload)} exceeds {MAX_FRAME}-byte frame limit")
    return b"\xaa\x55" + struct.pack("<H", len(payload)) + payload


# --------------------------------------------------------------------------
# RTP
# --------------------------------------------------------------------------

def rtp_header(seq: int, ts: int, ssrc: int = RTP_SSRC) -> bytes:
    """Build the 12-byte RTP header. seq wraps at 16 bits, ts at 32."""
    return struct.pack(
        ">BBHII",
        RTP_VERSION_BYTE,
        RTP_PAYLOAD_TYPE,
        seq & 0xFFFF,
        ts & 0xFFFFFFFF,
        ssrc & 0xFFFFFFFF,
    )


def build_audio(opus: bytes, seq: int, ts: int, ssrc: int = RTP_SSRC) -> bytes:
    """RTP header + Opus packet, ready to hand to frame()."""
    return rtp_header(seq, ts, ssrc) + opus


def parse_audio(payload: bytes):
    """Split an audio frame into (seq, ts, ssrc, opus_packet)."""
    if len(payload) <= RTP_HEADER_LEN:
        raise ProtocolError(f"audio frame of {len(payload)} bytes has no Opus payload")
    _v, _pt, seq, ts, ssrc = struct.unpack(">BBHII", payload[:RTP_HEADER_LEN])
    return seq, ts, ssrc, payload[RTP_HEADER_LEN:]


# --------------------------------------------------------------------------
# control frames
# --------------------------------------------------------------------------

def build_challenge(nonce: bytes) -> bytes:
    return CHALLENGE_PREFIX + nonce.hex().encode()


def parse_challenge(payload: bytes) -> bytes:
    """Return the raw nonce bytes from a CHALLENGE frame."""
    try:
        return bytes.fromhex(payload[len(CHALLENGE_PREFIX):].decode("ascii"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProtocolError(f"bad challenge: {exc}") from exc


def build_auth(device_id: str, mac_hex: str) -> bytes:
    return AUTH_PREFIX + f"{device_id}:{mac_hex}".encode("ascii")


def parse_auth(payload: bytes):
    """Split an AUTH frame into (device_id, mac_hex)."""
    body = payload[len(AUTH_PREFIX):].decode("ascii", "ignore")
    if ":" not in body:
        raise ProtocolError("AUTH frame missing the ':' between id and MAC")
    device_id, mac = body.split(":", 1)
    device_id, mac = device_id.strip(), mac.strip()
    if not device_id or not mac:
        raise ProtocolError("AUTH frame has an empty id or MAC")
    return device_id, mac


def build_ok(device_id: str) -> bytes:
    return OK_PREFIX + device_id.encode("ascii")


def build_deny(reason: str) -> bytes:
    # Deliberately coarse: a precise reason tells an attacker which half of the
    # guess was wrong ("unknown device" vs "bad MAC").
    return DENY_PREFIX + reason.encode("ascii")


def build_hello(imei: str) -> bytes:
    return HELLO_PREFIX + imei.encode("ascii")


def build_stat(stat: dict) -> bytes:
    return STAT_PREFIX + json.dumps(stat).encode()


def classify(payload: bytes):
    """Classify a frame the way the server does.

    Returns (kind, value) where kind is "hello" | "auth" | "stat" | "audio":
        hello -> the device id string
        auth  -> (device_id, mac_hex)
        stat  -> the decoded dict
        audio -> the raw payload (RTP header still attached)

    The server checks HELLO/STAT prefixes first and treats everything else as
    audio, so a device whose IMEI-bearing frame is lost still streams — it just
    lands under the "unknown" device.
    """
    if payload.startswith(HELLO_PREFIX):
        return "hello", payload[len(HELLO_PREFIX):].decode("ascii", "ignore").strip() or "unknown"
    if payload.startswith(AUTH_PREFIX):
        return "auth", parse_auth(payload)
    if payload.startswith(STAT_PREFIX):
        try:
            return "stat", json.loads(payload[len(STAT_PREFIX):].decode())
        except (ValueError, UnicodeDecodeError) as exc:
            raise ProtocolError(f"bad STAT json: {exc}") from exc
    return "audio", payload
