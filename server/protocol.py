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

def build_hello(imei: str) -> bytes:
    return HELLO_PREFIX + imei.encode("ascii")


def build_stat(stat: dict) -> bytes:
    return STAT_PREFIX + json.dumps(stat).encode()


def classify(payload: bytes):
    """Classify a frame the way the server does.

    Returns (kind, value) where kind is "hello" | "stat" | "audio":
        hello -> the IMEI string
        stat  -> the decoded dict
        audio -> the raw payload (RTP header still attached)

    The server checks HELLO/STAT prefixes first and treats everything else as
    audio, so a device whose IMEI-bearing frame is lost still streams — it just
    lands under the "unknown" device.
    """
    if payload.startswith(HELLO_PREFIX):
        return "hello", payload[len(HELLO_PREFIX):].decode("ascii", "ignore").strip() or "unknown"
    if payload.startswith(STAT_PREFIX):
        try:
            return "stat", json.loads(payload[len(STAT_PREFIX):].decode())
        except (ValueError, UnicodeDecodeError) as exc:
            raise ProtocolError(f"bad STAT json: {exc}") from exc
    return "audio", payload
