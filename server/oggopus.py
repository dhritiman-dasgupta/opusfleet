"""Minimal Ogg Opus muxer.

Storing Opus instead of WAV is a 12x space win (8 KB/s vs 96 KB/s per device),
but raw Opus packets are not a file -- they carry no framing, sample rate, or
channel count, so nothing can play them back. Wrapping them in Ogg keeps the
saving and produces a segment that browsers, ffmpeg, and VLC open directly,
with no decode step on the server.

Re-muxing is cheap because the packets are already encoded: the device did that
work. We never decode to PCM and re-encode, so segments are bit-identical to
what the microphone produced.

Layout per RFC 7845:
    page 0 (BOS)   OpusHead   identification header
    page 1         OpusTags   comment header
    page 2..n      audio packets, lacing-packed, granulepos = samples so far

Reference: RFC 3533 (Ogg), RFC 7845 (Ogg Opus).
"""

import struct

OGG_CAPTURE = b"OggS"
FLAG_CONTINUED = 0x01
FLAG_BOS = 0x02
FLAG_EOS = 0x04

# Ogg's CRC-32: polynomial 0x04c11db7, init 0, no input/output reflection, no
# final xor. This is NOT the zlib/PNG CRC -- that one reflects and inverts, and
# using it produces files every demuxer rejects.
def _make_crc_table():
    table = []
    for i in range(256):
        r = i << 24
        for _ in range(8):
            r = ((r << 1) ^ 0x04C11DB7) & 0xFFFFFFFF if r & 0x80000000 else (r << 1) & 0xFFFFFFFF
        table.append(r)
    return table


_CRC_TABLE = _make_crc_table()


def ogg_crc32(data: bytes) -> int:
    crc = 0
    for b in data:
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CRC_TABLE[((crc >> 24) & 0xFF) ^ b]
    return crc


def _lace(packet_len: int):
    """Segment-table entries for one packet.

    Ogg encodes a length as a run of 255s plus a final byte < 255. A packet whose
    length is an exact multiple of 255 therefore needs a trailing 0, otherwise the
    demuxer treats the packet as continuing into the next page.
    """
    segs = [255] * (packet_len // 255)
    segs.append(packet_len % 255)
    return segs


def _page(serial, seqno, granule, flags, segments, payload):
    header = (
        OGG_CAPTURE
        + bytes([0, flags])
        + struct.pack("<q", granule)
        + struct.pack("<I", serial)
        + struct.pack("<I", seqno)
        + b"\x00\x00\x00\x00"                 # CRC placeholder, filled in below
        + bytes([len(segments)])
        + bytes(segments)
    )
    crc = ogg_crc32(header + payload)
    return header[:22] + struct.pack("<I", crc) + header[26:] + payload


def opus_head(channels=1, pre_skip=312, input_rate=48000, output_gain=0):
    return (b"OpusHead" + bytes([1, channels]) + struct.pack("<H", pre_skip)
            + struct.pack("<I", input_rate) + struct.pack("<h", output_gain) + b"\x00")


def opus_tags(vendor=b"opusfleet"):
    return (b"OpusTags" + struct.pack("<I", len(vendor)) + vendor + struct.pack("<I", 0))


class OggOpusWriter:
    """Accumulates Opus packets and emits a complete Ogg Opus stream.

    Packets must all be the same duration (the device emits 20 ms frames), which
    is what lets granulepos be a simple running sample count.
    """

    MAX_SEGMENTS = 255          # hard limit of the Ogg segment table

    def __init__(self, serial, channels=1, frame_samples=960, pre_skip=312):
        self.serial = serial & 0xFFFFFFFF
        self.channels = channels
        self.frame_samples = frame_samples
        self.pre_skip = pre_skip
        self._out = bytearray()
        self._seqno = 0
        self._granule = 0
        self._packets = []          # packets buffered for the page being built
        self._segments = []
        self.packet_count = 0

        self._emit(opus_head(channels, pre_skip), FLAG_BOS, granule=0)
        self._emit(opus_tags(), 0, granule=0)

    def _emit(self, packet, flags, granule):
        page = _page(self.serial, self._seqno, granule, flags, _lace(len(packet)), packet)
        self._out += page
        self._seqno += 1

    def _flush_page(self, eos=False):
        if not self._packets:
            if eos:                                   # a zero-length EOS page still closes the stream
                self._out += _page(self.serial, self._seqno, self._granule, FLAG_EOS, [0], b"")
                self._seqno += 1
            return
        payload = b"".join(self._packets)
        page = _page(self.serial, self._seqno, self._granule,
                     FLAG_EOS if eos else 0, self._segments, payload)
        self._out += page
        self._seqno += 1
        self._packets = []
        self._segments = []

    def add_packet(self, packet: bytes):
        segs = _lace(len(packet))
        if len(self._segments) + len(segs) > self.MAX_SEGMENTS:
            self._flush_page()
        self._granule += self.frame_samples
        self._packets.append(packet)
        self._segments.extend(segs)
        self.packet_count += 1

    def finish(self) -> bytes:
        self._flush_page(eos=True)
        return bytes(self._out)

    @property
    def duration_s(self) -> float:
        return self.packet_count * self.frame_samples / 48000.0
