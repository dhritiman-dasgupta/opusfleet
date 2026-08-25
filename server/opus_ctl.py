"""Working encoder CTL calls for libopus.

`opus_encoder_ctl(OpusEncoder *st, int request, ...)` is variadic. opuslib 3.0.1
calls it through a ctypes handle with no argtypes set, so libffi uses the plain
fixed-argument convention. On Apple arm64 (and arm64 Linux) variadic arguments
travel on the stack while fixed ones travel in registers, so the value lands
somewhere libopus never reads and every request comes back OPUS_BAD_ARG:

    >>> e = opuslib.Encoder(48000, 1, opuslib.APPLICATION_AUDIO)
    >>> e.bitrate = 64000
    OpusError: b'invalid argument'

The fix is to declare argtypes for the FIXED portion only -- (state, request) --
and pass the value as an extra argument. ctypes then routes the call through
libffi's variadic path and places everything correctly. Declaring the function
as a pinned three-argument prototype does NOT work: that is the fixed
convention again, just spelled explicitly.

Because argtypes lives on the shared CDLL attribute, applying it here also
repairs opuslib's own Encoder property setters process-wide.

Requests are from opus_defines.h and are stable ABI.
"""

import ctypes

import opuslib.api

SET_BITRATE = 4002
SET_COMPLEXITY = 4010
SET_INBAND_FEC = 4012
SET_PACKET_LOSS_PERC = 4014
SET_SIGNAL = 4024
SET_VBR = 4006

SIGNAL_VOICE = 3001
SIGNAL_MUSIC = 3002

_encoder_ctl = opuslib.api.libopus.opus_encoder_ctl
_encoder_ctl.restype = ctypes.c_int
_encoder_ctl.argtypes = [ctypes.c_void_p, ctypes.c_int]   # fixed args only; value is variadic


class OpusCtlError(RuntimeError):
    pass


def encoder_set(encoder, request, value):
    """Apply one integer CTL to an opuslib.Encoder (or a raw encoder pointer)."""
    state = getattr(encoder, "encoder_state", encoder)
    rc = _encoder_ctl(ctypes.cast(state, ctypes.c_void_p), request, ctypes.c_int(int(value)))
    if rc != 0:
        raise OpusCtlError(
            f"opus_encoder_ctl(request={request}, value={value}) failed with {rc}")


def configure_like_firmware(encoder, bitrate, complexity, signal,
                            inband_fec=True, packet_loss_perc=10):
    """Apply exactly the CTLs the device firmware's encoder setup applies, in the same order."""
    encoder_set(encoder, SET_BITRATE, bitrate)
    encoder_set(encoder, SET_COMPLEXITY, complexity)
    encoder_set(encoder, SET_SIGNAL, signal)
    encoder_set(encoder, SET_INBAND_FEC, 1 if inband_fec else 0)
    encoder_set(encoder, SET_PACKET_LOSS_PERC, packet_loss_perc)
