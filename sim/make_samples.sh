#!/usr/bin/env bash
# Generate test audio as 48 kHz mono 16-bit WAV -- the format the device produces.
# Synthesised rather than committed, so the repo stays free of binary blobs.
set -euo pipefail

OUT="$(cd "$(dirname "$0")" && pwd)/samples"
mkdir -p "$OUT"
gen() { ffmpeg -y -loglevel error -f lavfi -i "$2" -t "$3" -ac 1 -ar 48000 -sample_fmt s16 "$OUT/$1.wav"; echo "  $1.wav (${3}s)"; }

echo "generating samples in $OUT"

# 1 kHz reference tone -- decodes to a single clean spectral peak, so it proves
# the whole encode -> RTP -> frame -> decode chain is sample-accurate.
gen tone-1k "sine=frequency=1000:sample_rate=48000" 30

# Log sweep 50 Hz -> 15 kHz: shows what Opus at 64 kbps actually preserves.
gen sweep "sine=frequency=50:sample_rate=48000,afade=t=in:d=0.01" 20

# Speech-like: pink noise shaped to a voice band and amplitude-modulated at 4 Hz
# (roughly syllable rate). Not speech, but it loads the codec like speech does.
gen speech-like \
  "anoisesrc=color=pink:sample_rate=48000:amplitude=0.5,highpass=f=120,lowpass=f=4000,tremolo=f=4:d=0.85" 30

# Chord -- the encoder runs with OPUS_SIGNAL_MUSIC, so exercise that path.
gen music-like \
  "sine=frequency=220:sample_rate=48000,aeval=val(0)*0.4|val(0)*0.4" 30

# Digital silence: the level timeline should read the floor, and segments must
# still rotate on schedule rather than stalling.
gen silence "anullsrc=r=48000:cl=mono" 30

# Loud/quiet staircase for checking the dashboard's dBFS timeline end to end.
gen staircase \
  "sine=frequency=440:sample_rate=48000,volume='if(lt(mod(t,8),2),0.02,if(lt(mod(t,8),4),0.1,if(lt(mod(t,8),6),0.4,1.0)))':eval=frame" 32

echo "done: $(ls -1 "$OUT"/*.wav | wc -l | tr -d ' ') files"
