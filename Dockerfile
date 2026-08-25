# One image, several roles. The entrypoint picks which process to run, so the
# ingest / segmenter / live / api / fleet services all share a layer cache and
# can never drift to different versions of protocol.py.
FROM python:3.12-slim AS base

# libopus is loaded at runtime through ctypes, so the shared object must exist
# in the image even though nothing links against it at build time.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libopus0 ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ server/
COPY sim/ sim/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# role is chosen by the compose service; default to the ingest listener
ENTRYPOINT ["python", "-m", "server.run"]
CMD ["ingest"]
