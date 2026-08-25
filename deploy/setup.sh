#!/usr/bin/env bash
# Provision the opusfleet pipeline on a fresh VPS (Ubuntu 24.04).
#
#   scp -r . root@<ip>:/opt/opusfleet/
#   ssh root@<ip> 'bash /opt/opusfleet/deploy/setup.sh'
#
# Idempotent: safe to re-run after editing .env or pulling new code.
set -euo pipefail

ROOT="${OPUSFLEET_ROOT:-/opt/opusfleet}"
ENV_FILE="$ROOT/.env"

log() { printf '\n=== %s ===\n' "$1"; }

log "docker engine"
if ! command -v docker >/dev/null 2>&1; then
  apt-get update
  apt-get install -y ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
else
  echo "docker present: $(docker --version)"
fi

log "secrets"
if [ ! -f "$ENV_FILE" ]; then
  # Generated once and kept out of git. Never reuse the local dev password here:
  # ingest is internet-facing, so MinIO holds every recording the fleet produces.
  cat > "$ENV_FILE" <<ENVEOF
MINIO_USER=opusfleet
MINIO_PASSWORD=$(openssl rand -base64 30 | tr -d '/+=' | head -c 32)
S3_BUCKET=opusfleet-recordings
SEGMENT_FORMAT=opus
SEGMENT_SECONDS=60
KAFKA_RETENTION_HOURS=12
OPUSFLEET_DOMAIN=${OPUSFLEET_DOMAIN:-audio.example.com}
ENVEOF
  chmod 600 "$ENV_FILE"
  echo "generated $ENV_FILE"
else
  echo "$ENV_FILE exists, leaving it alone"
fi

log "kernel tuning for a few hundred sockets"
cat > /etc/sysctl.d/99-opusfleet.conf <<'SYSEOF'
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.ip_local_port_range = 10000 65535
net.ipv4.tcp_fin_timeout = 20
fs.file-max = 200000
SYSEOF
sysctl -q --system

log "firewall"
# VPSes ship with ufw available but inactive. Only the device port and
# HTTP(S) should face the internet -- Kafka and MinIO stay on loopback and are
# reached through Caddy or an SSH tunnel.
if command -v ufw >/dev/null 2>&1; then
  ufw --force reset >/dev/null
  ufw default deny incoming
  ufw default allow outgoing
  ufw allow 22/tcp    comment 'ssh'
  ufw allow 80/tcp    comment 'acme http-01'
  ufw allow 443/tcp   comment 'cms api + live audio'
  ufw allow 6000/tcp  comment 'device ingest'
  ufw --force enable
  ufw status numbered
fi

log "bringing the stack up"
cd "$ROOT"
docker compose --env-file .env \
  -f docker-compose.yml \
  -f deploy/docker-compose.prod.yml \
  up -d --build

log "status"
docker compose ps
cat <<'DONE'

Next:
  * point a DNS A record at this box and set OPUSFLEET_DOMAIN in .env,
    then re-run, so Caddy can get a certificate (Let's Encrypt will not issue
    for a bare IP).
  * point your devices at this host's port 6000
  * watch it work:
        docker compose logs -f ingest segmenter
DONE
