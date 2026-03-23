#!/bin/bash
set -euo pipefail

# ─── FluxMQ Broker Bootstrap Script ─────────────────────────────────────────
# This runs on first boot via EC2 user_data.  It installs Docker, mounts the
# shared EFS volume, and starts the FluxMQ broker as a Docker container.

exec > /var/log/fluxmq-bootstrap.log 2>&1

echo "=== FluxMQ broker ${broker_id} bootstrap ==="

# ── Install Docker ───────────────────────────────────────────────────────────

apt-get update -y
apt-get install -y docker.io amazon-efs-utils
systemctl enable --now docker

# ── Mount EFS (shared cluster coordination directory) ────────────────────────

mkdir -p /cluster
mount -t efs -o tls ${efs_id}:/ /cluster

# Persist the mount across reboots.
echo "${efs_id}:/ /cluster efs _netdev,tls 0 0" >> /etc/fstab

# ── Create data directory ────────────────────────────────────────────────────

mkdir -p /data

# ── Detect private IP for broker-host advertisement ──────────────────────────

PRIVATE_IP=$(hostname -I | awk '{print $1}')

# ── Build and run FluxMQ ─────────────────────────────────────────────────────
# Clone the repo and build the Docker image on the instance.
# In production you'd push to ECR and pull — this keeps the demo self-contained.

apt-get install -y git g++ make
cd /opt
git clone https://github.com/justinsmith/FluxMQ.git fluxmq || true
cd /opt/fluxmq
docker build -t fluxmq-broker .

# ── Start the broker container ───────────────────────────────────────────────

docker run -d \
  --name fluxmq-broker \
  --restart unless-stopped \
  --network host \
  -v /data:/data \
  -v /cluster:/cluster \
  -e FLUXMQ_BROKER_ID="${broker_id}" \
  -e FLUXMQ_BROKER_HOST="$PRIVATE_IP" \
  -e FLUXMQ_PORT="9092" \
  -e FLUXMQ_DATA_DIR="/data" \
  -e FLUXMQ_CLUSTER_DIR="/cluster" \
  -e FLUXMQ_REPLICATION_FACTOR="${replication_factor}" \
  fluxmq-broker

echo "=== FluxMQ broker ${broker_id} started on $PRIVATE_IP:9092 ==="
