#!/bin/bash
# scripts/gcp/startup-node2.sh - runs automatically when the VM boots.
set -e
exec > /tmp/startup.log 2>&1

echo "Node 2 streaming startup script started at $(date)"

echo "Installing base operating-system packages..."
apt-get update -qq
apt-get install -y -qq ca-certificates curl git jq netcat-openbsd python3 python3-pip > /dev/null 2>&1 || true

# Install Docker
if ! command -v docker &> /dev/null; then
  echo "Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  usermod -aG docker $USER
  rm get-docker.sh
  echo "Docker installed"
fi

# Install Docker Compose v2
if ! command -v docker-compose &> /dev/null; then
  echo "Installing Docker Compose..."
  DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
  mkdir -p $DOCKER_CONFIG/cli-plugins
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 \
    -o $DOCKER_CONFIG/cli-plugins/docker-compose
  chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
  echo "Docker Compose installed"
fi

# Configure Docker for Artifact Registry
echo "Configuring Docker for Artifact Registry..."
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

# Prepare directories
echo "Preparing directories..."
mkdir -p /opt/traffic
chmod 775 /opt/traffic

if ! command -v uv &> /dev/null; then
  echo "Installing uv for producer and streaming worker scripts..."
  curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh
fi

# Configure firewall for internal communication
echo "Configuring internal ports..."
# Kafka: 9092, Flink: 8081/6123, Redis: 6379, Schema Registry: 8081
ufw allow 9092/tcp 2>/dev/null || true
ufw allow 8081/tcp 2>/dev/null || true
ufw allow 6379/tcp 2>/dev/null || true

echo "Node 2 startup completed at $(date)"
