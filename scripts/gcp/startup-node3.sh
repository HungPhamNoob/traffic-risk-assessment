#!/bin/bash
# scripts/gcp/startup-node3.sh - runs automatically when the VM boots.
set -e
exec > /tmp/startup.log 2>&1

echo "Node 3 batch startup script started at $(date)"

echo "Installing base operating-system packages..."
apt-get update -qq
apt-get install -y -qq ca-certificates curl git openjdk-17-jre-headless python3 python3-pip > /dev/null 2>&1 || true

# Install Docker
if ! command -v docker &> /dev/null; then
  echo "Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  TARGET_USER="${SUDO_USER:-${USER:-}}"
  if [ -n "${TARGET_USER}" ]; then
    usermod -aG docker "${TARGET_USER}" || true
  fi
  rm get-docker.sh
  echo "Docker installed"
fi

# Install Docker Compose v2
if ! docker compose version &> /dev/null; then
  echo "Installing Docker Compose..."
  DOCKER_CONFIG=${DOCKER_CONFIG:-/usr/local/lib/docker}
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
  echo "Installing uv for Spark and H2O training scripts..."
  curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh
fi

# Configure firewall for internal communication
echo "Configuring internal ports..."
# Spark: 7077, 8080
ufw allow 7077/tcp 2>/dev/null || true
ufw allow 8080/tcp 2>/dev/null || true

echo "Node 3 startup completed at $(date)"
