#!/bin/bash
# scripts/gcp/startup-node1.sh - runs automatically when the VM starts.

set -e

echo "Node 1 startup script running..."

echo "Installing base operating-system packages..."
apt-get update -qq
apt-get install -y -qq \
  ca-certificates \
  curl \
  git \
  openjdk-17-jre-headless \
  python3 \
  python3-pip \
  python3-venv > /dev/null 2>&1 || true

# Install Docker
if ! command -v docker &> /dev/null; then
  echo "Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  usermod -aG docker $USER
  rm get-docker.sh
fi

# Install Docker Compose v2
if ! command -v docker-compose &> /dev/null; then
  echo "Installing Docker Compose..."
  DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
  mkdir -p $DOCKER_CONFIG/cli-plugins
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
  chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
fi

if ! command -v gcloud &> /dev/null; then
  echo "Installing gcloud CLI..."
  curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  tar -xf google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  ./google-cloud-sdk/install.sh --quiet
  echo 'export PATH="$PATH:$HOME/google-cloud-sdk/bin"' >> ~/.bashrc
fi

echo "Preparing /opt/traffic directory..."
mkdir -p /opt/traffic
chmod 775 /opt/traffic

if ! command -v uv &> /dev/null; then
  echo "Installing uv for local Python workflow checks..."
  curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh
fi

# Configure Docker for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

echo "Node 1 startup complete."
