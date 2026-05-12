#!/bin/bash
# deployment/gcp/startup-node3.sh - Chạy tự động khi VM boot
set -e
exec > /tmp/startup.log 2>&1

echo "🚀 Node 3 (Batch & Spatial) startup script started at $(date)"

# Install Docker
if ! command -v docker &> /dev/null; then
  echo "🐳 Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  usermod -aG docker $USER
  rm get-docker.sh
  echo "✅ Docker installed"
fi

# Install Docker Compose v2
if ! command -v docker-compose &> /dev/null; then
  echo "📦 Installing Docker Compose..."
  DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
  mkdir -p $DOCKER_CONFIG/cli-plugins
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 \
    -o $DOCKER_CONFIG/cli-plugins/docker-compose
  chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
  echo "✅ Docker Compose installed"
fi

# Configure Docker for Artifact Registry
echo "🔐 Configuring Docker for Artifact Registry..."
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

# Install additional tools for Spark/Sedona
echo "📥 Installing additional tools..."
apt-get update -qq
apt-get install -y -qq openjdk-11-jdk-headless > /dev/null 2>&1 || true

# Prepare directories
echo "📁 Preparing directories..."
mkdir -p /opt/capstone
chown $USER:$USER /opt/capstone

# Configure firewall for internal communication
echo "🔥 Configuring internal ports..."
# Spark: 7077, 8080
ufw allow 7077/tcp 2>/dev/null || true
ufw allow 8080/tcp 2>/dev/null || true

echo "✅ Node 3 startup completed at $(date)"