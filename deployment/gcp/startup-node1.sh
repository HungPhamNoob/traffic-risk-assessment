#!/bin/bash
# deployment/gcp/startup-node1.sh - Chạy tự động khi VM start

set -e

echo "🚀 Node 1 startup script running..."

# Install Docker
if ! command -v docker &> /dev/null; then
  echo "🐳 Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  usermod -aG docker $USER
  rm get-docker.sh
fi

# Install Docker Compose v2
if ! command -v docker-compose &> /dev/null; then
  echo "📦 Installing Docker Compose..."
  DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
  mkdir -p $DOCKER_CONFIG/cli-plugins
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
  chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
fi

# Install gcloud CLI (nếu chưa có)
if ! command -v gcloud &> /dev/null; then
  echo "☁️ Installing gcloud CLI..."
  curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  tar -xf google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  ./google-cloud-sdk/install.sh --quiet
  echo 'export PATH="$PATH:$HOME/google-cloud-sdk/bin"' >> ~/.bashrc
fi

# Clone repo (chạy sau khi VM ready, có thể dùng Cloud Build hoặc manual)
echo "📦 Preparing /opt/capstone directory..."
mkdir -p /opt/capstone
chown $USER:$USER /opt/capstone

# Configure Docker for Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

echo "✅ Node 1 startup complete!"