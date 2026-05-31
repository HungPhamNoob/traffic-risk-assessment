#!/bin/bash
# scripts/gcp/startup-node2.sh - runs automatically when the VM boots.
set -e
exec > /tmp/startup.log 2>&1

echo "Node 2 streaming startup script started at $(date)"

ensure_apt_packages() {
  local missing_packages=()
  local package_name
  for package_name in "$@"; do
    if ! dpkg -s "${package_name}" >/dev/null 2>&1; then
      missing_packages+=("${package_name}")
    fi
  done

  if [ "${#missing_packages[@]}" -eq 0 ]; then
    return 0
  fi

  echo "Installing missing operating-system packages: ${missing_packages[*]}"
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${missing_packages[@]}"
}

ensure_apt_packages ca-certificates curl git jq netcat-openbsd python3 python3-pip

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
  mkdir -p "${DOCKER_CONFIG}/cli-plugins"
  curl -SL https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64 \
    -o "${DOCKER_CONFIG}/cli-plugins/docker-compose"
  chmod +x "${DOCKER_CONFIG}/cli-plugins/docker-compose"
  echo "Docker Compose installed"
fi

if ! command -v gcloud &> /dev/null; then
  echo "Installing gcloud CLI..."
  curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  tar -xf google-cloud-cli-460.0.0-linux-x86_64.tar.gz
  ./google-cloud-sdk/install.sh --quiet
  echo 'export PATH="$PATH:$HOME/google-cloud-sdk/bin"' >> ~/.bashrc
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
