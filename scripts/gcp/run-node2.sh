#!/bin/bash
# Start Node 2 streaming services.
#
# Node 2 responsibilities:
#   - Three Kafka brokers.
#   - Two raw topics with multiple partitions and three replicas each.
#   - Three replay producers split by row_index modulo producer index.
#   - One Flink job that reads US replay and TomTom live streams together.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"
NODE2_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node2-streaming/docker-compose.yaml"
NODE2_COMPOSE_DIR="$(dirname "${NODE2_COMPOSE_FILE}")"
NODE2_COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME:-node2-streaming}"
NODE2_REFRESH_US_PRODUCERS="${NODE2_REFRESH_US_PRODUCERS:-false}"
NODE2_MANAGED_NAME_PATTERN='^node2-(zookeeper|kafka-1|kafka-2|kafka-3|kafka-topic-init|producer-0|producer-1|producer-2|tomtom-producer|tomtom-live-consumer|flink-jm|flink-tm|redis|flink-python-job)$'
APT_CACHE_UPDATED=0

echo "Node 2 run script started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Project root: ${PROJECT_ROOT}"
echo "Environment file: ${ENV_FILE}"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

apt_install_if_missing() {
  if [ "${APT_CACHE_UPDATED}" -eq 0 ]; then
    sudo apt-get update
    APT_CACHE_UPDATED=1
  fi
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

ensure_command() {
  local command_name="$1"
  local package_name="$2"
  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for '${command_name}': ${package_name}"
  apt_install_if_missing "${package_name}"
}

ensure_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for 'docker compose': docker-compose-plugin"
  apt_install_if_missing docker-compose-plugin
}

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME}" docker compose "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME}" docker-compose "$@"
    return
  fi
  ensure_docker_compose
  COMPOSE_PROJECT_NAME="${NODE2_COMPOSE_PROJECT_NAME}" docker compose "$@"
}

ensure_us_replay_producers() {
  local missing_services=()
  local service_name
  local container_name

  for service_name in producer-1 producer-2 producer-3; do
    case "${service_name}" in
      producer-1) container_name="node2-producer-0" ;;
      producer-2) container_name="node2-producer-1" ;;
      producer-3) container_name="node2-producer-2" ;;
      *) continue ;;
    esac
    if ! docker ps --format '{{.Names}}' | grep -Fx "${container_name}" >/dev/null 2>&1; then
      missing_services+=("${service_name}")
    fi
  done

  if [ "${NODE2_REFRESH_US_PRODUCERS}" = "true" ]; then
    echo "Refreshing US replay producers because NODE2_REFRESH_US_PRODUCERS=true."
    compose_cmd \
      --project-directory "${NODE2_COMPOSE_DIR}" \
      --env-file "${ENV_FILE}" \
      -f "${NODE2_COMPOSE_FILE}" \
      up -d --build producer-1 producer-2 producer-3
    return 0
  fi

  if [ "${#missing_services[@]}" -eq 0 ]; then
    echo "Leaving existing US replay producers untouched to preserve in-flight replay progress."
    return 0
  fi

  echo "Starting missing US replay producers without resetting the healthy ones:"
  printf '  - %s\n' "${missing_services[@]}"
  compose_cmd \
    --project-directory "${NODE2_COMPOSE_DIR}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE2_COMPOSE_FILE}" \
    up -d --build "${missing_services[@]}"
}

remove_conflicting_node2_containers() {
  local conflicting_containers=()

  while IFS= read -r container_name; do
    if [ -z "${container_name}" ]; then
      continue
    fi

    local project_label
    local workdir_label
    project_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' "${container_name}" 2>/dev/null || true)"
    workdir_label="$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project.working_dir" }}' "${container_name}" 2>/dev/null || true)"

    if [ "${project_label}" != "${NODE2_COMPOSE_PROJECT_NAME}" ] || [ "${workdir_label}" != "${NODE2_COMPOSE_DIR}" ]; then
      conflicting_containers+=("${container_name}")
    fi
  done < <(docker ps -a --format '{{.Names}}' | grep -E "${NODE2_MANAGED_NAME_PATTERN}" || true)

  if [ "${#conflicting_containers[@]}" -eq 0 ]; then
    echo "No conflicting Node 2 containers were found."
    return 0
  fi

  echo "Removing Node 2 containers whose Compose labels do not match project '${NODE2_COMPOSE_PROJECT_NAME}':"
  printf '  - %s\n' "${conflicting_containers[@]}"
  docker rm -f "${conflicting_containers[@]}" >/dev/null
}

echo "Checking host dependencies required for Node 2 services."
ensure_command docker docker.io
ensure_docker_compose

echo "Starting Kafka, Redis, TomTom, and Flink streaming services..."

echo "Ensuring the shared Docker network exists before Compose starts."
docker network inspect capstone-net >/dev/null 2>&1 || docker network create capstone-net >/dev/null

echo "Removing conflicting Node 2 containers from previous Compose project names..."
remove_conflicting_node2_containers

compose_cmd \
  --project-directory "${NODE2_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE2_COMPOSE_FILE}" \
  up -d --build \
  zookeeper kafka-1 kafka-2 kafka-3 kafka-topic-init redis \
  flink-jobmanager flink-taskmanager flink-python-job tomtom-producer tomtom-live-consumer

ensure_us_replay_producers

echo "Verifying that the Flink job container is mounted from ${PROJECT_ROOT}."
FLINK_MOUNT_SOURCE="$(docker inspect node2-flink-python-job --format '{{range .Mounts}}{{if eq .Destination "/opt/traffic"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)"
if [ "${FLINK_MOUNT_SOURCE}" != "${PROJECT_ROOT}" ]; then
  echo "ERROR: node2-flink-python-job is mounted from '${FLINK_MOUNT_SOURCE}', expected '${PROJECT_ROOT}'."
  echo "ERROR: Aborting so the operator does not accidentally run an outdated checkout."
  exit 1
fi

echo "Node 2 services:"
compose_cmd \
  --project-directory "${NODE2_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE2_COMPOSE_FILE}" \
  ps

echo "Node 2 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
