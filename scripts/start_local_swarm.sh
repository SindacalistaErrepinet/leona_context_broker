#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${SWARM_PROJECT:-leona-swarm}"
BROKER_COUNT="${SWARM_NODE_COUNT:-10}"
SKIP_MONITOR="${SKIP_MONITOR:-0}"
SWARM_DOWN_ON_EXIT="${SWARM_DOWN_ON_EXIT:-0}"
export SWARM_DOCKERFILE="${SWARM_DOCKERFILE:-docker/Dockerfile.test}"
export SWARM_NODE_IMAGE="${SWARM_NODE_IMAGE:-leona-swarm-node:test}"
SINK_URL="http://127.0.0.1:19080"
MONITOR_BINARY="$ROOT_DIR/target/debug/leona-swarm-monitor"
NODE_SERVICES=()

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
else
  printf 'docker compose (v2 plugin) or docker-compose (v1) is required\n' >&2
  exit 1
fi

for index in $(seq 1 "$BROKER_COUNT"); do
  NODE_SERVICES+=("node${index}")
done

cleanup() {
  local status=$?

  if [[ $status -ne 0 ]]; then
    printf 'Local swarm startup failed. Recent compose logs:\n' >&2
    "${COMPOSE[@]}" logs --tail=100 >&2 || true
  fi

  if [[ "$SWARM_DOWN_ON_EXIT" == "1" ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  elif [[ $status -eq 0 ]]; then
    printf 'Swarm left running. Stop it with:\n'
    printf '  docker compose -f docker-compose.yml -p %s down -v --remove-orphans\n' "$PROJECT_NAME"
  fi

  exit "$status"
}

trap cleanup EXIT

broker_port() {
  printf '%s' "$((18080 + $1))"
}

defra_port() {
  printf '%s' "$((19180 + $1))"
}

sink_ready() {
  curl -fsS "$SINK_URL/health" >/dev/null 2>&1
}

broker_ready() {
  curl -fsS "http://127.0.0.1:$(broker_port "$1")/api-docs/openapi.json" >/dev/null 2>&1
}

wait_until() {
  local description=$1
  local timeout_seconds=$2
  shift 2

  for _ in $(seq 1 "$timeout_seconds"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s\n' "$description" >&2
  return 1
}

printf 'Starting %s combined swarm nodes with image %s\n' "$BROKER_COUNT" "$SWARM_NODE_IMAGE"
"${COMPOSE[@]}" up -d --build sink "${NODE_SERVICES[@]}"

wait_until 'notification sink' 60 sink_ready

printf 'Bootstrapping DefraDB P2P swarm\n'
bash "$ROOT_DIR/scripts/bootstrap_swarm.sh"

for index in $(seq 1 "$BROKER_COUNT"); do
  wait_until "broker${index}" 180 broker_ready "$index"
done

printf 'Swarm ready: brokers on 18081-%s, DefraDB on 19181-%s\n' \
  "$(broker_port "$BROKER_COUNT")" "$(defra_port "$BROKER_COUNT")"

if [[ "$SKIP_MONITOR" == "1" ]]; then
  printf 'SKIP_MONITOR=1 set, not starting the monitor\n'
  exit 0
fi

printf 'Building swarm monitor\n'
cargo build -p leona-swarm-monitor

monitor_args=()
for index in $(seq 1 "$BROKER_COUNT"); do
  monitor_args+=(--defradb "http://127.0.0.1:$(defra_port "$index")")
  monitor_args+=(--broker "http://127.0.0.1:$(broker_port "$index")")
done

printf 'Starting swarm monitor\n'
"$MONITOR_BINARY" "${monitor_args[@]}" "$@"
