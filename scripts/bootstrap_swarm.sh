#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${SWARM_PROJECT:-leona-swarm}"
COMPOSE=(docker-compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
DEFRA_SERVICES=()

for index in $(seq 1 10); do
  DEFRA_SERVICES+=("defra${index}")
done

ENTITY_SCHEMA='type EntityRecord { tenant: String, ngsiId: String, payload: String }'
TEMPORAL_SCHEMA='type TemporalRecord { tenant: String, ngsiId: String, payload: String, historyJson: String }'
SUBSCRIPTION_SCHEMA='type SubscriptionRecord { tenant: String, ngsiId: String, payload: String }'
ENTITY_MUTATION_SCHEMA='type EntityMutationRecord { tenant: String, eventId: String, entityId: String, operation: String, payload: String, changedAttributesJson: String, originBrokerId: String, createdAtMillis: Float }'

compose_exec() {
  local service=$1
  shift
  "${COMPOSE[@]}" exec -T "$service" "$@"
}

retry() {
  local attempts=$1
  shift

  local attempt
  for attempt in $(seq 1 "$attempts"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

wait_for_defradb() {
  local service=$1
  for attempt in $(seq 1 90); do
    if compose_exec "$service" /defradb client query --url 127.0.0.1:9181 \
      'query { __schema { queryType { name } } }' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s\n' "$service" >&2
  return 1
}

ensure_collection() {
  local service=$1
  local name=$2
  local schema=$3
  local existing

  existing="$(compose_exec "$service" /defradb client collection describe --url 127.0.0.1:9181 --name "$name" 2>/dev/null || printf '[]')"
  if [[ "$(printf '%s' "$existing" | jq 'length')" -eq 0 ]]; then
    compose_exec "$service" /defradb client collection add --url 127.0.0.1:9181 "$schema" >/dev/null
  fi
}

peer_address() {
  local service=$1
  compose_exec "$service" /defradb client p2p info --url 127.0.0.1:9181 \
    | jq -r 'map(select(contains("/ip4/127.0.0.1/") | not))[0]'
}

printf 'Waiting for DefraDB nodes\n'
for service in "${DEFRA_SERVICES[@]}"; do
  wait_for_defradb "$service"
done

printf 'Creating DefraDB collections\n'
for service in "${DEFRA_SERVICES[@]}"; do
  ensure_collection "$service" EntityRecord "$ENTITY_SCHEMA"
  ensure_collection "$service" TemporalRecord "$TEMPORAL_SCHEMA"
  ensure_collection "$service" SubscriptionRecord "$SUBSCRIPTION_SCHEMA"
  ensure_collection "$service" EntityMutationRecord "$ENTITY_MUTATION_SCHEMA"
done

declare -A PEER_ADDRS
for service in "${DEFRA_SERVICES[@]}"; do
  PEER_ADDRS[$service]="$(peer_address "$service")"
done

printf 'Connecting DefraDB peers\n'
for service in "${DEFRA_SERVICES[@]}"; do
  addresses=()
  for peer in "${DEFRA_SERVICES[@]}"; do
    if [[ "$peer" != "$service" ]]; then
      addresses+=("${PEER_ADDRS[$peer]}")
    fi
  done
  retry 20 compose_exec "$service" /defradb client p2p connect --url 127.0.0.1:9181 "${addresses[@]}" >/dev/null
done

printf 'Enabling replicated entity data and mutation-log pubsub synchronization only\n'
for service in "${DEFRA_SERVICES[@]}"; do
  retry 10 compose_exec "$service" /defradb client p2p collection add --url 127.0.0.1:9181 EntityRecord >/dev/null
  retry 10 compose_exec "$service" /defradb client p2p collection add --url 127.0.0.1:9181 EntityMutationRecord >/dev/null
done

printf 'Adding replicated entity data and mutation-log replicators only\n'
for service in "${DEFRA_SERVICES[@]}"; do
  addresses=()
  for peer in "${DEFRA_SERVICES[@]}"; do
    if [[ "$peer" != "$service" ]]; then
      addresses+=("${PEER_ADDRS[$peer]}")
    fi
  done
  retry 10 compose_exec "$service" /defradb client p2p replicator add --url 127.0.0.1:9181 -c EntityRecord "${addresses[@]}" >/dev/null
  retry 10 compose_exec "$service" /defradb client p2p replicator add --url 127.0.0.1:9181 -c EntityMutationRecord "${addresses[@]}" >/dev/null
done

printf 'DefraDB swarm bootstrap complete\n'
