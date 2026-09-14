#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${SWARM_PROJECT:-leona-swarm}"
NODE_COUNT="${SWARM_NODE_COUNT:-10}"
NODE_SERVICES=()

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
else
  printf 'docker compose (v2 plugin) or docker-compose (v1) is required\n' >&2
  exit 1
fi

for index in $(seq 1 "$NODE_COUNT"); do
  NODE_SERVICES+=("node${index}")
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
    if compose_exec "$service" defradb client query --url 127.0.0.1:9181 \
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

  existing="$(compose_exec "$service" defradb client collection describe --url 127.0.0.1:9181 --name "$name" 2>/dev/null || printf '[]')"
  if [[ "$(printf '%s' "$existing" | jq 'length')" -eq 0 ]]; then
    compose_exec "$service" defradb client collection add --url 127.0.0.1:9181 "$schema" >/dev/null
  fi
}

peer_address() {
  local service=$1
  compose_exec "$service" defradb client p2p info --url 127.0.0.1:9181 \
    | jq -r 'map(select(contains("/ip4/127.0.0.1/") | not))[0]'
}

printf 'Stage 1/6: waiting for DefraDB nodes\n'
printf '  Polls each node GraphQL endpoint (127.0.0.1:9181) with a trivial\n'
printf '  __schema query, up to 90s per node, until it answers.\n'
for service in "${NODE_SERVICES[@]}"; do
  wait_for_defradb "$service"
done

printf 'Stage 2/6: creating DefraDB collections\n'
printf '  Ensures the four collections the broker expects pre-exist on every\n'
printf '  node: EntityRecord, TemporalRecord, SubscriptionRecord,\n'
printf '  EntityMutationRecord. Existing collections are left untouched, so\n'
printf '  this stage is idempotent. Only EntityRecord and EntityMutationRecord\n'
printf '  get replicated later; TemporalRecord and SubscriptionRecord stay local.\n'
for service in "${NODE_SERVICES[@]}"; do
  ensure_collection "$service" EntityRecord "$ENTITY_SCHEMA"
  ensure_collection "$service" TemporalRecord "$TEMPORAL_SCHEMA"
  ensure_collection "$service" SubscriptionRecord "$SUBSCRIPTION_SCHEMA"
  ensure_collection "$service" EntityMutationRecord "$ENTITY_MUTATION_SCHEMA"
done

printf 'Stage 3/6: collecting peer addresses\n'
printf '  Reads each node p2p info and picks its first non-loopback multiaddr,\n'
printf '  giving the dial target used by stages 4 and 6.\n'
declare -A PEER_ADDRS
for service in "${NODE_SERVICES[@]}"; do
  PEER_ADDRS[$service]="$(peer_address "$service")"
done

printf 'Stage 4/6: connecting DefraDB peers\n'
printf '  Builds a full mesh: every node dials every other node via p2p\n'
printf '  connect (20 retries each). Without direct dials, pubsub topics would\n'
printf '  not propagate on a fresh local swarm.\n'
for service in "${NODE_SERVICES[@]}"; do
  addresses=()
  for peer in "${NODE_SERVICES[@]}"; do
    if [[ "$peer" != "$service" ]]; then
      addresses+=("${PEER_ADDRS[$peer]}")
    fi
  done
  retry 20 compose_exec "$service" defradb client p2p connect --url 127.0.0.1:9181 "${addresses[@]}" >/dev/null
done

printf 'Stage 5/6: enabling pubsub sync for replicated collections\n'
printf '  Marks EntityRecord and EntityMutationRecord as replicated on each\n'
printf '  node so live document updates and mutation-log events flow over\n'
printf '  DefraDB pubsub. SubscriptionRecord is deliberately excluded.\n'
for service in "${NODE_SERVICES[@]}"; do
  retry 10 compose_exec "$service" defradb client p2p collection add --url 127.0.0.1:9181 EntityRecord >/dev/null
  retry 10 compose_exec "$service" defradb client p2p collection add --url 127.0.0.1:9181 EntityMutationRecord >/dev/null
done

printf 'Stage 6/6: adding replicators\n'
printf '  Registers every other node as a replicator for EntityRecord and\n'
printf '  EntityMutationRecord, so each node pushes those collections to all\n'
printf '  peers on top of the pubsub path (catch-up for offline nodes).\n'
for service in "${NODE_SERVICES[@]}"; do
  addresses=()
  for peer in "${NODE_SERVICES[@]}"; do
    if [[ "$peer" != "$service" ]]; then
      addresses+=("${PEER_ADDRS[$peer]}")
    fi
  done
  retry 10 compose_exec "$service" defradb client p2p replicator add --url 127.0.0.1:9181 -c EntityRecord "${addresses[@]}" >/dev/null
  retry 10 compose_exec "$service" defradb client p2p replicator add --url 127.0.0.1:9181 -c EntityMutationRecord "${addresses[@]}" >/dev/null
done

printf 'DefraDB swarm bootstrap complete\n'
