#!/bin/sh
# Starts DefraDB and the Leona context broker inside one swarm node container.
set -eu

DATA_DIR="${DEFRA_DATA_DIR:-/data}"
DEFRA_URL="${DEFRA_URL:-0.0.0.0:9181}"
DEFRA_P2P_ADDR="${DEFRA_P2P_ADDR:-/ip4/0.0.0.0/tcp/9171}"

/usr/local/bin/defradb start \
  --rootdir "$DATA_DIR" \
  --url "$DEFRA_URL" \
  --p2paddr "$DEFRA_P2P_ADDR" \
  --development &
DEFRA_PID=$!

BROKER_PID=""

shutdown() {
  if [ -n "$BROKER_PID" ]; then
    kill "$BROKER_PID" 2>/dev/null || true
  fi
  kill "$DEFRA_PID" 2>/dev/null || true
}
trap shutdown TERM INT

process_alive() {
  [ -r "/proc/$1/stat" ] || return 1
  read -r _ _ state _ < "/proc/$1/stat" || return 1
  [ "$state" != "Z" ]
}

printf 'waiting for DefraDB at %s\n' "$DEFRA_URL"
attempt=0
until /usr/local/bin/defradb client query \
  --url 127.0.0.1:9181 \
  'query { __schema { queryType { name } } }' >/dev/null 2>&1; do
  if ! process_alive "$DEFRA_PID"; then
    printf 'DefraDB exited before becoming ready\n' >&2
    exit 1
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 180 ]; then
    printf 'timed out waiting for DefraDB\n' >&2
    kill "$DEFRA_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 1
done

/usr/local/bin/leona_context_broker &
BROKER_PID=$!

wait "$BROKER_PID"
status=$?
kill "$DEFRA_PID" 2>/dev/null || true
exit "$status"
