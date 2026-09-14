#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${SWARM_PROJECT:-leona-swarm}"
KEEP_SWARM="${KEEP_SWARM:-0}"
BROKER_COUNT=10
ENTITY_ID="urn:ngsi-ld:Vehicle:swarm-e2e"
CREATE_SPEED=10
UPDATE_SPEED=55
CONCURRENT_BASE_SPEED=200
LOAD_TEST_INSERT_COUNT="${LOAD_TEST_INSERT_COUNT:-5000}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-0.25}"
SEQUENTIAL_PROPAGATION_BUDGET_MS="${SEQUENTIAL_PROPAGATION_BUDGET_MS:-30000}"
CONCURRENT_PROPAGATION_BUDGET_MS="${CONCURRENT_PROPAGATION_BUDGET_MS:-60000}"
LOAD_PROPAGATION_BUDGET_MS="${LOAD_PROPAGATION_BUDGET_MS:-180000}"
NOTIFICATION_ALIGNMENT_BUDGET_MS="${NOTIFICATION_ALIGNMENT_BUDGET_MS:-5000}"
SINK_URL="http://127.0.0.1:19080"
ARTIFACT_DIR="$ROOT_DIR/tests/artifacts"
METRIC_JSON_PATH="$ARTIFACT_DIR/swarm_metrics.json"
METRIC_CSV_PATH="$ARTIFACT_DIR/swarm_metrics.csv"
COMPOSE=(docker-compose -f "$ROOT_DIR/docker-compose.yml" -p "$PROJECT_NAME")
DEFRA_SERVICES=()
BROKER_SERVICES=()
declare -A CHANNEL_BASELINE_COUNTS=()
declare -A SEQUENTIAL_READ_LATENCY_MS=()
declare -A SEQUENTIAL_NOTIFICATION_LATENCY_MS=()
declare -A CONCURRENT_START_MS=()
declare -A CONCURRENT_READ_LATENCY_MS=()
declare -A CONCURRENT_NOTIFICATION_LATENCY_MS=()
declare -A METRIC_VALUES=()

for index in $(seq 1 "$BROKER_COUNT"); do
  DEFRA_SERVICES+=("defra${index}")
  BROKER_SERVICES+=("broker${index}")
done

cleanup() {
  local status=$?
  trap - EXIT

  if [[ $status -ne 0 ]]; then
    printf 'Swarm test failed. Recent compose logs:\n' >&2
    "${COMPOSE[@]}" logs --tail=200 >&2 || true
  fi

  if [[ "$KEEP_SWARM" != "1" ]]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi

  exit "$status"
}

trap cleanup EXIT

mkdir -p "$ARTIFACT_DIR"

broker_port() {
  printf '%s' "$((18080 + $1))"
}

broker_api_base() {
  printf 'http://127.0.0.1:%s' "$(broker_port "$1")"
}

broker_url() {
  printf '%s/ngsi-ld/v1' "$(broker_api_base "$1")"
}

subscription_id() {
  printf 'urn:ngsi-ld:Subscription:swarm-%s' "$1"
}

urlencode() {
  printf '%s' "$1" | jq -sRr @uri
}

now_ms() {
  date +%s%3N
}

reset_metric_artifacts() {
  : > "$METRIC_CSV_PATH"
  printf 'phase,kind,scope,owner,node,latency_ms\n' > "$METRIC_CSV_PATH"
  printf '{}\n' > "$METRIC_JSON_PATH"
}

append_metric_json() {
  local phase=$1
  local kind=$2
  local scope=$3
  local owner=$4
  local node=$5
  local latency_ms=$6

  tmp_json="$(mktemp)"
  jq \
    --arg phase "$phase" \
    --arg kind "$kind" \
    --arg scope "$scope" \
    --arg owner "$owner" \
    --arg node "$node" \
    --argjson latency_ms "$latency_ms" \
    '.[$phase] = (.[$phase] // {})
     | .[$phase][$kind] = (.[$phase][$kind] // [])
     | .[$phase][$kind] += [{scope: $scope, owner: $owner, node: $node, latency_ms: $latency_ms}]' \
    "$METRIC_JSON_PATH" > "$tmp_json"
  mv "$tmp_json" "$METRIC_JSON_PATH"
}

record_metric() {
  local phase=$1
  local kind=$2
  local scope=$3
  local owner=$4
  local node=$5
  local latency_ms=$6
  local metric_key="${phase}|${kind}|${scope}|${owner}|${node}"

  METRIC_VALUES["$metric_key"]=$latency_ms
  printf '%s,%s,%s,%s,%s,%s\n' "$phase" "$kind" "$scope" "$owner" "$node" "$latency_ms" >> "$METRIC_CSV_PATH"
  append_metric_json "$phase" "$kind" "$scope" "$owner" "$node" "$latency_ms"
}

abs_value() {
  local value=$1

  if (( value < 0 )); then
    printf '%s' "$((-value))"
  else
    printf '%s' "$value"
  fi
}

wait_until() {
  local description=$1
  local timeout_seconds=$2
  shift 2

  for attempt in $(seq 1 "$timeout_seconds"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s\n' "$description" >&2
  return 1
}

max_of_values() {
  local max=$1
  shift
  local value

  for value in "$@"; do
    if (( value > max )); then
      max=$value
    fi
  done

  printf '%s' "$max"
}

print_latency_stats() {
  local label=$1
  shift
  local values=("$@")
  local count=${#values[@]}
  local min=${values[0]}
  local max=${values[0]}
  local sum=0
  local value

  for value in "${values[@]}"; do
    (( value < min )) && min=$value
    (( value > max )) && max=$value
    sum=$((sum + value))
  done

  printf '%s count=%s min=%sms avg=%sms max=%sms\n' \
    "$label" "$count" "$min" "$((sum / count))" "$max"
}

record_metric_series() {
  local phase=$1
  local kind=$2
  local scope=$3
  local -n samples=$4
  local key
  local owner
  local node

  for key in "${!samples[@]}"; do
    owner="-"
    node="-"
    if [[ "$key" == *".node"* ]]; then
      node="${key##*.node}"
      owner="${key%%.node*}"
    elif [[ "$key" == entity*.node* ]]; then
      owner="${key#entity}"
      owner="${owner%%.node*}"
      node="${key##*.node}"
    fi
    record_metric "$phase" "$kind" "$scope" "$owner" "$node" "${samples[$key]}"
  done
}

assert_max_within() {
  local label=$1
  local actual=$2
  local budget=$3

  if (( actual > budget )); then
    printf '%s max %sms exceeded budget %sms\n' "$label" "$actual" "$budget" >&2
    return 1
  fi

  return 0
}

sink_ready() {
  curl -fsS "$SINK_URL/health" >/dev/null 2>&1
}

broker_ready() {
  local index=$1
  curl -fsS "$(broker_api_base "$index")/api-docs/openapi.json" >/dev/null 2>&1
}

channel_messages() {
  local channel=$1
  curl -fsS "$SINK_URL/messages?channel=$channel"
}

capture_channel_baselines() {
  local index

  for index in $(seq 1 "$BROKER_COUNT"); do
    CHANNEL_BASELINE_COUNTS["node${index}"]="$(channel_messages "node${index}" | jq 'length')"
  done
}

entity_state_matches() {
  local entity_id=$1
  local index=$2
  local state=$3
  local expected_speed=${4:-}
  local encoded_id
  local body_file
  local status_code

  encoded_id="$(urlencode "$entity_id")"
  body_file="$(mktemp)"
  status_code="$(curl -sS -o "$body_file" -w '%{http_code}' "$(broker_url "$index")/entities/$encoded_id")" || {
    rm -f "$body_file"
    return 1
  }

  if [[ "$state" == "deleted" ]]; then
    rm -f "$body_file"
    [[ "$status_code" == "404" ]]
    return
  fi

  if [[ "$status_code" != "200" ]]; then
    rm -f "$body_file"
    return 1
  fi

  if [[ "$(jq -r '.speed.value // empty' "$body_file")" != "$expected_speed" ]]; then
    rm -f "$body_file"
    return 1
  fi

  rm -f "$body_file"
  return 0
}

all_channels_match() {
  local expected_count=$1
  local expected_speed=$2
  local payload

  for index in $(seq 1 "$BROKER_COUNT"); do
    payload="$(channel_messages "node${index}")" || return 1
    [[ "$(printf '%s' "$payload" | jq 'length')" == "$expected_count" ]] || return 1
    [[ "$(printf '%s' "$payload" | jq -r '.[-1].body.data[0].id // empty')" == "$ENTITY_ID" ]] || return 1
    [[ "$(printf '%s' "$payload" | jq -r '.[-1].body.data[0].speed.value // empty')" == "$expected_speed" ]] || return 1
  done

  return 0
}

all_entities_match_speed() {
  local expected_speed=$1
  for index in $(seq 1 "$BROKER_COUNT"); do
    entity_state_matches "$ENTITY_ID" "$index" present "$expected_speed" || return 1
  done

  return 0
}

all_entities_deleted() {
  for index in $(seq 1 "$BROKER_COUNT"); do
    entity_state_matches "$ENTITY_ID" "$index" deleted || return 1
  done

  return 0
}

single_entity_samples_complete() {
  local phase=$1
  local -n samples=$2
  local index

  for index in $(seq 1 "$BROKER_COUNT"); do
    [[ -n "${samples["${phase}.node${index}"]:-}" ]] || return 1
  done

  return 0
}

measure_single_entity_read_latencies() {
  local phase=$1
  local start_ms=$2
  local expected_state=$3
  local expected_speed=$4
  local budget_ms=$5
  local deadline=$((start_ms + budget_ms))
  local now
  local index
  local key

  SEQUENTIAL_READ_LATENCY_MS=()

  while true; do
    now="$(now_ms)"
    for index in $(seq 1 "$BROKER_COUNT"); do
      key="${phase}.node${index}"
      [[ -n "${SEQUENTIAL_READ_LATENCY_MS[$key]:-}" ]] && continue
      if entity_state_matches "$ENTITY_ID" "$index" "$expected_state" "$expected_speed"; then
        SEQUENTIAL_READ_LATENCY_MS[$key]=$((now - start_ms))
      fi
    done

    if single_entity_samples_complete "$phase" SEQUENTIAL_READ_LATENCY_MS; then
      return 0
    fi

    if (( now >= deadline )); then
      printf 'Timed out waiting for %s data propagation samples\n' "$phase" >&2
      return 1
    fi

    sleep "$POLL_INTERVAL_SECONDS"
  done
}

measure_single_entity_notification_latencies() {
  local phase=$1
  local start_ms=$2
  local expected_speed=$3
  local budget_ms=$4
  local deadline=$((start_ms + budget_ms))
  local now
  local index
  local key
  local channel
  local payload
  local received_ms

  SEQUENTIAL_NOTIFICATION_LATENCY_MS=()

  while true; do
    now="$(now_ms)"
    for index in $(seq 1 "$BROKER_COUNT"); do
      key="${phase}.node${index}"
      [[ -n "${SEQUENTIAL_NOTIFICATION_LATENCY_MS[$key]:-}" ]] && continue
      channel="node${index}"
      payload="$(channel_messages "$channel")" || continue
      received_ms="$(printf '%s' "$payload" | jq -r \
        --argjson baseline "${CHANNEL_BASELINE_COUNTS[$channel]}" \
        --arg entity_id "$ENTITY_ID" \
        --argjson expected_speed "$expected_speed" \
        'if (length > $baseline)
         and (.[ $baseline ].body.data[0].id == $entity_id)
         and ((.[ $baseline ].body.data[0].speed.value // -1) == $expected_speed)
         then (.[ $baseline ].receivedAtEpochMs // empty)
         else empty
         end')"
      if [[ -n "$received_ms" && "$received_ms" != "null" ]]; then
        SEQUENTIAL_NOTIFICATION_LATENCY_MS[$key]=$((received_ms - start_ms))
      fi
    done

    if single_entity_samples_complete "$phase" SEQUENTIAL_NOTIFICATION_LATENCY_MS; then
      return 0
    fi

    if (( now >= deadline )); then
      printf 'Timed out waiting for %s notification propagation samples\n' "$phase" >&2
      return 1
    fi

    sleep "$POLL_INTERVAL_SECONDS"
  done
}

report_single_entity_phase_metrics() {
  local phase=$1
  local budget_ms=$2
  local read_values=()
  local notification_values=()
  local alignment_values=()
  local index
  local key
  local read_latency
  local notification_latency
  local delta
  local read_max
  local notification_max
  local alignment_max

  for index in $(seq 1 "$BROKER_COUNT"); do
    key="${phase}.node${index}"
    read_latency="${SEQUENTIAL_READ_LATENCY_MS[$key]}"
    notification_latency="${SEQUENTIAL_NOTIFICATION_LATENCY_MS[$key]}"
    delta="$(abs_value "$((notification_latency - read_latency))")"
    read_values+=("$read_latency")
    notification_values+=("$notification_latency")
    alignment_values+=("$delta")
  done

  print_latency_stats "$phase data propagation ms" "${read_values[@]}"
  print_latency_stats "$phase notification receipt ms" "${notification_values[@]}"
  print_latency_stats "$phase notification/data alignment delta ms" "${alignment_values[@]}"
  record_metric_series "$phase" data_latency sequential SEQUENTIAL_READ_LATENCY_MS
  record_metric_series "$phase" notification_latency sequential SEQUENTIAL_NOTIFICATION_LATENCY_MS

  read_max="$(max_of_values "${read_values[@]}")"
  notification_max="$(max_of_values "${notification_values[@]}")"
  alignment_max="$(max_of_values "${alignment_values[@]}")"

  assert_max_within "$phase data propagation" "$read_max" "$budget_ms"
  assert_max_within "$phase notification receipt" "$notification_max" "$budget_ms"
  assert_max_within "$phase notification/data alignment" "$alignment_max" "$NOTIFICATION_ALIGNMENT_BUDGET_MS"
}

run_single_entity_phase() {
  local phase=$1
  local operation_fn=$2
  local expected_state=$3
  local expected_read_speed=$4
  local expected_notification_speed=$5

  capture_channel_baselines
  local start_ms
  start_ms="$(now_ms)"
  "$operation_fn"
  measure_single_entity_read_latencies "$phase" "$start_ms" "$expected_state" "$expected_read_speed" "$SEQUENTIAL_PROPAGATION_BUDGET_MS"
  measure_single_entity_notification_latencies "$phase" "$start_ms" "$expected_notification_speed" "$SEQUENTIAL_PROPAGATION_BUDGET_MS"
  report_single_entity_phase_metrics "$phase" "$SEQUENTIAL_PROPAGATION_BUDGET_MS"
}

concurrent_entity_id() {
  printf 'urn:ngsi-ld:Vehicle:swarm-concurrent-%s' "$1"
}

concurrent_entity_speed() {
  printf '%s' "$((CONCURRENT_BASE_SPEED + $1))"
}

load_entity_id() {
  printf 'urn:ngsi-ld:Vehicle:swarm-load-%05d' "$1"
}

load_entity_speed() {
  printf '%s' "$((1000 + $1))"
}

concurrent_samples_complete() {
  local -n samples=$1
  local owner_index
  local index

  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    for index in $(seq 1 "$BROKER_COUNT"); do
      [[ -n "${samples["entity${owner_index}.node${index}"]:-}" ]] || return 1
    done
  done

  return 0
}

concurrent_create_request() {
  local owner_index=$1
  local entity_id
  local speed
  local payload

  entity_id="$(concurrent_entity_id "$owner_index")"
  speed="$(concurrent_entity_speed "$owner_index")"
  payload="$(jq -nc \
    --arg id "$entity_id" \
    --argjson speed "$speed" \
    '{
      id: $id,
      type: "Vehicle",
      speed: {
        type: "Property",
        value: $speed
      }
    }')"

  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url "$owner_index")/entities" \
    -o /dev/null
}

concurrent_update_request() {
  local owner_index=$1
  local entity_id
  local encoded_id
  local speed
  local payload

  entity_id="$(concurrent_entity_id "$owner_index")"
  encoded_id="$(urlencode "$entity_id")"
  speed="$((CONCURRENT_BASE_SPEED + 100 + owner_index))"
  payload="$(jq -nc --argjson speed "$speed" '{type: "Property", value: $speed}')"

  curl -fsS \
    -X PUT \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url "$owner_index")/entities/$encoded_id/attrs/speed" \
    -o /dev/null
}

concurrent_delete_request() {
  local owner_index=$1
  local entity_id
  local encoded_id

  entity_id="$(concurrent_entity_id "$owner_index")"
  encoded_id="$(urlencode "$entity_id")"

  curl --fail-with-body -sS \
    -X DELETE \
    "$(broker_url "$owner_index")/entities/$encoded_id" \
    -o /dev/null
}

run_concurrent_insert_requests() {
  local owner_index
  local pids=()

  CONCURRENT_START_MS=()
  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    CONCURRENT_START_MS["entity${owner_index}"]="$(now_ms)"
    concurrent_create_request "$owner_index" &
    pids+=("$!")
  done

  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

run_concurrent_update_requests() {
  local owner_index
  local pids=()

  CONCURRENT_START_MS=()
  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    CONCURRENT_START_MS["entity${owner_index}"]="$(now_ms)"
    concurrent_update_request "$owner_index" &
    pids+=("$!")
  done

  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

run_concurrent_delete_requests() {
  local owner_index
  local pids=()

  CONCURRENT_START_MS=()
  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    CONCURRENT_START_MS["entity${owner_index}"]="$(now_ms)"
    concurrent_delete_request "$owner_index" &
    pids+=("$!")
  done

  for pid in "${pids[@]}"; do
    wait "$pid"
  done
}

measure_concurrent_read_latencies() {
  local state=${1:-present}
  local max_start_ms=0
  local owner_index
  local index
  local entity_id
  local speed
  local start_ms
  local deadline
  local now
  local key

  CONCURRENT_READ_LATENCY_MS=()
  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    start_ms="${CONCURRENT_START_MS["entity${owner_index}"]}"
    (( start_ms > max_start_ms )) && max_start_ms=$start_ms
  done
  deadline=$((max_start_ms + CONCURRENT_PROPAGATION_BUDGET_MS))

  while true; do
    now="$(now_ms)"
    for owner_index in $(seq 1 "$BROKER_COUNT"); do
      entity_id="$(concurrent_entity_id "$owner_index")"
      if [[ "$state" == "present" ]]; then
        speed="$(concurrent_entity_speed "$owner_index")"
      elif [[ "$state" == "updated" ]]; then
        speed="$((CONCURRENT_BASE_SPEED + 100 + owner_index))"
      else
        speed=''
      fi
      start_ms="${CONCURRENT_START_MS["entity${owner_index}"]}"
      for index in $(seq 1 "$BROKER_COUNT"); do
        key="entity${owner_index}.node${index}"
        [[ -n "${CONCURRENT_READ_LATENCY_MS[$key]:-}" ]] && continue
        if [[ "$state" == "deleted" ]]; then
          entity_state_matches "$entity_id" "$index" deleted || continue
        else
          entity_state_matches "$entity_id" "$index" present "$speed" || continue
        fi
        CONCURRENT_READ_LATENCY_MS[$key]=$((now - start_ms))
      done
    done

    if concurrent_samples_complete CONCURRENT_READ_LATENCY_MS; then
      return 0
    fi

    if (( now >= deadline )); then
      printf 'Timed out waiting for concurrent data propagation samples\n' >&2
      return 1
    fi

    sleep "$POLL_INTERVAL_SECONDS"
  done
}

measure_concurrent_notification_latencies() {
  local phase=$1
  local expected_speed_mode=$2
  local max_start_ms=0
  local owner_index
  local index
  local channel
  local payload
  local start_ms
  local deadline
  local now
  local key
  local entity_id
  local received_ms

  CONCURRENT_NOTIFICATION_LATENCY_MS=()
  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    start_ms="${CONCURRENT_START_MS["entity${owner_index}"]}"
    (( start_ms > max_start_ms )) && max_start_ms=$start_ms
  done
  deadline=$((max_start_ms + CONCURRENT_PROPAGATION_BUDGET_MS))

  while true; do
    now="$(now_ms)"
    for index in $(seq 1 "$BROKER_COUNT"); do
      channel="node${index}"
      payload="$(channel_messages "$channel")" || continue
      for owner_index in $(seq 1 "$BROKER_COUNT"); do
        key="entity${owner_index}.node${index}"
        [[ -n "${CONCURRENT_NOTIFICATION_LATENCY_MS[$key]:-}" ]] && continue
        entity_id="$(concurrent_entity_id "$owner_index")"
        if [[ "$expected_speed_mode" == "created" ]]; then
          expected_speed="$(concurrent_entity_speed "$owner_index")"
        elif [[ "$expected_speed_mode" == "updated" ]]; then
          expected_speed="$((CONCURRENT_BASE_SPEED + 100 + owner_index))"
        else
          expected_speed=''
        fi
        received_ms="$(printf '%s' "$payload" | jq -r \
          --argjson baseline "${CHANNEL_BASELINE_COUNTS[$channel]}" \
          --arg entity_id "$entity_id" \
          --arg expected_speed "$expected_speed" \
          '([.[ $baseline: ][]
             | select(.body.data[0].id == $entity_id)
             | select(($expected_speed == "") or ((.body.data[0].speed.value | tostring) == $expected_speed))
             | .receivedAtEpochMs][0]) // empty')"
        if [[ -n "$received_ms" && "$received_ms" != "null" ]]; then
          start_ms="${CONCURRENT_START_MS["entity${owner_index}"]}"
          CONCURRENT_NOTIFICATION_LATENCY_MS[$key]=$((received_ms - start_ms))
        fi
      done
    done

    if concurrent_samples_complete CONCURRENT_NOTIFICATION_LATENCY_MS; then
      return 0
    fi

    if (( now >= deadline )); then
      printf 'Timed out waiting for concurrent notification propagation samples\n' >&2
      return 1
    fi

    sleep "$POLL_INTERVAL_SECONDS"
  done
}

report_concurrent_insert_metrics() {
  local phase_name=${1:-concurrent insert}
  local read_values=()
  local notification_values=()
  local alignment_values=()
  local owner_index
  local index
  local key
  local read_latency
  local notification_latency
  local delta
  local read_max
  local notification_max
  local alignment_max

  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    for index in $(seq 1 "$BROKER_COUNT"); do
      key="entity${owner_index}.node${index}"
      read_latency="${CONCURRENT_READ_LATENCY_MS[$key]}"
      notification_latency="${CONCURRENT_NOTIFICATION_LATENCY_MS[$key]}"
      delta="$(abs_value "$((notification_latency - read_latency))")"
      read_values+=("$read_latency")
      notification_values+=("$notification_latency")
      alignment_values+=("$delta")
    done
  done

  print_latency_stats "$phase_name data propagation ms" "${read_values[@]}"
  print_latency_stats "$phase_name notification receipt ms" "${notification_values[@]}"
  print_latency_stats "$phase_name notification/data alignment delta ms" "${alignment_values[@]}"
  record_metric_series "$phase_name" data_latency concurrent CONCURRENT_READ_LATENCY_MS
  record_metric_series "$phase_name" notification_latency concurrent CONCURRENT_NOTIFICATION_LATENCY_MS

  read_max="$(max_of_values "${read_values[@]}")"
  notification_max="$(max_of_values "${notification_values[@]}")"
  alignment_max="$(max_of_values "${alignment_values[@]}")"

  assert_max_within "$phase_name data propagation" "$read_max" "$CONCURRENT_PROPAGATION_BUDGET_MS"
  assert_max_within "$phase_name notification receipt" "$notification_max" "$CONCURRENT_PROPAGATION_BUDGET_MS"
  assert_max_within "$phase_name notification/data alignment" "$alignment_max" "$NOTIFICATION_ALIGNMENT_BUDGET_MS"
}

run_concurrent_insert_phase() {
  capture_channel_baselines
  run_concurrent_insert_requests
  measure_concurrent_read_latencies present
  measure_concurrent_notification_latencies 'concurrent insert' created
  report_concurrent_insert_metrics 'concurrent insert'
}

run_concurrent_update_phase() {
  capture_channel_baselines
  run_concurrent_update_requests
  measure_concurrent_read_latencies updated
  measure_concurrent_notification_latencies 'concurrent update' updated
  report_concurrent_insert_metrics 'concurrent update'
}

run_concurrent_delete_phase() {
  capture_channel_baselines
  run_concurrent_delete_requests
  measure_concurrent_read_latencies deleted
  measure_concurrent_notification_latencies 'concurrent delete' updated
  report_concurrent_insert_metrics 'concurrent delete'
}

load_insert_batch() {
  local broker_index=$1
  local start_index=$2
  local count=$3
  local body_file
  local payload
  local status_code

  payload="$(jq -nc \
    --argjson start_index "$start_index" \
    --argjson count "$count" \
    '[range(0; $count) | {
      id: ("urn:ngsi-ld:Vehicle:swarm-load-" + ("00000" + (($start_index + .) | tostring))[-5:]),
      type: "Vehicle",
      speed: {
        type: "Property",
        value: (1000 + $start_index + .)
      }
    }]')"

  body_file="$(mktemp)"
  status_code="$(curl -sS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url "$broker_index")/entityOperations/create" \
    -o "$body_file" \
    -w '%{http_code}')" || {
    printf 'bulk insert broker%s curl failed: %s\n' "$broker_index" "$(<"$body_file")" >&2
    rm -f "$body_file"
    return 1
  }
  if [[ "$status_code" != "201" ]]; then
    printf 'bulk insert broker%s returned HTTP %s: %s\n' "$broker_index" "$status_code" "$(<"$body_file")" >&2
    rm -f "$body_file"
    return 1
  fi
  rm -f "$body_file"
}

all_brokers_report_load_count() {
  local expected_count=$1
  local index

  for index in $(seq 1 "$BROKER_COUNT"); do
    count="$(curl -fsS -D - -o /dev/null "$(broker_url "$index")/entities?type=Vehicle&count=true&limit=1" | tr -d '\r' | awk -F': ' 'tolower($1) == "ngsild-results-count" {print $2}')"
    [[ "$count" == "$expected_count" ]] || return 1
  done

  return 0
}

print_load_counts() {
  local index
  local count

  printf 'Current load counts by broker:\n' >&2
  for index in $(seq 1 "$BROKER_COUNT"); do
    count="$(curl -fsS -D - -o /dev/null "$(broker_url "$index")/entities?type=Vehicle&count=true&limit=1" | tr -d '\r' | awk -F': ' 'tolower($1) == "ngsild-results-count" {print $2}')" || count="curl failed"
    printf '  broker%s: %s\n' "$index" "${count:-missing}" >&2
  done
}

load_entities_probe_visible_everywhere() {
  local index
  local probe_index
  local entity_id
  local speed
  local probes

  probes="$(jq -nc \
    --argjson total "$LOAD_TEST_INSERT_COUNT" \
    '[1, (($total + 1) / 2 | floor), $total] | map(select(. >= 1 and . <= $total)) | unique[]')"
  for probe_index in $probes; do
    entity_id="$(load_entity_id "$probe_index")"
    speed="$(load_entity_speed "$probe_index")"
    for index in $(seq 1 "$BROKER_COUNT"); do
      entity_state_matches "$entity_id" "$index" present "$speed" || return 1
    done
  done

  return 0
}

run_bulk_insert_phase() {
  local per_broker=$((LOAD_TEST_INSERT_COUNT / BROKER_COUNT))
  local remainder=$((LOAD_TEST_INSERT_COUNT % BROKER_COUNT))
  local index
  local offset=1
  local count
  local pids=()
  local start_ms
  local end_ms

  printf 'Running %s insert load test\n' "$LOAD_TEST_INSERT_COUNT"
  capture_channel_baselines
  start_ms="$(now_ms)"
  for index in $(seq 1 "$BROKER_COUNT"); do
    count=$per_broker
    if (( index <= remainder )); then
      count=$((count + 1))
    fi
    load_insert_batch "$index" "$offset" "$count" &
    pids+=("$!")
    offset=$((offset + count))
  done

  for pid in "${pids[@]}"; do
    wait "$pid"
  done

  if ! wait_until "$LOAD_TEST_INSERT_COUNT insert count replication" $((LOAD_PROPAGATION_BUDGET_MS / 1000)) all_brokers_report_load_count "$LOAD_TEST_INSERT_COUNT"; then
    print_load_counts
    return 1
  fi
  wait_until "$LOAD_TEST_INSERT_COUNT insert probe replication" $((LOAD_PROPAGATION_BUDGET_MS / 1000)) load_entities_probe_visible_everywhere
  end_ms="$(now_ms)"
  record_metric 'bulk_insert' total_duration load all all "$((end_ms - start_ms))"
  printf 'bulk insert %s total duration=%sms\n' "$LOAD_TEST_INSERT_COUNT" "$((end_ms - start_ms))"
}

subscription_locality_holds() {
  local owner_index=$1
  local encoded_id
  local body_file
  local status_code

  encoded_id="$(urlencode "$(subscription_id "$owner_index")")"

  for index in $(seq 1 "$BROKER_COUNT"); do
    body_file="$(mktemp)"
    status_code="$(curl -sS -o "$body_file" -w '%{http_code}' "$(broker_url "$index")/subscriptions/$encoded_id")" || {
      rm -f "$body_file"
      return 1
    }
    rm -f "$body_file"

    if [[ "$index" == "$owner_index" ]]; then
      [[ "$status_code" == "200" ]] || return 1
    else
      [[ "$status_code" == "404" ]] || return 1
    fi
  done

  return 0
}

subscriptions_remain_local() {
  local owner_index

  for owner_index in $(seq 1 "$BROKER_COUNT"); do
    subscription_locality_holds "$owner_index" || return 1
  done

  return 0
}

create_subscription() {
  local index=$1
  local payload

  payload="$(jq -nc \
    --arg id "$(subscription_id "$index")" \
    --arg endpoint "http://sink:8080/notify/node${index}" \
    '{
      id: $id,
      type: "Subscription",
      entities: [{type: "Vehicle"}],
      watchedAttributes: ["speed"],
      notificationTrigger: [
        "entityCreated",
        "entityUpdated",
        "entityDeleted",
        "attributeCreated",
        "attributeUpdated",
        "attributeDeleted"
      ],
      notification: {
        endpoint: {
          uri: $endpoint
        }
      }
    }')"

  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url "$index")/subscriptions" \
    -o /dev/null
}

create_entity() {
  local payload

  payload="$(jq -nc \
    --arg id "$ENTITY_ID" \
    --argjson speed "$CREATE_SPEED" \
    '{
      id: $id,
      type: "Vehicle",
      speed: {
        type: "Property",
        value: $speed
      }
    }')"

  curl -fsS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url 1)/entities" \
    -o /dev/null
}

update_entity() {
  local payload
  local encoded_id

  payload="$(jq -nc --argjson speed "$UPDATE_SPEED" '{type: "Property", value: $speed}')"
  encoded_id="$(urlencode "$ENTITY_ID")"

  curl -fsS \
    -X PUT \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$(broker_url 5)/entities/$encoded_id/attrs/speed" \
    -o /dev/null
}

delete_entity() {
  local encoded_id

  encoded_id="$(urlencode "$ENTITY_ID")"
  curl -fsS \
    -X DELETE \
    "$(broker_url 9)/entities/$encoded_id" \
    -o /dev/null
}

printf 'Starting 10-node swarm stack\n'
printf 'Building broker binary on host\n'
cargo build >/dev/null

reset_metric_artifacts

"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d sink "${DEFRA_SERVICES[@]}"

wait_until 'notification sink' 60 sink_ready

printf 'Bootstrapping DefraDB P2P swarm\n'
bash "$ROOT_DIR/scripts/bootstrap_swarm.sh"

printf 'Starting broker nodes\n'
"${COMPOSE[@]}" up -d "${BROKER_SERVICES[@]}"

for index in $(seq 1 "$BROKER_COUNT"); do
  wait_until "broker${index}" 180 broker_ready "$index"
done

printf 'Resetting sink state\n'
curl -fsS -X DELETE "$SINK_URL/messages" -o /dev/null

printf 'Creating local subscriptions on all brokers\n'
for index in $(seq 1 "$BROKER_COUNT"); do
  create_subscription "$index"
done
wait_until 'subscriptions remain local' 20 subscriptions_remain_local

printf 'Creating entity on broker1\n'
run_single_entity_phase create create_entity present "$CREATE_SPEED" "$CREATE_SPEED"

printf 'Updating entity on broker5\n'
run_single_entity_phase update update_entity present "$UPDATE_SPEED" "$UPDATE_SPEED"

printf 'Deleting entity on broker9\n'
run_single_entity_phase delete delete_entity deleted '' "$UPDATE_SPEED"
wait_until 'subscriptions remain local after entity lifecycle' 20 subscriptions_remain_local

printf 'Running concurrent inserts across all brokers\n'
run_concurrent_insert_phase
wait_until 'subscriptions remain local after concurrent inserts' 20 subscriptions_remain_local

printf 'Running concurrent updates across all brokers\n'
run_concurrent_update_phase
wait_until 'subscriptions remain local after concurrent updates' 20 subscriptions_remain_local

printf 'Running concurrent deletes across all brokers\n'
run_concurrent_delete_phase
wait_until 'subscriptions remain local after concurrent deletes' 20 subscriptions_remain_local

run_bulk_insert_phase
wait_until "subscriptions remain local after $LOAD_TEST_INSERT_COUNT inserts" 20 subscriptions_remain_local

printf 'metric artifacts: %s %s\n' "$METRIC_JSON_PATH" "$METRIC_CSV_PATH"

printf '10-node swarm integration test passed\n'
