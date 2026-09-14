# Testing

This file explains how to verify normal unit coverage and live DefraDB swarm behavior.

## Fast Local Checks

Run from repository root:

```bash
cargo fmt --check
cargo test
```

Expected result:

- all unit and API tests pass
- ignored swarm integration test stays ignored unless explicitly requested

## Live 10-Node Swarm Test

The live swarm test starts:

- 10 DefraDB nodes
- 10 broker nodes
- one HTTP notification sink

Run:

```bash
bash tests/swarm_integration.sh
```

Or through Cargo:

```bash
cargo test --test swarm_integration -- --ignored
```

## What Swarm Test Verifies

The script verifies:

- DefraDB collections are created on all nodes
- `EntityRecord` is replicated across DefraDB nodes
- `EntityMutationRecord` is replicated across DefraDB nodes
- `SubscriptionRecord` remains local to each broker
- local subscriptions receive notifications for remote create/update/delete
- delete notifications carry old entity payload through mutation log
- concurrent insert/update/delete phases converge across brokers
- metric artifacts are written under `tests/artifacts/`

## Expected Notification Flow

```text
entity write on broker N
  -> EntityRecord persisted
  -> EntityMutationRecord appended
  -> local subscriptions notified inline
  -> DefraDB replicates mutation record
  -> remote broker mutation-log watcher sees event
  -> remote broker evaluates local subscriptions
  -> remote broker sends HTTP notification
```

Snapshot watcher still runs as fallback:

```text
periodic snapshot scan
  -> compares entity cache
  -> emits missed create/update/delete events
  -> duplicate suppression skips events already handled by mutation log
```

## Important Environment Variables

Fast mutation-log watcher:

```text
BROKER_ENTITY_EVENT_WATCH_ENABLED=true
BROKER_ENTITY_EVENT_WATCH_INTERVAL_MS=100
```

Snapshot fallback watcher:

```text
BROKER_ENTITY_WATCH_ENABLED=true
BROKER_ENTITY_WATCH_INTERVAL_MS=5000
```

## Debugging

Keep containers running after failure:

```bash
KEEP_SWARM=1 bash tests/swarm_integration.sh
```

Inspect logs:

```bash
docker-compose -f docker-compose.yml -p leona-swarm logs broker1
docker-compose -f docker-compose.yml -p leona-swarm logs defra1
```

Remove swarm containers and volumes:

```bash
docker-compose -f docker-compose.yml -p leona-swarm down -v --remove-orphans
```

## Success Signal

Successful live test ends with:

```text
10-node swarm integration test passed
```

Metric paths are printed at end of script.
