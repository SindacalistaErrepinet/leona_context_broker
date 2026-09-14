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

- 10 combined node containers, each running DefraDB plus the broker
- one HTTP notification sink

The script builds the debug node image from `docker/Dockerfile.test`. Override
with `SWARM_DOCKERFILE=docker/Dockerfile` and `SWARM_NODE_IMAGE=leona-swarm-node:latest`
to run the release image.

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
docker compose -f docker-compose.yml -p leona-swarm logs node1
docker compose -f docker-compose.yml -p leona-swarm exec node1 defradb client p2p info --url 127.0.0.1:9181
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

## Swarm Monitor Checks

Start swarm plus dashboard in one step:

```bash
bash scripts/start_local_swarm.sh
```

`SKIP_MONITOR=1` starts only the environment; `SWARM_DOWN_ON_EXIT=1` removes it when the monitor exits.

With the swarm already running, start the dashboard directly:

```bash
cargo run -p leona-swarm-monitor -- --defradb http://127.0.0.1:19181 --broker http://127.0.0.1:18081
```

Verify:

- all 10 DefraDB nodes and 10 brokers appear within a few poll cycles
- `Topology` shows p2p and replicator edges for `EntityRecord` and `EntityMutationRecord`
- `Nodes` shows broker counters from `GET /internal/stats`
- `Traffic` counters increase after sending a payload with `s` or writing entities through the API
- stopping a node marks it offline, then removes it after `--offline-grace` cycles
- restarting the node makes it reappear through DefraDB P2P discovery
