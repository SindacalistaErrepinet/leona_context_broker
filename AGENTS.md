# AGENTS.md

## Response Style
- Chat replies: load caveman skill (`.agents/skills/caveman/SKILL.md`), terse "smart caveman". Code, comments, commits, docs: normal prose.

## Verify
- Pre-handoff: `cargo fmt --check && cargo test`. Verified: 34 tests pass, no external services needed.
- Single test: `cargo test <filter>`.

## Run
- `cargo run` needs DefraDB up at `BROKER_DEFRADB_URL` (default `http://127.0.0.1:9181/api/v0/graphql`). No MongoDB, no Redis.
- Broker never creates DefraDB collections. Required collections must pre-exist: `EntityRecord`, `TemporalRecord`, `SubscriptionRecord`, `EntityMutationRecord`. `scripts/bootstrap_swarm.sh` creates them for the swarm; create manually for local runs.
- Config from `std::env`; `.env` is not auto-loaded, `.env.example` is reference only.
- Full 10-node swarm: `docker-compose.yml` + `tests/swarm_integration.sh`.

## Architecture
- `src/api.rs` wires Actix routes only; behavior lives in `src/services/*`; query planning in `src/query/planner.rs`; tenant/`Link`/`Via` in `src/context/headers.rs`.
- Storage is DefraDB GraphQL, not raw NGSI-LD. Wrappers: entity/subscription `{ tenant, id, doc }`; temporal adds `history`. DefraDB rows keep `doc`/history as encoded JSON strings in `payload`/`historyJson`.
- Repository traits in `src/persistence/repository.rs`; runtime impl `defradb.rs`; tests use `memory.rs`.
- Every repository key is tenant-scoped. `NGSILD-Tenant` defaults to `default`; missing tenant filter causes cross-tenant bugs.
- `Link` backfills `@context` only when payload lacks it. `Via` is forwarding/loop-avoidance metadata; preserve and extend it.
- Write path: persist locally, append `EntityMutationRecord`, deliver local subscriptions inline. Delivery is best-effort; no retry/backoff/dead-letter.
- Cross-node: DefraDB P2P replicates `EntityRecord` + `EntityMutationRecord` only; `SubscriptionRecord` must stay local. `src/app/entity_watch.rs` fast watcher polls the mutation log, dedupes `event_id`, skips own `origin_broker_id`; snapshot watcher is slower fallback.
- `/internal/entities/batch` is root-level (no `/ngsi-ld/v1` prefix) and unauthenticated; used for large create batches only. `BROKER_P2P_SEEDS` are public `/ngsi-ld/v1` endpoints; code strips the suffix and appends the internal path.

## Tests
- Unit/API tests use in-memory repos via `api::tests::test_state()` (`AppConfig::for_tests()`, `memory::repositories()`). No services needed.
- Live swarm test ignored by default: `bash tests/swarm_integration.sh` (docker-compose, 10 DefraDB + 10 broker + sink). `KEEP_SWARM=1` keeps containers.

## Stale Docs
- `IMPLEMENTATION_STATUS.md` describes an old MongoDB/SWIM design; trust code and `README.md`. README route list may lag `src/api.rs`.
