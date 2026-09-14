# Implementation Status

This document tracks the current code state of `leona_context_broker` after the MongoDB refactor and the decentralized swarm-sync pass.

## Implemented

- Modular source layout with separate `api`, `app`, `context`, `domain`, `federation`, `persistence`, `query`, `services`, and `utils` areas.
- MongoDB-backed persistence for:
- `entities`
- `temporal_entities`
- `subscriptions`
- `peers`
- `swarm_mutations`
- Entity CRUD routes and attribute mutation routes.
- Batch entity operations for create, upsert, update, delete, and POST query.
- Subscription CRUD with queued notification dispatch.
- Temporal entity create, query, retrieve, attribute retrieve, delete, append attribute, patch instance, and delete instance routes.
- Type and attribute discovery routes for `/types`, `/types/{type}`, `/attributes`, and `/attributes/{attrId}`.
- Root-level internal SWIM routes for `/internal/swim` and `/internal/swim/mutations`.
- Entity query planning for `id`, `type`, `idPattern`, `attrs`, `q`, geo filters, and `limit`.
- Response projections for normalized JSON, `keyValues`, and GeoJSON.
- Temporal filtering for `before`, `after`, `between`, `lastN`, and basic aggregated output.
- Redis Stream queue worker for outbound notifications and outbound SWIM delivery.
- Decentralized swarm sync primitives:
- peer persistence in MongoDB
- SWIM membership states (`alive`, `suspect`, `dead`)
- durable mutation log in MongoDB
- root-level broker-to-broker internal SWIM HTTP endpoints
- duplicate suppression by `mutationId`
- newer-only application based on version timestamps
- local mutation recording for entity and temporal write paths
- NGSI-LD subscription notifications for mutations applied through SWIM
- Handler-level problem-details responses for invalid requests.
- Automated regression tests for planner logic, temporal helpers, notification matching, internal URL handling, mutation ordering helpers, SWIM membership helpers, and selected API validation paths.

## Current P2P Model

Current implementation uses SWIM-style membership plus root-level internal HTTP anti-entropy and event intake.

Current model combines:

- periodic SWIM liveness rounds
- membership state transitions between `alive`, `suspect`, and `dead`
- local Redis Stream queueing for outbound delivery work
- broker-to-broker internal HTTP for SWIM event intake and mutation sync
- durable local mutation persistence instead of transient in-memory gossip
- deduplication by mutation id
- version checks before applying remote state
- state snapshots instead of replaying public write requests with non-deterministic side effects

For current codebase, this gives one membership model and one broker-to-broker sync flow. Registration-based federation and old internal gossip paths are removed.

## Partial Or Missing

- Live end-to-end tests against real MongoDB and Redis services are not part of the current automated suite.
- The implementation is not ETSI NGSI-LD complete.
- Public NGSI-LD API families such as JSON-LD context cache endpoints are not implemented.
- Entity query DTOs accept parameters that are not yet implemented end-to-end:
- `csf`
- `scopeQ`
- `containedBy`
- `join`
- `joinLevel`
- `datasetId`
- `entityMap`
- `geometryProperty`
- `lang`
- `pick` and `omit` are implemented only as response projection, not as database-side filtering.
- Geo query support does not implement `georel=disjoint`; that path returns `501 Not Implemented`.
- Temporal query execution still loads matching temporal documents first and then filters history in service code; there is no dedicated MongoDB temporal aggregation pipeline yet.
- `aggrPeriodDuration` is accepted by the temporal query type but not applied.
- Swarm sync still uses timestamp-based last-write-wins ordering derived from `modifiedAt`; there is no CRDT or vector-clock conflict model yet.
- Mutation-log retention, pruning, and compaction are not implemented.
- Peer authentication, trust validation, and access control are not implemented for SWIM participants.
- SWIM worker still discovers tenants by probing Mongo collections; there is no dedicated tenant registry yet.
- Notification delivery has basic enqueue and status accounting, but no retry scheduler, no backoff policy, and no dead-letter handling.
- Notification delivery stats are coarse counters only; there is no persisted per-attempt audit log.

## Verification

Current command status:

- `cargo fmt && cargo test` passes
- current automated result: `35 passed; 0 failed`

The current tests cover:

- query planner and BSON filter generation
- geo and temporal validation
- temporal filtering and aggregation helpers
- temporal swarm snapshot generation
- notification matching rules
- SWIM membership merge rules
- mutation ordering helper behavior
- internal target URL normalization
- selected Actix handler validation responses
- type and attribute discovery endpoints
- inbound SWIM mutation application and notification fanout

## Notes From This Pass

- Reworked broker sync to SWIM-only membership plus root-level internal SWIM endpoints.
- Removed registration-based federation API surface.
- Kept Redis as internal queue only.
- Added root-level `/internal/swim` and `/internal/swim/mutations` handlers.
- Kept NGSI-LD app notifications for mutations applied through SWIM.
