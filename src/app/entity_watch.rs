//! Watchers for externally observed entity changes.
//!
//! Local write paths already enqueue notifications directly. Fast mutation-log
//! watcher handles replicated broker-originated changes. Snapshot watcher stays
//! as slower reconciliation fallback for missed events and direct storage writes.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use log::{info, warn};
use serde_json::Value;

use crate::{
    app::state::AppState,
    domain::types::{EntityEvent, EntityEventKind, EntityMutationOperation},
    error::BrokerError,
    query::planner::QueryPlan,
    services::notifications,
    utils::json::{entity_attribute_names, reserved_member},
    utils::time::now_timestamp_millis,
};

const MUTATION_LOG_CURSOR_OVERLAP_MS: i64 = 60_000;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
/// Internal key for one tenant-scoped entity snapshot.
struct EntityKey {
    tenant: String,
    entity_id: String,
}

#[derive(Clone, Default)]
/// Tracks locally written entities so polling worker can suppress duplicates.
pub struct EntityWatchState {
    pending_local: Arc<Mutex<HashMap<EntityKey, Option<Value>>>>,
}

impl EntityWatchState {
    /// Records local write so watcher can suppress duplicate subscription delivery.
    pub fn record_local_upsert(&self, tenant: &str, entity_id: &str, entity: &Value) {
        self.pending_local
            .lock()
            .unwrap()
            .insert(key(tenant, entity_id), Some(entity.clone()));
    }

    /// Records local delete so watcher can suppress duplicate subscription delivery.
    pub fn record_local_delete(&self, tenant: &str, entity_id: &str) {
        self.pending_local
            .lock()
            .unwrap()
            .insert(key(tenant, entity_id), None);
    }

    fn suppress_if_local(&self, key: &EntityKey, current: Option<&Value>) -> bool {
        let mut pending = self.pending_local.lock().unwrap();
        match pending.get(key) {
            Some(Some(expected)) if current == Some(expected) => {
                pending.remove(key);
                true
            }
            Some(None) if current.is_none() => {
                pending.remove(key);
                true
            }
            _ => false,
        }
    }
}

/// Starts configured entity change watchers.
pub fn spawn(state: AppState) {
    if state.config.entity_event_watch_enabled {
        let interval = Duration::from_millis(state.config.entity_event_watch_interval_ms.max(25));
        let event_state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = entity_event_watch_loop(event_state, interval).await {
                warn!("entity mutation-log watcher stopped: {error}");
            }
        });
    }

    if state.config.entity_watch_enabled {
        let interval = Duration::from_millis(state.config.entity_watch_interval_ms.max(100));
        tokio::spawn(async move {
            if let Err(error) = entity_watch_loop(state, interval).await {
                warn!("entity snapshot watcher stopped: {error}");
            }
        });
    }
}

/// Polls replicated mutation log and emits remote broker events quickly.
async fn entity_event_watch_loop(state: AppState, interval: Duration) -> Result<(), BrokerError> {
    info!("entity mutation-log watcher started");
    let mut cursor = 0_i64;
    let mut seen = HashSet::new();

    loop {
        // DefraDB P2P can replicate same-time peer writes out of timestamp
        // order. Keep a bounded overlap so a local self-origin event cannot
        // advance cursor past older remote events that arrive slightly later.
        let after = cursor.saturating_sub(MUTATION_LOG_CURSOR_OVERLAP_MS);
        match state.repositories.entity_mutations.list_after(after).await {
            Ok(events) => {
                let mut notification_events: HashMap<String, Vec<(Value, EntityEvent)>> =
                    HashMap::new();
                for event in events {
                    cursor = cursor.max(event.created_at_millis);
                    if event.origin_broker_id == state.config.broker_id {
                        seen.insert(event.event_id);
                        continue;
                    }
                    if !seen.insert(event.event_id.clone()) {
                        continue;
                    }

                    let mutation_bytes = serde_json::to_vec(&event.payload)
                        .map(|value| value.len() as u64)
                        .unwrap_or(0);
                    state
                        .stats
                        .record_remote_mutations(&event.origin_broker_id, 1, mutation_bytes);

                    let entity_key = key(&event.tenant, &event.entity_id);
                    let current_document = match event.operation {
                        EntityMutationOperation::Deleted => None,
                        EntityMutationOperation::Created | EntityMutationOperation::Updated => {
                            Some(&event.payload)
                        }
                    };
                    if state
                        .entity_watch
                        .suppress_if_local(&entity_key, current_document)
                    {
                        continue;
                    }

                    let entity_event = EntityEvent {
                        kind: match event.operation {
                            EntityMutationOperation::Created => EntityEventKind::Created,
                            EntityMutationOperation::Updated => EntityEventKind::Updated,
                            EntityMutationOperation::Deleted => EntityEventKind::Deleted,
                        },
                        changed_attributes: event.changed_attributes,
                    };
                    match entity_event.kind {
                        EntityEventKind::Deleted => state
                            .entity_watch
                            .record_local_delete(&event.tenant, &event.entity_id),
                        EntityEventKind::Created | EntityEventKind::Updated => state
                            .entity_watch
                            .record_local_upsert(&event.tenant, &event.entity_id, &event.payload),
                    }
                    notification_events
                        .entry(event.tenant)
                        .or_default()
                        .push((event.payload, entity_event));
                }

                for (tenant, events) in notification_events {
                    if let Err(error) =
                        notifications::enqueue_notifications_batch(&state, &tenant, &events).await
                    {
                        warn!(
                            "entity mutation-log notification batch failed in tenant {}: {}",
                            tenant, error
                        );
                    }
                }
                cursor = cursor.max(now_timestamp_millis());
            }
            Err(error) => warn!("entity mutation-log poll failed: {error}"),
        }

        tokio::time::sleep(interval).await;
    }
}

/// Polls storage for current entity snapshots and emits inferred events.
async fn entity_watch_loop(state: AppState, interval: Duration) -> Result<(), BrokerError> {
    info!("entity watch worker started");
    let mut previous = HashMap::new();

    loop {
        let current = match collect_snapshots(&state).await {
            Ok(current) => current,
            Err(error) => {
                warn!("entity watch poll failed: {error}");
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let mut events = Vec::new();
        for (entity_key, document) in &current {
            match previous.get(entity_key) {
                None => events.push((
                    entity_key.clone(),
                    document.clone(),
                    EntityEvent {
                        kind: EntityEventKind::Created,
                        changed_attributes: entity_attribute_names(document),
                    },
                )),
                Some(previous_document) if previous_document != document => events.push((
                    entity_key.clone(),
                    document.clone(),
                    EntityEvent {
                        kind: EntityEventKind::Updated,
                        changed_attributes: diff_attributes(previous_document, document),
                    },
                )),
                _ => {}
            }
        }

        for (entity_key, document) in &previous {
            if !current.contains_key(entity_key) {
                events.push((
                    entity_key.clone(),
                    document.clone(),
                    EntityEvent {
                        kind: EntityEventKind::Deleted,
                        changed_attributes: entity_attribute_names(document),
                    },
                ));
            }
        }

        let mut notification_events: HashMap<String, Vec<(Value, EntityEvent)>> = HashMap::new();
        for (entity_key, document, event) in events {
            let current_document = current.get(&entity_key);
            if state
                .entity_watch
                .suppress_if_local(&entity_key, current_document)
            {
                continue;
            }

            notification_events
                .entry(entity_key.tenant)
                .or_default()
                .push((document, event));
        }

        for (tenant, events) in notification_events {
            if let Err(error) =
                notifications::enqueue_notifications_batch(&state, &tenant, &events).await
            {
                warn!(
                    "entity watch notification batch failed in tenant {}: {}",
                    tenant, error
                );
            }
        }

        previous = current;
        tokio::time::sleep(interval).await;
    }
}

/// Collects current entity snapshots for all tenants that matter to watcher.
async fn collect_snapshots(state: &AppState) -> Result<HashMap<EntityKey, Value>, BrokerError> {
    let tenants = watched_tenants(state).await?;
    let mut snapshots = HashMap::new();

    for tenant in tenants {
        let documents = state
            .repositories
            .entities
            .query(&tenant, &QueryPlan::default())
            .await?;
        for document in documents {
            snapshots.insert(key(&tenant, &document.ngsi_id), document.doc);
        }
    }

    Ok(snapshots)
}

/// Returns tenants that currently have entities or subscriptions.
async fn watched_tenants(state: &AppState) -> Result<Vec<String>, BrokerError> {
    let mut tenants = HashSet::new();
    tenants.extend(state.repositories.entities.list_tenants().await?);
    tenants.extend(state.repositories.subscriptions.list_tenants().await?);
    Ok(tenants.into_iter().collect())
}

/// Computes changed top-level attribute names between two entity snapshots.
fn diff_attributes(previous: &Value, current: &Value) -> Vec<String> {
    let previous = previous
        .as_object()
        .map(|object| object.iter().collect::<HashMap<_, _>>())
        .unwrap_or_default();
    let current = current
        .as_object()
        .map(|object| object.iter().collect::<HashMap<_, _>>())
        .unwrap_or_default();

    let mut changed = HashSet::new();
    for key in previous.keys().chain(current.keys()) {
        if reserved_member(key) {
            continue;
        }
        if previous.get(key) != current.get(key) {
            changed.insert((*key).to_string());
        }
    }

    changed.into_iter().collect()
}

/// Builds tenant-scoped entity key.
fn key(tenant: &str, entity_id: &str) -> EntityKey {
    EntityKey {
        tenant: tenant.to_string(),
        entity_id: entity_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::diff_attributes;

    #[test]
    fn diff_attributes_ignores_reserved_members() {
        let previous = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle",
            "speed": {"type": "Property", "value": 10},
            "modifiedAt": "2024-01-01T00:00:00Z"
        });
        let current = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle",
            "speed": {"type": "Property", "value": 20},
            "modifiedAt": "2024-01-01T00:00:01Z"
        });

        let changed = diff_attributes(&previous, &current);

        assert_eq!(changed, vec!["speed".to_string()]);
    }
}
