//! Subscription matching and delivery.
//!
//! Notification delivery happens inline on write paths. This module loads local
//! subscriptions, evaluates entity and query predicates, builds NGSI-LD
//! notification payloads, and records delivery accounting.
use std::{collections::HashMap, sync::Arc};

use log::warn;
use serde_json::Value;

use crate::{
    app::state::AppState,
    context::headers::HEADER_TENANT,
    domain::types::{EntityEvent, EntityEventKind, SubscriptionDocument},
    error::BrokerError,
    query::{
        context::resolve_context_terms,
        language::{
            QueryMatchOptions, extract_scope_values, parse_q_expression,
            q_expression_uses_linked_entity, query_matches_value_with_options,
            scope_query_matches_values,
        },
    },
    services::common::build_notification_payload,
    utils::time::now_timestamp,
};

type DeliveryCounts = HashMap<String, (u64, u64)>;

/// Matches local subscriptions and delivers notifications inline.
pub async fn enqueue_notifications(
    state: &AppState,
    tenant: &str,
    entity: &Value,
    event: &EntityEvent,
) -> Result<(), BrokerError> {
    let subscriptions = state.repositories.subscriptions.list(tenant, None).await?;
    let attempted_at = now_timestamp();
    let mut delivery_counts = HashMap::new();
    deliver_notifications(
        state,
        tenant,
        &subscriptions,
        entity,
        event,
        &mut delivery_counts,
    )
    .await?;
    record_delivery_counts(state, tenant, delivery_counts, &attempted_at).await;
    Ok(())
}

/// Matches and delivers notifications for many entity events with one subscription scan.
pub async fn enqueue_notifications_batch(
    state: &AppState,
    tenant: &str,
    events: &[(Value, EntityEvent)],
) -> Result<(), BrokerError> {
    if events.is_empty() {
        return Ok(());
    }

    let subscriptions = state.repositories.subscriptions.list(tenant, None).await?;
    let attempted_at = now_timestamp();
    let mut delivery_counts = HashMap::new();
    for (entity, event) in events {
        deliver_notifications(
            state,
            tenant,
            &subscriptions,
            entity,
            event,
            &mut delivery_counts,
        )
        .await?;
    }
    record_delivery_counts(state, tenant, delivery_counts, &attempted_at).await;
    Ok(())
}

/// Delivers notifications against caller-provided subscription snapshot.
async fn deliver_notifications(
    state: &AppState,
    tenant: &str,
    subscriptions: &[SubscriptionDocument],
    entity: &Value,
    event: &EntityEvent,
    delivery_counts: &mut DeliveryCounts,
) -> Result<(), BrokerError> {
    let linked_entities = if subscriptions
        .iter()
        .any(|subscription| subscription_uses_linked_graph(&subscription.doc))
    {
        Arc::new(HashMap::from_iter(
            entity_graph(tenant, state, entity).await?,
        ))
    } else {
        Arc::default()
    };

    for subscription in subscriptions {
        let options =
            subscription_match_options(state, &subscription.doc, entity, linked_entities.clone())
                .await?;
        if !subscription_matches_event(&subscription.doc, entity, event, &options) {
            continue;
        }

        let Some(subscription_id) = subscription.doc.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Some(endpoint) = subscription
            .doc
            .get("notification")
            .and_then(Value::as_object)
            .and_then(|notification| notification.get("endpoint"))
            .and_then(Value::as_object)
            .and_then(|endpoint| endpoint.get("uri"))
            .and_then(Value::as_str)
        else {
            continue;
        };

        let payload = build_notification_payload(subscription_id, &subscription.doc, entity);
        let response = state
            .http_client
            .post(endpoint)
            .header(HEADER_TENANT, tenant)
            .json(&payload)
            .send()
            .await;
        let success = response
            .as_ref()
            .map(|response| response.status().is_success())
            .unwrap_or(false);
        state.stats.record_notification_attempt(success);
        let entry = delivery_counts
            .entry(subscription_id.to_string())
            .or_insert((0_u64, 0_u64));
        if success {
            entry.0 += 1;
        } else {
            entry.1 += 1;
        }

        match response {
            Ok(response) if !response.status().is_success() => {
                warn!(
                    "notification delivery failed for {subscription_id}: HTTP {}",
                    response.status().as_u16()
                );
            }
            Err(error) => {
                warn!("notification delivery failed for {subscription_id}: {error}");
            }
            Ok(_) => {}
        }
    }

    Ok(())
}

/// Persists coalesced delivery accounting after notification HTTP work completes.
async fn record_delivery_counts(
    state: &AppState,
    tenant: &str,
    delivery_counts: DeliveryCounts,
    attempted_at: &str,
) {
    for (subscription_id, (successes, failures)) in delivery_counts {
        if let Err(error) = state
            .repositories
            .subscriptions
            .mark_delivery_batch(tenant, &subscription_id, successes, failures, &attempted_at)
            .await
        {
            warn!("failed updating notification delivery state for {subscription_id}: {error}");
        }
    }
}

/// Evaluates whether entity event matches subscription filters.
fn subscription_matches_event(
    subscription: &Value,
    entity: &Value,
    event: &EntityEvent,
    options: &QueryMatchOptions,
) -> bool {
    if subscription.get("isActive").and_then(Value::as_bool) == Some(false) {
        return false;
    }

    if let Some(entities) = subscription.get("entities").and_then(Value::as_array) {
        let entity_id = entity.get("id").and_then(Value::as_str).unwrap_or_default();
        let entity_type = entity.get("type");
        if !entities.iter().any(|selector| {
            let id_ok = selector
                .get("id")
                .and_then(Value::as_str)
                .map(|candidate| candidate == entity_id)
                .unwrap_or(true);
            let pattern_ok = selector
                .get("idPattern")
                .and_then(Value::as_str)
                .map(|pattern| {
                    regex::Regex::new(pattern)
                        .ok()
                        .is_some_and(|regex| regex.is_match(entity_id))
                })
                .unwrap_or(true);
            let type_ok = selector
                .get("type")
                .and_then(Value::as_str)
                .map(|candidate| match entity_type {
                    Some(Value::String(actual)) => actual == candidate,
                    Some(Value::Array(items)) => {
                        items.iter().any(|item| item.as_str() == Some(candidate))
                    }
                    _ => false,
                })
                .unwrap_or(true);
            id_ok && pattern_ok && type_ok
        }) {
            return false;
        }
    }

    if !query_matches_value_with_options(
        entity,
        subscription.get("q").and_then(Value::as_str),
        options,
    )
    .unwrap_or(false)
    {
        return false;
    }

    if !scope_query_matches_values(
        &extract_scope_values(entity),
        subscription.get("scopeQ").and_then(Value::as_str),
    )
    .unwrap_or(false)
    {
        return false;
    }

    if let Some(watched) = subscription
        .get("watchedAttributes")
        .and_then(Value::as_array)
    {
        let watched = watched.iter().filter_map(Value::as_str).collect::<Vec<_>>();
        if !event
            .changed_attributes
            .iter()
            .any(|attr| watched.iter().any(|watched| watched == &attr.as_str()))
        {
            return false;
        }
    }

    let triggers = subscription
        .get("notificationTrigger")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_else(|| vec!["attributeCreated", "attributeUpdated"]);

    event_triggers(event)
        .iter()
        .any(|trigger| triggers.iter().any(|candidate| candidate == trigger))
}

/// Maps entity lifecycle event into NGSI-LD trigger names.
fn event_triggers(event: &EntityEvent) -> Vec<&'static str> {
    match event.kind {
        EntityEventKind::Created => vec!["entityCreated", "attributeCreated"],
        EntityEventKind::Updated => vec!["entityUpdated", "attributeUpdated", "attributeCreated"],
        EntityEventKind::Deleted => vec!["entityDeleted", "attributeDeleted"],
    }
}

/// Builds query match options for subscription-side `q` evaluation.
async fn subscription_match_options(
    state: &AppState,
    subscription: &Value,
    entity: &Value,
    linked_entities: Arc<HashMap<String, Value>>,
) -> Result<crate::query::language::QueryMatchOptions, BrokerError> {
    let context_terms = resolve_context_terms(
        &state.http_client,
        subscription
            .get("jsonldContext")
            .or_else(|| subscription.get("@context"))
            .or_else(|| entity.get("@context")),
        None,
    )
    .await?;
    Ok(QueryMatchOptions {
        expand_values: option_field_values(subscription, "expandValues")
            .into_iter()
            .collect(),
        json_keys: option_field_values(subscription, "jsonKeys")
            .into_iter()
            .collect(),
        context_terms,
        linked_entities,
    })
}

/// Loads entity graph used by linked-entity subscription queries.
async fn entity_graph(
    tenant: &str,
    state: &AppState,
    entity: &Value,
) -> Result<Vec<(String, Value)>, BrokerError> {
    let mut entities = state
        .repositories
        .entities
        .query(tenant, &crate::query::planner::QueryPlan::default())
        .await?
        .into_iter()
        .map(|document| document.doc)
        .filter_map(|candidate| {
            let id = candidate.get("id").and_then(Value::as_str)?.to_string();
            Some((id, candidate))
        })
        .collect::<Vec<_>>();

    if let Some(id) = entity.get("id").and_then(Value::as_str)
        && !entities.iter().any(|(candidate_id, _)| candidate_id == id)
    {
        entities.push((id.to_string(), entity.clone()));
    }

    Ok(entities)
}

/// Returns true when subscription `q` traverses linked entities.
fn subscription_uses_linked_graph(subscription: &Value) -> bool {
    subscription
        .get("q")
        .and_then(Value::as_str)
        .and_then(|raw| parse_q_expression(Some(raw)).ok().flatten())
        .as_ref()
        .is_some_and(q_expression_uses_linked_entity)
}

/// Reads option field as CSV string or string array.
fn option_field_values(document: &Value, field: &str) -> Vec<String> {
    match document.get(field) {
        Some(Value::String(value)) => crate::utils::json::parse_csv(Some(value)),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;

    use super::*;
    use crate::query::language::QueryMatchOptions;

    #[test]
    fn ignores_inactive_subscriptions() {
        let subscription = json!({
            "isActive": false,
            "entities": [{"type": "Vehicle"}]
        });
        let entity = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle",
            "speed": {"type": "Property", "value": 10}
        });
        let event = EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec!["speed".to_string()],
        };

        assert!(!subscription_matches_event(
            &subscription,
            &entity,
            &event,
            &QueryMatchOptions::default(),
        ));
    }

    #[test]
    fn matches_watched_attributes_and_update_trigger() {
        let subscription = json!({
            "entities": [{"type": "Vehicle"}],
            "watchedAttributes": ["speed"],
            "notificationTrigger": ["attributeUpdated"]
        });
        let entity = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle"
        });
        let event = EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec!["speed".to_string()],
        };

        assert!(subscription_matches_event(
            &subscription,
            &entity,
            &event,
            &QueryMatchOptions::default(),
        ));
    }

    #[test]
    fn rejects_events_when_watched_attributes_do_not_overlap() {
        let subscription = json!({
            "entities": [{"type": "Vehicle"}],
            "watchedAttributes": ["speed"]
        });
        let entity = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle"
        });
        let event = EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec!["temperature".to_string()],
        };

        assert!(!subscription_matches_event(
            &subscription,
            &entity,
            &event,
            &QueryMatchOptions::default(),
        ));
    }

    #[test]
    fn delete_events_match_attribute_deleted_trigger() {
        let subscription = json!({
            "entities": [{"id": "urn:ngsi-ld:Vehicle:1"}],
            "notificationTrigger": ["attributeDeleted"]
        });
        let entity = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle"
        });
        let event = EntityEvent {
            kind: EntityEventKind::Deleted,
            changed_attributes: vec!["speed".to_string()],
        };

        assert!(subscription_matches_event(
            &subscription,
            &entity,
            &event,
            &QueryMatchOptions::default(),
        ));
    }

    #[test]
    fn subscription_q_honors_expand_values_and_linked_entities() {
        let subscription = json!({
            "entities": [{"type": "Vehicle"}],
            "q": "brandCode==https://example.org/brands/Mercedes;sensor{Device:humidity}==40",
            "expandValues": ["brandCode"],
            "jsonldContext": {
                "MercedesBrand": "https://example.org/brands/Mercedes"
            }
        });
        let entity = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle",
            "brandCode": {"type": "Property", "value": "MercedesBrand"},
            "sensor": {"type": "Relationship", "object": "urn:ngsi-ld:Device:1"}
        });
        let event = EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec!["brandCode".to_string()],
        };
        let options = QueryMatchOptions {
            expand_values: ["brandCode".to_string()].into_iter().collect(),
            context_terms: HashMap::from([(
                "MercedesBrand".to_string(),
                "https://example.org/brands/Mercedes".to_string(),
            )]),
            linked_entities: Arc::new(HashMap::from([(
                "urn:ngsi-ld:Device:1".to_string(),
                json!({
                    "id": "urn:ngsi-ld:Device:1",
                    "type": "Device",
                    "humidity": {"type": "Property", "value": 40}
                }),
            )])),
            ..Default::default()
        };

        assert!(subscription_matches_event(
            &subscription,
            &entity,
            &event,
            &options,
        ));
    }
}
