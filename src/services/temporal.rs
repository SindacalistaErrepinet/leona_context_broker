//! Temporal entity services.
//!
//! This module stores current temporal snapshots plus append-only history, then
//! renders snapshot or history-oriented views based on temporal query
//! parameters.
use std::{collections::HashMap, sync::Arc};

use serde_json::{Map, Value};

use crate::{
    app::state::AppState,
    context::headers::RequestContext,
    domain::{
        temporal::TemporalQueryResult,
        types::{TemporalAttributeRecord, TemporalEntityDocument},
    },
    error::BrokerError,
    query::{
        context::resolve_context_terms,
        planner::{QueryPlan, TemporalFilter},
        types::{Representation, TemporalEntityQuery},
    },
    services::common::{
        build_query_match_options, entity_matches_query_with_options, make_temporal_instance,
        prepare_entity_for_create, query_needs_linked_graph, representation_from_request,
        temporal_entity_to_response,
    },
    utils::{json::editable_fragment_members, time::now_timestamp},
};

/// Creates or updates temporal entity for current tenant.
///
/// Current entity payload becomes latest snapshot, while helper-generated
/// history entries are extracted from top-level attributes and stored alongside
/// snapshot for later temporal queries.
pub async fn upsert(
    state: &AppState,
    context: &RequestContext,
    mut entity: Value,
    _local_only: bool,
) -> Result<(String, bool), BrokerError> {
    let entity_id = prepare_entity_for_create(&mut entity, context)?;
    let history = temporal_history_from_entity(&entity);
    let created = state
        .repositories
        .temporals
        .upsert(TemporalEntityDocument {
            tenant: context.tenant.clone(),
            ngsi_id: entity_id.clone(),
            doc: entity.clone(),
            history,
        })
        .await?;
    state.stats.record_temporal_write();

    Ok((entity_id, created))
}

/// Queries temporal entities and formats requested history projection.
///
/// Storage-side query plan narrows candidate snapshots first. Then service runs
/// context-aware `q` evaluation, selects matching history records for requested
/// temporal window, and renders them into chosen representation.
pub async fn query(
    state: &AppState,
    context: &RequestContext,
    request: &actix_web::HttpRequest,
    query: &TemporalEntityQuery,
) -> Result<TemporalQueryResult, BrokerError> {
    let plan = QueryPlan::from_entity_query(&query.entity)?;
    let temporal = TemporalFilter::from_query(query)?;
    let representation = representation_from_request(
        request,
        query.entity.format.as_deref(),
        query.entity.options.as_deref(),
    );
    let documents = state
        .repositories
        .temporals
        .query(&context.tenant, &plan, &temporal, plan.geo.as_ref())
        .await?;

    let linked_entities = temporal_query_linked_graph(state, context, query, &documents).await?;
    let mut filtered = Vec::new();
    for document in documents {
        let options = build_query_match_options(
            &query.entity,
            resolve_context_terms(
                &state.http_client,
                document.doc.get("@context"),
                context.link_header.as_deref(),
            )
            .await?,
            linked_entities.clone(),
        );
        if entity_matches_query_with_options(&document.doc, &query.entity, &options) {
            filtered.push(document);
        }
    }
    let documents = filtered;

    let total_count = documents.len();
    let payload = documents
        .iter()
        .map(|document| {
            let history = select_temporal_history(&document.history, &temporal);
            let history = format_temporal_history(&history, representation, &temporal);
            temporal_entity_to_response(&document.doc, &history, query, representation)
        })
        .collect::<Vec<_>>();

    Ok(TemporalQueryResult {
        body: Value::Array(payload),
        total_count,
    })
}

/// Retrieves one temporal entity with selected history projection.
pub async fn get(
    state: &AppState,
    context: &RequestContext,
    request: &actix_web::HttpRequest,
    entity_id: &str,
    query: &TemporalEntityQuery,
) -> Result<Value, BrokerError> {
    let document = state
        .repositories
        .temporals
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("temporal entity {entity_id} was not found"))
        })?;
    let temporal = TemporalFilter::from_query(query)?;
    let representation = representation_from_request(
        request,
        query.entity.format.as_deref(),
        query.entity.options.as_deref(),
    );
    let history = select_temporal_history(&document.history, &temporal);
    let history = format_temporal_history(&history, representation, &temporal);
    Ok(temporal_entity_to_response(
        &document.doc,
        &history,
        query,
        representation,
    ))
}

/// Returns all temporal attributes for one entity.
///
/// Response strips reserved entity metadata members and keeps only attribute
/// history payloads.
pub async fn get_attrs(
    state: &AppState,
    context: &RequestContext,
    request: &actix_web::HttpRequest,
    entity_id: &str,
    query: &TemporalEntityQuery,
) -> Result<Value, BrokerError> {
    let document = get_temporal_document(state, context, entity_id).await?;
    let temporal = TemporalFilter::from_query(query)?;
    let representation = representation_from_request(
        request,
        query.entity.format.as_deref(),
        query.entity.options.as_deref(),
    );
    let history = select_temporal_history(&document.history, &temporal);
    let history = format_temporal_history(&history, representation, &temporal);
    let response = temporal_entity_to_response(&document.doc, &history, query, representation);
    Ok(response
        .as_object()
        .map(|object| {
            let attrs = object
                .iter()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "id" | "type"
                            | "@context"
                            | "createdAt"
                            | "modifiedAt"
                            | "deletedAt"
                            | "scope"
                    )
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<serde_json::Map<_, _>>();
            Value::Object(attrs)
        })
        .unwrap_or(Value::Object(Map::new())))
}

/// Returns one temporal attribute history.
pub async fn get_attr(
    state: &AppState,
    context: &RequestContext,
    request: &actix_web::HttpRequest,
    entity_id: &str,
    attr_id: &str,
    query: &TemporalEntityQuery,
) -> Result<Value, BrokerError> {
    let attrs = get_attrs(state, context, request, entity_id, query).await?;
    attrs
        .get(attr_id)
        .cloned()
        .ok_or_else(|| BrokerError::NotFound(format!("temporal attribute {attr_id} was not found")))
}

/// Returns one temporal attribute instance.
pub async fn get_attr_instance(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    instance_id: &str,
    query: &TemporalEntityQuery,
) -> Result<Value, BrokerError> {
    let document = get_temporal_document(state, context, entity_id).await?;
    let temporal = TemporalFilter::from_query(query)?;
    let history = select_temporal_history(&document.history, &temporal);
    history
        .into_iter()
        .find(|item| {
            item.get("attrId").and_then(Value::as_str) == Some(attr_id)
                && item.get("instanceId").and_then(Value::as_str) == Some(instance_id)
        })
        .ok_or_else(|| {
            BrokerError::NotFound(format!(
                "temporal attribute instance {instance_id} was not found"
            ))
        })
}

/// Deletes temporal entity snapshot and its stored history.
pub async fn delete_entity(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    _local_only: bool,
) -> Result<(), BrokerError> {
    let deleted = state
        .repositories
        .temporals
        .delete(&context.tenant, entity_id)
        .await?;
    if !deleted {
        return Err(BrokerError::NotFound(format!(
            "temporal entity {entity_id} was not found"
        )));
    }

    Ok(())
}

/// Appends temporal attributes and records new history instances.
pub async fn append_attrs(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    fragment: Value,
    _local_only: bool,
) -> Result<(), BrokerError> {
    let mut document = state
        .repositories
        .temporals
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("temporal entity {entity_id} was not found"))
        })?;

    let attrs = editable_fragment_members(&fragment);
    for (attr_id, value) in attrs {
        document.history.push(record_from_value(&attr_id, &value));
        if let Some(object) = document.doc.as_object_mut() {
            object.insert(attr_id, value);
        }
    }
    set_temporal_modified_at(&mut document.doc);
    state
        .repositories
        .temporals
        .upsert(document.clone())
        .await?;

    Ok(())
}

/// Deletes all stored instances for one temporal attribute.
pub async fn delete_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    _local_only: bool,
) -> Result<(), BrokerError> {
    let mut document = state
        .repositories
        .temporals
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("temporal entity {entity_id} was not found"))
        })?;

    document.history.retain(|record| record.attr_id != attr_id);
    if let Some(object) = document.doc.as_object_mut() {
        object.remove(attr_id);
    }
    set_temporal_modified_at(&mut document.doc);
    state
        .repositories
        .temporals
        .upsert(document.clone())
        .await?;

    Ok(())
}

/// Patches one temporal attribute instance in stored history.
pub async fn patch_instance(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    instance_id: &str,
    fragment: Value,
    _local_only: bool,
) -> Result<(), BrokerError> {
    let mut document = state
        .repositories
        .temporals
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("temporal entity {entity_id} was not found"))
        })?;

    let mut updated = false;
    for record in &mut document.history {
        if record.attr_id == attr_id && record.instance_id == instance_id {
            if let Some(value_object) = record.value.as_object_mut() {
                if let Some(fragment_object) = fragment.as_object() {
                    for (key, value) in fragment_object {
                        value_object.insert(key.clone(), value.clone());
                    }
                }
            }
            record.modified_at = Some(now_timestamp());
            updated = true;
        }
    }

    if !updated {
        return Err(BrokerError::NotFound(format!(
            "temporal attribute instance {instance_id} was not found"
        )));
    }

    set_temporal_modified_at(&mut document.doc);
    state
        .repositories
        .temporals
        .upsert(document.clone())
        .await?;

    Ok(())
}

/// Deletes one temporal attribute instance.
pub async fn delete_instance(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    instance_id: &str,
    _local_only: bool,
) -> Result<(), BrokerError> {
    let mut document = state
        .repositories
        .temporals
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("temporal entity {entity_id} was not found"))
        })?;

    let before = document.history.len();
    document
        .history
        .retain(|record| !(record.attr_id == attr_id && record.instance_id == instance_id));
    if before == document.history.len() {
        return Err(BrokerError::NotFound(format!(
            "temporal attribute instance {instance_id} was not found"
        )));
    }

    set_temporal_modified_at(&mut document.doc);
    state
        .repositories
        .temporals
        .upsert(document.clone())
        .await?;

    Ok(())
}

/// Extracts temporal history records from current entity attributes.
fn temporal_history_from_entity(entity: &Value) -> Vec<TemporalAttributeRecord> {
    entity
        .as_object()
        .into_iter()
        .flat_map(|object| object.iter())
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "id" | "type" | "scope" | "@context" | "createdAt" | "modifiedAt" | "deletedAt"
            )
        })
        .map(|(attr_id, value)| record_from_value(attr_id, value))
        .collect()
}

/// Converts attribute value into temporal history record.
fn record_from_value(attr_id: &str, value: &Value) -> TemporalAttributeRecord {
    let instance = make_temporal_instance(attr_id, value);
    TemporalAttributeRecord {
        attr_id: attr_id.to_string(),
        instance_id: instance
            .get("instanceId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        observed_at: instance
            .get("observedAt")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        created_at: instance
            .get("createdAt")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        modified_at: instance
            .get("modifiedAt")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        deleted_at: instance
            .get("deletedAt")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        value: instance
            .get("value")
            .cloned()
            .unwrap_or_else(|| value.clone()),
    }
}

/// Filters temporal history and applies `lastN` semantics.
fn select_temporal_history(
    history: &[TemporalAttributeRecord],
    temporal: &TemporalFilter,
) -> Vec<Value> {
    let mut selected = history
        .iter()
        .filter(|record| temporal_matches(record, temporal))
        .cloned()
        .collect::<Vec<_>>();

    if let Some(last_n) = temporal.last_n {
        selected.sort_by(|left, right| {
            temporal_record_time(right, temporal).cmp(&temporal_record_time(left, temporal))
        });
        selected.truncate(last_n as usize);
    }

    selected.into_iter().map(record_to_value).collect()
}

/// Formats temporal history for requested temporal representation.
fn format_temporal_history(
    history: &[Value],
    representation: Representation,
    temporal: &TemporalFilter,
) -> Vec<Value> {
    match representation {
        Representation::AggregatedValues => aggregate_temporal_history(history, temporal),
        Representation::TemporalValues
        | Representation::Normalized
        | Representation::KeyValues
        | Representation::GeoJson => history.to_vec(),
    }
}

/// Aggregates numeric temporal history per attribute.
fn aggregate_temporal_history(history: &[Value], temporal: &TemporalFilter) -> Vec<Value> {
    let mut grouped: std::collections::BTreeMap<String, Vec<f64>> =
        std::collections::BTreeMap::new();
    for item in history {
        let Some(attr_id) = item.get("attrId").and_then(Value::as_str) else {
            continue;
        };
        let number = item
            .get("value")
            .and_then(Value::as_object)
            .and_then(|value| value.get("value"))
            .and_then(Value::as_f64);
        if let Some(number) = number {
            grouped.entry(attr_id.to_string()).or_default().push(number);
        }
    }

    grouped
        .into_iter()
        .map(|(attr_id, values)| {
            let mut object = Map::new();
            object.insert("attrId".to_string(), Value::String(attr_id));
            object.insert(
                "aggrMethods".to_string(),
                Value::Array(
                    temporal
                        .aggr_methods
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            );

            let mut metrics = Map::new();
            for method in &temporal.aggr_methods {
                let value = match method.as_str() {
                    "sum" => values.iter().sum::<f64>(),
                    "avg" => values.iter().sum::<f64>() / values.len().max(1) as f64,
                    "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                    "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    "totalCount" | "distinctCount" => values.len() as f64,
                    _ => values.iter().sum::<f64>(),
                };
                metrics.insert(
                    method.clone(),
                    serde_json::Number::from_f64(value)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                );
            }
            object.insert("aggregated".to_string(), Value::Object(metrics));
            Value::Object(object)
        })
        .collect()
}

/// Evaluates one temporal record against temporal filter window.
fn temporal_matches(record: &TemporalAttributeRecord, temporal: &TemporalFilter) -> bool {
    let Some(reference) = temporal_record_time(record, temporal) else {
        return false;
    };
    let lower = chrono::DateTime::parse_from_rfc3339(&temporal.time_at).ok();
    let upper = temporal
        .end_time_at
        .as_deref()
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok());

    match temporal.time_rel.as_str() {
        "before" => lower.map(|lower| reference < lower).unwrap_or(false),
        "after" => lower.map(|lower| reference > lower).unwrap_or(false),
        "between" => lower
            .zip(upper)
            .map(|(lower, upper)| reference >= lower && reference <= upper)
            .unwrap_or(false),
        _ => false,
    }
}

/// Selects effective timestamp field for temporal filtering.
fn temporal_record_time(
    record: &TemporalAttributeRecord,
    temporal: &TemporalFilter,
) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    let value = match temporal.time_property.as_str() {
        "createdAt" => record.created_at.as_deref(),
        "modifiedAt" => record.modified_at.as_deref(),
        "deletedAt" => record.deleted_at.as_deref(),
        _ => record
            .observed_at
            .as_deref()
            .or(record.created_at.as_deref()),
    }?;
    chrono::DateTime::parse_from_rfc3339(value).ok()
}

/// Serializes temporal history record into response shape.
fn record_to_value(record: TemporalAttributeRecord) -> Value {
    serde_json::json!({
        "attrId": record.attr_id,
        "instanceId": record.instance_id,
        "observedAt": record.observed_at,
        "createdAt": record.created_at,
        "modifiedAt": record.modified_at,
        "deletedAt": record.deleted_at,
        "value": record.value,
    })
}

/// Loads temporal entity document or returns not found.
async fn get_temporal_document(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
) -> Result<TemporalEntityDocument, BrokerError> {
    state
        .repositories
        .temporals
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("temporal entity {entity_id} was not found")))
}

/// Advances `modifiedAt` on temporal entity document.
fn set_temporal_modified_at(document: &mut Value) {
    if let Some(object) = document.as_object_mut() {
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
}

/// Builds linked-entity graph for temporal queries when needed.
async fn temporal_query_linked_graph(
    state: &AppState,
    context: &RequestContext,
    query: &TemporalEntityQuery,
    documents: &[TemporalEntityDocument],
) -> Result<Arc<HashMap<String, Value>>, BrokerError> {
    if !query_needs_linked_graph(&query.entity) {
        return Ok(Arc::default());
    }

    let mut linked_entities = state
        .repositories
        .temporals
        .query(
            &context.tenant,
            &QueryPlan::default(),
            &TemporalFilter::from_query(query)?,
            None,
        )
        .await?
        .into_iter()
        .map(|document| document.doc)
        .filter_map(|entity| {
            let id = entity.get("id").and_then(Value::as_str)?.to_string();
            Some((id, entity))
        })
        .collect::<HashMap<_, _>>();

    for document in documents {
        if let Some(id) = document.doc.get("id").and_then(Value::as_str) {
            linked_entities
                .entry(id.to_string())
                .or_insert_with(|| document.doc.clone());
        }
    }

    Ok(Arc::new(linked_entities))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn temporal_filter(time_rel: &str) -> TemporalFilter {
        TemporalFilter {
            time_property: "observedAt".to_string(),
            time_rel: time_rel.to_string(),
            time_at: "2024-01-01T00:00:00Z".to_string(),
            end_time_at: Some("2024-01-04T00:00:00Z".to_string()),
            last_n: None,
            aggr_methods: vec![
                "sum".to_string(),
                "avg".to_string(),
                "totalCount".to_string(),
            ],
            aggr_period_duration: None,
            representation: Representation::TemporalValues,
        }
    }

    fn record(
        attr_id: &str,
        instance_id: &str,
        observed_at: &str,
        value: f64,
    ) -> TemporalAttributeRecord {
        TemporalAttributeRecord {
            attr_id: attr_id.to_string(),
            instance_id: instance_id.to_string(),
            observed_at: Some(observed_at.to_string()),
            created_at: None,
            modified_at: None,
            deleted_at: None,
            value: json!({
                "type": "Property",
                "value": value,
                "observedAt": observed_at,
            }),
        }
    }

    #[test]
    fn selects_last_matching_temporal_instances_in_descending_time_order() {
        let history = vec![
            record("speed", "i1", "2024-01-01T00:00:00Z", 10.0),
            record("speed", "i2", "2024-01-02T00:00:00Z", 20.0),
            record("speed", "i3", "2024-01-03T00:00:00Z", 30.0),
        ];
        let mut filter = temporal_filter("after");
        filter.time_at = "2024-01-01T12:00:00Z".to_string();
        filter.last_n = Some(2);

        let selected = select_temporal_history(&history, &filter);

        assert_eq!(selected.len(), 2);
        assert_eq!(
            selected[0].get("instanceId").and_then(Value::as_str),
            Some("i3")
        );
        assert_eq!(
            selected[1].get("instanceId").and_then(Value::as_str),
            Some("i2")
        );
    }

    #[test]
    fn aggregates_numeric_history_by_attribute() {
        let history = vec![
            record_to_value(record("speed", "i1", "2024-01-01T00:00:00Z", 10.0)),
            record_to_value(record("speed", "i2", "2024-01-02T00:00:00Z", 20.0)),
            record_to_value(record("temperature", "i3", "2024-01-02T00:00:00Z", 5.0)),
        ];
        let filter = temporal_filter("between");

        let aggregated = aggregate_temporal_history(&history, &filter);

        assert_eq!(aggregated.len(), 2);

        let speed = aggregated
            .iter()
            .find(|value| value.get("attrId").and_then(Value::as_str) == Some("speed"))
            .unwrap();
        let metrics = speed.get("aggregated").and_then(Value::as_object).unwrap();

        assert_eq!(metrics.get("sum").and_then(Value::as_f64), Some(30.0));
        assert_eq!(metrics.get("avg").and_then(Value::as_f64), Some(15.0));
        assert_eq!(metrics.get("totalCount").and_then(Value::as_f64), Some(2.0));
    }

    #[test]
    fn matches_temporal_records_using_selected_time_property() {
        let record = TemporalAttributeRecord {
            attr_id: "speed".to_string(),
            instance_id: "i1".to_string(),
            observed_at: None,
            created_at: Some("2024-01-03T00:00:00Z".to_string()),
            modified_at: None,
            deleted_at: None,
            value: json!({"type": "Property", "value": 10.0}),
        };
        let mut filter = temporal_filter("after");
        filter.time_property = "createdAt".to_string();
        filter.time_at = "2024-01-02T00:00:00Z".to_string();

        assert!(temporal_matches(&record, &filter));
    }
}
