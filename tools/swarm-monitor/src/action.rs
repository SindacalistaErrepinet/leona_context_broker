//! Broker actions triggered from the TUI.
use std::{
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use reqwest::Client;

use crate::model::{EventLevel, MonitorState};

/// Builds a fresh test entity payload.
pub fn generated_payload() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos())
        .unwrap_or(0);
    serde_json::json!({
        "id": format!("urn:ngsi-ld:Vehicle:monitor-{}", uuid::Uuid::new_v4()),
        "type": "Vehicle",
        "speed": {
            "type": "Property",
            "value": nanos % 100
        }
    })
    .to_string()
}

/// Sends one payload to a broker `/entities` endpoint.
pub fn spawn_send(
    client: Client,
    base_url: String,
    payload: String,
    state: Arc<Mutex<MonitorState>>,
) {
    tokio::spawn(async move {
        let url = format!("{}/entities", base_url.trim_end_matches('/'));
        let result = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await;

        match result {
            Ok(response) => {
                let status = response.status();
                let location = response
                    .headers()
                    .get("location")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("-")
                    .to_string();
                let body = if status.is_success() {
                    String::new()
                } else {
                    response.text().await.unwrap_or_default()
                };
                let mut state = state.lock().unwrap();
                if status.is_success() {
                    state.push_event(
                        EventLevel::Info,
                        format!(
                            "sent payload to {}: HTTP {} location={}",
                            base_url,
                            status.as_u16(),
                            location
                        ),
                    );
                } else {
                    state.push_event(
                        EventLevel::Error,
                        format!(
                            "send to {} failed: HTTP {} {}",
                            base_url,
                            status.as_u16(),
                            body
                        ),
                    );
                }
            }
            Err(error) => {
                let mut state = state.lock().unwrap();
                state.push_event(
                    EventLevel::Error,
                    format!("send to {} failed: {error}", base_url),
                );
            }
        }
    });
}
