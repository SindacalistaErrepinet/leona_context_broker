//! Runtime configuration loaded from environment variables.
use std::env;

/// Process-wide broker configuration.
#[derive(Clone, Debug)]
pub struct AppConfig {
    /// HTTP bind host for Actix server.
    pub host: String,
    /// HTTP bind port for Actix server.
    pub port: u16,
    /// Stable broker identifier used in headers and source identity output.
    pub broker_id: String,
    /// Public NGSI-LD base endpoint exposed to peers and clients.
    pub public_endpoint: String,
    /// DefraDB GraphQL endpoint used by runtime storage adapter.
    pub defradb_url: String,
    /// Timeout for DefraDB storage HTTP requests in milliseconds.
    pub defradb_timeout_ms: u64,
    /// Timeout for outbound HTTP requests in milliseconds.
    pub outbound_timeout_ms: u64,
    /// Enables slower snapshot reconcile watcher for missed events and direct writes.
    pub entity_watch_enabled: bool,
    /// Polling interval for snapshot reconcile watcher in milliseconds.
    pub entity_watch_interval_ms: u64,
    /// Enables fast replicated mutation-log watcher.
    pub entity_event_watch_enabled: bool,
    /// Polling interval for mutation-log watcher in milliseconds.
    pub entity_event_watch_interval_ms: u64,
    /// Enables broker-to-broker helper sync for large batches.
    pub p2p_enabled: bool,
    /// Public NGSI-LD base endpoints for peer brokers.
    pub p2p_seeds: Vec<String>,
    /// Timeout for internal broker-to-broker sync requests in milliseconds.
    pub peer_sync_timeout_ms: u64,
}

impl AppConfig {
    /// Builds runtime configuration from process environment.
    pub fn from_env() -> Self {
        let host = env_or("BROKER_HOST", "127.0.0.1");
        let port = env_or("BROKER_PORT", "8080").parse().unwrap_or(8080);
        let broker_id = env_or("BROKER_ID", "leona-context-broker");
        let public_endpoint = env_or(
            "BROKER_PUBLIC_ENDPOINT",
            &format!("http://{host}:{port}/ngsi-ld/v1"),
        );

        Self {
            host,
            port,
            broker_id: broker_id.clone(),
            public_endpoint,
            defradb_url: env_or("BROKER_DEFRADB_URL", "http://127.0.0.1:9181/api/v0/graphql"),
            defradb_timeout_ms: env_or("BROKER_DEFRADB_TIMEOUT_MS", "30000")
                .parse()
                .unwrap_or(30000),
            outbound_timeout_ms: env_or("BROKER_OUTBOUND_TIMEOUT_MS", "5000")
                .parse()
                .unwrap_or(5000),
            entity_watch_enabled: env_bool_or("BROKER_ENTITY_WATCH_ENABLED", true),
            entity_watch_interval_ms: env_or("BROKER_ENTITY_WATCH_INTERVAL_MS", "30000")
                .parse()
                .unwrap_or(5000),
            entity_event_watch_enabled: env_bool_or("BROKER_ENTITY_EVENT_WATCH_ENABLED", true),
            entity_event_watch_interval_ms: env_or("BROKER_ENTITY_EVENT_WATCH_INTERVAL_MS", "100")
                .parse()
                .unwrap_or(100),
            p2p_enabled: env_bool_or("BROKER_P2P_ENABLED", true),
            p2p_seeds: parse_csv(&env_or("BROKER_P2P_SEEDS", "")),
            peer_sync_timeout_ms: env_or("BROKER_PEER_SYNC_TIMEOUT_MS", "120000")
                .parse()
                .unwrap_or(120000),
        }
    }

    #[cfg(test)]
    /// Builds isolated configuration defaults for tests.
    pub fn for_tests() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            broker_id: "test-broker".to_string(),
            public_endpoint: "http://127.0.0.1:8080/ngsi-ld/v1".to_string(),
            defradb_url: "http://127.0.0.1:9181/api/v0/graphql".to_string(),
            defradb_timeout_ms: 30000,
            outbound_timeout_ms: 2000,
            entity_watch_enabled: true,
            entity_watch_interval_ms: 30000,
            entity_event_watch_enabled: true,
            entity_event_watch_interval_ms: 100,
            p2p_enabled: false,
            p2p_seeds: Vec::new(),
            peer_sync_timeout_ms: 120000,
        }
    }
}

/// Reads environment variable or falls back to default value.
fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Reads boolean environment variable or falls back to default value.
fn env_bool_or(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

/// Splits comma-separated environment values into non-empty trimmed strings.
fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}
