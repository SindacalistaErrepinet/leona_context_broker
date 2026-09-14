//! Shared application state passed to HTTP handlers and services.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::Client;
use tokio::sync::{Mutex, OnceCell};

use crate::{
    app::{entity_watch::EntityWatchState, stats::Stats},
    config::AppConfig,
    error::BrokerError,
    persistence::repository::Repositories,
};

/// Shared runtime state for request handling and background work.
#[derive(Clone)]
pub struct AppState {
    /// Static configuration derived from environment variables.
    pub config: AppConfig,
    /// Storage adapters used by services.
    pub repositories: Repositories,
    /// Outbound HTTP client for context resolution and notifications.
    pub http_client: Client,
    /// Process start timestamp used for uptime reporting.
    pub started_at: Instant,
    /// Duplicate-suppression state for entity watch worker.
    pub entity_watch: EntityWatchState,
    /// Serializes bulk entity writes hitting same local DefraDB node.
    pub entity_write_lock: Arc<Mutex<()>>,
    /// Process-local observability counters.
    pub stats: Arc<Stats>,
    /// Cached local DefraDB P2P peer id when reachable.
    pub defradb_peer_id: Arc<OnceCell<String>>,
}

impl AppState {
    /// Creates shared application state and outbound HTTP client.
    pub fn new(config: AppConfig, repositories: Repositories) -> Result<Self, BrokerError> {
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.outbound_timeout_ms))
            .build()
            .map_err(|error| {
                BrokerError::internal(format!("failed to create HTTP client: {error}"))
            })?;

        Ok(Self {
            config,
            repositories,
            http_client,
            started_at: Instant::now(),
            entity_watch: EntityWatchState::default(),
            entity_write_lock: Arc::new(Mutex::new(())),
            stats: Arc::new(Stats::default()),
            defradb_peer_id: Arc::new(OnceCell::new()),
        })
    }
}
