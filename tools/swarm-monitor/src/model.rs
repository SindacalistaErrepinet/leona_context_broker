//! Shared data model for the swarm monitor.
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;

/// Observed node reachability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    /// Last poll succeeded.
    Online,
    /// Last poll failed but node is kept for grace period.
    Offline,
}

impl NodeStatus {
    /// Short UI marker for node status.
    pub fn marker(self) -> &'static str {
        match self {
            Self::Online => "ok",
            Self::Offline => "offline",
        }
    }
}

/// Per-peer exchange counters reported by a broker.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct PeerStats {
    #[serde(default)]
    pub docs_sent: u64,
    #[serde(default)]
    pub bytes_sent: u64,
    #[serde(default)]
    pub docs_received: u64,
    #[serde(default)]
    pub bytes_received: u64,
    #[serde(default)]
    pub mutations_consumed: u64,
    #[serde(default)]
    pub mutation_bytes_consumed: u64,
    #[serde(default)]
    pub last_seen_millis: u64,
}

impl PeerStats {
    /// Total exchanged units (docs + mutations) for display.
    pub fn total_exchanges(&self) -> u64 {
        self.docs_sent + self.docs_received + self.mutations_consumed
    }

    /// Total exchanged bytes for display.
    pub fn total_bytes(&self) -> u64 {
        self.bytes_sent + self.bytes_received + self.mutation_bytes_consumed
    }
}

/// Global broker counters reported by `/internal/stats`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct BrokerCounters {
    #[serde(default)]
    pub entities_created: u64,
    #[serde(default)]
    pub entities_updated: u64,
    #[serde(default)]
    pub entities_deleted: u64,
    #[serde(default)]
    pub temporal_writes: u64,
    #[serde(default)]
    pub mutations_appended: u64,
    #[serde(default)]
    pub mutation_bytes_appended: u64,
    #[serde(default)]
    pub mutations_consumed_remote: u64,
    #[serde(default)]
    pub mutation_bytes_consumed_remote: u64,
    #[serde(default)]
    pub notifications_attempted: u64,
    #[serde(default)]
    pub notifications_succeeded: u64,
    #[serde(default)]
    pub notifications_failed: u64,
    #[serde(default)]
    pub peer_batches_sent: u64,
    #[serde(default)]
    pub peer_batch_docs_sent: u64,
    #[serde(default)]
    pub peer_batch_bytes_sent: u64,
    #[serde(default)]
    pub peer_batches_received: u64,
    #[serde(default)]
    pub peer_batch_docs_received: u64,
    #[serde(default)]
    pub peer_batch_bytes_received: u64,
}

impl BrokerCounters {
    /// Total mutation-log events observed locally and remotely.
    pub fn mutations_total(&self) -> u64 {
        self.mutations_appended + self.mutations_consumed_remote
    }

    /// Total mutation-log payload bytes observed locally and remotely.
    pub fn mutation_bytes_total(&self) -> u64 {
        self.mutation_bytes_appended + self.mutation_bytes_consumed_remote
    }

    /// Total entity lifecycle writes.
    pub fn entity_writes(&self) -> u64 {
        self.entities_created + self.entities_updated + self.entities_deleted
    }
}

/// Broker `/internal/stats` response.
#[derive(Clone, Debug, Deserialize)]
pub struct BrokerStatsResponse {
    pub broker_id: String,
    #[serde(default)]
    pub public_endpoint: String,
    #[serde(default)]
    pub defradb_url: String,
    #[serde(default)]
    pub defradb_peer_id: Option<String>,
    #[serde(default)]
    pub uptime_ms: u64,
    #[serde(default)]
    pub p2p_seeds: Vec<String>,
    #[serde(default)]
    pub counters: BrokerCounters,
    #[serde(default)]
    pub peers: HashMap<String, PeerStats>,
}

/// DefraDB replicator entry from `/api/v0/p2p/replicators`.
#[derive(Clone, Debug, Deserialize)]
pub struct DefraReplicator {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Addresses", default)]
    pub addresses: Vec<String>,
    #[serde(rename = "CollectionIDs", default)]
    pub collection_ids: Vec<String>,
    #[serde(rename = "Status", default)]
    pub status: u8,
    #[serde(rename = "LastStatusChange", default)]
    pub last_status_change: Option<String>,
}

impl DefraReplicator {
    /// True when DefraDB reports this replicator as actively replicating.
    pub fn is_active(&self) -> bool {
        self.status == 0
    }
}

/// Observed DefraDB node.
#[derive(Clone, Debug)]
pub struct DefraNode {
    pub peer_id: String,
    pub api_base: String,
    pub addresses: Vec<String>,
    pub active_peers: Vec<String>,
    pub replicators: Vec<DefraReplicator>,
    pub status: NodeStatus,
    pub last_seen: Option<Instant>,
    pub last_error: Option<String>,
    pub misses: u32,
}

impl DefraNode {
    /// Creates an unseen node entry for one API base.
    pub fn new(peer_id: String, api_base: String) -> Self {
        Self {
            peer_id,
            api_base,
            addresses: Vec::new(),
            active_peers: Vec::new(),
            replicators: Vec::new(),
            status: NodeStatus::Offline,
            last_seen: None,
            last_error: None,
            misses: 0,
        }
    }
}

/// Observed broker node.
#[derive(Clone, Debug)]
pub struct BrokerNode {
    pub broker_id: String,
    pub base_url: String,
    pub public_endpoint: String,
    pub defradb_url: String,
    pub defradb_peer_id: Option<String>,
    pub uptime_ms: u64,
    pub p2p_seeds: Vec<String>,
    pub counters: BrokerCounters,
    pub peers: HashMap<String, PeerStats>,
    pub status: NodeStatus,
    pub last_seen: Option<Instant>,
    pub last_error: Option<String>,
    pub mutation_rate: f64,
    pub bytes_rate: f64,
    pub misses: u32,
}

/// Topology edge kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeKind {
    /// Live DefraDB P2P connection.
    DefraPeer,
    /// Configured DefraDB replicator.
    DefraReplicator,
    /// Broker to its local DefraDB storage.
    BrokerStorage,
    /// Broker to another broker from seed configuration.
    BrokerSeed,
}

impl EdgeKind {
    /// Short UI label for edge kind.
    pub fn label(self) -> &'static str {
        match self {
            Self::DefraPeer => "p2p",
            Self::DefraReplicator => "replicator",
            Self::BrokerStorage => "storage",
            Self::BrokerSeed => "broker",
        }
    }
}

/// Topology edge status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeStatus {
    Active,
    Inactive,
    Configured,
    Offline,
}

impl EdgeStatus {
    /// Short UI label for edge status.
    pub fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Inactive => "inactive",
            Self::Configured => "configured",
            Self::Offline => "offline",
        }
    }
}

/// One rendered topology edge.
#[derive(Clone, Debug)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub kind: EdgeKind,
    pub status: EdgeStatus,
    pub detail: String,
    pub docs: u64,
    pub bytes: u64,
}

/// Event log level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventLevel {
    Info,
    Warn,
    Error,
}

/// One timestamped monitor event.
#[derive(Clone, Debug)]
pub struct Event {
    pub at: String,
    pub level: EventLevel,
    pub message: String,
}

/// Shared mutable monitor state updated by pollers and actions.
#[derive(Debug)]
pub struct MonitorState {
    pub defra_nodes: BTreeMap<String, DefraNode>,
    pub broker_nodes: BTreeMap<String, BrokerNode>,
    pub edges: Vec<Edge>,
    pub events: VecDeque<Event>,
    pub payload: String,
    pub paused: bool,
    pub poll_count: u64,
    pub last_poll: Option<Instant>,
    pub rate_history: VecDeque<u64>,
    pub status: String,
}

impl MonitorState {
    /// Creates empty state with the default test payload.
    pub fn new(payload: String) -> Self {
        Self {
            defra_nodes: BTreeMap::new(),
            broker_nodes: BTreeMap::new(),
            edges: Vec::new(),
            events: VecDeque::new(),
            payload,
            paused: false,
            poll_count: 0,
            last_poll: None,
            rate_history: VecDeque::new(),
            status: String::new(),
        }
    }

    /// Appends one event, keeping a bounded log.
    pub fn push_event(&mut self, level: EventLevel, message: impl Into<String>) {
        self.events.push_back(Event {
            at: timestamp_hms(),
            level,
            message: message.into(),
        });
        while self.events.len() > 500 {
            self.events.pop_front();
        }
    }

    /// Total mutation events across all online brokers.
    pub fn total_mutations(&self) -> u64 {
        self.broker_nodes
            .values()
            .map(|node| node.counters.mutations_total())
            .sum()
    }

    /// Total exchanged bytes across all broker peer maps.
    pub fn total_peer_bytes(&self) -> u64 {
        self.broker_nodes
            .values()
            .map(|node| node.peers.values().map(PeerStats::total_bytes).sum::<u64>())
            .sum()
    }
}

/// Formats current UTC wall-clock time as `HH:MM:SS`.
pub fn timestamp_hms() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let hours = (seconds / 3600) % 24;
    let minutes = (seconds / 60) % 60;
    let seconds = seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}
