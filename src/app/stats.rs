//! Runtime counters exposed through the internal swarm statistics endpoint.
//!
//! Counters are process-local and best-effort: they describe observed write,
//! replication, and delivery activity. They never influence broker behavior.
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::Serialize;

use crate::{
    app::state::AppState, domain::types::EntityEventKind, utils::time::now_timestamp_millis,
};

/// Header carrying origin broker id on internal peer sync requests.
pub const HEADER_ORIGIN_BROKER: &str = "X-Leona-Broker-Id";

/// Per-peer broker exchange counters.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PeerStats {
    /// Entity documents pushed to peer through internal batch sync.
    pub docs_sent: u64,
    /// Serialized payload bytes pushed to peer through internal batch sync.
    pub bytes_sent: u64,
    /// Entity documents received from peer through internal batch sync.
    pub docs_received: u64,
    /// Serialized payload bytes received from peer through internal batch sync.
    pub bytes_received: u64,
    /// Replicated mutation events consumed from this peer.
    pub mutations_consumed: u64,
    /// Mutation payload bytes consumed from this peer.
    pub mutation_bytes_consumed: u64,
    /// Last time this peer was observed in milliseconds since epoch.
    pub last_seen_millis: u64,
}

/// Point-in-time global counters.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CounterSnapshot {
    pub entities_created: u64,
    pub entities_updated: u64,
    pub entities_deleted: u64,
    pub temporal_writes: u64,
    pub mutations_appended: u64,
    pub mutation_bytes_appended: u64,
    pub mutations_consumed_remote: u64,
    pub mutation_bytes_consumed_remote: u64,
    pub notifications_attempted: u64,
    pub notifications_succeeded: u64,
    pub notifications_failed: u64,
    pub peer_batches_sent: u64,
    pub peer_batch_docs_sent: u64,
    pub peer_batch_bytes_sent: u64,
    pub peer_batches_received: u64,
    pub peer_batch_docs_received: u64,
    pub peer_batch_bytes_received: u64,
}

/// Internal statistics snapshot returned by `/internal/stats`.
#[derive(Clone, Debug, Serialize)]
pub struct InternalStatsSnapshot {
    pub broker_id: String,
    pub public_endpoint: String,
    pub defradb_url: String,
    pub defradb_peer_id: Option<String>,
    pub uptime_ms: u64,
    pub p2p_seeds: Vec<String>,
    pub counters: CounterSnapshot,
    pub peers: HashMap<String, PeerStats>,
}

/// Lock-free global counters plus per-peer exchange map.
#[derive(Debug, Default)]
pub struct Stats {
    entities_created: AtomicU64,
    entities_updated: AtomicU64,
    entities_deleted: AtomicU64,
    temporal_writes: AtomicU64,
    mutations_appended: AtomicU64,
    mutation_bytes_appended: AtomicU64,
    mutations_consumed_remote: AtomicU64,
    mutation_bytes_consumed_remote: AtomicU64,
    notifications_attempted: AtomicU64,
    notifications_succeeded: AtomicU64,
    notifications_failed: AtomicU64,
    peer_batches_sent: AtomicU64,
    peer_batch_docs_sent: AtomicU64,
    peer_batch_bytes_sent: AtomicU64,
    peer_batches_received: AtomicU64,
    peer_batch_docs_received: AtomicU64,
    peer_batch_bytes_received: AtomicU64,
    peers: Mutex<HashMap<String, PeerStats>>,
}

impl Stats {
    /// Records one local entity lifecycle event.
    pub fn record_entity_event(&self, kind: EntityEventKind) {
        match kind {
            EntityEventKind::Created => self.entities_created.fetch_add(1, Ordering::Relaxed),
            EntityEventKind::Updated => self.entities_updated.fetch_add(1, Ordering::Relaxed),
            EntityEventKind::Deleted => self.entities_deleted.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Records one temporal entity write.
    pub fn record_temporal_write(&self) {
        self.temporal_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records appended local mutation-log events and payload bytes.
    pub fn record_mutations_appended(&self, count: u64, bytes: u64) {
        self.mutations_appended.fetch_add(count, Ordering::Relaxed);
        self.mutation_bytes_appended
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records consumed remote mutation-log events attributed to origin broker.
    pub fn record_remote_mutations(&self, origin: &str, count: u64, bytes: u64) {
        self.mutations_consumed_remote
            .fetch_add(count, Ordering::Relaxed);
        self.mutation_bytes_consumed_remote
            .fetch_add(bytes, Ordering::Relaxed);
        update_peer(&self.peers, origin, |peer| {
            peer.mutations_consumed += count;
            peer.mutation_bytes_consumed += bytes;
        });
    }

    /// Records one notification delivery attempt.
    pub fn record_notification_attempt(&self, success: bool) {
        self.notifications_attempted.fetch_add(1, Ordering::Relaxed);
        if success {
            self.notifications_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.notifications_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records one successful internal peer batch push.
    pub fn record_peer_batch_sent(&self, peer: &str, docs: u64, bytes: u64) {
        self.peer_batches_sent.fetch_add(1, Ordering::Relaxed);
        self.peer_batch_docs_sent.fetch_add(docs, Ordering::Relaxed);
        self.peer_batch_bytes_sent
            .fetch_add(bytes, Ordering::Relaxed);
        update_peer(&self.peers, peer, |stats| {
            stats.docs_sent += docs;
            stats.bytes_sent += bytes;
        });
    }

    /// Records one internal peer batch received from another broker.
    pub fn record_peer_batch_received(&self, peer: &str, docs: u64, bytes: u64) {
        self.peer_batches_received.fetch_add(1, Ordering::Relaxed);
        self.peer_batch_docs_received
            .fetch_add(docs, Ordering::Relaxed);
        self.peer_batch_bytes_received
            .fetch_add(bytes, Ordering::Relaxed);
        update_peer(&self.peers, peer, |stats| {
            stats.docs_received += docs;
            stats.bytes_received += bytes;
        });
    }

    /// Returns point-in-time global counters.
    pub fn counters(&self) -> CounterSnapshot {
        CounterSnapshot {
            entities_created: self.entities_created.load(Ordering::Relaxed),
            entities_updated: self.entities_updated.load(Ordering::Relaxed),
            entities_deleted: self.entities_deleted.load(Ordering::Relaxed),
            temporal_writes: self.temporal_writes.load(Ordering::Relaxed),
            mutations_appended: self.mutations_appended.load(Ordering::Relaxed),
            mutation_bytes_appended: self.mutation_bytes_appended.load(Ordering::Relaxed),
            mutations_consumed_remote: self.mutations_consumed_remote.load(Ordering::Relaxed),
            mutation_bytes_consumed_remote: self
                .mutation_bytes_consumed_remote
                .load(Ordering::Relaxed),
            notifications_attempted: self.notifications_attempted.load(Ordering::Relaxed),
            notifications_succeeded: self.notifications_succeeded.load(Ordering::Relaxed),
            notifications_failed: self.notifications_failed.load(Ordering::Relaxed),
            peer_batches_sent: self.peer_batches_sent.load(Ordering::Relaxed),
            peer_batch_docs_sent: self.peer_batch_docs_sent.load(Ordering::Relaxed),
            peer_batch_bytes_sent: self.peer_batch_bytes_sent.load(Ordering::Relaxed),
            peer_batches_received: self.peer_batches_received.load(Ordering::Relaxed),
            peer_batch_docs_received: self.peer_batch_docs_received.load(Ordering::Relaxed),
            peer_batch_bytes_received: self.peer_batch_bytes_received.load(Ordering::Relaxed),
        }
    }

    /// Returns point-in-time per-peer exchange counters.
    pub fn peers(&self) -> HashMap<String, PeerStats> {
        self.peers.lock().unwrap().clone()
    }
}

/// Builds the internal statistics snapshot for one broker.
pub fn snapshot(state: &AppState) -> InternalStatsSnapshot {
    InternalStatsSnapshot {
        broker_id: state.config.broker_id.clone(),
        public_endpoint: state.config.public_endpoint.clone(),
        defradb_url: state.config.defradb_url.clone(),
        defradb_peer_id: state.defradb_peer_id.get().cloned(),
        uptime_ms: state.started_at.elapsed().as_millis() as u64,
        p2p_seeds: state.config.p2p_seeds.clone(),
        counters: state.stats.counters(),
        peers: state.stats.peers(),
    }
}

/// Resolves local DefraDB P2P peer id when reachable.
pub async fn resolve_defradb_peer_id(state: &AppState) -> Option<String> {
    let url = defradb_p2p_url(&state.config.defradb_url, "info")?;
    let addresses = state
        .http_client
        .get(url)
        .send()
        .await
        .ok()?
        .json::<Vec<String>>()
        .await
        .ok()?;
    addresses
        .iter()
        .find_map(|address| peer_id_from_multiaddr(address))
}

/// Extracts peer id from a libp2p multiaddr.
pub fn peer_id_from_multiaddr(address: &str) -> Option<String> {
    let peer_id = address.split("/p2p/").nth(1)?.trim_matches('/');
    (!peer_id.is_empty()).then(|| peer_id.to_string())
}

/// Builds a DefraDB P2P API URL from configured GraphQL endpoint.
pub fn defradb_p2p_url(defradb_url: &str, endpoint: &str) -> Option<String> {
    let trimmed = defradb_url.trim_end_matches('/');
    let base = trimmed
        .strip_suffix("/graphql")
        .unwrap_or(trimmed)
        .trim_end_matches('/');
    (!base.is_empty()).then(|| format!("{base}/p2p/{endpoint}"))
}

/// Updates one peer entry and refreshes its last-seen timestamp.
fn update_peer(
    peers: &Mutex<HashMap<String, PeerStats>>,
    peer: &str,
    update: impl FnOnce(&mut PeerStats),
) {
    let mut peers = peers.lock().unwrap();
    let entry = peers.entry(peer.to_string()).or_default();
    update(entry);
    entry.last_seen_millis = now_timestamp_millis().max(0) as u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_peer_id_from_multiaddr() {
        let address = "/ip4/172.20.0.3/tcp/9171/p2p/12D3KooWExamplePeerId";
        assert_eq!(
            peer_id_from_multiaddr(address),
            Some("12D3KooWExamplePeerId".to_string())
        );
        assert_eq!(peer_id_from_multiaddr("/ip4/127.0.0.1/tcp/9171"), None);
    }

    #[test]
    fn builds_defradb_p2p_url_from_graphql_endpoint() {
        assert_eq!(
            defradb_p2p_url("http://127.0.0.1:9181/api/v0/graphql", "info"),
            Some("http://127.0.0.1:9181/api/v0/p2p/info".to_string())
        );
        assert_eq!(
            defradb_p2p_url("http://defra1:9181/api/v0/graphql/", "replicators"),
            Some("http://defra1:9181/api/v0/p2p/replicators".to_string())
        );
    }

    #[test]
    fn records_and_snapshots_counters() {
        let stats = Stats::default();
        stats.record_entity_event(EntityEventKind::Created);
        stats.record_entity_event(EntityEventKind::Updated);
        stats.record_entity_event(EntityEventKind::Deleted);
        stats.record_mutations_appended(2, 100);
        stats.record_remote_mutations("node2", 3, 300);
        stats.record_notification_attempt(true);
        stats.record_notification_attempt(false);
        stats.record_peer_batch_sent("http://node2:8080/internal/entities/batch", 5, 500);
        stats.record_peer_batch_received("node3", 4, 400);

        let counters = stats.counters();
        assert_eq!(counters.entities_created, 1);
        assert_eq!(counters.entities_updated, 1);
        assert_eq!(counters.entities_deleted, 1);
        assert_eq!(counters.mutations_appended, 2);
        assert_eq!(counters.mutation_bytes_appended, 100);
        assert_eq!(counters.mutations_consumed_remote, 3);
        assert_eq!(counters.notifications_attempted, 2);
        assert_eq!(counters.notifications_succeeded, 1);
        assert_eq!(counters.notifications_failed, 1);
        assert_eq!(counters.peer_batch_docs_sent, 5);
        assert_eq!(counters.peer_batch_docs_received, 4);

        let peers = stats.peers();
        assert_eq!(peers["node2"].mutations_consumed, 3);
        assert_eq!(
            peers["http://node2:8080/internal/entities/batch"].docs_sent,
            5
        );
        assert_eq!(peers["node3"].docs_received, 4);
    }
}
