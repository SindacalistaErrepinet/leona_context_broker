//! Node discovery and topology edge building.
//!
//! Discovery starts from configured seeds. DefraDB nodes are crawled through
//! `/api/v0/p2p/active-peers` and `/api/v0/p2p/replicators` multiaddrs. Broker
//! nodes are found from explicit broker seeds and from the co-located broker of
//! each discovered DefraDB node.
use crate::model::{BrokerNode, DefraNode, Edge, EdgeKind, EdgeStatus, MonitorState, NodeStatus};

/// Parsed libp2p multiaddr parts used by discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MultiaddrParts {
    pub host: Option<String>,
    pub peer_id: Option<String>,
}

/// Parses host and peer id from a libp2p multiaddr.
pub fn parse_multiaddr(address: &str) -> MultiaddrParts {
    let mut parts = MultiaddrParts::default();
    let components = address.split('/').filter(|value| !value.is_empty());
    let mut expect_host = false;
    let mut expect_peer = false;

    for component in components {
        if expect_host {
            parts.host = Some(component.to_string());
            expect_host = false;
            continue;
        }
        if expect_peer {
            parts.peer_id = Some(component.to_string());
            expect_peer = false;
            continue;
        }
        match component {
            "ip4" | "ip6" | "dns" | "dns4" | "dns6" | "dnsaddr" => expect_host = true,
            "p2p" => expect_peer = true,
            _ => {}
        }
    }

    parts
}

/// Extracts the peer id from a multiaddr, if present.
pub fn peer_id_from_multiaddr(address: &str) -> Option<String> {
    parse_multiaddr(address).peer_id
}

/// Builds a DefraDB API base from host and HTTP port.
pub fn defra_api_base(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("http://[{host}]:{port}")
    } else {
        format!("http://{host}:{port}")
    }
}

/// Derives a co-located broker base from a DefraDB API base.
///
/// Loopback hosts are skipped because published host ports do not map 1:1 to
/// container-internal ports; those nodes must be supplied as broker seeds.
pub fn derive_broker_base(defra_api_base: &str, broker_port: u16) -> Option<String> {
    let url = reqwest::Url::parse(defra_api_base).ok()?;
    let host = url.host_str()?;
    if is_loopback_host(host) {
        return None;
    }
    Some(format!("http://{host}:{broker_port}/ngsi-ld/v1"))
}

/// Returns true for loopback and local host names.
pub fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") || host.starts_with("127.")
}

/// Trims trailing slashes from a base URL.
pub fn normalize_base(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
}

/// Extracts host from a URL or bare host value.
pub fn host_of(value: &str) -> Option<String> {
    if let Ok(url) = reqwest::Url::parse(value) {
        return url.host_str().map(ToString::to_string);
    }
    value
        .split('/')
        .next()
        .filter(|host| !host.is_empty())
        .map(|host| host.split(':').next().unwrap_or(host).to_string())
}

/// Returns stable display id for one DefraDB node.
pub fn defra_display_id(node: &DefraNode) -> String {
    if !node.peer_id.is_empty() {
        return short_peer_id(&node.peer_id);
    }
    host_of(&node.api_base).unwrap_or_else(|| node.api_base.clone())
}

/// Shortens long peer ids for table and edge labels.
pub fn short_peer_id(peer_id: &str) -> String {
    if peer_id.len() > 14 {
        format!("{}..{}", &peer_id[..8], &peer_id[peer_id.len() - 4..])
    } else {
        peer_id.to_string()
    }
}

/// Builds topology edges from current monitor state.
pub fn build_edges(state: &MonitorState) -> Vec<Edge> {
    let mut edges = Vec::new();
    build_defra_edges(state, &mut edges);
    build_broker_storage_edges(state, &mut edges);
    build_broker_seed_edges(state, &mut edges);
    edges
}

fn build_defra_edges(state: &MonitorState, edges: &mut Vec<Edge>) {
    for node in state.defra_nodes.values() {
        let from = defra_display_id(node);
        for address in &node.active_peers {
            let Some(peer_id) = peer_id_from_multiaddr(address) else {
                continue;
            };
            edges.push(Edge {
                from: from.clone(),
                to: short_peer_id(&peer_id),
                kind: EdgeKind::DefraPeer,
                status: if node.status == NodeStatus::Online {
                    EdgeStatus::Active
                } else {
                    EdgeStatus::Offline
                },
                detail: address.clone(),
                docs: 0,
                bytes: 0,
            });
        }
        for replicator in &node.replicators {
            for address in &replicator.addresses {
                let Some(peer_id) = peer_id_from_multiaddr(address) else {
                    continue;
                };
                let replicator_id = if replicator.id.is_empty() {
                    "-".to_string()
                } else {
                    short_peer_id(&replicator.id)
                };
                let detail = match &replicator.last_status_change {
                    Some(changed) => format!(
                        "{} | {} collections | {}",
                        replicator_id,
                        replicator.collection_ids.len(),
                        changed
                    ),
                    None => format!(
                        "{} | {} collections",
                        replicator_id,
                        replicator.collection_ids.len()
                    ),
                };
                edges.push(Edge {
                    from: from.clone(),
                    to: short_peer_id(&peer_id),
                    kind: EdgeKind::DefraReplicator,
                    status: if replicator.is_active() {
                        EdgeStatus::Active
                    } else {
                        EdgeStatus::Inactive
                    },
                    detail,
                    docs: 0,
                    bytes: 0,
                });
            }
        }
    }
}

fn build_broker_storage_edges(state: &MonitorState, edges: &mut Vec<Edge>) {
    for broker in state.broker_nodes.values() {
        let target = broker
            .defradb_peer_id
            .as_deref()
            .filter(|peer_id| state.defra_nodes.contains_key(*peer_id))
            .map(short_peer_id)
            .or_else(|| {
                state
                    .defra_nodes
                    .values()
                    .find(|defra| host_of(&defra.api_base) == host_of(&broker.defradb_url))
                    .map(defra_display_id)
            });

        let Some(target) = target else {
            continue;
        };
        edges.push(Edge {
            from: broker.broker_id.clone(),
            to: target,
            kind: EdgeKind::BrokerStorage,
            status: if broker.status == NodeStatus::Online {
                EdgeStatus::Active
            } else {
                EdgeStatus::Offline
            },
            detail: broker.defradb_url.clone(),
            docs: 0,
            bytes: 0,
        });
    }
}

fn build_broker_seed_edges(state: &MonitorState, edges: &mut Vec<Edge>) {
    for broker in state.broker_nodes.values() {
        for seed in &broker.p2p_seeds {
            let target = state.broker_nodes.values().find(|candidate| {
                normalize_base(&candidate.base_url) == normalize_base(seed)
                    || host_of(&candidate.base_url) == host_of(seed)
            });
            let target_id = target.map(|candidate| candidate.broker_id.clone());
            let status = match target {
                Some(candidate) if candidate.status == NodeStatus::Online => EdgeStatus::Active,
                Some(_) => EdgeStatus::Offline,
                None => EdgeStatus::Configured,
            };
            let (docs, bytes) = peer_traffic(broker, seed, target_id.as_deref());
            edges.push(Edge {
                from: broker.broker_id.clone(),
                to: target_id.unwrap_or_else(|| seed.clone()),
                kind: EdgeKind::BrokerSeed,
                status,
                detail: seed.clone(),
                docs,
                bytes,
            });
        }
    }
}

fn peer_traffic(broker: &BrokerNode, seed: &str, target_id: Option<&str>) -> (u64, u64) {
    let seed_host = host_of(seed);
    let mut docs = 0;
    let mut bytes = 0;
    for (key, stats) in &broker.peers {
        let matches = target_id == Some(key.as_str())
            || seed_host.as_deref().is_some_and(|host| key.contains(host));
        if matches {
            docs += stats.total_exchanges();
            bytes += stats.total_bytes();
        }
    }
    (docs, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BrokerCounters, BrokerNode, DefraNode, NodeStatus};
    use std::collections::{BTreeMap, HashMap, VecDeque};

    #[test]
    fn parses_ip4_multiaddr() {
        let parts = parse_multiaddr("/ip4/172.20.0.3/tcp/9171/p2p/12D3KooWExamplePeerId");
        assert_eq!(parts.host.as_deref(), Some("172.20.0.3"));
        assert_eq!(parts.peer_id.as_deref(), Some("12D3KooWExamplePeerId"));
    }

    #[test]
    fn parses_dns_multiaddr_without_peer_id() {
        let parts = parse_multiaddr("/dns4/defra1/tcp/9171");
        assert_eq!(parts.host.as_deref(), Some("defra1"));
        assert_eq!(parts.peer_id, None);
    }

    #[test]
    fn skips_loopback_broker_derivation() {
        assert_eq!(derive_broker_base("http://127.0.0.1:19181", 8080), None);
        assert_eq!(
            derive_broker_base("http://172.20.0.3:9181", 8080),
            Some("http://172.20.0.3:8080/ngsi-ld/v1".to_string())
        );
    }

    #[test]
    fn builds_defra_and_broker_edges() {
        let mut state = MonitorState::new("{}".to_string());
        state.defra_nodes.insert(
            "peer-a".to_string(),
            DefraNode {
                peer_id: "peer-a".to_string(),
                api_base: "http://172.20.0.3:9181".to_string(),
                addresses: vec![],
                active_peers: vec!["/ip4/172.20.0.4/tcp/9171/p2p/peer-b".to_string()],
                replicators: vec![],
                status: NodeStatus::Online,
                last_seen: None,
                last_error: None,
                misses: 0,
            },
        );
        state.broker_nodes.insert(
            "node1".to_string(),
            BrokerNode {
                broker_id: "node1".to_string(),
                base_url: "http://172.20.0.3:8080/ngsi-ld/v1".to_string(),
                public_endpoint: "http://node1:8080/ngsi-ld/v1".to_string(),
                defradb_url: "http://127.0.0.1:9181/api/v0/graphql".to_string(),
                defradb_peer_id: Some("peer-a".to_string()),
                uptime_ms: 0,
                p2p_seeds: vec!["http://172.20.0.4:8080/ngsi-ld/v1".to_string()],
                counters: BrokerCounters::default(),
                peers: HashMap::new(),
                status: NodeStatus::Online,
                last_seen: None,
                last_error: None,
                mutation_rate: 0.0,
                bytes_rate: 0.0,
                misses: 0,
            },
        );

        let edges = build_edges(&state);
        assert!(
            edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::DefraPeer && edge.to == "peer-b")
        );
        assert!(
            edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::BrokerStorage && edge.to == "peer-a")
        );
        assert!(
            edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::BrokerSeed && edge.from == "node1")
        );
    }

    #[test]
    fn keeps_empty_state_empty() {
        let state = MonitorState {
            defra_nodes: BTreeMap::new(),
            broker_nodes: BTreeMap::new(),
            edges: Vec::new(),
            events: VecDeque::new(),
            payload: "{}".to_string(),
            paused: false,
            poll_count: 0,
            last_poll: None,
            rate_history: VecDeque::new(),
            status: String::new(),
        };
        assert!(build_edges(&state).is_empty());
    }
}
