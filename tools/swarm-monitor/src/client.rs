//! Async polling of DefraDB P2P APIs and broker internal statistics.
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use reqwest::Client;

use crate::{
    discovery::{
        build_edges, defra_api_base, derive_broker_base, normalize_base, parse_multiaddr,
        peer_id_from_multiaddr,
    },
    model::{
        BrokerNode, BrokerStatsResponse, DefraNode, DefraReplicator, EventLevel, MonitorState,
        NodeStatus,
    },
};

/// Poller runtime configuration.
#[derive(Clone, Debug)]
pub struct PollConfig {
    pub defra_seeds: Vec<String>,
    pub broker_seeds: Vec<String>,
    pub defradb_http_port: u16,
    pub broker_port: u16,
    pub interval: Duration,
    pub offline_grace: u32,
}

/// Builds the shared HTTP client used by polling and actions.
pub fn http_client(timeout: Duration) -> anyhow::Result<Client> {
    Ok(Client::builder().timeout(timeout).build()?)
}

/// Runs the polling loop until the process exits.
pub async fn run_poller(state: Arc<Mutex<MonitorState>>, config: PollConfig, client: Client) {
    loop {
        let paused = state.lock().unwrap().paused;
        if !paused {
            poll_once(&client, &config, &state).await;
        }
        tokio::time::sleep(config.interval).await;
    }
}

async fn poll_once(client: &Client, config: &PollConfig, state: &Arc<Mutex<MonitorState>>) {
    let known_defra = {
        let state = state.lock().unwrap();
        state
            .defra_nodes
            .values()
            .map(|node| node.api_base.clone())
            .collect::<Vec<_>>()
    };
    let known_brokers = {
        let state = state.lock().unwrap();
        state
            .broker_nodes
            .values()
            .map(|node| node.base_url.clone())
            .collect::<Vec<_>>()
    };

    let mut defra_bases = dedupe(config.defra_seeds.iter().cloned().chain(known_defra));
    let mut seen_defra = HashSet::new();
    let mut index = 0;

    while index < defra_bases.len() {
        let base = defra_bases[index].clone();
        index += 1;
        if !seen_defra.insert(base.clone()) {
            continue;
        }

        match fetch_defra(client, &base).await {
            Ok(fetch) => {
                let peer_id = fetch
                    .addresses
                    .iter()
                    .find_map(|address| peer_id_from_multiaddr(address))
                    .unwrap_or_else(|| base.clone());
                for address in fetch.active_peers.iter().chain(
                    fetch
                        .replicators
                        .iter()
                        .flat_map(|item| item.addresses.iter()),
                ) {
                    let Some(host) = parse_multiaddr(address).host else {
                        continue;
                    };
                    let discovered = defra_api_base(&host, config.defradb_http_port);
                    if !defra_bases.contains(&discovered) {
                        defra_bases.push(discovered);
                    }
                }
                upsert_defra(state, base, peer_id, fetch);
            }
            Err(error) => mark_defra_failed(state, &base, &error),
        }
    }

    let mut broker_bases = dedupe(config.broker_seeds.iter().cloned().chain(known_brokers));
    let discovered_defra = {
        let state = state.lock().unwrap();
        state
            .defra_nodes
            .values()
            .map(|node| node.api_base.clone())
            .collect::<Vec<_>>()
    };
    for base in discovered_defra {
        if let Some(derived) = derive_broker_base(&base, config.broker_port)
            && !broker_bases.contains(&derived)
        {
            broker_bases.push(derived);
        }
    }

    let mut seen_brokers = HashSet::new();
    let mut rates = Vec::new();
    for base in broker_bases {
        if !seen_brokers.insert(base.clone()) {
            continue;
        }
        match fetch_broker(client, &base).await {
            Ok(stats) => rates.push(upsert_broker(state, base, stats)),
            Err(error) => mark_broker_failed(state, &base, &error),
        }
    }

    finalize_poll(state, rates, config.offline_grace);
}

struct DefraFetch {
    addresses: Vec<String>,
    active_peers: Vec<String>,
    replicators: Vec<DefraReplicator>,
}

async fn fetch_defra(client: &Client, base: &str) -> anyhow::Result<DefraFetch> {
    let addresses = get_json::<Vec<String>>(client, &format!("{base}/api/v0/p2p/info")).await?;
    let active_peers =
        get_json::<Vec<String>>(client, &format!("{base}/api/v0/p2p/active-peers")).await?;
    let replicators =
        get_json::<Vec<DefraReplicator>>(client, &format!("{base}/api/v0/p2p/replicators")).await?;
    Ok(DefraFetch {
        addresses,
        active_peers,
        replicators,
    })
}

async fn fetch_broker(client: &Client, base: &str) -> anyhow::Result<BrokerStatsResponse> {
    let root = broker_root(base);
    get_json::<BrokerStatsResponse>(client, &format!("{root}/internal/stats")).await
}

async fn get_json<T: serde::de::DeserializeOwned>(client: &Client, url: &str) -> anyhow::Result<T> {
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("HTTP {} from {url}", response.status().as_u16());
    }
    Ok(response.json::<T>().await?)
}

fn upsert_defra(
    state: &Arc<Mutex<MonitorState>>,
    base: String,
    peer_id: String,
    fetch: DefraFetch,
) {
    let mut state = state.lock().unwrap();
    if let Some(stale_key) = state
        .defra_nodes
        .iter()
        .find(|(key, node)| node.api_base == base && key.as_str() != peer_id)
        .map(|(key, _)| key.clone())
    {
        state.defra_nodes.remove(&stale_key);
    }

    let is_new = !state.defra_nodes.contains_key(&peer_id);
    {
        let node = state
            .defra_nodes
            .entry(peer_id.clone())
            .or_insert_with(|| DefraNode::new(peer_id.clone(), base.clone()));
        node.api_base = base.clone();
        node.addresses = fetch.addresses;
        node.active_peers = fetch.active_peers;
        node.replicators = fetch.replicators;
        node.status = NodeStatus::Online;
        node.last_seen = Some(Instant::now());
        node.last_error = None;
        node.misses = 0;
    }
    if is_new {
        state.push_event(
            EventLevel::Info,
            format!("discovered DefraDB node {} at {}", peer_id, base),
        );
    }
}

fn mark_defra_failed(state: &Arc<Mutex<MonitorState>>, base: &str, error: &anyhow::Error) {
    let mut state = state.lock().unwrap();
    let message = error.to_string();
    for node in state.defra_nodes.values_mut() {
        if node.api_base == base {
            node.status = NodeStatus::Offline;
            node.misses += 1;
            node.last_error = Some(message);
            return;
        }
    }
    let node = state
        .defra_nodes
        .entry(base.to_string())
        .or_insert_with(|| DefraNode::new(String::new(), base.to_string()));
    node.status = NodeStatus::Offline;
    node.misses += 1;
    node.last_error = Some(message);
}

fn upsert_broker(
    state: &Arc<Mutex<MonitorState>>,
    base: String,
    stats: BrokerStatsResponse,
) -> f64 {
    let mut state = state.lock().unwrap();
    let base_url = format!("{}/ngsi-ld/v1", broker_root(&base));
    if let Some(stale_key) = state
        .broker_nodes
        .iter()
        .find(|(key, node)| node.base_url == base_url && key.as_str() != stats.broker_id)
        .map(|(key, _)| key.clone())
    {
        state.broker_nodes.remove(&stale_key);
    }

    let is_new = !state.broker_nodes.contains_key(&stats.broker_id);
    let now = Instant::now();
    let rate = {
        let node = state
            .broker_nodes
            .entry(stats.broker_id.clone())
            .or_insert_with(|| BrokerNode {
                broker_id: stats.broker_id.clone(),
                base_url: base_url.clone(),
                public_endpoint: stats.public_endpoint.clone(),
                defradb_url: stats.defradb_url.clone(),
                defradb_peer_id: stats.defradb_peer_id.clone(),
                uptime_ms: stats.uptime_ms,
                p2p_seeds: stats.p2p_seeds.clone(),
                counters: stats.counters.clone(),
                peers: stats.peers.clone(),
                status: NodeStatus::Online,
                last_seen: Some(now),
                last_error: None,
                mutation_rate: 0.0,
                bytes_rate: 0.0,
                misses: 0,
            });

        let elapsed = node
            .last_seen
            .map(|last| now.duration_since(last).as_secs_f64())
            .unwrap_or(0.0)
            .max(0.001);
        let mutations_delta = stats
            .counters
            .mutations_total()
            .saturating_sub(node.counters.mutations_total());
        let bytes_delta = stats
            .counters
            .mutation_bytes_total()
            .saturating_sub(node.counters.mutation_bytes_total());
        let rate = mutations_delta as f64 / elapsed;
        let bytes_rate = bytes_delta as f64 / elapsed;

        node.base_url = base_url.clone();
        node.public_endpoint = stats.public_endpoint;
        node.defradb_url = stats.defradb_url;
        node.defradb_peer_id = stats.defradb_peer_id;
        node.uptime_ms = stats.uptime_ms;
        node.p2p_seeds = stats.p2p_seeds;
        node.counters = stats.counters;
        node.peers = stats.peers;
        node.status = NodeStatus::Online;
        node.last_seen = Some(now);
        node.last_error = None;
        node.misses = 0;
        node.mutation_rate = rate;
        node.bytes_rate = bytes_rate;
        rate
    };

    if is_new {
        state.push_event(
            EventLevel::Info,
            format!("discovered broker {} at {}", stats.broker_id, base_url),
        );
    }
    rate
}

fn mark_broker_failed(state: &Arc<Mutex<MonitorState>>, base: &str, error: &anyhow::Error) {
    let mut state = state.lock().unwrap();
    let base_url = format!("{}/ngsi-ld/v1", broker_root(base));
    let message = error.to_string();
    for node in state.broker_nodes.values_mut() {
        if node.base_url == base_url {
            node.status = NodeStatus::Offline;
            node.misses += 1;
            node.last_error = Some(message);
            return;
        }
    }
    let node = state
        .broker_nodes
        .entry(base.to_string())
        .or_insert_with(|| BrokerNode {
            broker_id: base.to_string(),
            base_url: base_url.clone(),
            public_endpoint: String::new(),
            defradb_url: String::new(),
            defradb_peer_id: None,
            uptime_ms: 0,
            p2p_seeds: Vec::new(),
            counters: Default::default(),
            peers: Default::default(),
            status: NodeStatus::Offline,
            last_seen: None,
            last_error: None,
            mutation_rate: 0.0,
            bytes_rate: 0.0,
            misses: 0,
        });
    node.status = NodeStatus::Offline;
    node.misses += 1;
    node.last_error = Some(message);
}

fn finalize_poll(state: &Arc<Mutex<MonitorState>>, rates: Vec<f64>, grace: u32) {
    let mut state = state.lock().unwrap();
    let total_rate = rates.iter().sum::<f64>().round().max(0.0) as u64;
    state.rate_history.push_back(total_rate);
    while state.rate_history.len() > 60 {
        state.rate_history.pop_front();
    }

    let removed_defra = state
        .defra_nodes
        .iter()
        .filter(|(_, node)| node.status == NodeStatus::Offline && node.misses > grace)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in removed_defra {
        state.defra_nodes.remove(&key);
        state.push_event(EventLevel::Warn, format!("removed DefraDB node {key}"));
    }

    let removed_brokers = state
        .broker_nodes
        .iter()
        .filter(|(_, node)| node.status == NodeStatus::Offline && node.misses > grace)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in removed_brokers {
        state.broker_nodes.remove(&key);
        state.push_event(EventLevel::Warn, format!("removed broker node {key}"));
    }

    state.edges = build_edges(&state);
    state.poll_count += 1;
    state.last_poll = Some(Instant::now());
    state.status = format!(
        "poll #{} | defra {} | brokers {} | edges {}",
        state.poll_count,
        state.defra_nodes.len(),
        state.broker_nodes.len(),
        state.edges.len()
    );
}

fn broker_root(base: &str) -> String {
    let normalized = normalize_base(base);
    normalized
        .strip_suffix("/ngsi-ld/v1")
        .unwrap_or(&normalized)
        .to_string()
}

fn dedupe(values: impl Iterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .map(|value| normalize_base(&value))
        .filter(|value| !value.is_empty())
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_ngsi_ld_suffix_from_broker_base() {
        assert_eq!(
            broker_root("http://127.0.0.1:18081"),
            "http://127.0.0.1:18081"
        );
        assert_eq!(
            broker_root("http://127.0.0.1:18081/ngsi-ld/v1/"),
            "http://127.0.0.1:18081"
        );
    }

    #[test]
    fn dedupes_normalized_bases() {
        let values = vec![
            "http://a:1/".to_string(),
            "http://a:1".to_string(),
            "http://b:2".to_string(),
        ];
        assert_eq!(
            dedupe(values.into_iter()),
            vec!["http://a:1".to_string(), "http://b:2".to_string()]
        );
    }
}
