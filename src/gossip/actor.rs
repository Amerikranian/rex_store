use actix::dev::ContextFutureSpawner;
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, ResponseActFuture, WrapFuture};
use anyhow::{Result, anyhow};
use futures::future::join_all;
use hashbrown::HashMap;
use hashbrown::HashSet;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::config::Config;
use crate::gossip::GossipMessage;
use crate::gossip::tcp::GossipTcpClient;
use crate::kvstore::{Get, KVStoreActor, Snapshot, Update, UpdateResult, VersionedValue};
use crate::ring::{ConsistentHashRing, RingNode};
use crate::vector_clock::VectorClock;

// Message types for the GossipActor
#[derive(Message)]
#[rtype(result = "Result<GossipMessage>")]
pub struct ProcessGossip {
    pub message: GossipMessage,
}

#[derive(Message)]
#[rtype(result = "Vec<String>")]
pub struct GetActivePeers;

#[derive(Message)]
#[rtype(result = "Vec<String>")]
pub struct GetInactivePeers;

#[derive(Message)]
#[rtype(result = "()")]
pub struct AddPeer {
    pub peer: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RemovePeer {
    pub peer: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct StartGossip;

// Quorum operation messages
#[derive(Message)]
#[rtype(result = "Result<QuorumReadResult>")]
pub struct QuorumRead {
    pub key: String,
}

#[derive(Clone, Debug)]
pub struct QuorumReadResult {
    pub key: String,
    pub value: Option<String>,
    pub found: bool,
    pub vector_clock: Option<VectorClock>,
    pub successful_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
}

#[derive(Message)]
#[rtype(result = "Result<QuorumWriteResult>")]
pub struct QuorumWrite {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct QuorumWriteResult {
    pub key: String,
    pub value: String,
    pub vector_clock: VectorClock,
    pub successful_nodes: Vec<String>,
    pub failed_nodes: Vec<String>,
}

#[derive(Clone, Debug)]
struct PendingQuorumOperation {
    request_id: String,
    key: String,
    successful_reads: Vec<VersionedValue>,
    successful_writes: Vec<String>,
    failed_nodes: Vec<String>,
    created_at: Instant,
}

pub struct GossipActor {
    config: Config,
    kvstore: Addr<KVStoreActor>,
    active_peers: HashSet<String>,
    inactive_peers: HashSet<String>,
    tcp_clients: HashMap<String, GossipTcpClient>,
    peer_failure_counts: HashMap<String, u32>,
    peer_last_attempt: HashMap<String, Instant>,
    max_failures: u32,
    backoff_base_ms: u64,
    hash_ring: ConsistentHashRing,
    pending_quorum_ops: HashMap<String, PendingQuorumOperation>,
}

impl GossipActor {
    pub fn new(
        config: Config,
        kvstore: Addr<KVStoreActor>,
        replication_factor: usize,
        read_quorum: usize,
        write_quorum: usize,
    ) -> Self {
        let mut active_peers = HashSet::new();
        let inactive_peers = HashSet::new();
        let mut tcp_clients = HashMap::new();
        let mut hash_ring = ConsistentHashRing::new(replication_factor, read_quorum, write_quorum);

        // Add self to the ring
        let self_node = RingNode::new(config.node_id.clone(), config.endpoint());
        hash_ring.add_node(self_node);

        for peer in &config.peers {
            active_peers.insert(peer.clone());
            tcp_clients.insert(peer.clone(), GossipTcpClient::new(peer.clone()));

            let ring_node = RingNode::new(peer.clone(), peer.clone());
            hash_ring.add_node(ring_node);
        }

        GossipActor {
            config: config.clone(),
            kvstore,
            active_peers,
            inactive_peers,
            tcp_clients,
            peer_failure_counts: HashMap::new(),
            peer_last_attempt: HashMap::new(),
            max_failures: 3,
            backoff_base_ms: 1000,
            hash_ring,
            pending_quorum_ops: HashMap::new(),
        }
    }

    // Determine if we should attempt to contact an inactive peer
    fn should_attempt_inactive_peer(&self, peer: &str) -> bool {
        let failure_count = self.peer_failure_counts.get(peer).copied().unwrap_or(0);
        let last_attempt = self
            .peer_last_attempt
            .get(peer)
            .copied()
            .unwrap_or_else(Instant::now);

        let backoff_ms = self.backoff_base_ms * (2_u64.pow(failure_count.min(10)));

        Instant::now().duration_since(last_attempt) > Duration::from_millis(backoff_ms)
    }

    // Get TCP client for a peer, creating one if needed
    fn get_client(&mut self, peer: &str) -> &mut GossipTcpClient {
        if !self.tcp_clients.contains_key(peer) {
            self.tcp_clients
                .insert(peer.to_string(), GossipTcpClient::new(peer.to_string()));
        }
        self.tcp_clients.get_mut(peer).unwrap()
    }

    // Clean up expired pending quorum operations
    fn cleanup_pending_operations(&mut self) {
        let now = Instant::now();
        let expired_ids: Vec<String> = self
            .pending_quorum_ops
            .iter()
            .filter(|(_, op)| now.duration_since(op.created_at) > Duration::from_secs(30))
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired_ids {
            self.pending_quorum_ops.remove(&id);
        }
    }

    // Get replica nodes for a key
    fn get_replicas_for_key(&self, key: &str) -> Vec<RingNode> {
        self.hash_ring.get_nodes_for_key(key)
    }

    // Handle a quorum read response message
    async fn handle_quorum_read_response(
        &mut self,
        key: String,
        value: Option<VersionedValue>,
        request_id: String,
        node_id: String,
    ) -> Result<()> {
        if let Some(op) = self.pending_quorum_ops.get_mut(&request_id) {
            if value.is_some() {
                op.successful_reads.push(value.unwrap());
            } else {
                op.failed_nodes.push(node_id);
            }
        }
        Ok(())
    }

    // Handle a quorum write response message
    async fn handle_quorum_write_response(
        &mut self,
        key: String,
        success: bool,
        request_id: String,
        node_id: String,
    ) -> Result<()> {
        if let Some(op) = self.pending_quorum_ops.get_mut(&request_id) {
            if success {
                op.successful_writes.push(node_id);
            } else {
                op.failed_nodes.push(node_id);
            }
        }
        Ok(())
    }

    // Merge vector clocks from multiple versioned values
    fn merge_vector_clocks(&self, values: &[VersionedValue]) -> VectorClock {
        let mut merged = VectorClock::new();
        for val in values {
            merged.merge(&val.vector_clock);
        }
        merged
    }

    fn check_inactive_peers(&mut self, ctx: &mut Context<Self>) {
        let inactive_peers: Vec<String> = self.inactive_peers.iter().cloned().collect();
        let addr = ctx.address();

        let mut peers_to_try = Vec::new();
        let mut client_futures = Vec::new();

        for peer in &inactive_peers {
            if self.should_attempt_inactive_peer(peer) {
                peers_to_try.push(peer.clone());
                self.peer_last_attempt.insert(peer.clone(), Instant::now());

                let client = self.get_client(peer).clone();
                let peer_clone = peer.clone();

                client_futures.push(async move {
                    let result = client.ping().await;
                    (peer_clone, result)
                });
            }
        }

        if !client_futures.is_empty() {
            async move {
                let results = join_all(client_futures).await;

                for (peer, result) in results {
                    match result {
                        Ok(true) => {
                            tracing::info!("Inactive peer {} is reachable again", peer);
                            let _ = addr.send(AddPeer { peer }).await;
                        }
                        _ => {
                            tracing::debug!("Inactive peer {} is still unreachable", peer);
                        }
                    }
                }
            }
            .into_actor(self)
            .spawn(ctx);
        }
    }

    // Ping all active peers
    fn ping_peers(&mut self, ctx: &mut Context<Self>) {
        let active_peers: Vec<String> = self.active_peers.iter().cloned().collect();
        let addr = ctx.address();

        if !active_peers.is_empty() {
            let mut client_futures = Vec::new();

            for peer in &active_peers {
                let client = self.get_client(peer).clone();
                let peer_clone = peer.clone();

                client_futures.push(async move {
                    let result = client.ping().await;
                    (peer_clone, result)
                });
            }

            async move {
                let results = join_all(client_futures).await;

                for (peer, result) in results {
                    match result {
                        Ok(true) => {
                            let _ = addr.send(AddPeer { peer }).await;
                        }
                        _ => {
                            let _ = addr.send(RemovePeer { peer }).await;
                        }
                    }
                }
            }
            .into_actor(self)
            .spawn(ctx);
        }
    }

    // Sync with a random peer
    fn sync_with_random_peer(&mut self, ctx: &mut Context<Self>) {
        if !self.active_peers.is_empty() {
            let mut rng = StdRng::from_os_rng();
            let active_peers: Vec<String> = self.active_peers.iter().cloned().collect();

            if let Some(peer) = active_peers.iter().choose(&mut rng) {
                let peer_str = peer.clone();
                let client = self.get_client(&peer_str).clone();
                let kvstore = self.kvstore.clone();
                let addr = ctx.address();

                async move {
                    let snapshot = match kvstore.send(Snapshot).await {
                        Ok(Ok(data)) => data,
                        _ => HashMap::new(),
                    };

                    let message = GossipMessage::Update { updates: snapshot };

                    match client.send_gossip(message).await {
                        Ok(_) => {
                            let _ = addr.send(AddPeer { peer: peer_str }).await;
                        }
                        Err(_) => {
                            let _ = addr
                                .send(RemovePeer {
                                    peer: peer_str.clone(),
                                })
                                .await;
                            tracing::warn!("Failed to sync with peer {}", peer_str);
                        }
                    }
                }
                .into_actor(self)
                .spawn(ctx);
            }
        }
    }
}

impl Actor for GossipActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |act, ctx| {
            act.ping_peers(ctx);
        });

        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            act.check_inactive_peers(ctx);
        });

        ctx.run_interval(Duration::from_secs(2), |act, ctx| {
            act.sync_with_random_peer(ctx);
        });

        ctx.run_interval(Duration::from_secs(5), |act, _ctx| {
            act.cleanup_pending_operations();
        });
    }
}

impl Handler<ProcessGossip> for GossipActor {
    type Result = ResponseActFuture<Self, Result<GossipMessage>>;

    fn handle(&mut self, msg: ProcessGossip, _ctx: &mut Context<Self>) -> Self::Result {
        let kvstore = self.kvstore.clone();
        let config = self.config.clone();
        let active_peers: Vec<String> = self.active_peers.iter().cloned().collect();

        Box::pin(
            async move {
                match msg.message {
                    GossipMessage::Update { updates } => {
                        for (key, value) in updates {
                            let _ = kvstore.send(Update { key, value }).await;
                        }

                        Ok(GossipMessage::Ping)
                    }

                    GossipMessage::SyncRequest => {
                        let data = match kvstore.send(Snapshot).await {
                            Ok(Ok(data)) => data,
                            _ => HashMap::new(),
                        };

                        Ok(GossipMessage::SyncResponse { data })
                    }

                    GossipMessage::SyncResponse { data } => {
                        for (key, value) in data {
                            let _ = kvstore.send(Update { key, value }).await;
                        }

                        Ok(GossipMessage::Ping)
                    }

                    GossipMessage::Ping => Ok(GossipMessage::Pong {
                        sender: config.endpoint(),
                        members: active_peers,
                    }),

                    GossipMessage::Pong {
                        sender: _,
                        members: _,
                    } => Ok(GossipMessage::Ping),

                    GossipMessage::QuorumRead { key, request_id } => {
                        let result = kvstore
                            .send(Get { key: key.clone() })
                            .await
                            .map_err(|e| anyhow!("Failed to get key: {}", e))?;

                        Ok(GossipMessage::QuorumReadResponse {
                            key,
                            value: result,
                            request_id,
                            node_id: config.node_id,
                        })
                    }

                    GossipMessage::QuorumReadResponse {
                        key,
                        value,
                        request_id,
                        node_id,
                    } => Ok(GossipMessage::Ping),

                    GossipMessage::QuorumWrite {
                        key,
                        value,
                        request_id,
                    } => {
                        let result = kvstore
                            .send(Update {
                                key: key.clone(),
                                value: value.clone(),
                            })
                            .await
                            .map_err(|e| anyhow!("Failed to update key: {}", e))?;

                        let success = match result {
                            UpdateResult::Updated(_) => true,
                            UpdateResult::Conflict(_, _) => false,
                            UpdateResult::NotUpdated(_) => false,
                        };

                        Ok(GossipMessage::QuorumWriteResponse {
                            key,
                            success,
                            value: Some(value),
                            request_id,
                            node_id: config.node_id,
                        })
                    }

                    GossipMessage::QuorumWriteResponse {
                        key,
                        success,
                        value: _,
                        request_id,
                        node_id,
                    } => Ok(GossipMessage::Ping),

                    GossipMessage::ConflictResolution {
                        key,
                        resolved_value,
                    } => {
                        let _ = kvstore
                            .send(Update {
                                key: key.clone(),
                                value: resolved_value.clone(),
                            })
                            .await;

                        Ok(GossipMessage::Ping)
                    }
                }
            }
            .into_actor(self),
        )
    }
}

impl Handler<GetActivePeers> for GossipActor {
    type Result = Vec<String>;

    fn handle(&mut self, _msg: GetActivePeers, _ctx: &mut Context<Self>) -> Self::Result {
        self.active_peers.iter().cloned().collect()
    }
}

impl Handler<GetInactivePeers> for GossipActor {
    type Result = Vec<String>;

    fn handle(&mut self, _msg: GetInactivePeers, _ctx: &mut Context<Self>) -> Self::Result {
        self.inactive_peers.iter().cloned().collect()
    }
}

impl Handler<AddPeer> for GossipActor {
    type Result = ();

    fn handle(&mut self, msg: AddPeer, _ctx: &mut Context<Self>) -> Self::Result {
        self.peer_failure_counts.insert(msg.peer.clone(), 0);

        self.inactive_peers.remove(&msg.peer);

        if !self.active_peers.insert(msg.peer.clone()) {
            return;
        }

        let ring_node = RingNode::new(msg.peer.clone(), msg.peer.clone());
        self.hash_ring.add_node(ring_node);
    }
}

impl Handler<RemovePeer> for GossipActor {
    type Result = ();

    fn handle(&mut self, msg: RemovePeer, _ctx: &mut Context<Self>) -> Self::Result {
        if self.active_peers.contains(&msg.peer) {
            let failure_count = self
                .peer_failure_counts
                .entry(msg.peer.clone())
                .or_insert(0);
            *failure_count += 1;

            if *failure_count >= self.max_failures {
                self.active_peers.remove(&msg.peer);
                self.inactive_peers.insert(msg.peer.clone());

                let ring_node = RingNode::new(msg.peer.clone(), msg.peer.clone());
                self.hash_ring.remove_node(&ring_node);

                tracing::info!(
                    "Peer {} moved to inactive after {} failures",
                    msg.peer,
                    failure_count
                );
            }
        }
    }
}

impl Handler<StartGossip> for GossipActor {
    type Result = ();

    fn handle(&mut self, _msg: StartGossip, _ctx: &mut Context<Self>) -> Self::Result {
        // No-op as gossip is started automatically in Actor::started
    }
}

impl Handler<QuorumRead> for GossipActor {
    type Result = ResponseActFuture<Self, Result<QuorumReadResult>>;

    fn handle(&mut self, msg: QuorumRead, _ctx: &mut Context<Self>) -> Self::Result {
        let key = msg.key.clone();
        let replicas = self.get_replicas_for_key(&key);
        let (_, mut read_quorum, _) = self.hash_ring.get_quorum_settings();
        let config = self.config.clone();
        let kvstore = self.kvstore.clone();

        let request_id = Uuid::new_v4().to_string();

        self.pending_quorum_ops.insert(
            request_id.clone(),
            PendingQuorumOperation {
                request_id: request_id.clone(),
                key: key.clone(),
                successful_reads: Vec::new(),
                successful_writes: Vec::new(),
                failed_nodes: Vec::new(),
                created_at: Instant::now(),
            },
        );

        let local_future = kvstore.send(Get { key: key.clone() });

        let mut client_futures = Vec::new();
        let mut client_nodes = Vec::new();

        // Prepare futures for remote reads
        for node in &replicas {
            // Skip the local node
            if node.node_id == config.node_id {
                continue;
            }

            if let Some(client) = self.tcp_clients.get(&node.endpoint) {
                let client = client.clone();
                let request_id = request_id.clone();
                let key = key.clone();
                let node_id = node.node_id.clone();

                client_nodes.push(node_id.clone());

                client_futures.push(async move {
                    let read_msg = GossipMessage::QuorumRead {
                        key: key.clone(),
                        request_id: request_id.clone(),
                    };

                    match client.send_gossip(read_msg).await {
                        Ok(GossipMessage::QuorumReadResponse {
                            key,
                            value,
                            request_id,
                            node_id,
                        }) => (node_id, key, value, true),
                        _ => (node_id, key, None, false),
                    }
                });
            }
        }

        let read_quorum = read_quorum;
        let node_id = config.node_id.clone();

        Box::pin(
            async move {
                let local_result = local_future
                    .await
                    .map_err(|e| anyhow!("Local read failed: {}", e))?;

                let remote_results = join_all(client_futures).await;

                let mut successful_nodes = Vec::new();
                let mut failed_nodes = Vec::new();
                let mut all_values = Vec::new();

                successful_nodes.push(node_id.clone());
                all_values.push(local_result);

                for (node_id, _key, value, success) in remote_results {
                    if success {
                        successful_nodes.push(node_id.clone());
                        all_values.push(value);
                    } else {
                        failed_nodes.push(node_id);
                    }
                }

                if successful_nodes.len() < read_quorum {
                    return Err(anyhow!(
                        "Failed to achieve read quorum ({}/{})",
                        successful_nodes.len(),
                        read_quorum
                    ));
                }

                // Find the most recent value based on vector clocks
                let most_recent = if !all_values.is_empty() {
                    all_values
                        .into_iter()
                        .filter(|a| a.is_some())
                        .map(|a| a.unwrap())
                        .reduce(|a, b| {
                            match a.vector_clock.compare(&b.vector_clock) {
                                Some(std::cmp::Ordering::Less) => b,
                                Some(std::cmp::Ordering::Greater) => a,
                                Some(std::cmp::Ordering::Equal) => {
                                    // If vector clocks are equal, use lexicographically greater node_id
                                    if a.node_id < b.node_id { b } else { a }
                                }
                                None => {
                                    // If there's a conflict, merge the vector clocks and use the one with
                                    // the lexicographically greater node_id
                                    if a.node_id < b.node_id { b } else { a }
                                }
                            }
                        })
                } else {
                    None
                };

                let result = QuorumReadResult {
                    key: key.clone(),
                    value: most_recent.as_ref().map(|v| v.value.clone()),
                    found: most_recent.is_some(),
                    vector_clock: most_recent.as_ref().map(|v| v.vector_clock.clone()),
                    successful_nodes,
                    failed_nodes,
                };

                Ok(result)
            }
            .into_actor(self),
        )
    }
}

impl Handler<QuorumWrite> for GossipActor {
    type Result = ResponseActFuture<Self, Result<QuorumWriteResult>>;

    fn handle(&mut self, msg: QuorumWrite, _ctx: &mut Context<Self>) -> Self::Result {
        let key = msg.key.clone();
        let value = msg.value.clone();
        let replicas = self.get_replicas_for_key(&key);
        let (_, _, write_quorum) = self.hash_ring.get_quorum_settings();
        let config = self.config.clone();
        let kvstore = self.kvstore.clone();
        let node_id = config.node_id.clone();

        let request_id = Uuid::new_v4().to_string();

        self.pending_quorum_ops.insert(
            request_id.clone(),
            PendingQuorumOperation {
                request_id: request_id.clone(),
                key: key.clone(),
                successful_reads: Vec::new(),
                successful_writes: Vec::new(),
                failed_nodes: Vec::new(),
                created_at: Instant::now(),
            },
        );

        let read_future = kvstore.send(Get { key: key.clone() });

        let remote_nodes: Vec<(String, String, GossipTcpClient)> = replicas
            .iter()
            .filter(|node| node.node_id != node_id)
            .filter_map(|node| {
                if let Some(client) = self.tcp_clients.get(&node.endpoint) {
                    Some((node.node_id.clone(), node.endpoint.clone(), client.clone()))
                } else {
                    None
                }
            })
            .collect();

        let write_quorum = write_quorum;

        Box::pin(
            async move {
                let current = read_future
                    .await
                    .map_err(|e| anyhow!("Failed to read current value: {}", e))?;

                let mut vector_clock = VectorClock::new();
                if let Some(current_value) = &current {
                    vector_clock = current_value.vector_clock.clone();
                }

                vector_clock.increment(&node_id);

                let versioned_value = VersionedValue {
                    value: value.clone(),
                    vector_clock: vector_clock.clone(),
                    node_id: node_id.clone(),
                };

                let local_future = kvstore.send(Update {
                    key: key.clone(),
                    value: versioned_value.clone(),
                });

                let mut client_futures: Vec<
                    futures::future::BoxFuture<'static, (String, String, bool)>,
                > = Vec::new();

                for (remote_node_id, _endpoint, client) in remote_nodes {
                    let request_id = request_id.clone();
                    let key = key.clone();
                    let value = versioned_value.clone();
                    let node_id = remote_node_id.clone();

                    client_futures.push(Box::pin(async move {
                        let write_msg = GossipMessage::QuorumWrite {
                            key: key.clone(),
                            value,
                            request_id: request_id.clone(),
                        };

                        match client.send_gossip(write_msg).await {
                            Ok(GossipMessage::QuorumWriteResponse {
                                key,
                                success,
                                value: _,
                                request_id: _,
                                node_id: response_node_id,
                            }) => (response_node_id, key, success),
                            _ => (node_id, key, false),
                        }
                    }));
                }

                let local_result = local_future
                    .await
                    .map_err(|e| anyhow!("Local write failed: {}", e))?;

                let remote_results = join_all(client_futures).await;

                let mut successful_nodes = Vec::new();
                let mut failed_nodes = Vec::new();

                match local_result {
                    UpdateResult::Updated(_) => {
                        successful_nodes.push(node_id.clone());
                    }
                    _ => {
                        failed_nodes.push(node_id.clone());
                    }
                }

                for (node_id, _key, success) in remote_results {
                    if success {
                        successful_nodes.push(node_id);
                    } else {
                        failed_nodes.push(node_id);
                    }
                }

                if successful_nodes.len() < write_quorum {
                    return Err(anyhow!(
                        "Failed to achieve write quorum ({}/{})",
                        successful_nodes.len(),
                        write_quorum
                    ));
                }

                let result = QuorumWriteResult {
                    key,
                    value,
                    vector_clock,
                    successful_nodes,
                    failed_nodes,
                };

                Ok(result)
            }
            .into_actor(self),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::gossip::GossipMessage;
    use crate::kvstore::{Get, KVStoreActor, Set, VersionedValue};
    use actix::Actor;
    use rand::Rng;
    use std::collections::HashSet;
    use std::time::{Duration, Instant};
    use tokio::time::sleep;
    use uuid::Uuid;

    // Helper function to create a test config
    fn create_test_config(node_id: &str, port: u16) -> Config {
        let mut peers = HashSet::new();
        for i in 1..4 {
            let peer_id = format!("node-{}", i);
            if peer_id != node_id {
                peers.insert(format!("{}@127.0.0.1:{}", peer_id, 8000 + i));
            }
        }

        Config {
            node_id: node_id.to_string(),
            host: "127.0.0.1".to_string(),
            port,
            peers,
            replication_factor: 3,
            read_quorum: 2,
            write_quorum: 2,
        }
    }

    #[actix_rt::test]
    async fn test_gossip_actor_initialization() {
        let config = create_test_config("node-test", 8000);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 3);

        let inactive_peers = gossip_addr.send(GetInactivePeers).await.unwrap();
        assert_eq!(inactive_peers.len(), 0);
    }

    #[actix_rt::test]
    async fn test_add_remove_peers() {
        let config = create_test_config("node-test", 8001);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let new_peer = "node-new@127.0.0.1:9000";
        gossip_addr
            .send(AddPeer {
                peer: new_peer.to_string(),
            })
            .await
            .unwrap();

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        assert!(active_peers.contains(&new_peer.to_string()));

        gossip_addr
            .send(RemovePeer {
                peer: new_peer.to_string(),
            })
            .await
            .unwrap();

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        assert!(active_peers.contains(&new_peer.to_string()));

        for _ in 0..4 {
            gossip_addr
                .send(RemovePeer {
                    peer: new_peer.to_string(),
                })
                .await
                .unwrap();
        }

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        assert!(!active_peers.contains(&new_peer.to_string()));

        let inactive_peers = gossip_addr.send(GetInactivePeers).await.unwrap();
        assert!(inactive_peers.contains(&new_peer.to_string()));
    }

    #[actix_rt::test]
    async fn test_process_ping() {
        let config = create_test_config("node-test", 8002);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::Ping,
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Pong { sender, members }) => {
                assert_eq!(sender, "127.0.0.1:8002");
                assert_eq!(members.len(), 3); // Should have 3 peers
            }
            _ => panic!("Expected Pong message"),
        }
    }

    #[actix_rt::test]
    async fn test_process_update() {
        let config = create_test_config("node-test", 8003);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let mut vc = VectorClock::new();
        vc.increment("node-test");

        let versioned_value = VersionedValue {
            value: "test-value".to_string(),
            vector_clock: vc,
            node_id: "node-test".to_string(),
        };

        let mut updates = HashMap::new();
        updates.insert("test-key".to_string(), versioned_value);

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::Update { updates },
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Ping) => {}
            _ => panic!("Expected Ping message"),
        }

        let value = kvstore_addr
            .send(Get {
                key: "test-key".to_string(),
            })
            .await
            .unwrap();

        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.value, "test-value");
    }

    #[actix_rt::test]
    async fn test_sync_request_response() {
        let config = create_test_config("node-test", 8004);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let _ = kvstore_addr
            .send(Set {
                key: "sync-key".to_string(),
                value: "sync-value".to_string(),
                node_id: "node-test".to_string(),
            })
            .await
            .unwrap();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::SyncRequest,
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::SyncResponse { data }) => {
                assert!(data.contains_key("sync-key"));
                let value = data.get("sync-key").unwrap();
                assert_eq!(value.value, "sync-value");
            }
            _ => panic!("Expected SyncResponse message"),
        }

        let mut vc = VectorClock::new();
        vc.increment("other-node");

        let versioned_value = VersionedValue {
            value: "response-value".to_string(),
            vector_clock: vc,
            node_id: "other-node".to_string(),
        };

        let mut data = HashMap::new();
        data.insert("response-key".to_string(), versioned_value);

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::SyncResponse { data },
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Ping) => {}
            _ => panic!("Expected Ping message"),
        }

        let value = kvstore_addr
            .send(Get {
                key: "response-key".to_string(),
            })
            .await
            .unwrap();

        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.value, "response-value");
    }

    #[actix_rt::test]
    async fn test_quorum_read() {
        let config = create_test_config("node-test", 8005);

        let mut config = config.clone();
        config.replication_factor = 1;
        config.read_quorum = 1;
        config.write_quorum = 1;
        config.peers = HashSet::new();

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let _ = kvstore_addr
            .send(Set {
                key: "quorum-key".to_string(),
                value: "quorum-value".to_string(),
                node_id: "node-test".to_string(),
            })
            .await
            .unwrap();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let result = gossip_addr
            .send(QuorumRead {
                key: "quorum-key".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_ok());
        let read_result = result.unwrap();
        assert_eq!(read_result.key, "quorum-key");
        assert_eq!(read_result.value, Some("quorum-value".to_string()));
        assert!(read_result.found);
        assert!(
            read_result
                .successful_nodes
                .contains(&"node-test".to_string())
        );
        assert_eq!(read_result.failed_nodes.len(), 0);
    }

    #[actix_rt::test]
    async fn test_quorum_write() {
        let config = create_test_config("node-test", 8006);

        let mut config = config.clone();
        config.replication_factor = 1;
        config.read_quorum = 1;
        config.write_quorum = 1;
        config.peers = HashSet::new();

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let result = gossip_addr
            .send(QuorumWrite {
                key: "quorum-write-key".to_string(),
                value: "quorum-write-value".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_ok());
        let write_result = result.unwrap();
        assert_eq!(write_result.key, "quorum-write-key");
        assert_eq!(write_result.value, "quorum-write-value");
        assert!(
            write_result
                .successful_nodes
                .contains(&"node-test".to_string())
        );
        assert_eq!(write_result.failed_nodes.len(), 0);

        let value = kvstore_addr
            .send(Get {
                key: "quorum-write-key".to_string(),
            })
            .await
            .unwrap();

        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.value, "quorum-write-value");
    }

    #[actix_rt::test]
    async fn test_process_quorum_read_message() {
        let config = create_test_config("node-test", 8007);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let _ = kvstore_addr
            .send(Set {
                key: "read-key".to_string(),
                value: "read-value".to_string(),
                node_id: "node-test".to_string(),
            })
            .await
            .unwrap();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::QuorumRead {
                    key: "read-key".to_string(),
                    request_id: "test-id".to_string(),
                },
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::QuorumReadResponse {
                key,
                value,
                request_id,
                node_id,
            }) => {
                assert_eq!(key, "read-key");
                assert!(value.is_some());
                let value = value.unwrap();
                assert_eq!(value.value, "read-value");
                assert_eq!(request_id, "test-id");
                assert_eq!(node_id, "node-test");
            }
            _ => panic!("Expected QuorumReadResponse message"),
        }
    }

    #[actix_rt::test]
    async fn test_process_quorum_write_message() {
        let config = create_test_config("node-test", 8008);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let mut vc = VectorClock::new();
        vc.increment("sender-node");

        let versioned_value = VersionedValue {
            value: "write-value".to_string(),
            vector_clock: vc,
            node_id: "sender-node".to_string(),
        };

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::QuorumWrite {
                    key: "write-key".to_string(),
                    value: versioned_value,
                    request_id: "test-id".to_string(),
                },
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::QuorumWriteResponse {
                key,
                success,
                value: _,
                request_id,
                node_id,
            }) => {
                assert_eq!(key, "write-key");
                assert!(success);
                assert_eq!(request_id, "test-id");
                assert_eq!(node_id, "node-test");
            }
            _ => panic!("Expected QuorumWriteResponse message"),
        }

        let value = kvstore_addr
            .send(Get {
                key: "write-key".to_string(),
            })
            .await
            .unwrap();

        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.value, "write-value");
    }

    #[actix_rt::test]
    async fn test_conflict_resolution_behavior() {
        let config = create_test_config("node-test", 8009);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let mut base_vc = VectorClock::new();
        base_vc.increment("base-node");

        let base_value = VersionedValue {
            value: "base-value".to_string(),
            vector_clock: base_vc.clone(),
            node_id: "base-node".to_string(),
        };

        let _ = kvstore_addr
            .send(Update {
                key: "conflict-key".to_string(),
                value: base_value,
            })
            .await
            .unwrap();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let mut conflict_vc = VectorClock::new();
        conflict_vc.increment("base-node");
        conflict_vc.increment("base-node");

        let conflict_value = VersionedValue {
            value: "conflict-value".to_string(),
            vector_clock: conflict_vc,
            node_id: "conflict-node".to_string(),
        };

        let result = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::ConflictResolution {
                    key: "conflict-key".to_string(),
                    resolved_value: conflict_value.clone(),
                },
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Ping) => {}
            _ => panic!("Expected Ping message"),
        }

        let value = kvstore_addr
            .send(Get {
                key: "conflict-key".to_string(),
            })
            .await
            .unwrap();

        assert!(value.is_some());
        let value = value.unwrap();
        assert_eq!(value.value, "conflict-value");
    }

    #[actix_rt::test]
    async fn test_quorum_failure() {
        let config = create_test_config("node-test", 8010);

        let mut config = config.clone();
        config.replication_factor = 5;
        config.read_quorum = 5;
        config.write_quorum = 5;
        config.peers = HashSet::new(); // No peers, so we can't meet quorum

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let result = gossip_addr
            .send(QuorumRead {
                key: "quorum-key".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to achieve read quorum")
        );

        let result = gossip_addr
            .send(QuorumWrite {
                key: "quorum-key".to_string(),
                value: "quorum-value".to_string(),
            })
            .await
            .unwrap();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to achieve write quorum")
        );
    }

    #[actix_rt::test]
    async fn test_handle_quorum_read_response() {
        let config = create_test_config("node-test", 8011);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let mut gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );

        let request_id = "test-request".to_string();
        gossip.pending_quorum_ops.insert(
            request_id.clone(),
            PendingQuorumOperation {
                request_id: request_id.clone(),
                key: "test-key".to_string(),
                successful_reads: Vec::new(),
                successful_writes: Vec::new(),
                failed_nodes: Vec::new(),
                created_at: Instant::now(),
            },
        );

        let mut vc = VectorClock::new();
        vc.increment("test-node");

        let value = VersionedValue {
            value: "test-value".to_string(),
            vector_clock: vc,
            node_id: "test-node".to_string(),
        };

        let _ = gossip
            .handle_quorum_read_response(
                "test-key".to_string(),
                Some(value),
                request_id.clone(),
                "test-node".to_string(),
            )
            .await;

        let op = gossip.pending_quorum_ops.get(&request_id).unwrap();
        assert_eq!(op.successful_reads.len(), 1);

        let _ = gossip
            .handle_quorum_read_response(
                "test-key".to_string(),
                None,
                request_id.clone(),
                "failed-node".to_string(),
            )
            .await;

        let op = gossip.pending_quorum_ops.get(&request_id).unwrap();
        assert_eq!(op.failed_nodes.len(), 1);
        assert_eq!(op.failed_nodes[0], "failed-node");
    }

    #[actix_rt::test]
    async fn test_handle_quorum_write_response() {
        let config = create_test_config("node-test", 8012);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let mut gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );

        let request_id = "test-request".to_string();
        gossip.pending_quorum_ops.insert(
            request_id.clone(),
            PendingQuorumOperation {
                request_id: request_id.clone(),
                key: "test-key".to_string(),
                successful_reads: Vec::new(),
                successful_writes: Vec::new(),
                failed_nodes: Vec::new(),
                created_at: Instant::now(),
            },
        );

        let _ = gossip
            .handle_quorum_write_response(
                "test-key".to_string(),
                true,
                request_id.clone(),
                "success-node".to_string(),
            )
            .await;

        let op = gossip.pending_quorum_ops.get(&request_id).unwrap();
        assert_eq!(op.successful_writes.len(), 1);
        assert_eq!(op.successful_writes[0], "success-node");

        let _ = gossip
            .handle_quorum_write_response(
                "test-key".to_string(),
                false,
                request_id.clone(),
                "failed-node".to_string(),
            )
            .await;

        let op = gossip.pending_quorum_ops.get(&request_id).unwrap();
        assert_eq!(op.failed_nodes.len(), 1);
        assert_eq!(op.failed_nodes[0], "failed-node");
    }

    #[actix_rt::test]
    async fn test_cleanup_pending_operations() {
        let config = create_test_config("node-test", 8013);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let mut gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );

        let old_request_id = "old-request".to_string();
        gossip.pending_quorum_ops.insert(
            old_request_id.clone(),
            PendingQuorumOperation {
                request_id: old_request_id.clone(),
                key: "test-key".to_string(),
                successful_reads: Vec::new(),
                successful_writes: Vec::new(),
                failed_nodes: Vec::new(),
                created_at: Instant::now() - Duration::from_secs(31), // 31 seconds old (expired)
            },
        );

        let new_request_id = "new-request".to_string();
        gossip.pending_quorum_ops.insert(
            new_request_id.clone(),
            PendingQuorumOperation {
                request_id: new_request_id.clone(),
                key: "test-key".to_string(),
                successful_reads: Vec::new(),
                successful_writes: Vec::new(),
                failed_nodes: Vec::new(),
                created_at: Instant::now() - Duration::from_secs(5), // 5 seconds old (not expired)
            },
        );

        gossip.cleanup_pending_operations();

        assert!(!gossip.pending_quorum_ops.contains_key(&old_request_id));
        assert!(gossip.pending_quorum_ops.contains_key(&new_request_id));
    }

    #[actix_rt::test]
    async fn test_merge_vector_clocks() {
        let config = create_test_config("node-test", 8014);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );

        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        let value1 = VersionedValue {
            value: "value1".to_string(),
            vector_clock: vc1,
            node_id: "node1".to_string(),
        };

        let mut vc2 = VectorClock::new();
        vc2.increment("node2");
        vc2.increment("node2");
        let value2 = VersionedValue {
            value: "value2".to_string(),
            vector_clock: vc2,
            node_id: "node2".to_string(),
        };

        let mut vc3 = VectorClock::new();
        vc3.increment("node3");
        vc3.increment("node1");
        let value3 = VersionedValue {
            value: "value3".to_string(),
            vector_clock: vc3,
            node_id: "node3".to_string(),
        };

        let values = vec![value1, value2, value3];
        let merged = gossip.merge_vector_clocks(&values);

        assert_eq!(*merged.counters.get("node1").unwrap(), 1);
        assert_eq!(*merged.counters.get("node2").unwrap(), 2);
        assert_eq!(*merged.counters.get("node3").unwrap(), 1);
    }

    #[actix_rt::test]
    async fn test_quorum_read_write_end_to_end() {
        let config = create_test_config("node-test", 8015);

        let mut config = config.clone();
        config.replication_factor = 1;
        config.read_quorum = 1;
        config.write_quorum = 1;
        config.peers = HashSet::new();

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let write_result = gossip_addr
            .send(QuorumWrite {
                key: "e2e-key".to_string(),
                value: "e2e-value".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(write_result.key, "e2e-key");
        assert_eq!(write_result.value, "e2e-value");
        assert!(
            write_result
                .successful_nodes
                .contains(&"node-test".to_string())
        );

        let read_result = gossip_addr
            .send(QuorumRead {
                key: "e2e-key".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_result.key, "e2e-key");
        assert_eq!(read_result.value, Some("e2e-value".to_string()));
        assert!(read_result.found);
        assert!(
            read_result
                .successful_nodes
                .contains(&"node-test".to_string())
        );

        let write_result2 = gossip_addr
            .send(QuorumWrite {
                key: "e2e-key".to_string(),
                value: "e2e-value-updated".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(write_result2.key, "e2e-key");
        assert_eq!(write_result2.value, "e2e-value-updated");

        assert!(
            write_result2
                .vector_clock
                .counters
                .get("node-test")
                .unwrap()
                > write_result.vector_clock.counters.get("node-test").unwrap()
        );

        let read_result2 = gossip_addr
            .send(QuorumRead {
                key: "e2e-key".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_result2.key, "e2e-key");
        assert_eq!(read_result2.value, Some("e2e-value-updated".to_string()));
    }

    #[actix_rt::test]
    async fn test_multiple_concurrent_quorum_operations() {
        let config = create_test_config("node-test", 8016);

        let mut config = config.clone();
        config.replication_factor = 1;
        config.read_quorum = 1;
        config.write_quorum = 1;
        config.peers = HashSet::new();

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let num_ops = 100;
        let mut write_futures = Vec::new();

        for i in 0..num_ops {
            let key = format!("concurrent-key-{}", i);
            let value = format!("concurrent-value-{}", i);
            let future = gossip_addr.send(QuorumWrite { key, value });
            write_futures.push(future);
        }

        let write_results = futures::future::join_all(write_futures).await;

        for result in write_results {
            assert!(result.is_ok());
            let inner_result = result.unwrap();
            assert!(inner_result.is_ok());
        }

        let mut read_futures = Vec::new();

        for i in 0..num_ops {
            let key = format!("concurrent-key-{}", i);
            let future = gossip_addr.send(QuorumRead { key });
            read_futures.push(future);
        }

        let read_results = futures::future::join_all(read_futures).await;

        for (i, result) in read_results.into_iter().enumerate() {
            assert!(result.is_ok());
            let inner_result = result.unwrap();
            assert!(inner_result.is_ok());

            let read_result = inner_result.unwrap();
            let expected_value = format!("concurrent-value-{}", i);
            assert_eq!(read_result.value, Some(expected_value));
        }
    }

    #[actix_rt::test]
    async fn test_get_client() {
        let config = create_test_config("node-test", 8018);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let mut gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );

        let new_peer = "new-peer@127.0.0.1:9001";
        let client = gossip.get_client(new_peer);

        assert!(gossip.tcp_clients.contains_key(new_peer));

        let client2 = gossip.get_client(new_peer);

        assert_eq!(gossip.tcp_clients.len(), 4); // 3 initial peers + new peer
    }

    #[actix_rt::test]
    async fn test_check_inactive_peers() {
        let config = create_test_config("node-test", 8019);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let inactive_peer = "inactive@127.0.0.1:9002";
        gossip_addr
            .send(AddPeer {
                peer: inactive_peer.to_string(),
            })
            .await
            .unwrap();

        for _ in 0..4 {
            gossip_addr
                .send(RemovePeer {
                    peer: inactive_peer.to_string(),
                })
                .await
                .unwrap();
        }

        let inactive_peers = gossip_addr.send(GetInactivePeers).await.unwrap();
        assert!(inactive_peers.contains(&inactive_peer.to_string()));
    }

    #[actix_rt::test]
    async fn test_ping_peers() {
        let config = create_test_config("node-test", 8020);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 3);
    }

    #[actix_rt::test]
    async fn test_vector_clock_ordering_conflict_handling() {
        let config = create_test_config("node-test", 8022);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let _ = kvstore_addr
            .send(Set {
                key: "order-key".to_string(),
                value: "base".to_string(),
                node_id: "common".to_string(),
            })
            .await
            .unwrap();

        let current = kvstore_addr
            .send(Get {
                key: "order-key".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        let mut successor_vc = current.vector_clock.clone();
        successor_vc.increment("successor");
        successor_vc.increment("node-test");

        let successor_value = VersionedValue {
            value: "successor".to_string(),
            vector_clock: successor_vc,
            node_id: "successor".to_string(),
        };

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let mut updates = HashMap::new();
        updates.insert("order-key".to_string(), successor_value.clone());

        let _ = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::Update { updates },
            })
            .await
            .unwrap();

        let value = kvstore_addr
            .send(Get {
                key: "order-key".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(value.value, "successor");

        let mut concurrent_vc = current.vector_clock.clone();
        concurrent_vc.increment("concurrent");

        let concurrent_value = VersionedValue {
            value: "concurrent".to_string(),
            vector_clock: concurrent_vc,
            node_id: "concurrent".to_string(),
        };

        let mut updates = HashMap::new();
        updates.insert("order-key".to_string(), concurrent_value.clone());

        let _ = gossip_addr
            .send(ProcessGossip {
                message: GossipMessage::Update { updates },
            })
            .await
            .unwrap();

        let value = kvstore_addr
            .send(Get {
                key: "order-key".to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(value.value, "successor");
    }

    #[actix_rt::test]
    async fn test_node_failure_recovery() {
        let config = create_test_config("node-test", 8023);
        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let failing_node = "failing-node@127.0.0.1:9999";
        gossip_addr
            .send(AddPeer {
                peer: failing_node.to_string(),
            })
            .await
            .unwrap();

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        assert!(active_peers.contains(&failing_node.to_string()));

        for _ in 0..4 {
            gossip_addr
                .send(RemovePeer {
                    peer: failing_node.to_string(),
                })
                .await
                .unwrap();
        }

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        let inactive_peers = gossip_addr.send(GetInactivePeers).await.unwrap();
        assert!(!active_peers.contains(&failing_node.to_string()));
        assert!(inactive_peers.contains(&failing_node.to_string()));

        gossip_addr
            .send(AddPeer {
                peer: failing_node.to_string(),
            })
            .await
            .unwrap();

        let active_peers = gossip_addr.send(GetActivePeers).await.unwrap();
        let inactive_peers = gossip_addr.send(GetInactivePeers).await.unwrap();
        assert!(active_peers.contains(&failing_node.to_string()));
        assert!(!inactive_peers.contains(&failing_node.to_string()));
    }

    #[actix_rt::test]
    async fn test_stress_quorum_operations() {
        let config = create_test_config("node-test", 8024);

        let mut config = config.clone();
        config.replication_factor = 1;
        config.read_quorum = 1;
        config.write_quorum = 1;
        config.peers = HashSet::new();

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let num_ops = 100;
        let mut futures = Vec::new();

        for i in 0..num_ops {
            let key = format!("stress-key-{}", i);
            let value = format!("stress-value-{}", i);
            let gossip_addr_clone = gossip_addr.clone();

            let write_future = gossip_addr_clone.send(QuorumWrite {
                key: key.clone(),
                value: value.clone(),
            });

            let gossip_addr_for_read = gossip_addr.clone();

            let read_future = async move {
                let write_result = write_future.await.unwrap().unwrap();
                assert_eq!(write_result.value, value);

                sleep(Duration::from_millis(10)).await;

                let read_result = gossip_addr_for_read
                    .send(QuorumRead { key: key.clone() })
                    .await
                    .unwrap()
                    .unwrap();

                assert_eq!(read_result.value, Some(value));
            };

            futures.push(read_future);
        }

        futures::future::join_all(futures).await;
    }

    #[actix_rt::test]
    async fn test_random_operations_sequence() {
        let config = create_test_config("node-test", 8025);

        let mut config = config.clone();
        config.replication_factor = 1;
        config.read_quorum = 1;
        config.write_quorum = 1;
        config.peers = HashSet::new();

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let num_keys = 50;
        let num_ops = 100;

        let mut rng = StdRng::from_os_rng();

        for _ in 0..num_ops {
            let key_idx = rng.random_range(0..num_keys);
            let key = format!("random-key-{}", key_idx);

            let op_type = rng.random_range(0..3);

            match op_type {
                0 => {
                    let value = format!("value-{}", Uuid::new_v4());
                    let _ = gossip_addr
                        .send(QuorumWrite {
                            key: key.clone(),
                            value: value.clone(),
                        })
                        .await;
                }
                1 => {
                    let _ = gossip_addr.send(QuorumRead { key: key.clone() }).await;
                }
                2 => {
                    let mut vc = VectorClock::new();
                    vc.increment("random-node");

                    let versioned_value = VersionedValue {
                        value: format!("gossip-value-{}", Uuid::new_v4()),
                        vector_clock: vc,
                        node_id: "random-node".to_string(),
                    };

                    let mut updates = HashMap::new();
                    updates.insert(key, versioned_value);

                    let _ = gossip_addr
                        .send(ProcessGossip {
                            message: GossipMessage::Update { updates },
                        })
                        .await;
                }
                _ => unreachable!(),
            }
        }

        // The test passes if no panics occur
    }
}
