use actix::dev::ContextFutureSpawner;
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, ResponseActFuture, WrapFuture};
use anyhow::Result;
use futures::future::join_all;
use hashbrown::HashMap;
use hashbrown::HashSet;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::gossip::GossipMessage;
use crate::gossip::tcp::GossipTcpClient;
use crate::kvstore::{KVStoreActor, Snapshot, Update};

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

// The GossipActor processing gossip-related messages
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
}

impl GossipActor {
    pub fn new(config: Config, kvstore: Addr<KVStoreActor>) -> Self {
        let mut active_peers = HashSet::new();
        let mut tcp_clients = HashMap::new();

        for peer in &config.peers {
            active_peers.insert(peer.clone());
            tcp_clients.insert(peer.clone(), GossipTcpClient::new(peer.clone()));
        }

        GossipActor {
            config: config.clone(),
            kvstore,
            active_peers,
            inactive_peers: HashSet::new(),
            tcp_clients,
            peer_failure_counts: HashMap::new(),
            peer_last_attempt: HashMap::new(),
            max_failures: 3,
            backoff_base_ms: 1000,
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
}

impl Actor for GossipActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Schedule periodic ping to peers
        ctx.run_interval(Duration::from_secs(1), |act, ctx| {
            act.ping_peers(ctx);
        });

        // Schedule periodic check for inactive peers
        ctx.run_interval(Duration::from_secs(5), |act, ctx| {
            act.check_inactive_peers(ctx);
        });

        // Schedule periodic sync with random peer
        ctx.run_interval(Duration::from_secs(2), |act, ctx| {
            act.sync_with_random_peer(ctx);
        });
    }
}

impl GossipActor {
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
                    } => {
                        // Learn about new peers from pong messages
                        // This would ideally be handled in a cleaner way, perhaps
                        // through a PeerDiscovery message

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
        self.active_peers.insert(msg.peer);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::gossip::GossipMessage;
    use crate::gossip::actor::{
        AddPeer, GetActivePeers, GetInactivePeers, GossipActor, ProcessGossip, RemovePeer,
        StartGossip,
    };
    use crate::kvstore::{Get, KVStoreActor, VersionedValue};
    use actix::Actor;
    use actix_rt::test as actix_test;
    use hashbrown::HashMap;

    // Helper function to create a test config
    fn create_test_config(node_id: &str, port: u16, peers: Vec<String>) -> Config {
        Config {
            node_id: node_id.to_string(),
            host: "127.0.0.1".to_string(),
            port,
            peers: peers.into_iter().collect(),
        }
    }

    #[actix_test]
    async fn test_gossip_actor_initialization() {
        let peers = vec!["127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string()];
        let config = create_test_config("test-node", 8000, peers);

        let kvstore = KVStoreActor::new().start();

        let gossip = GossipActor::new(config.clone(), kvstore).start();

        let active_peers = gossip.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 2);
        assert!(active_peers.contains(&"127.0.0.1:8001".to_string()));
        assert!(active_peers.contains(&"127.0.0.1:8002".to_string()));

        let inactive_peers = gossip.send(GetInactivePeers).await.unwrap();
        assert_eq!(inactive_peers.len(), 0);
    }

    #[actix_test]
    async fn test_add_remove_peers() {
        let config = create_test_config("test-node", 8000, vec![]);

        let kvstore = KVStoreActor::new().start();

        let gossip = GossipActor::new(config.clone(), kvstore).start();

        let active_peers = gossip.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 0);

        // Add a peer
        // Make this more robust?
        gossip
            .send(AddPeer {
                peer: "127.0.0.1:8001".to_string(),
            })
            .await
            .unwrap();

        let active_peers = gossip.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 1);
        assert!(active_peers.contains(&"127.0.0.1:8001".to_string()));

        gossip
            .send(AddPeer {
                peer: "127.0.0.1:8002".to_string(),
            })
            .await
            .unwrap();

        let active_peers = gossip.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 2);
        assert!(active_peers.contains(&"127.0.0.1:8001".to_string()));
        assert!(active_peers.contains(&"127.0.0.1:8002".to_string()));

        for _ in 0..4 {
            gossip
                .send(RemovePeer {
                    peer: "127.0.0.1:8001".to_string(),
                })
                .await
                .unwrap();
        }

        let active_peers = gossip.send(GetActivePeers).await.unwrap();
        assert_eq!(active_peers.len(), 1);
        assert!(active_peers.contains(&"127.0.0.1:8002".to_string()));

        let inactive_peers = gossip.send(GetInactivePeers).await.unwrap();
        assert_eq!(inactive_peers.len(), 1);
        assert!(inactive_peers.contains(&"127.0.0.1:8001".to_string()));
    }

    #[actix_test]
    async fn test_process_gossip_update() {
        let config = create_test_config("test-node", 8000, vec![]);

        let kvstore = KVStoreActor::new().start();

        let init_value = VersionedValue {
            value: "initial-value".to_string(),
            timestamp: 1000,
            node_id: "node1".to_string(),
        };

        let update_msg = Update {
            key: "test-key".to_string(),
            value: init_value,
        };

        let updated = kvstore.send(update_msg).await.unwrap();
        assert!(updated);

        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let mut updates = HashMap::new();
        updates.insert(
            "test-key".to_string(),
            VersionedValue {
                value: "updated-value".to_string(),
                timestamp: 2000,
                node_id: "node2".to_string(),
            },
        );

        let gossip_msg = GossipMessage::Update { updates };

        let result = gossip
            .send(ProcessGossip {
                message: gossip_msg,
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Ping) => (),
            _ => panic!("Expected Ping message"),
        }

        let get_msg = Get {
            key: "test-key".to_string(),
        };

        let value = kvstore.send(get_msg).await.unwrap();
        assert!(value.is_some());
        let versioned = value.unwrap();
        assert_eq!(versioned.value, "updated-value");
        assert_eq!(versioned.timestamp, 2000);
        assert_eq!(versioned.node_id, "node2");
    }

    #[actix_test]
    async fn test_process_gossip_sync_request() {
        let config = create_test_config("test-node", 8000, vec![]);

        let kvstore = KVStoreActor::new().start();

        let value1 = VersionedValue {
            value: "value1".to_string(),
            timestamp: 1000,
            node_id: "node1".to_string(),
        };

        let value2 = VersionedValue {
            value: "value2".to_string(),
            timestamp: 2000,
            node_id: "node2".to_string(),
        };

        let _ = kvstore
            .send(Update {
                key: "key1".to_string(),
                value: value1.clone(),
            })
            .await
            .unwrap();

        let _ = kvstore
            .send(Update {
                key: "key2".to_string(),
                value: value2.clone(),
            })
            .await
            .unwrap();

        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let result = gossip
            .send(ProcessGossip {
                message: GossipMessage::SyncRequest,
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::SyncResponse { data }) => {
                assert_eq!(data.len(), 2);
                assert!(data.contains_key("key1"));
                assert!(data.contains_key("key2"));
                assert_eq!(data.get("key1").unwrap().value, "value1");
                assert_eq!(data.get("key2").unwrap().value, "value2");
            }
            _ => panic!("Expected SyncResponse message"),
        }
    }

    #[actix_test]
    async fn test_process_gossip_sync_response() {
        let config = create_test_config("test-node", 8000, vec![]);

        let kvstore = KVStoreActor::new().start();

        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let mut data = HashMap::new();
        data.insert(
            "sync-key1".to_string(),
            VersionedValue {
                value: "sync-value1".to_string(),
                timestamp: 1000,
                node_id: "node1".to_string(),
            },
        );
        data.insert(
            "sync-key2".to_string(),
            VersionedValue {
                value: "sync-value2".to_string(),
                timestamp: 2000,
                node_id: "node2".to_string(),
            },
        );

        let gossip_msg = GossipMessage::SyncResponse { data };

        let result = gossip
            .send(ProcessGossip {
                message: gossip_msg,
            })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Ping) => (),
            _ => panic!("Expected Ping message"),
        }

        let get_msg1 = Get {
            key: "sync-key1".to_string(),
        };
        let get_msg2 = Get {
            key: "sync-key2".to_string(),
        };

        let value1 = kvstore.send(get_msg1).await.unwrap();
        let value2 = kvstore.send(get_msg2).await.unwrap();

        assert!(value1.is_some());
        assert!(value2.is_some());

        assert_eq!(value1.unwrap().value, "sync-value1");
        assert_eq!(value2.unwrap().value, "sync-value2");
    }

    #[actix_test]
    async fn test_process_gossip_ping_pong() {
        let peers = vec!["127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string()];
        let config = create_test_config("test-node", 8000, peers);

        let kvstore = KVStoreActor::new().start();

        let gossip = GossipActor::new(config.clone(), kvstore).start();

        let ping_msg = GossipMessage::Ping;

        let result = gossip
            .send(ProcessGossip { message: ping_msg })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Pong { sender, members }) => {
                assert_eq!(sender, "127.0.0.1:8000");
                assert_eq!(members.len(), 2);
                assert!(members.contains(&"127.0.0.1:8001".to_string()));
                assert!(members.contains(&"127.0.0.1:8002".to_string()));
            }
            _ => panic!("Expected Pong message"),
        }

        let pong_msg = GossipMessage::Pong {
            sender: "127.0.0.1:9000".to_string(),
            members: vec!["127.0.0.1:9001".to_string()],
        };

        let result = gossip
            .send(ProcessGossip { message: pong_msg })
            .await
            .unwrap();

        match result {
            Ok(GossipMessage::Ping) => (),
            _ => panic!("Expected Ping message"),
        }
    }

    #[actix_test]
    async fn test_start_gossip() {
        let config = create_test_config("test-node", 8000, vec![]);

        let kvstore = KVStoreActor::new().start();

        let gossip = GossipActor::new(config.clone(), kvstore).start();

        let _ = gossip.send(StartGossip).await.unwrap();

        // Nothing to assert, just make sure it doesn't crash
    }
}
