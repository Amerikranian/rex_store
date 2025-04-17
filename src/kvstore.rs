use actix::{Actor, Context, Handler, Message, MessageResult};
use anyhow::Result;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

// A versioned value in our key-value store
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionedValue {
    pub value: String,
    pub timestamp: u128,
    pub node_id: String,
}

// Message types for the KVStore actor
#[derive(Message)]
#[rtype(result = "Option<VersionedValue>")]
pub struct Get {
    pub key: String,
}

#[derive(Message)]
#[rtype(result = "VersionedValue")]
pub struct Set {
    pub key: String,
    pub value: String,
    pub node_id: String,
}

#[derive(Message)]
#[rtype(result = "bool")]
pub struct Update {
    pub key: String,
    pub value: VersionedValue,
}

#[derive(Message)]
#[rtype(result = "Result<HashMap<String, VersionedValue>>")]
pub struct Snapshot;

#[derive(Message)]
#[rtype(result = "HashMap<String, VersionedValue>")]
pub struct GetAll;

// The KVStore actor
pub struct KVStoreActor {
    data: HashMap<String, VersionedValue>,
    clock: u128,
}

impl KVStoreActor {
    pub fn new() -> Self {
        KVStoreActor {
            data: HashMap::new(),
            clock: 0,
        }
    }

    fn next_timestamp(&mut self) -> u128 {
        self.clock += 1;
        self.clock
    }

    fn sync_clock(&mut self, remote_ts: u128) {
        self.clock = self.clock.max(remote_ts) + 1;
    }
}

impl Actor for KVStoreActor {
    type Context = Context<Self>;
}

impl Handler<Get> for KVStoreActor {
    type Result = Option<VersionedValue>;

    fn handle(&mut self, msg: Get, _ctx: &mut Context<Self>) -> Self::Result {
        self.data.get(&msg.key).cloned()
    }
}

impl Handler<Set> for KVStoreActor {
    type Result = MessageResult<Set>;

    fn handle(&mut self, msg: Set, _ctx: &mut Context<Self>) -> Self::Result {
        let timestamp = self.next_timestamp();

        let versioned_value = VersionedValue {
            value: msg.value,
            timestamp,
            node_id: msg.node_id,
        };

        self.data.insert(msg.key, versioned_value.clone());
        MessageResult(versioned_value)
    }
}

impl Handler<Update> for KVStoreActor {
    type Result = bool;

    fn handle(&mut self, msg: Update, _ctx: &mut Context<Self>) -> Self::Result {
        self.sync_clock(msg.value.timestamp);

        let updated = match self.data.get(&msg.key) {
            Some(current) => {
                if current.timestamp < msg.value.timestamp
                    || (current.timestamp == msg.value.timestamp
                        && current.node_id < msg.value.node_id)
                {
                    self.data.insert(msg.key, msg.value);
                    true
                } else {
                    false
                }
            }
            None => {
                self.data.insert(msg.key, msg.value);
                true
            }
        };

        updated
    }
}

impl Handler<Snapshot> for KVStoreActor {
    type Result = Result<HashMap<String, VersionedValue>>;

    fn handle(&mut self, _msg: Snapshot, _ctx: &mut Context<Self>) -> Self::Result {
        Ok(self.data.clone())
    }
}

impl Handler<GetAll> for KVStoreActor {
    type Result = MessageResult<GetAll>;

    fn handle(&mut self, _msg: GetAll, _ctx: &mut Context<Self>) -> Self::Result {
        MessageResult(self.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_rt::test as actix_test;
    use rand::{SeedableRng, rngs::StdRng};

    #[actix_test]
    async fn test_kvstore_set_get() {
        let kvstore = KVStoreActor::new().start();

        let set_result = kvstore
            .send(Set {
                key: "test_key".to_string(),
                value: "test_value".to_string(),
                node_id: "test_node".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(set_result.value, "test_value");

        let get_result = kvstore
            .send(Get {
                key: "test_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        assert_eq!(get_result.unwrap().value, "test_value");
    }

    #[actix_test]
    async fn test_kvstore_update() {
        let kvstore = KVStoreActor::new().start();

        let _ = kvstore
            .send(Set {
                key: "test_key".to_string(),
                value: "initial_value".to_string(),
                node_id: "node1".to_string(),
            })
            .await
            .unwrap();

        let newer_value = VersionedValue {
            value: "newer_value".to_string(),
            timestamp: 1000,
            node_id: "node2".to_string(),
        };

        let updated = kvstore
            .send(Update {
                key: "test_key".to_string(),
                value: newer_value.clone(),
            })
            .await
            .unwrap();

        assert!(updated);

        let get_result = kvstore
            .send(Get {
                key: "test_key".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(get_result.unwrap().value, "newer_value");
    }

    #[actix_test]
    async fn test_update_with_newer_timestamp() {
        let kvstore = KVStoreActor::new().start();

        let initial_value = VersionedValue {
            value: "initial".to_string(),
            timestamp: 1000,
            node_id: "node1".to_string(),
        };

        let _ = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: initial_value,
            })
            .await
            .unwrap();

        let newer_value = VersionedValue {
            value: "newer".to_string(),
            timestamp: 2000,
            node_id: "node2".to_string(),
        };

        let updated = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: newer_value,
            })
            .await
            .unwrap();

        assert!(updated);

        let get_result = kvstore
            .send(Get {
                key: "conflict_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "newer");
        assert_eq!(value.timestamp, 2000);
        assert_eq!(value.node_id, "node2");
    }

    #[actix_test]
    async fn test_update_with_older_timestamp() {
        let kvstore = KVStoreActor::new().start();

        let initial_value = VersionedValue {
            value: "initial".to_string(),
            timestamp: 2000,
            node_id: "node1".to_string(),
        };

        let _ = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: initial_value,
            })
            .await
            .unwrap();

        let older_value = VersionedValue {
            value: "older".to_string(),
            timestamp: 1000,
            node_id: "node2".to_string(),
        };

        let updated = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: older_value,
            })
            .await
            .unwrap();

        assert!(!updated);

        let get_result = kvstore
            .send(Get {
                key: "conflict_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "initial");
        assert_eq!(value.timestamp, 2000);
        assert_eq!(value.node_id, "node1");
    }

    #[actix_test]
    async fn test_update_with_same_timestamp_different_nodes() {
        let kvstore = KVStoreActor::new().start();

        let initial_value = VersionedValue {
            value: "node1_value".to_string(),
            timestamp: 1000,
            node_id: "node1".to_string(),
        };

        let _ = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: initial_value,
            })
            .await
            .unwrap();

        let same_time_value = VersionedValue {
            value: "node2_value".to_string(),
            timestamp: 1000,
            node_id: "node2".to_string(), // node2 > node1 lexicographically
        };

        let updated = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: same_time_value,
            })
            .await
            .unwrap();

        assert!(updated);

        let get_result = kvstore
            .send(Get {
                key: "conflict_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "node2_value");
        assert_eq!(value.timestamp, 1000);
        assert_eq!(value.node_id, "node2");

        let node0_value = VersionedValue {
            value: "node0_value".to_string(),
            timestamp: 1000,
            node_id: "node0".to_string(), // node0 < node2 lexicographically
        };

        let updated = kvstore
            .send(Update {
                key: "conflict_key".to_string(),
                value: node0_value,
            })
            .await
            .unwrap();

        assert!(!updated);

        let get_result = kvstore
            .send(Get {
                key: "conflict_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "node2_value"); // Still node2's value
        assert_eq!(value.timestamp, 1000);
        assert_eq!(value.node_id, "node2");
    }

    #[actix_test]
    async fn test_snapshot_and_getall() {
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

        let snapshot = kvstore.send(Snapshot).await.unwrap().unwrap();

        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains_key("key1"));
        assert!(snapshot.contains_key("key2"));
        assert_eq!(snapshot.get("key1").unwrap().value, "value1");
        assert_eq!(snapshot.get("key2").unwrap().value, "value2");

        let all_data = kvstore.send(GetAll).await.unwrap();

        assert_eq!(all_data.len(), 2);
        assert!(all_data.contains_key("key1"));
        assert!(all_data.contains_key("key2"));
        assert_eq!(all_data.get("key1").unwrap().value, "value1");
        assert_eq!(all_data.get("key2").unwrap().value, "value2");
    }

    #[actix_test]
    async fn test_concurrent_updates() {
        let kvstore = KVStoreActor::new().start();

        for i in 1..=100 {
            let value = VersionedValue {
                value: format!("value{}", i),
                timestamp: i,
                node_id: "node1".to_string(),
            };

            let _ = kvstore
                .send(Update {
                    key: "concurrent_key".to_string(),
                    value,
                })
                .await
                .unwrap();
        }

        let get_result = kvstore
            .send(Get {
                key: "concurrent_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "value100");
        assert_eq!(value.timestamp, 100);

        use rand::Rng;
        use std::sync::Arc;
        use tokio::sync::Barrier;

        let kvstore_arc = Arc::new(kvstore);
        let barrier = Arc::new(Barrier::new(10)); // Synchronize 10 concurrent updates

        let mut handles = vec![];
        let mut max_ts = 0;

        for _ in 0..10 {
            let mut rng = StdRng::from_os_rng();
            let timestamp = rng.random_range(200..300);
            max_ts = max_ts.max(timestamp);
            let kvstore_clone = kvstore_arc.clone();
            let barrier_clone = barrier.clone();

            let handle = tokio::spawn(async move {
                let node_id = format!("node{}", rng.random_range(1..5));

                let value = VersionedValue {
                    value: format!("value{}-{}", timestamp, node_id),
                    timestamp,
                    node_id,
                };

                barrier_clone.wait().await;

                let _ = kvstore_clone
                    .send(Update {
                        key: "random_order_key".to_string(),
                        value,
                    })
                    .await
                    .unwrap();
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }

        let get_result = kvstore_arc
            .send(Get {
                key: "random_order_key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();

        assert!(value.timestamp >= 200);
        assert_eq!(max_ts, value.timestamp);
    }

    #[actix_test]
    async fn test_multiple_nodes_with_concurrent_updates() {
        let node1_store = KVStoreActor::new().start();
        let node2_store = KVStoreActor::new().start();
        let node3_store = KVStoreActor::new().start();

        let node1_value = VersionedValue {
            value: "node1_value".to_string(),
            timestamp: 1000,
            node_id: "node1".to_string(),
        };

        let node2_value = VersionedValue {
            value: "node2_value".to_string(),
            timestamp: 1000,
            node_id: "node2".to_string(),
        };

        let node3_value = VersionedValue {
            value: "node3_value".to_string(),
            timestamp: 1000,
            node_id: "node3".to_string(),
        };

        let _ = node1_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node1_value.clone(),
            })
            .await
            .unwrap();

        let _ = node2_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node2_value.clone(),
            })
            .await
            .unwrap();

        let _ = node3_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node3_value.clone(),
            })
            .await
            .unwrap();

        // Apply node1's value to node2 and node3
        let updated = node2_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node1_value.clone(),
            })
            .await
            .unwrap();
        assert!(!updated);

        let updated = node3_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node1_value.clone(),
            })
            .await
            .unwrap();
        assert!(!updated);

        // Apply node2's value to node1 and node3
        let updated = node1_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node2_value.clone(),
            })
            .await
            .unwrap();
        assert!(updated);

        let updated = node3_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node2_value.clone(),
            })
            .await
            .unwrap();
        assert!(!updated);

        // Apply node3's value to node1 and node2
        let updated = node1_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node3_value.clone(),
            })
            .await
            .unwrap();
        assert!(updated);

        let updated = node2_store
            .send(Update {
                key: "shared_key".to_string(),
                value: node3_value.clone(),
            })
            .await
            .unwrap();
        assert!(updated);

        // Sanity check
        let node1_result = node1_store
            .send(Get {
                key: "shared_key".to_string(),
            })
            .await
            .unwrap();

        let node2_result = node2_store
            .send(Get {
                key: "shared_key".to_string(),
            })
            .await
            .unwrap();

        let node3_result = node3_store
            .send(Get {
                key: "shared_key".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(node1_result.unwrap().value, "node3_value");
        assert_eq!(node2_result.unwrap().value, "node3_value");
        assert_eq!(node3_result.unwrap().value, "node3_value");
    }
}
