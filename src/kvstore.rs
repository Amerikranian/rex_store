use actix::{Actor, Context, Handler, Message, MessageResult};
use anyhow::Result;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::vector_clock::VectorClock;

// A versioned value
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VersionedValue {
    pub value: String,
    pub vector_clock: VectorClock,
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
#[rtype(result = "UpdateResult")]
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

// Result of an update operation
#[derive(Debug, PartialEq)]
pub enum UpdateResult {
    Updated(VersionedValue),
    Conflict(VersionedValue, VersionedValue), // (stored, received)
    NotUpdated(VersionedValue),
}

// The KVStore actor
pub struct KVStoreActor {
    data: HashMap<String, VersionedValue>,
    node_id: String,
}

impl KVStoreActor {
    pub fn new(node_id: String) -> Self {
        KVStoreActor {
            data: HashMap::new(),
            node_id,
        }
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
        // Create a new vector clock or get the existing one and increment
        let mut vector_clock = VectorClock::new();

        if let Some(existing) = self.data.get(&msg.key) {
            vector_clock = existing.vector_clock.clone();
        }

        vector_clock.increment(&self.node_id);

        let versioned_value = VersionedValue {
            value: msg.value,
            vector_clock,
            node_id: msg.node_id,
        };

        self.data.insert(msg.key, versioned_value.clone());
        MessageResult(versioned_value)
    }
}

impl Handler<Update> for KVStoreActor {
    type Result = MessageResult<Update>;

    fn handle(&mut self, msg: Update, _ctx: &mut Context<Self>) -> Self::Result {
        let result = match self.data.get(&msg.key) {
            Some(current) => {
                // Compare vector clocks
                match current.vector_clock.compare(&msg.value.vector_clock) {
                    Some(Ordering::Less) => {
                        // The incoming value is newer
                        let updated_value = msg.value.clone();
                        self.data.insert(msg.key, updated_value.clone());
                        UpdateResult::Updated(updated_value)
                    }
                    Some(Ordering::Greater) => {
                        // The current value is newer
                        UpdateResult::NotUpdated(current.clone())
                    }
                    Some(Ordering::Equal) => {
                        // Equal vector clocks, use node id as tie breaker
                        if current.node_id < msg.value.node_id {
                            // Use lexicographically greater node_id
                            let updated_value = msg.value.clone();
                            self.data.insert(msg.key, updated_value.clone());
                            UpdateResult::Updated(updated_value)
                        } else {
                            UpdateResult::NotUpdated(current.clone())
                        }
                    }
                    None => {
                        // Conflict: neither value is newer, we have a conflict
                        UpdateResult::Conflict(current.clone(), msg.value)
                    }
                }
            }
            None => {
                // No current value, just insert
                let new_value = msg.value.clone();
                self.data.insert(msg.key, new_value.clone());
                UpdateResult::Updated(new_value)
            }
        };

        MessageResult(result)
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
    use actix::Actor;

    #[actix_rt::test]
    async fn test_kvstore_set_get() {
        let kvstore = KVStoreActor::new("node-1".to_string());
        let kvstore_addr = kvstore.start();

        let set_result = kvstore_addr
            .send(Set {
                key: "test-key".to_string(),
                value: "test-value".to_string(),
                node_id: "node-1".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(set_result.value, "test-value");
        assert_eq!(set_result.node_id, "node-1");

        let counter = set_result.vector_clock.counters.get("node-1").unwrap();
        assert_eq!(*counter, 1);

        let get_result = kvstore_addr
            .send(Get {
                key: "test-key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "test-value");
        assert_eq!(value.node_id, "node-1");

        let get_result = kvstore_addr
            .send(Get {
                key: "non-existent".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_none());
    }

    #[actix_rt::test]
    async fn test_kvstore_update() {
        let kvstore = KVStoreActor::new("node-1".to_string());
        let kvstore_addr = kvstore.start();

        let mut vc = VectorClock::new();
        vc.increment("node-2");

        let original_value = VersionedValue {
            value: "original".to_string(),
            vector_clock: vc.clone(),
            node_id: "node-2".to_string(),
        };

        let update_result = kvstore_addr
            .send(Update {
                key: "update-key".to_string(),
                value: original_value.clone(),
            })
            .await
            .unwrap();

        match update_result {
            UpdateResult::Updated(value) => {
                assert_eq!(value.value, "original");
                assert_eq!(value.node_id, "node-2");
            }
            _ => panic!("Expected UpdateResult::Updated"),
        }

        let mut newer_vc = vc.clone();
        newer_vc.increment("node-2");

        let newer_value = VersionedValue {
            value: "newer".to_string(),
            vector_clock: newer_vc,
            node_id: "node-2".to_string(),
        };

        let update_result = kvstore_addr
            .send(Update {
                key: "update-key".to_string(),
                value: newer_value,
            })
            .await
            .unwrap();

        match update_result {
            UpdateResult::Updated(value) => {
                assert_eq!(value.value, "newer");
                let counter = value.vector_clock.counters.get("node-2").unwrap();
                assert_eq!(*counter, 2);
            }
            _ => panic!("Expected UpdateResult::Updated"),
        }

        let older_value = VersionedValue {
            value: "older".to_string(),
            vector_clock: vc.clone(),
            node_id: "node-2".to_string(),
        };

        let update_result = kvstore_addr
            .send(Update {
                key: "update-key".to_string(),
                value: older_value,
            })
            .await
            .unwrap();

        match update_result {
            UpdateResult::NotUpdated(value) => {
                assert_eq!(value.value, "newer");
                let counter = value.vector_clock.counters.get("node-2").unwrap();
                assert_eq!(*counter, 2);
            }
            _ => panic!("Expected UpdateResult::NotUpdated"),
        }
    }

    #[actix_rt::test]
    async fn test_kvstore_concurrent_updates() {
        let kvstore = KVStoreActor::new("node-1".to_string());
        let kvstore_addr = kvstore.start();

        let mut base_vc = VectorClock::new();
        base_vc.increment("common");

        let base_value = VersionedValue {
            value: "base".to_string(),
            vector_clock: base_vc.clone(),
            node_id: "common".to_string(),
        };

        let update_result = kvstore_addr
            .send(Update {
                key: "concurrent-key".to_string(),
                value: base_value,
            })
            .await
            .unwrap();

        assert!(matches!(update_result, UpdateResult::Updated(_)));

        let mut vc_a = base_vc.clone();
        vc_a.increment("node-a");

        let value_a = VersionedValue {
            value: "value-a".to_string(),
            vector_clock: vc_a,
            node_id: "node-a".to_string(),
        };

        let mut vc_b = base_vc.clone();
        vc_b.increment("node-b");

        let value_b = VersionedValue {
            value: "value-b".to_string(),
            vector_clock: vc_b,
            node_id: "node-b".to_string(),
        };

        let update_result = kvstore_addr
            .send(Update {
                key: "concurrent-key".to_string(),
                value: value_a.clone(),
            })
            .await
            .unwrap();

        assert!(matches!(update_result, UpdateResult::Updated(_)));

        let update_result = kvstore_addr
            .send(Update {
                key: "concurrent-key".to_string(),
                value: value_b.clone(),
            })
            .await
            .unwrap();

        match update_result {
            UpdateResult::Conflict(stored, received) => {
                assert_eq!(stored.value, "value-a");
                assert_eq!(stored.node_id, "node-a");
                assert_eq!(received.value, "value-b");
                assert_eq!(received.node_id, "node-b");
            }
            _ => panic!("Expected UpdateResult::Conflict"),
        }
    }

    #[actix_rt::test]
    async fn test_kvstore_tie_breaker() {
        let kvstore = KVStoreActor::new("node-1".to_string());
        let kvstore_addr = kvstore.start();

        let mut vc = VectorClock::new();
        vc.increment("common");

        let value_a = VersionedValue {
            value: "value-a".to_string(),
            vector_clock: vc.clone(),
            node_id: "node-a".to_string(),
        };

        let value_b = VersionedValue {
            value: "value-b".to_string(),
            vector_clock: vc.clone(),
            node_id: "node-b".to_string(),
        };

        let update_result = kvstore_addr
            .send(Update {
                key: "tie-key".to_string(),
                value: value_a,
            })
            .await
            .unwrap();

        assert!(matches!(update_result, UpdateResult::Updated(_)));

        let update_result = kvstore_addr
            .send(Update {
                key: "tie-key".to_string(),
                value: value_b.clone(),
            })
            .await
            .unwrap();

        match update_result {
            UpdateResult::Updated(value) => {
                assert_eq!(value.value, "value-b");
                assert_eq!(value.node_id, "node-b");
            }
            _ => panic!("Expected UpdateResult::Updated"),
        }

        let get_result = kvstore_addr
            .send(Get {
                key: "tie-key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "value-b");
        assert_eq!(value.node_id, "node-b");
    }

    #[actix_rt::test]
    async fn test_kvstore_multiple_updates() {
        let kvstore = KVStoreActor::new("node-1".to_string());
        let kvstore_addr = kvstore.start();

        for i in 1..5 {
            let value = format!("value-{}", i);

            let set_result = kvstore_addr
                .send(Set {
                    key: "multi-key".to_string(),
                    value: value.clone(),
                    node_id: "node-1".to_string(),
                })
                .await
                .unwrap();

            assert_eq!(set_result.value, value);

            let counter = set_result.vector_clock.counters.get("node-1").unwrap();
            assert_eq!(*counter, i as u64);
        }

        let get_result = kvstore_addr
            .send(Get {
                key: "multi-key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_result.is_some());
        let value = get_result.unwrap();
        assert_eq!(value.value, "value-4");
        let counter = value.vector_clock.counters.get("node-1").unwrap();
        assert_eq!(*counter, 4);
    }
}
