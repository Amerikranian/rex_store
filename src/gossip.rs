use crate::kvstore::VersionedValue;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

// Message types for the gossip protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipMessage {
    // Regular gossip message containing updates
    Update {
        updates: HashMap<String, VersionedValue>,
    },

    // Request a full sync from a peer
    SyncRequest,

    // Response to a sync request
    SyncResponse {
        data: HashMap<String, VersionedValue>,
    },

    // Ping message for cluster membership
    Ping,

    // Pong response to a ping
    Pong {
        sender: String,
        members: Vec<String>,
    },
}

// Command enum for TCP protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipCommand {
    // Gossip protocol commands
    Gossip(GossipMessage),

    // KV store commands
    Get { key: String },
    Set { key: String, value: String },

    // Cluster management commands
    GetPeers,
    Health,
}

// Response types for the TCP protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipResponse {
    // Success response with optional message
    Ok(Option<GossipResponseData>),

    // Error response
    Error { message: String },
}

// Data types that can be returned in a response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipResponseData {
    // KV response types
    Value {
        key: String,
        value: Option<String>,
        found: bool,
    },
    SetResult {
        key: String,
        value: String,
        timestamp: u128,
    },

    // Gossip protocol responses
    GossipMessage(GossipMessage),

    // Cluster info responses
    PeerList {
        peers: Vec<String>,
    },
    HealthInfo {
        status: String,
        node_id: String,
        endpoint: String,
    },
}

pub mod actor;
pub mod tcp;
