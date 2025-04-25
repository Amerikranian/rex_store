use crate::kvstore::VersionedValue;
use crate::vector_clock::VectorClock;
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

    // Quorum-based operations
    QuorumRead {
        key: String,
        request_id: String,
    },

    QuorumReadResponse {
        key: String,
        value: Option<VersionedValue>,
        request_id: String,
        node_id: String,
    },

    QuorumWrite {
        key: String,
        value: VersionedValue,
        request_id: String,
    },

    QuorumWriteResponse {
        key: String,
        success: bool,
        value: Option<VersionedValue>,
        request_id: String,
        node_id: String,
    },

    // For conflict resolution
    ConflictResolution {
        key: String,
        resolved_value: VersionedValue,
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

    // Quorum-based commands
    QuorumGet { key: String },
    QuorumSet { key: String, value: String },

    // Cluster management commands
    GetPeers,
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
        vector_clock: Option<VectorClock>,
    },

    SetResult {
        key: String,
        value: String,
        vector_clock: VectorClock,
    },

    // Quorum response types
    QuorumReadResult {
        key: String,
        value: Option<String>,
        found: bool,
        successful_nodes: Vec<String>,
        failed_nodes: Vec<String>,
        vector_clock: Option<VectorClock>,
    },

    QuorumWriteResult {
        key: String,
        value: String,
        successful_nodes: Vec<String>,
        failed_nodes: Vec<String>,
        vector_clock: VectorClock,
    },

    // Gossip protocol responses
    GossipMessage(GossipMessage),

    // Cluster info responses
    PeerList {
        peers: Vec<String>,
    },

    ReplicaList {
        key: String,
        replicas: Vec<String>,
    },

    HealthInfo {
        status: String,
        node_id: String,
        endpoint: String,
    },
}

pub mod actor;
pub mod tcp;
