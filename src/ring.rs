use hashring::HashRing;
use std::fmt;
use std::hash::Hash;

// Node type for our ring
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RingNode {
    pub node_id: String,
    pub endpoint: String,
}

impl RingNode {
    pub fn new(node_id: String, endpoint: String) -> Self {
        RingNode { node_id, endpoint }
    }
}

impl fmt::Display for RingNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.node_id, self.endpoint)
    }
}

// The consistent hash ring manager
pub struct ConsistentHashRing {
    // The hash ring
    ring: HashRing<RingNode>,
    // Replication factor
    replication_factor: usize,
    // Read quorum
    read_quorum: usize,
    // Write quorum
    write_quorum: usize,
}

impl ConsistentHashRing {
    pub fn new(replication_factor: usize, read_quorum: usize, write_quorum: usize) -> Self {
        // Validate quorum settings
        assert!(
            replication_factor >= write_quorum,
            "Write quorum must be <= replication factor"
        );
        assert!(
            replication_factor >= read_quorum,
            "Read quorum must be <= replication factor"
        );
        assert!(
            read_quorum + write_quorum > replication_factor,
            "Read + Write quorums must be > replication factor for consistency"
        );

        ConsistentHashRing {
            ring: HashRing::new(),
            replication_factor,
            read_quorum,
            write_quorum,
        }
    }

    // Add a node to the ring
    pub fn add_node(&mut self, node: RingNode) {
        self.ring.add(node);
    }

    // Remove a node from the ring
    pub fn remove_node(&mut self, node: &RingNode) -> Option<RingNode> {
        self.ring.remove(node)
    }

    // Get the nodes responsible for a key
    pub fn get_nodes_for_key(&self, key: &str) -> Vec<RingNode> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let replicas = self.replication_factor.saturating_sub(1);
        match self.ring.get_with_replicas(&key, replicas) {
            Some(nodes) => nodes,
            None => Vec::new(),
        }
    }

    // Get R/W quorum settings
    pub fn get_quorum_settings(&self) -> (usize, usize, usize) {
        (self.replication_factor, self.read_quorum, self.write_quorum)
    }

    // Get the current size of the ring
    pub fn size(&self) -> usize {
        self.ring.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashSet;

    #[test]
    fn test_add_remove_nodes() {
        let mut ring = ConsistentHashRing::new(3, 2, 2);

        assert_eq!(ring.size(), 0);

        let node1 = RingNode::new("node1".to_string(), "127.0.0.1:8001".to_string());
        let node2 = RingNode::new("node2".to_string(), "127.0.0.1:8002".to_string());

        ring.add_node(node1.clone());
        assert_eq!(ring.size(), 1);

        ring.add_node(node2.clone());
        assert_eq!(ring.size(), 2);

        let removed = ring.remove_node(&node1);
        assert_eq!(removed, Some(node1));
        assert_eq!(ring.size(), 1);
    }

    #[test]
    fn test_get_nodes_for_key() {
        let mut ring = ConsistentHashRing::new(3, 2, 2);

        let node1 = RingNode::new("node1".to_string(), "127.0.0.1:8001".to_string());
        let node2 = RingNode::new("node2".to_string(), "127.0.0.1:8002".to_string());
        let node3 = RingNode::new("node3".to_string(), "127.0.0.1:8003".to_string());
        let node4 = RingNode::new("node4".to_string(), "127.0.0.1:8004".to_string());

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);
        ring.add_node(node4);

        let nodes = ring.get_nodes_for_key("test-key");
        assert_eq!(nodes.len(), 3); // Should return 3 nodes (replication factor)

        // All nodes should be unique
        let mut seen = HashSet::new();
        for node in &nodes {
            assert!(!seen.contains(&node.node_id));
            seen.insert(node.node_id.clone());
        }
    }

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new(3, 2, 2);
        let nodes = ring.get_nodes_for_key("test-key");
        assert_eq!(nodes.len(), 0);
    }

    #[test]
    fn test_fewer_nodes_than_replication() {
        let mut ring = ConsistentHashRing::new(5, 3, 3);

        let node1 = RingNode::new("node1".to_string(), "127.0.0.1:8001".to_string());
        let node2 = RingNode::new("node2".to_string(), "127.0.0.1:8002".to_string());

        ring.add_node(node1);
        ring.add_node(node2);

        let nodes = ring.get_nodes_for_key("test-key");
        assert_eq!(nodes.len(), 2); // Should return only the available nodes
    }

    #[test]
    #[should_panic]
    fn test_invalid_quorum_settings() {
        // This should panic because R + W <= N
        let _ring = ConsistentHashRing::new(3, 1, 1);
    }
}
