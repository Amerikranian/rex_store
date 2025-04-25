use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    // Node ID to counter mapping
    pub counters: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        VectorClock {
            counters: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str) {
        let counter = self.counters.entry(node_id.to_string()).or_insert(0);
        *counter += 1;
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &counter) in &other.counters {
            let entry = self.counters.entry(node.clone()).or_insert(0);
            if counter > *entry {
                *entry = counter;
            }
        }
    }

    // Compare vector clocks
    // Returns:
    // - Ordering::Less if self is ancestor of other
    // - Ordering::Greater if self is descendant of other
    // - Ordering::Equal if self and other are the same
    // - None if self and other are concurrent (conflict)
    pub fn compare(&self, other: &VectorClock) -> Option<Ordering> {
        let mut self_greater = false;
        let mut other_greater = false;

        // Check all keys in self
        for (node, &counter) in &self.counters {
            match other.counters.get(node) {
                Some(&other_counter) => {
                    if counter > other_counter {
                        self_greater = true;
                    } else if counter < other_counter {
                        other_greater = true;
                    }
                }
                None => {
                    if counter > 0 {
                        self_greater = true;
                    }
                }
            }
        }

        // Check for keys in other but not in self
        for (node, &counter) in &other.counters {
            if !self.counters.contains_key(node) && counter > 0 {
                other_greater = true;
            }
        }

        match (self_greater, other_greater) {
            (true, true) => None, // Concurrent changes
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => Some(Ordering::Equal),
        }
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        assert_eq!(vc.counters.get("node1"), Some(&1));
        vc.increment("node1");
        assert_eq!(vc.counters.get("node1"), Some(&2));
    }

    #[test]
    fn test_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node1");
        vc1.increment("node2");

        let mut vc2 = VectorClock::new();
        vc2.increment("node1");
        vc2.increment("node3");

        vc1.merge(&vc2);
        assert_eq!(vc1.counters.get("node1"), Some(&2));
        assert_eq!(vc1.counters.get("node2"), Some(&1));
        assert_eq!(vc1.counters.get("node3"), Some(&1));
    }

    #[test]
    fn test_compare_equal() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node2");

        let mut vc2 = VectorClock::new();
        vc2.increment("node1");
        vc2.increment("node2");

        assert_eq!(vc1.compare(&vc2), Some(Ordering::Equal));
    }

    #[test]
    fn test_compare_ancestor() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");

        let mut vc2 = VectorClock::new();
        vc2.increment("node1");
        vc2.increment("node2");

        assert_eq!(vc1.compare(&vc2), Some(Ordering::Less));
    }

    #[test]
    fn test_compare_descendant() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node2");

        let mut vc2 = VectorClock::new();
        vc2.increment("node1");

        assert_eq!(vc1.compare(&vc2), Some(Ordering::Greater));
    }

    #[test]
    fn test_compare_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node1");

        let mut vc2 = VectorClock::new();
        vc2.increment("node2");
        vc2.increment("node2");

        assert_eq!(vc1.compare(&vc2), None);
    }
}
