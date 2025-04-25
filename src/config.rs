use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Config {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub peers: HashSet<String>,
    pub replication_factor: usize,
    pub read_quorum: usize,
    pub write_quorum: usize,
}

impl Config {
    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn validate_quorum_settings(&self) -> Result<(), String> {
        if self.replication_factor < self.read_quorum {
            return Err("Read quorum must be <= replication factor".to_string());
        }

        if self.replication_factor < self.write_quorum {
            return Err("Write quorum must be <= replication factor".to_string());
        }

        if self.read_quorum + self.write_quorum <= self.replication_factor {
            return Err(
                "Read quorum + Write quorum must be > replication factor for consistency"
                    .to_string(),
            );
        }

        Ok(())
    }
}
