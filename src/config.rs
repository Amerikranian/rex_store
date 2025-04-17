use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Config {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub peers: HashSet<String>,
}

impl Config {
    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
