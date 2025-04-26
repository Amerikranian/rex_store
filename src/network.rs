use actix::Addr;
use anyhow::Result;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::gossip::actor::GossipActor;
use crate::gossip::tcp::{GossipTcpServer, create_shutdown_channel};

// The network server
pub struct NetworkServer {
    config: Config,
    gossip: Addr<GossipActor>,
    shutdown_sender: Option<broadcast::Sender<()>>,
    shutdown_receiver: Option<broadcast::Receiver<()>>,
}

impl NetworkServer {
    pub fn new(config: Config, gossip: Addr<GossipActor>) -> Self {
        let (shutdown_sender, shutdown_receiver) = create_shutdown_channel();

        Self {
            config,
            gossip,
            shutdown_sender: Some(shutdown_sender),
            shutdown_receiver: Some(shutdown_receiver),
        }
    }

    // Get a handle to initiate shutdown
    pub fn get_shutdown_handle(&self) -> Option<broadcast::Sender<()>> {
        self.shutdown_sender.clone()
    }

    // Run the server - take ownership of self
    pub async fn run(mut self) -> Result<()> {
        let tcp_server = GossipTcpServer::new(
            self.config.clone(),
            self.gossip.clone(),
            self.shutdown_receiver.take(), // Pass ownership of the receiver
        );

        let host = self.config.host.clone();
        let port = self.config.port;
        let node_id = self.config.node_id.clone();
        let replication_factor = self.config.replication_factor;
        let read_quorum = self.config.read_quorum;
        let write_quorum = self.config.write_quorum;

        tracing::info!(
            "Starting node {} on {}:{} with N={}, R={}, W={}",
            node_id,
            host,
            port,
            replication_factor,
            read_quorum,
            write_quorum
        );

        tcp_server.run().await
    }
}
