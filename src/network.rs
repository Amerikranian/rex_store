use actix::Addr;
use anyhow::Result;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::gossip::actor::GossipActor;
use crate::gossip::tcp::{GossipTcpServer, create_shutdown_channel};
use crate::kvstore::KVStoreActor;

// The network server
pub struct NetworkServer {
    config: Config,
    kvstore: Addr<KVStoreActor>,
    gossip: Addr<GossipActor>,
    shutdown_sender: Option<broadcast::Sender<()>>,
    shutdown_receiver: Option<broadcast::Receiver<()>>,
}

impl NetworkServer {
    pub fn new(config: Config, kvstore: Addr<KVStoreActor>, gossip: Addr<GossipActor>) -> Self {
        // Create a shutdown channel
        let (shutdown_sender, shutdown_receiver) = create_shutdown_channel();

        Self {
            config,
            kvstore,
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
            self.kvstore.clone(),
            self.gossip.clone(),
            self.shutdown_receiver.take(), // Pass ownership of the receiver
        );

        // Clone these values for logging
        let host = self.config.host.clone();
        let port = self.config.port;
        let node_id = self.config.node_id.clone();

        tracing::info!("Starting node {} on {}:{}", node_id, host, port);

        tcp_server.run().await
    }
}
