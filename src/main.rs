use actix::Actor;
use anyhow::Result;
use clap::Parser;

mod config;
mod gossip;
mod kvstore;
mod network;

use config::Config;
use gossip::actor::GossipActor;
use kvstore::KVStoreActor;
use network::NetworkServer;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about)]
struct Args {
    /// Number of nodes to run
    #[clap(short, long, default_value = "1")]
    num_nodes: u16,

    /// Base port to start nodes on
    #[clap(short, long, default_value = "8000")]
    base_port: u16,

    /// Host to bind to
    #[clap(short = 'H', long, default_value = "127.0.0.1")]
    host: String,
}

#[actix_web::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mut join_handles = Vec::new();

    for i in 0..args.num_nodes {
        let port = args.base_port + i;

        let config = Config {
            node_id: format!("node-{}", i),
            host: args.host.clone(),
            port,
            peers: (0..args.num_nodes)
                .filter(|&j| j != i)
                .map(|j| format!("{}:{}", args.host, args.base_port + j))
                .collect(),
        };

        // Create KVStore actor
        let kvstore = KVStoreActor::new();
        let kvstore_addr = kvstore.start();

        // Create Gossip actor with a reference to KVStore
        let gossip = GossipActor::new(config.clone(), kvstore_addr.clone());
        let gossip_addr = gossip.start();

        // Create the network server
        let server = NetworkServer::new(config.clone(), kvstore_addr.clone(), gossip_addr.clone());

        // Run the network server
        let server_handle = actix_web::rt::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!("Network server error: {:?}", e);
            }
        });

        join_handles.push(server_handle);
    }

    // Wait for all servers to complete
    for handle in join_handles {
        let _ = handle.await;
    }

    Ok(())
}
