use actix::Actor;
use anyhow::Result;
use clap::Parser;
use tokio::signal;

use rex_store::config::Config;
use rex_store::gossip::actor::GossipActor;
use rex_store::kvstore::KVStoreActor;
use rex_store::network::NetworkServer;

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

    /// Replication factor (N)
    #[clap(short = 'N', long, default_value = "3")]
    replication_factor: usize,

    /// Read quorum (R)
    #[clap(short = 'r', long, default_value = "2")]
    read_quorum: usize,

    /// Write quorum (W)
    #[clap(short = 'w', long, default_value = "2")]
    write_quorum: usize,
}

#[actix_web::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    if args.replication_factor < args.read_quorum {
        return Err(anyhow::anyhow!("Read quorum must be <= replication factor"));
    }

    if args.replication_factor < args.write_quorum {
        return Err(anyhow::anyhow!(
            "Write quorum must be <= replication factor"
        ));
    }

    if args.read_quorum + args.write_quorum <= args.replication_factor {
        return Err(anyhow::anyhow!(
            "Read quorum + Write quorum must be > replication factor for consistency"
        ));
    }

    let mut join_handles = Vec::new();
    let mut shutdown_handles = Vec::new();

    for i in 0..args.num_nodes {
        let port = args.base_port + i;
        let node_id = format!("{}:{}", args.host, port);

        let peers = (0..args.num_nodes)
            .filter(|&j| j != i)
            .map(|j| format!("{}:{}", args.host, args.base_port + j))
            .collect();

        let config = Config {
            node_id: node_id.clone(),
            host: args.host.clone(),
            port,
            peers,
            replication_factor: args.replication_factor,
            read_quorum: args.read_quorum,
            write_quorum: args.write_quorum,
        };

        if let Err(err) = config.validate_quorum_settings() {
            return Err(anyhow::anyhow!("Invalid quorum settings: {}", err));
        }

        let kvstore = KVStoreActor::new(node_id.clone());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            args.replication_factor,
            args.read_quorum,
            args.write_quorum,
        );
        let gossip_addr = gossip.start();

        let server = NetworkServer::new(config.clone(), kvstore_addr.clone(), gossip_addr.clone());

        if let Some(shutdown_handle) = server.get_shutdown_handle() {
            shutdown_handles.push(shutdown_handle);
        }

        let server_handle = actix_web::rt::spawn(async move {
            if let Err(e) = server.run().await {
                tracing::error!("Network server error (node-{}): {:?}", i, e);
            }
        });

        join_handles.push(server_handle);

        tracing::info!("Started node-{} on {}:{}", i, args.host, port);
    }

    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("CTRL+C received, initiating graceful shutdown");

                for handle in shutdown_handles {
                    let _ = handle.send(());
                }
            }
            Err(err) => {
                tracing::error!("Error listening for CTRL+C: {:?}", err);
            }
        }
    });

    // Wait for all node handles to complete
    for handle in join_handles {
        let _ = handle.await;
    }

    Ok(())
}
