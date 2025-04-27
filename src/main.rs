use actix::Actor;
use anyhow::Result;
use clap::Parser;
use core_affinity;
use core_affinity::CoreId;
use hwloc2::Topology;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use tokio::sync::Notify;

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

fn main() -> Result<()> {
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

    let core_ids = get_physical_core_ids();
    let num_cores = core_ids.len();

    tracing::info!("System has {} cores available", num_cores);

    if args.num_nodes as usize > num_cores {
        tracing::warn!(
            "More nodes ({}) than cores ({}). Some cores will run multiple nodes.",
            args.num_nodes,
            num_cores
        );
    }

    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = shutdown.clone();

    ctrlc::set_handler(move || {
        tracing::info!("Received Ctrl+C, initiating shutdown");
        shutdown_clone.notify_waiters();
    })
    .expect("Error setting Ctrl+C handler");

    let mut handles = Vec::new();

    for i in 0..args.num_nodes {
        let port = args.base_port + i;
        let node_id = format!("{}:{}", args.host, port);
        let core_id = core_ids[i as usize % num_cores].clone();

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

        let node_shutdown = shutdown.clone();
        let args_clone = args.clone();

        let handle = thread::spawn(move || {
            //core_affinity::set_for_current(core_id);

            actix_rt::System::new().block_on(async move {
                tracing::info!(
                    "Starting node-{} on {}:{} with core {:?}",
                    i,
                    &config.host,
                    config.port,
                    core_id
                );

                let kvstore = KVStoreActor::new(node_id.clone());
                let kvstore_addr = kvstore.start();

                let gossip = GossipActor::new(
                    config.clone(),
                    kvstore_addr.clone(),
                    args_clone.replication_factor,
                    args_clone.read_quorum,
                    args_clone.write_quorum,
                );
                let gossip_addr = gossip.start();

                let server = NetworkServer::new(config.clone(), gossip_addr.clone());
                let server_shutdown_handle = server.get_shutdown_handle();

                let server_handle = tokio::spawn(async move {
                    if let Err(e) = server.run().await {
                        tracing::error!("Network server error (node-{}): {:?}", i, e);
                    }
                });

                node_shutdown.notified().await;
                tracing::info!("Node-{} received shutdown signal", i);

                if let Some(handle) = server_shutdown_handle {
                    let _ = handle.send(());
                }

                let _ = server_handle.await;

                tracing::info!("Node-{} shutdown complete", i);
            });
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }

    tracing::info!("All nodes have shut down. Exiting.");
    Ok(())
}

fn get_physical_core_ids() -> Vec<CoreId> {
    if let Some(topology) = Topology::new() {
        let mut seen_pus = HashSet::new();
        let mut result = Vec::new();

        for obj in topology
            .objects_with_type(&hwloc2::ObjectType::Core)
            .unwrap_or_default()
        {
            if let Some(pu) = obj.first_child() {
                let os_index = pu.os_index();
                if seen_pus.insert(os_index) {
                    result.push(CoreId {
                        id: os_index as usize,
                    });
                }
            }
        }

        if !result.is_empty() {
            return result;
        }
    }

    tracing::info!(
        "Either hwloc2 not found or its collected core IDs are empty: falling back to scheduling every other thread"
    );

    core_affinity::get_core_ids()
        .unwrap_or_default()
        .into_iter()
        .filter(|core| core.id % 2 == 0)
        .collect()
}
