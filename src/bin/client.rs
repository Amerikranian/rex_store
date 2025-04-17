use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Server address to connect to
    #[clap(short, long, default_value = "127.0.0.1:8000")]
    server: String,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Get a value
    Get {
        /// Key to retrieve
        key: String,
    },
    /// Set a value
    Set {
        /// Key to set
        key: String,
        /// Value to set
        value: String,
        /// Node ID (defaults to client)
        #[clap(short, long, default_value = "client")]
        node_id: String,
    },
    /// Get active peers
    Peers,
    /// Health check
    Health,
    /// Ping a node
    Ping {
        /// Sender ID
        #[clap(short, long, default_value = "client")]
        sender: String,
    },
}

// Constants for protocol
const LENGTH_PREFIX_SIZE: usize = 4;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Connect to server
    let mut stream = TcpStream::connect(&args.server).await?;

    // Prepare command
    let command = match &args.command {
        Command::Get { key } => {
            serde_json::json!({
                "Get": { "key": key }
            })
        }
        Command::Set {
            key,
            value,
            node_id,
        } => {
            serde_json::json!({
                "Set": { "key": key, "value": value, "node_id": node_id }
            })
        }
        Command::Peers => {
            serde_json::json!("GetPeers")
        }
        Command::Health => {
            serde_json::json!("Health")
        }
        Command::Ping { sender } => {
            serde_json::json!({
                "Gossip": {
                    "Ping": { "sender": sender }
                }
            })
        }
    };

    // Serialize command
    let command_bytes = serde_json::to_vec(&command)?;
    let command_len = command_bytes.len() as u32;

    // Send length-prefixed command
    stream.write_all(&command_len.to_be_bytes()).await?;
    stream.write_all(&command_bytes).await?;

    // Read response length
    let mut len_bytes = [0u8; LENGTH_PREFIX_SIZE];
    stream.read_exact(&mut len_bytes).await?;
    let response_len = u32::from_be_bytes(len_bytes) as usize;

    // Read response
    let mut response_buf = vec![0u8; response_len];
    stream.read_exact(&mut response_buf).await?;

    // Parse and display response
    let response: serde_json::Value = serde_json::from_slice(&response_buf)?;
    println!("{}", serde_json::to_string_pretty(&response)?);

    Ok(())
}
