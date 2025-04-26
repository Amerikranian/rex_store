use crate::config::Config;
use crate::gossip::actor::{GetActivePeers, GossipActor, ProcessGossip, QuorumRead, QuorumWrite};
use crate::gossip::{GossipCommand, GossipMessage, GossipResponse, GossipResponseData};

use actix::Addr;
use anyhow::{Result, anyhow};
use bytes::BytesMut;
use serde_json;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

// Length prefix of message (4 bytes for message length)
const LENGTH_PREFIX_SIZE: usize = 4;

pub fn create_shutdown_channel() -> (
    tokio::sync::broadcast::Sender<()>,
    tokio::sync::broadcast::Receiver<()>,
) {
    tokio::sync::broadcast::channel(1)
}

// TCP server that handles gossip protocol
pub struct GossipTcpServer {
    config: Config,
    gossip: Addr<GossipActor>,
    shutdown: Option<tokio::sync::broadcast::Receiver<()>>,
}

impl GossipTcpServer {
    pub fn new(
        config: Config,
        gossip: Addr<GossipActor>,
        shutdown: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> Self {
        Self {
            config,
            gossip,
            shutdown,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        tracing::info!("Gossip TCP server listening on {}", addr);

        let connections = Arc::new(Mutex::new(HashMap::new()));

        let mut shutdown_receiver = self.shutdown.take();

        loop {
            let accept_result = if let Some(ref mut shutdown) = shutdown_receiver {
                tokio::select! {
                    result = listener.accept() => Some(result),
                    _ = shutdown.recv() => {
                        tracing::info!("Shutdown signal received, stopping server");
                        break;
                    }
                }
            } else {
                Some(listener.accept().await)
            };

            if let Some(result) = accept_result {
                let (socket, peer_addr) = match result {
                    Ok(accept) => accept,
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                        continue;
                    }
                };

                tracing::info!("Connection accepted from {}", peer_addr);

                let gossip = self.gossip.clone();
                let connections_clone = connections.clone();

                let connection_shutdown = if let Some(ref shutdown) = shutdown_receiver {
                    Some(shutdown.resubscribe())
                } else {
                    None
                };

                tokio::spawn(async move {
                    {
                        let mut conns = connections_clone.lock().await;
                        conns.insert(peer_addr, ());
                    }

                    if let Err(e) = handle_connection(socket, gossip, connection_shutdown).await {
                        tracing::error!("Error handling connection from {}: {}", peer_addr, e);
                    }

                    {
                        let mut conns = connections_clone.lock().await;
                        conns.remove(&peer_addr);
                    }
                });
            }
        }

        tracing::info!("Waiting for all connections to complete");
        while !connections.lock().await.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        tracing::info!("Server gracefully shut down");
        Ok(())
    }
}

// Client for sending TCP gossip messages
pub struct GossipTcpClient {
    peer: String,
    connection: Arc<Mutex<Option<TcpStream>>>,
}

impl GossipTcpClient {
    pub fn new(peer: String) -> Self {
        Self {
            peer,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    async fn ensure_connected(&self) -> Result<()> {
        let mut conn = self.connection.lock().await;
        if conn.is_none() {
            *conn = Some(TcpStream::connect(&self.peer).await?);
        }
        Ok(())
    }

    pub async fn send_command(&self, command: GossipCommand) -> Result<GossipResponse> {
        self.ensure_connected().await?;

        let command_bytes = serde_json::to_vec(&command)?;
        let command_len = command_bytes.len() as u32;

        let mut conn_guard = self.connection.lock().await;
        let conn = conn_guard
            .as_mut()
            .ok_or_else(|| anyhow!("No connection"))?;

        conn.write_all(&command_len.to_be_bytes()).await?;
        conn.write_all(&command_bytes).await?;

        let mut len_bytes = [0u8; LENGTH_PREFIX_SIZE];
        match conn.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) => {
                *conn_guard = None;
                return Err(anyhow!("Failed to read response length: {}", e));
            }
        }

        let response_len = u32::from_be_bytes(len_bytes) as usize;
        if response_len > 10_000_000 {
            return Err(anyhow!("Message too large: {} bytes", response_len));
        }

        let mut response_buf = vec![0u8; response_len];

        match conn.read_exact(&mut response_buf).await {
            Ok(_) => {}
            Err(e) => {
                *conn_guard = None;
                return Err(anyhow!("Failed to read response: {}", e));
            }
        }

        let response: GossipResponse = serde_json::from_slice(&response_buf)?;
        Ok(response)
    }

    // Send a gossip message
    pub async fn send_gossip(&self, message: GossipMessage) -> Result<GossipMessage> {
        let command = GossipCommand::Gossip(message);
        let response = self.send_command(command).await?;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::GossipMessage(msg))) => Ok(msg),
            GossipResponse::Error { message } => Err(anyhow!("Gossip error: {}", message)),
            _ => Err(anyhow!("Unexpected response type")),
        }
    }

    // Check if peer is alive
    pub async fn ping(&self) -> Result<bool> {
        if self.ensure_connected().await.is_err() {
            return Ok(false);
        }

        let ping = GossipMessage::Ping;
        match self.send_gossip(ping).await {
            Ok(_) => Ok(true),
            Err(_) => {
                let mut conn = self.connection.lock().await;
                *conn = None;
                Ok(false)
            }
        }
    }

    // Perform a quorum get operation directly
    pub async fn quorum_get(&self, key: String) -> Result<GossipResponseData> {
        let command = GossipCommand::QuorumGet { key };
        let response = self.send_command(command).await?;

        match response {
            GossipResponse::Ok(Some(data)) => Ok(data),
            GossipResponse::Error { message } => Err(anyhow!("Quorum get error: {}", message)),
            _ => Err(anyhow!("Unexpected response type")),
        }
    }

    // Perform a quorum set operation directly
    pub async fn quorum_set(&self, key: String, value: String) -> Result<GossipResponseData> {
        let command = GossipCommand::QuorumSet { key, value };
        let response = self.send_command(command).await?;

        match response {
            GossipResponse::Ok(Some(data)) => Ok(data),
            GossipResponse::Error { message } => Err(anyhow!("Quorum set error: {}", message)),
            _ => Err(anyhow!("Unexpected response type")),
        }
    }
}

impl Clone for GossipTcpClient {
    fn clone(&self) -> Self {
        Self {
            peer: self.peer.clone(),
            connection: Arc::clone(&self.connection),
        }
    }
}

// Handle a client connection
async fn handle_connection(
    mut socket: TcpStream,
    gossip: Addr<GossipActor>,
    mut shutdown: Option<tokio::sync::broadcast::Receiver<()>>,
) -> Result<()> {
    let (mut reader, mut writer) = socket.split();
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let mut len_bytes = [0u8; LENGTH_PREFIX_SIZE];

        let read_result = if let Some(ref mut shutdown_signal) = shutdown {
            tokio::select! {
                result = reader.read_exact(&mut len_bytes) => Some(result),
                _ = shutdown_signal.recv() => {
                    tracing::debug!("Connection received shutdown signal");
                    break;
                }
            }
        } else {
            Some(reader.read_exact(&mut len_bytes).await)
        };

        if let Some(read_result) = read_result {
            match read_result {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    return Err(anyhow!("Failed to read message length: {}", e));
                }
            }

            let message_len = u32::from_be_bytes(len_bytes) as usize;
            if message_len > 10_000_000 {
                return Err(anyhow!("Message too large: {} bytes", message_len));
            }

            if buffer.len() < message_len {
                buffer.resize(message_len, 0);
            }

            match reader.read_exact(&mut buffer[..message_len]).await {
                Ok(_) => {}
                Err(e) => {
                    return Err(anyhow!("Failed to read message content: {}", e));
                }
            }

            let command: GossipCommand = match serde_json::from_slice(&buffer[..message_len]) {
                Ok(cmd) => cmd,
                Err(e) => {
                    let error_response = GossipResponse::Error {
                        message: format!("Invalid command format: {}", e),
                    };

                    send_response(&mut writer, &error_response).await?;
                    continue;
                }
            };

            let response = process_command(command, &gossip).await;

            send_response(&mut writer, &response).await?;
        }
    }

    Ok(())
}

// Send a response back to the client
async fn send_response(
    writer: &mut tokio::net::tcp::WriteHalf<'_>,
    response: &GossipResponse,
) -> Result<()> {
    let response_bytes = serde_json::to_vec(response)?;
    let response_len = response_bytes.len() as u32;

    writer.write_all(&response_len.to_be_bytes()).await?;
    writer.write_all(&response_bytes).await?;

    Ok(())
}

// Process a command and generate a response
async fn process_command(command: GossipCommand, gossip: &Addr<GossipActor>) -> GossipResponse {
    match command {
        GossipCommand::Gossip(message) => match gossip.send(ProcessGossip { message }).await {
            Ok(Ok(response)) => {
                GossipResponse::Ok(Some(GossipResponseData::GossipMessage(response)))
            }
            Ok(Err(e)) => GossipResponse::Error {
                message: format!("Gossip processing error: {:?}", e),
            },
            Err(_) => GossipResponse::Error {
                message: "Gossip actor error".to_string(),
            },
        },

        GossipCommand::Get { key } => match gossip.send(QuorumRead { key: key.clone() }).await {
            Ok(Ok(result)) => GossipResponse::Ok(Some(GossipResponseData::Value {
                key: result.key,
                value: result.value,
                found: result.found,
                vector_clock: result.vector_clock,
            })),
            Ok(Err(e)) => GossipResponse::Error {
                message: format!("Quorum read error: {}", e),
            },
            Err(_) => GossipResponse::Error {
                message: "Gossip actor error".to_string(),
            },
        },

        GossipCommand::Set { key, value } => {
            match gossip
                .send(QuorumWrite {
                    key: key.clone(),
                    value: value.clone(),
                })
                .await
            {
                Ok(Ok(result)) => GossipResponse::Ok(Some(GossipResponseData::SetResult {
                    key: result.key,
                    value: result.value,
                    vector_clock: result.vector_clock,
                })),
                Ok(Err(e)) => GossipResponse::Error {
                    message: format!("Quorum write error: {}", e),
                },
                Err(_) => GossipResponse::Error {
                    message: "Gossip actor error".to_string(),
                },
            }
        }

        GossipCommand::QuorumGet { key } => {
            match gossip.send(QuorumRead { key: key.clone() }).await {
                Ok(Ok(result)) => GossipResponse::Ok(Some(GossipResponseData::QuorumReadResult {
                    key: result.key,
                    value: result.value,
                    found: result.found,
                    successful_nodes: result.successful_nodes,
                    failed_nodes: result.failed_nodes,
                    vector_clock: result.vector_clock,
                })),
                Ok(Err(e)) => GossipResponse::Error {
                    message: format!("Quorum read error: {}", e),
                },
                Err(_) => GossipResponse::Error {
                    message: "Gossip actor error".to_string(),
                },
            }
        }

        GossipCommand::QuorumSet { key, value } => {
            match gossip
                .send(QuorumWrite {
                    key: key.clone(),
                    value: value.clone(),
                })
                .await
            {
                Ok(Ok(result)) => GossipResponse::Ok(Some(GossipResponseData::QuorumWriteResult {
                    key: result.key,
                    value: result.value,
                    successful_nodes: result.successful_nodes,
                    failed_nodes: result.failed_nodes,
                    vector_clock: result.vector_clock,
                })),
                Ok(Err(e)) => GossipResponse::Error {
                    message: format!("Quorum write error: {}", e),
                },
                Err(_) => GossipResponse::Error {
                    message: "Gossip actor error".to_string(),
                },
            }
        }

        GossipCommand::GetPeers => match gossip.send(GetActivePeers).await {
            Ok(peers) => GossipResponse::Ok(Some(GossipResponseData::PeerList { peers })),
            Err(_) => GossipResponse::Error {
                message: "Gossip actor error".to_string(),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::gossip::actor::GossipActor;
    use crate::gossip::{GossipCommand, GossipMessage, GossipResponse, GossipResponseData};
    use crate::kvstore::KVStoreActor;
    use actix::Actor;
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    // Helper function to create a test config
    fn create_test_config(node_id: &str, port: u16) -> Config {
        let mut peers = HashSet::new();

        // Add other test nodes as peers
        for i in 1..4 {
            let peer_id = format!("node-{}", i);
            if peer_id != node_id {
                peers.insert(format!("{}@127.0.0.1:{}", peer_id, 8000 + i));
            }
        }

        Config {
            node_id: node_id.to_string(),
            host: "127.0.0.1".to_string(),
            port,
            peers,
            replication_factor: 3,
            read_quorum: 2,
            write_quorum: 2,
        }
    }

    // Test that the server starts and stops correctly
    #[actix_rt::test]
    async fn test_server_start_stop() {
        let config = create_test_config("node-test", 8123);

        let kvstore = KVStoreActor::new("node-test".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            config.clone(),
            kvstore_addr.clone(),
            config.replication_factor,
            config.read_quorum,
            config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(config.clone(), gossip_addr.clone(), Some(shutdown_rx));

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let connect_result = TcpStream::connect("127.0.0.1:8123").await;
        assert!(connect_result.is_ok(), "Failed to connect to server");

        let _ = shutdown_tx.send(());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let timeout_duration = Duration::from_millis(500);
        let connect_result = timeout(timeout_duration, TcpStream::connect("127.0.0.1:8123")).await;

        assert!(connect_result.is_err() || connect_result.unwrap().is_err());

        let _ = server_handle.await;
    }

    // Test the GossipTcpClient
    #[actix_rt::test]
    async fn test_tcp_client_basic() {
        let server_config = create_test_config("node-server", 8124);

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new("127.0.0.1:8124".to_string());

        let ping_result = client.ping().await;
        assert!(ping_result.is_ok());
        assert!(ping_result.unwrap());

        let gossip_result = client.send_gossip(GossipMessage::Ping).await;
        assert!(gossip_result.is_ok());

        match gossip_result.unwrap() {
            GossipMessage::Pong { sender, members } => {
                assert_eq!(sender, "127.0.0.1:8124");
                assert!(members.len() > 0);
            }
            _ => panic!("Expected Pong message"),
        }

        let _ = shutdown_tx.send(());

        let _ = server_handle.await;
    }

    // Test basic key-value operations over TCP
    #[actix_rt::test]
    async fn test_tcp_key_value_ops() {
        let mut server_config = create_test_config("node-server", 8125);
        server_config.replication_factor = 1;
        server_config.read_quorum = 1;
        server_config.write_quorum = 1;

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new("127.0.0.1:8125".to_string());

        let set_cmd = GossipCommand::Set {
            key: "test-key".to_string(),
            value: "test-value".to_string(),
        };

        let set_result = client.send_command(set_cmd).await;
        assert!(set_result.is_ok());

        match set_result.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::SetResult { key, value, .. })) => {
                assert_eq!(key, "test-key");
                assert_eq!(value, "test-value");
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let get_cmd = GossipCommand::Get {
            key: "test-key".to_string(),
        };
        let get_result = client.send_command(get_cmd).await;
        assert!(get_result.is_ok());

        match get_result.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::Value {
                key, value, found, ..
            })) => {
                assert_eq!(key, "test-key");
                assert_eq!(value, Some("test-value".to_string()));
                assert!(found);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let get_missing_cmd = GossipCommand::Get {
            key: "missing-key".to_string(),
        };
        let get_missing_result = client.send_command(get_missing_cmd).await;
        assert!(get_missing_result.is_ok());

        match get_missing_result.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::Value {
                key, value, found, ..
            })) => {
                assert_eq!(key, "missing-key");
                assert_eq!(value, None);
                assert!(!found);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let _ = shutdown_tx.send(());

        let _ = server_handle.await;
    }

    // Test quorum operations over TCP
    #[actix_rt::test]
    async fn test_tcp_quorum_ops() {
        let mut server_config = create_test_config("node-server", 8126);
        server_config.replication_factor = 1;
        server_config.read_quorum = 1;
        server_config.write_quorum = 1;

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new("127.0.0.1:8126".to_string());

        let set_result = client
            .quorum_set("quorum-key".to_string(), "quorum-value".to_string())
            .await;
        assert!(set_result.is_ok());

        match set_result.unwrap() {
            GossipResponseData::QuorumWriteResult {
                key,
                value,
                successful_nodes,
                ..
            } => {
                assert_eq!(key, "quorum-key");
                assert_eq!(value, "quorum-value");
                assert!(successful_nodes.contains(&"node-server".to_string()));
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let get_result = client.quorum_get("quorum-key".to_string()).await;
        assert!(get_result.is_ok());

        match get_result.unwrap() {
            GossipResponseData::QuorumReadResult {
                key,
                value,
                found,
                successful_nodes,
                ..
            } => {
                assert_eq!(key, "quorum-key");
                assert_eq!(value, Some("quorum-value".to_string()));
                assert!(found);
                assert!(successful_nodes.contains(&"node-server".to_string()));
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let _ = shutdown_tx.send(());

        let _ = server_handle.await;
    }

    // Test error handling
    #[actix_rt::test]
    async fn test_tcp_error_handling() {
        let client = GossipTcpClient::new("127.0.0.1:9999".to_string());

        let ping_result = client.ping().await;
        assert!(ping_result.is_ok());
        assert!(!ping_result.unwrap());

        let cmd = GossipCommand::Get {
            key: "test-key".to_string(),
        };
        let cmd_result = client.send_command(cmd).await;
        assert!(cmd_result.is_err());
    }

    // Test the GetPeers command
    #[actix_rt::test]
    async fn test_tcp_get_peers() {
        let server_config = create_test_config("node-server", 8127);

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new("127.0.0.1:8127".to_string());

        let cmd = GossipCommand::GetPeers;
        let result = client.send_command(cmd).await;
        assert!(result.is_ok());

        match result.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::PeerList { peers })) => {
                assert_eq!(peers.len(), 3); // Should have 3 peers from our test config
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let _ = shutdown_tx.send(());

        let _ = server_handle.await;
    }

    // Test TCP connection timeout and reconnection
    #[actix_rt::test]
    async fn test_tcp_reconnection() {
        let server_config = create_test_config("node-server", 8129);

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let client = GossipTcpClient::new("127.0.0.1:8129".to_string());

        let initial_ping = client.ping().await;
        assert!(initial_ping.is_ok());
        assert!(!initial_ping.unwrap());

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let reconnect_ping = client.ping().await;
        assert!(reconnect_ping.is_ok());
        assert!(reconnect_ping.unwrap());

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    // Test command timeout/error handling
    #[actix_rt::test]
    async fn test_tcp_command_timeout() {
        let server_config = create_test_config("node-server", 8130);

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new("127.0.0.1:8130".to_string());

        let cmd_future = client.send_command(GossipCommand::Get {
            key: "test-key".to_string(),
        });
        let _ = shutdown_tx.send(());

        // This could either error or succeed depending on timing
        // Just check that it doesn't hang/crash
        let _ = tokio::time::timeout(Duration::from_millis(500), cmd_future).await;

        let _ = server_handle.await;
    }

    // Test handling of invalid messages
    #[actix_rt::test]
    async fn test_tcp_invalid_message() {
        let server_config = create_test_config("node-server", 8131);

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut stream = TcpStream::connect("127.0.0.1:8131").await.unwrap();

        // Send a valid length prefix but invalid JSON data
        let invalid_data = b"not valid json data";
        let length = (invalid_data.len() as u32).to_be_bytes();

        stream.write_all(&length).await.unwrap();
        stream.write_all(invalid_data).await.unwrap();

        let mut len_buf = [0u8; LENGTH_PREFIX_SIZE];
        let read_result = stream.read_exact(&mut len_buf).await;
        assert!(read_result.is_ok());

        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        let read_result = stream.read_exact(&mut resp_buf).await;
        assert!(read_result.is_ok());

        let response: GossipResponse = serde_json::from_slice(&resp_buf).unwrap();

        match response {
            GossipResponse::Error { message } => {
                assert!(message.contains("Invalid command format"));
            }
            _ => panic!("Expected error response for invalid message"),
        }

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    // Test handling of very large messages
    #[actix_rt::test]
    async fn test_tcp_large_message() {
        let mut server_config = create_test_config("node-server", 8133);
        server_config.replication_factor = 1;
        server_config.read_quorum = 1;
        server_config.write_quorum = 1;

        let kvstore = KVStoreActor::new("node-server".to_string());
        let kvstore_addr = kvstore.start();

        let gossip = GossipActor::new(
            server_config.clone(),
            kvstore_addr.clone(),
            server_config.replication_factor,
            server_config.read_quorum,
            server_config.write_quorum,
        );
        let gossip_addr = gossip.start();

        let (shutdown_tx, shutdown_rx) = create_shutdown_channel();

        let server = GossipTcpServer::new(
            server_config.clone(),
            gossip_addr.clone(),
            Some(shutdown_rx),
        );

        let server_handle = tokio::spawn(async move {
            let _ = server.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new("127.0.0.1:8133".to_string());

        let large_value = "x".repeat(100_000); // 100 KB string

        let set_cmd = GossipCommand::Set {
            key: "large-key".to_string(),
            value: large_value.clone(),
        };

        let set_result = client.send_command(set_cmd).await;
        assert!(set_result.is_ok());

        match set_result.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::SetResult { key, value, .. })) => {
                assert_eq!(key, "large-key");
                assert_eq!(value.len(), large_value.len());
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let get_cmd = GossipCommand::Get {
            key: "large-key".to_string(),
        };
        let get_result = client.send_command(get_cmd).await;
        assert!(get_result.is_ok());

        match get_result.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::Value {
                key, value, found, ..
            })) => {
                assert_eq!(key, "large-key");
                assert!(found);
                assert_eq!(value.unwrap().len(), large_value.len());
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let mut stream = TcpStream::connect("127.0.0.1:8133").await.unwrap();

        let too_large_len = (20_000_000u32).to_be_bytes(); // 20 MB (over the 10 MB limit)
        stream.write_all(&too_large_len).await.unwrap();

        let mut buf = [0u8; 1];
        let read_result = stream.read_exact(&mut buf).await;
        assert!(read_result.is_err());

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }
}
