use crate::config::Config;
use crate::gossip::actor::{GetActivePeers, GossipActor, ProcessGossip};
use crate::gossip::{GossipCommand, GossipMessage, GossipResponse, GossipResponseData};
use crate::kvstore::{Get, KVStoreActor, Set};

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
    kvstore: Addr<KVStoreActor>,
    gossip: Addr<GossipActor>,
    shutdown: Option<tokio::sync::broadcast::Receiver<()>>,
}

impl GossipTcpServer {
    pub fn new(
        config: Config,
        kvstore: Addr<KVStoreActor>,
        gossip: Addr<GossipActor>,
        shutdown: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> Self {
        Self {
            config,
            kvstore,
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

                let kvstore = self.kvstore.clone();
                let gossip = self.gossip.clone();
                let config = self.config.clone();
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

                    if let Err(e) =
                        handle_connection(socket, kvstore, gossip, config, connection_shutdown)
                            .await
                    {
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
        if let Err(_) = self.ensure_connected().await {
            return Ok(false);
        }

        let ping = GossipMessage::Ping;
        match self.send_gossip(ping).await {
            Ok(_) => Ok(true),
            Err(_) => {
                // Reset connection on error
                let mut conn = self.connection.lock().await;
                *conn = None;
                Ok(false)
            }
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
    kvstore: Addr<KVStoreActor>,
    gossip: Addr<GossipActor>,
    config: Config,
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

            let response = process_command(command, &kvstore, &gossip, &config).await;

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
async fn process_command(
    command: GossipCommand,
    kvstore: &Addr<KVStoreActor>,
    gossip: &Addr<GossipActor>,
    config: &Config,
) -> GossipResponse {
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

        GossipCommand::Get { key } => match kvstore.send(Get { key: key.clone() }).await {
            Ok(Some(versioned)) => GossipResponse::Ok(Some(GossipResponseData::Value {
                key: key.clone(),
                value: Some(versioned.value),
                found: true,
            })),
            Ok(None) => GossipResponse::Ok(Some(GossipResponseData::Value {
                key: key.clone(),
                value: None,
                found: false,
            })),
            Err(_) => GossipResponse::Error {
                message: "KVStore actor error".to_string(),
            },
        },

        GossipCommand::Set { key, value } => {
            match kvstore
                .send(Set {
                    key: key.clone(),
                    value: value.clone(),
                    node_id: config.node_id.clone(),
                })
                .await
            {
                Ok(versioned) => GossipResponse::Ok(Some(GossipResponseData::SetResult {
                    key: key.clone(),
                    value: versioned.value,
                    timestamp: versioned.timestamp,
                })),
                Err(_) => GossipResponse::Error {
                    message: "KVStore actor error".to_string(),
                },
            }
        }

        GossipCommand::GetPeers => match gossip.send(GetActivePeers).await {
            Ok(peers) => GossipResponse::Ok(Some(GossipResponseData::PeerList { peers })),
            Err(_) => GossipResponse::Error {
                message: "Gossip actor error".to_string(),
            },
        },

        GossipCommand::Health => GossipResponse::Ok(Some(GossipResponseData::HealthInfo {
            status: "ok".to_string(),
            node_id: config.node_id.clone(),
            endpoint: config.endpoint(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::gossip::actor::GossipActor;
    use crate::gossip::tcp::{
        GossipTcpClient, GossipTcpServer, create_shutdown_channel, process_command,
    };
    use crate::gossip::{GossipCommand, GossipMessage, GossipResponse, GossipResponseData};
    use crate::kvstore::{Get, KVStoreActor, Set, VersionedValue};
    use actix::Actor;
    use actix_rt::test as actix_test;
    use std::time::Duration;
    use tokio::time::sleep;

    // Helper function to create a test config
    fn create_test_config(node_id: &str, port: u16, peers: Vec<String>) -> Config {
        Config {
            node_id: node_id.to_string(),
            host: "127.0.0.1".to_string(),
            port,
            peers: peers.into_iter().collect(),
        }
    }

    #[actix_test]
    async fn test_process_command_get() {
        let kvstore = KVStoreActor::new().start();

        kvstore
            .send(Set {
                key: "test-key".to_string(),
                value: "test-value".to_string(),
                node_id: "test-node".to_string(),
            })
            .await
            .unwrap();

        let config = create_test_config("test-node", 8000, vec![]);
        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let command = GossipCommand::Get {
            key: "test-key".to_string(),
        };

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::Value { key, value, found })) => {
                assert_eq!(key, "test-key");
                assert_eq!(value, Some("test-value".to_string()));
                assert!(found);
            }
            _ => panic!("Unexpected response: {:?}", response),
        }

        let command = GossipCommand::Get {
            key: "non-existent".to_string(),
        };

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::Value { key, value, found })) => {
                assert_eq!(key, "non-existent");
                assert_eq!(value, None);
                assert!(!found);
            }
            _ => panic!("Unexpected response: {:?}", response),
        }
    }

    #[actix_test]
    async fn test_process_command_set() {
        let kvstore = KVStoreActor::new().start();

        let config = create_test_config("test-node", 8000, vec![]);
        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let command = GossipCommand::Set {
            key: "test-set-key".to_string(),
            value: "test-set-value".to_string(),
        };

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::SetResult {
                key,
                value,
                timestamp,
            })) => {
                assert_eq!(key, "test-set-key");
                assert_eq!(value, "test-set-value");
                assert!(timestamp > 0); // Timestamp should be set
            }
            _ => panic!("Unexpected response: {:?}", response),
        }

        let get_response = kvstore
            .send(Get {
                key: "test-set-key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_response.is_some());
        assert_eq!(get_response.unwrap().value, "test-set-value");
    }

    #[actix_test]
    async fn test_process_command_get_peers() {
        let kvstore = KVStoreActor::new().start();

        let peers = vec!["127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string()];
        let config = create_test_config("test-node", 8000, peers);
        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let command = GossipCommand::GetPeers;

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::PeerList { peers })) => {
                assert_eq!(peers.len(), 2);
                assert!(peers.contains(&"127.0.0.1:8001".to_string()));
                assert!(peers.contains(&"127.0.0.1:8002".to_string()));
            }
            _ => panic!("Unexpected response: {:?}", response),
        }
    }

    #[actix_test]
    async fn test_process_command_health() {
        let kvstore = KVStoreActor::new().start();

        let config = create_test_config("test-health-node", 8003, vec![]);
        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let command = GossipCommand::Health;

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::HealthInfo {
                status,
                node_id,
                endpoint,
            })) => {
                assert_eq!(status, "ok");
                assert_eq!(node_id, "test-health-node");
                assert_eq!(endpoint, "127.0.0.1:8003");
            }
            _ => panic!("Unexpected response: {:?}", response),
        }
    }

    #[actix_test]
    async fn test_process_command_gossip_ping() {
        let kvstore = KVStoreActor::new().start();

        let peers = vec!["127.0.0.1:8001".to_string(), "127.0.0.1:8002".to_string()];
        let config = create_test_config("test-node", 8000, peers);
        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let ping_msg = GossipMessage::Ping;
        let command = GossipCommand::Gossip(ping_msg);

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::GossipMessage(GossipMessage::Pong {
                sender,
                members,
            }))) => {
                assert_eq!(sender, "127.0.0.1:8000");
                assert_eq!(members.len(), 2);
                assert!(members.contains(&"127.0.0.1:8001".to_string()));
                assert!(members.contains(&"127.0.0.1:8002".to_string()));
            }
            _ => panic!("Unexpected response: {:?}", response),
        }
    }

    #[actix_test]
    async fn test_process_command_gossip_update() {
        let kvstore = KVStoreActor::new().start();

        let config = create_test_config("test-node", 8000, vec![]);
        let gossip = GossipActor::new(config.clone(), kvstore.clone()).start();

        let mut updates = hashbrown::HashMap::new();
        updates.insert(
            "gossip-key".to_string(),
            VersionedValue {
                value: "gossip-value".to_string(),
                timestamp: 1000,
                node_id: "gossip-node".to_string(),
            },
        );
        let update_msg = GossipMessage::Update { updates };
        let command = GossipCommand::Gossip(update_msg);

        let response = process_command(command, &kvstore, &gossip, &config).await;

        match response {
            GossipResponse::Ok(Some(GossipResponseData::GossipMessage(GossipMessage::Ping))) => (),
            _ => panic!("Unexpected response: {:?}", response),
        }

        let get_response = kvstore
            .send(Get {
                key: "gossip-key".to_string(),
            })
            .await
            .unwrap();

        assert!(get_response.is_some());
        assert_eq!(get_response.unwrap().value, "gossip-value");
    }

    #[actix_test]
    async fn test_tcp_client_server_integration() {
        const SERVER_PORT: u16 = 9876;

        let server_config = create_test_config("server-node", SERVER_PORT, vec![]);
        let server_kvstore = KVStoreActor::new().start();

        let (shutdown_sender, shutdown_receiver) = create_shutdown_channel();

        server_kvstore
            .send(Set {
                key: "server-key".to_string(),
                value: "server-value".to_string(),
                node_id: "server-node".to_string(),
            })
            .await
            .unwrap();

        let server_gossip = GossipActor::new(server_config.clone(), server_kvstore.clone()).start();
        let server = GossipTcpServer::new(
            server_config.clone(),
            server_kvstore.clone(),
            server_gossip.clone(),
            Some(shutdown_receiver),
        );

        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                panic!("Server error: {:?}", e);
            }
        });

        sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new(format!("127.0.0.1:{}", SERVER_PORT));

        let get_command = GossipCommand::Get {
            key: "server-key".to_string(),
        };
        let response = client.send_command(get_command).await;
        assert!(response.is_ok());
        match response.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::Value { key, value, found })) => {
                assert_eq!(key, "server-key");
                assert_eq!(value, Some("server-value".to_string()));
                assert!(found);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        let set_command = GossipCommand::Set {
            key: "client-key".to_string(),
            value: "client-value".to_string(),
        };
        let response = client.send_command(set_command).await;
        assert!(response.is_ok());

        let ping_result = client.ping().await;
        assert!(ping_result.is_ok());
        assert!(ping_result.unwrap());

        shutdown_sender.send(()).unwrap();

        sleep(Duration::from_millis(500)).await;

        let ping_result = client.ping().await;
        println!("Ping result after shutdown: {:?}", ping_result);

        assert!(ping_result.is_ok());
        assert!(!ping_result.unwrap());
    }

    #[actix_test]
    async fn test_client_nonexistent_server() {
        const UNUSED_PORT: u16 = 49999;

        let client = GossipTcpClient::new(format!("127.0.0.1:{}", UNUSED_PORT));

        let ping_result = client.ping().await;
        assert!(ping_result.is_ok());
        assert!(!ping_result.unwrap());

        let get_command = GossipCommand::Get {
            key: "some-key".to_string(),
        };
        let result = client.send_command(get_command).await;
        assert!(result.is_err());
    }

    #[actix_test]
    async fn test_multiple_clients() {
        const SERVER_PORT: u16 = 9877;

        let server_config = create_test_config("multi-client-server", SERVER_PORT, vec![]);
        let server_kvstore = KVStoreActor::new().start();
        let (shutdown_sender, shutdown_receiver) = create_shutdown_channel();

        let server_gossip = GossipActor::new(server_config.clone(), server_kvstore.clone()).start();
        let server = GossipTcpServer::new(
            server_config.clone(),
            server_kvstore.clone(),
            server_gossip.clone(),
            Some(shutdown_receiver),
        );

        tokio::spawn(async move {
            if let Err(e) = server.run().await {
                panic!("Server error: {:?}", e);
            }
        });

        sleep(Duration::from_millis(100)).await;

        let mut client1 = GossipTcpClient::new(format!("127.0.0.1:{}", SERVER_PORT));
        let mut client2 = GossipTcpClient::new(format!("127.0.0.1:{}", SERVER_PORT));
        let mut client3 = GossipTcpClient::new(format!("127.0.0.1:{}", SERVER_PORT));

        // Each client sets a value
        let set_command1 = GossipCommand::Set {
            key: "client1-key".to_string(),
            value: "client1-value".to_string(),
        };
        let response1 = client1.send_command(set_command1).await;
        assert!(response1.is_ok());

        let set_command2 = GossipCommand::Set {
            key: "client2-key".to_string(),
            value: "client2-value".to_string(),
        };
        let response2 = client2.send_command(set_command2).await;
        assert!(response2.is_ok());

        let set_command3 = GossipCommand::Set {
            key: "client3-key".to_string(),
            value: "client3-value".to_string(),
        };
        let response3 = client3.send_command(set_command3).await;
        assert!(response3.is_ok());

        // Each client reads all values
        for (client_num, client) in [&mut client1, &mut client2, &mut client3]
            .iter_mut()
            .enumerate()
        {
            for i in 1..=3 {
                let get_command = GossipCommand::Get {
                    key: format!("client{}-key", i),
                };
                let response = client.send_command(get_command).await;
                assert!(response.is_ok());

                match response.unwrap() {
                    GossipResponse::Ok(Some(GossipResponseData::Value { key, value, found })) => {
                        assert_eq!(key, format!("client{}-key", i));
                        assert_eq!(value, Some(format!("client{}-value", i)));
                        assert!(found);
                    }
                    other => panic!(
                        "Client {} unexpected response reading key client{}-key: {:?}",
                        client_num + 1,
                        i,
                        other
                    ),
                }
            }
        }

        shutdown_sender.send(()).unwrap();
    }

    // Test client reconnection behavior
    #[actix_test]
    async fn test_client_reconnection() {
        const SERVER_PORT: u16 = 9878;

        // Start the first server instance
        let server_config = create_test_config("reconnect-test", SERVER_PORT, vec![]);
        let server_kvstore = KVStoreActor::new().start();
        let (shutdown_sender1, shutdown_receiver1) = create_shutdown_channel();

        let server_gossip = GossipActor::new(server_config.clone(), server_kvstore.clone()).start();
        let server1 = GossipTcpServer::new(
            server_config.clone(),
            server_kvstore.clone(),
            server_gossip.clone(),
            Some(shutdown_receiver1),
        );

        tokio::spawn(async move {
            if let Err(e) = server1.run().await {
                panic!("Server 1 error: {:?}", e);
            }
        });

        sleep(Duration::from_millis(100)).await;

        let client = GossipTcpClient::new(format!("127.0.0.1:{}", SERVER_PORT));

        // Set a value on the first server
        let set_command = GossipCommand::Set {
            key: "reconnect-key".to_string(),
            value: "initial-value".to_string(),
        };
        let response = client.send_command(set_command).await;
        assert!(response.is_ok());

        shutdown_sender1.send(()).unwrap();
        sleep(Duration::from_millis(500)).await;

        // Verify client detects the server is down
        let ping_result = client.ping().await;
        assert!(ping_result.is_ok());
        assert!(!ping_result.unwrap());

        // Start a second server instance on the same port
        let (shutdown_sender2, shutdown_receiver2) = create_shutdown_channel();

        // Create a new KVStore for the second server
        let server_kvstore2 = KVStoreActor::new().start();

        // Set the initial data in the new KVStore
        server_kvstore2
            .send(Set {
                key: "reconnect-key".to_string(),
                value: "updated-value".to_string(),
                node_id: "server2".to_string(),
            })
            .await
            .unwrap();

        let server_gossip2 =
            GossipActor::new(server_config.clone(), server_kvstore2.clone()).start();
        let server2 = GossipTcpServer::new(
            server_config.clone(),
            server_kvstore2.clone(),
            server_gossip2.clone(),
            Some(shutdown_receiver2),
        );

        tokio::spawn(async move {
            if let Err(e) = server2.run().await {
                panic!("Server 2 error: {:?}", e);
            }
        });

        sleep(Duration::from_millis(500)).await;

        // Client should reconnect to the new server
        let ping_result = client.ping().await;
        assert!(ping_result.is_ok());
        assert!(ping_result.unwrap());

        // Get the value from the second server
        let get_command = GossipCommand::Get {
            key: "reconnect-key".to_string(),
        };
        let response = client.send_command(get_command).await;
        assert!(response.is_ok());

        match response.unwrap() {
            GossipResponse::Ok(Some(GossipResponseData::Value { key, value, found })) => {
                assert_eq!(key, "reconnect-key");
                assert_eq!(value, Some("updated-value".to_string()));
                assert!(found);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Clean up
        shutdown_sender2.send(()).unwrap();
    }
}
