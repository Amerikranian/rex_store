use anyhow::{Context, Result, anyhow};
use clap::Parser;
use rustyline::Editor;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

// Constants for protocol
const LENGTH_PREFIX_SIZE: usize = 4;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Default server address to connect to
    #[clap(short, long, default_value = "127.0.0.1:8000")]
    server: String,

    /// Path to alias configuration file
    #[clap(short, long, default_value = "repl_aliases.json")]
    aliases_file: String,
}

// Connection pool to reuse connections
struct ConnectionPool {
    connections: HashMap<String, TcpStream>,
}

impl ConnectionPool {
    fn new() -> Self {
        ConnectionPool {
            connections: HashMap::new(),
        }
    }

    async fn get_connection(&mut self, server: &str) -> Result<&mut TcpStream> {
        if !self.connections.contains_key(server) {
            let stream = TcpStream::connect(server)
                .await
                .with_context(|| format!("Failed to connect to {}", server))?;
            self.connections.insert(server.to_string(), stream);
        }

        Ok(self.connections.get_mut(server).unwrap())
    }
}

// Alias manager to handle server aliases
struct AliasManager {
    aliases: HashMap<String, String>,
    aliases_file: PathBuf,
}

impl AliasManager {
    fn new(aliases_file: &str) -> Result<Self> {
        let aliases_file = PathBuf::from(aliases_file);
        let aliases = if aliases_file.exists() {
            let content = fs::read_to_string(&aliases_file)
                .with_context(|| format!("Failed to read aliases file: {:?}", aliases_file))?;
            serde_json::from_str(&content).unwrap_or_else(|_| HashMap::new())
        } else {
            HashMap::new()
        };

        Ok(AliasManager {
            aliases,
            aliases_file,
        })
    }

    fn add_alias(&mut self, alias: &str, server: &str) -> Result<()> {
        self.aliases.insert(alias.to_string(), server.to_string());
        self.save_aliases()
    }

    fn remove_alias(&mut self, alias: &str) -> Result<()> {
        if self.aliases.remove(alias).is_none() {
            return Err(anyhow!("Alias '{}' not found", alias));
        }
        self.save_aliases()
    }

    fn get_aliases(&self) -> &HashMap<String, String> {
        &self.aliases
    }

    fn resolve_alias(&self, server_or_alias: &str) -> String {
        self.aliases
            .get(server_or_alias)
            .cloned()
            .unwrap_or_else(|| server_or_alias.to_string())
    }

    fn save_aliases(&self) -> Result<()> {
        if let Some(parent) = self.aliases_file.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = serde_json::to_string_pretty(&self.aliases)?;
        fs::write(&self.aliases_file, content)
            .with_context(|| format!("Failed to write aliases file: {:?}", self.aliases_file))?;
        Ok(())
    }
}

// Executes a command by sending to server and reading response
async fn execute_command(stream: &mut TcpStream, command: Value) -> Result<Value> {
    let command_bytes = serde_json::to_vec(&command)?;
    let command_len = command_bytes.len() as u32;

    stream.write_all(&command_len.to_be_bytes()).await?;
    stream.write_all(&command_bytes).await?;

    // Read response length
    let mut len_bytes = [0u8; LENGTH_PREFIX_SIZE];
    stream.read_exact(&mut len_bytes).await?;
    let response_len = u32::from_be_bytes(len_bytes) as usize;

    // Read response
    let mut response_buf = vec![0u8; response_len];
    stream.read_exact(&mut response_buf).await?;

    let response: Value = serde_json::from_slice(&response_buf)?;
    Ok(response)
}

// Parse interactive command input
fn parse_interactive_command(
    input: &str,
    alias_manager: &AliasManager,
) -> Result<(Value, Option<String>)> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Err(anyhow!("Empty command"));
    }

    // Extract target server if specified with @ prefix
    let mut target_server = None;
    for part in &parts {
        if part.starts_with('@') {
            let server_or_alias = &part[1..];
            let resolved_server = alias_manager.resolve_alias(server_or_alias);
            target_server = Some(resolved_server);
            break;
        }
    }

    match parts[0].to_lowercase().as_str() {
        "get" => {
            if parts.len() < 2 {
                return Err(anyhow!("Usage: get <key>"));
            }
            Ok((json!({"Get": {"key": parts[1]}}), target_server))
        }
        "set" => {
            if parts.len() < 4 {
                return Err(anyhow!("Usage: set <key> <value> <node_id>"));
            }
            Ok((
                json!({"Set": {"key": parts[1], "value": parts[2], "node_id": parts[3]}}),
                target_server,
            ))
        }
        "peers" => Ok((json!("GetPeers"), target_server)),
        "health" => Ok((json!("Health"), target_server)),
        "ping" => {
            if parts.len() < 2 {
                return Err(anyhow!("Usage: ping <sender>"));
            }
            Ok((
                json!({"Gossip": {"Ping": {"sender": parts[1]}}}),
                target_server,
            ))
        }
        "alias" => {
            if parts.len() == 1 {
                // Special case to just show alias help
                println!("Alias commands:");
                println!("  alias list              - List all server aliases");
                println!("  alias add <name> <addr> - Add a server alias");
                println!("  alias remove <name>     - Remove a server alias");
                return Err(anyhow!("Alias help command"));
            }

            if parts.len() >= 2 {
                match parts[1].to_lowercase().as_str() {
                    "list" => {
                        // This will be handled in the REPL, not sent to server
                        return Ok((json!("AliasList"), None));
                    }
                    "add" => {
                        if parts.len() < 4 {
                            return Err(anyhow!("Usage: alias add <name> <server_addr>"));
                        }
                        // This will be handled in the REPL, not sent to server
                        return Ok((
                            json!({"AliasAdd": {"name": parts[2], "addr": parts[3]}}),
                            None,
                        ));
                    }
                    "remove" | "rm" | "delete" | "del" => {
                        if parts.len() < 3 {
                            return Err(anyhow!("Usage: alias remove <name>"));
                        }
                        // This will be handled in the REPL, not sent to server
                        return Ok((json!({"AliasRemove": {"name": parts[2]}}), None));
                    }
                    _ => return Err(anyhow!("Unknown alias subcommand: {}", parts[1])),
                }
            }

            Err(anyhow!("Invalid alias command"))
        }
        "help" => {
            println!("Available commands:");
            println!("  get <key> [@server]                - Get a value");
            println!("  set <key> <value> <node_id> [@server] - Set a value");
            println!("  peers [@server]                    - Get active peers");
            println!("  health [@server]                   - Health check");
            println!("  ping <sender> [@server]            - Ping a node");
            println!("  connect <server>                   - Connect to a new server");
            println!("  alias list                         - List all server aliases");
            println!("  alias add <name> <addr>            - Add a server alias");
            println!("  alias remove <name>                - Remove a server alias");
            println!("  exit                               - Exit the REPL");
            println!("  help                               - Show this help message");
            println!();
            println!(
                "You can specify a target server by adding @server or @alias (e.g. get key @node1)"
            );
            println!("If no server is specified, the default or last connected server is used");

            Err(anyhow!("Help command")) // Special case to not send anything
        }
        "connect" => {
            if parts.len() < 2 {
                return Err(anyhow!("Usage: connect <server>"));
            }

            // Check if an alias was provided
            let server_or_alias = parts[1];
            let resolved_server = alias_manager.resolve_alias(server_or_alias);

            // Just return the connection target, no command to send
            Ok((json!(null), Some(resolved_server)))
        }
        "exit" | "quit" => {
            Err(anyhow!("Exit command")) // Special case for exiting
        }
        _ => Err(anyhow!("Unknown command: {}", parts[0])),
    }
}

// REPL loop
async fn repl_loop(default_server: String, aliases_file: String) -> Result<()> {
    let mut rl = Editor::<(), DefaultHistory>::new()?;
    if let Err(_) = rl.load_history("repl_history.txt") {
        println!("No previous history.");
    }

    let pool = Arc::new(Mutex::new(ConnectionPool::new()));
    let mut current_server = default_server;

    let mut alias_manager = match AliasManager::new(&aliases_file) {
        Ok(am) => am,
        Err(e) => {
            println!("Warning: Failed to initialize alias manager: {}", e);
            println!("Continuing without alias support.");
            AliasManager::new("~/.node_repl_aliases_temp.json")?
        }
    };

    println!("Node REPL Client");
    println!("Type 'help' for available commands or 'exit' to quit");
    println!("Using default server: {}", current_server);

    let aliases = alias_manager.get_aliases();
    if !aliases.is_empty() {
        println!("Loaded {} server aliases:", aliases.len());
        for (alias, server) in aliases {
            println!("  {} -> {}", alias, server);
        }
    }

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }

                rl.add_history_entry(line.as_str())
                    .expect("Failed to add history");

                match parse_interactive_command(&line, &alias_manager) {
                    Ok((command, target_server)) => {
                        // Handle alias commands locally
                        if let Some(cmd_type) = command.as_str() {
                            if cmd_type == "AliasList" {
                                let aliases = alias_manager.get_aliases();
                                if aliases.is_empty() {
                                    println!(
                                        "No aliases defined. Use 'alias add <name> <addr>' to create one."
                                    );
                                } else {
                                    println!("Server aliases:");
                                    for (alias, server) in aliases {
                                        println!("  {} -> {}", alias, server);
                                    }
                                }
                                continue;
                            }
                        } else if let Some(alias_add) = command.get("AliasAdd") {
                            if let (Some(name), Some(addr)) = (
                                alias_add.get("name").and_then(Value::as_str),
                                alias_add.get("addr").and_then(Value::as_str),
                            ) {
                                match alias_manager.add_alias(name, addr) {
                                    Ok(_) => println!("Added alias: {} -> {}", name, addr),
                                    Err(e) => println!("Failed to add alias: {}", e),
                                }
                            }
                            continue;
                        } else if let Some(alias_remove) = command.get("AliasRemove") {
                            if let Some(name) = alias_remove.get("name").and_then(Value::as_str) {
                                match alias_manager.remove_alias(name) {
                                    Ok(_) => println!("Removed alias: {}", name),
                                    Err(e) => println!("Failed to remove alias: {}", e),
                                }
                            }
                            continue;
                        }

                        // If a new server is specified, update current_server
                        if let Some(server) = target_server {
                            current_server = server;
                            println!("Using server: {}", current_server);

                            if command == json!(null) {
                                match pool.lock().await.get_connection(&current_server).await {
                                    Ok(_) => println!("Connected to {}", current_server),
                                    Err(e) => println!("Failed to connect: {}", e),
                                }
                                continue;
                            }
                        }

                        match pool.lock().await.get_connection(&current_server).await {
                            Ok(stream) => match execute_command(stream, command).await {
                                Ok(response) => {
                                    println!("{}", serde_json::to_string_pretty(&response)?);
                                }
                                Err(e) => {
                                    println!("Error executing command: {}", e);
                                }
                            },
                            Err(e) => {
                                println!("Connection error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        if e.to_string() == "Exit command" {
                            println!("Goodbye!");
                            break;
                        } else if e.to_string() != "Help command"
                            && e.to_string() != "Alias help command"
                        {
                            println!("Error: {}", e);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history("repl_history.txt")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    repl_loop(args.server, args.aliases_file).await
}
