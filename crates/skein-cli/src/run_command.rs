use std::path::{Path, PathBuf};

use clap::Parser;
use skein_edl::EditOp;
use skein_store::{ChunkStore, LocalChunkStore};
use tokio::io::{AsyncBufReadExt, BufReader};
use uuid::Uuid;

use crate::error::CliError;
use crate::state::{CliState, execute_branch, execute_edit_trim, execute_root_hash};
use crate::{
    ActiveConnections, ActiveTransport, PeerId, background_task::spawn_message_receiver,
    print_statement,
};

#[non_exhaustive]
#[derive(Parser, Debug)]
#[command(name = "skein", multicall = true, disable_help_subcommand = true)]
pub enum Command {
    Connect {
        id: String,
    },

    #[command(name = "send", alias = "s")]
    Send {
        #[arg(trailing_var_arg = true, num_args = 1..)]
        message: Vec<String>,
    },

    #[command(name = "sendfile", alias = "sf")]
    SendFile {
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },

    Branch {
        name: String,
        #[arg(default_value = "master")]
        from: String,
    },

    Edit {
        branch: String,
        #[arg(default_value = "0")]
        in_point: u64,
        #[arg(default_value = "1000")]
        out_point: u64,
        clip_id: Option<Uuid>,
    },

    Sync,

    #[command(name = "roothash", alias = "hash")]
    RootHash {
        branch: String,
    },

    #[command(alias = "whoami", alias = "iam")]
    Id,

    #[command(alias = "?", alias = "h")]
    Help,

    #[command(alias = "exit", alias = "q", alias = "bye")]
    Quit,
}

pub async fn run_command(
    transport: ActiveTransport,
    active_connections: ActiveConnections,
) -> Result<(), CliError> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let state_file = Path::new("skein_state.json");
    let db_path = Path::new("skein_index.db");

    while let Some(line) = lines.next_line().await? {
        let line = line.trim();

        if line.is_empty() {
            print_statement::print_prompt();
            continue;
        }

        match Command::try_parse_from(line.split_whitespace()) {
            Ok(Command::Connect { id }) => {
                let remote_id: PeerId = id
                    .parse()
                    .map_err(|e: String| CliError::InvalidPeerId(e.to_string()))?;

                {
                    let connections = active_connections.lock().await;

                    if connections.contains_key(&remote_id) {
                        println!("Already connected to {remote_id}");
                        print_statement::print_prompt();
                        continue;
                    }
                }

                println!("Connecting to {remote_id}...");

                let connection = transport.connect(remote_id).await?;

                println!("Connected to {:?}", transport.remote_id(&connection));

                active_connections
                    .lock()
                    .await
                    .insert(remote_id, connection.clone());

                spawn_message_receiver(transport.clone(), connection);
            }

            Ok(Command::Send { message }) => {
                let message = message.join(" ");

                let connections = {
                    let connections = active_connections.lock().await;
                    connections.values().cloned().collect::<Vec<_>>()
                };

                if connections.is_empty() {
                    println!("No connected peers. Use: connect <id>");
                    continue;
                }

                for connection in connections {
                    transport.send(&connection, message.as_bytes()).await?;
                }

                println!("> {message}");
            }

            Ok(Command::SendFile { file }) => {
                let local = LocalChunkStore::from_store(transport.store());
                let hash = match local.add(file.clone()).await {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("Failed to import file: {e}");
                        continue;
                    }
                };

                let filename = file
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                let file_size = tokio::fs::metadata(&file)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
                let notice = format!("SKEIN_FILE:{}:{}:{}", hash, filename, file_size);

                let connections = {
                    let connections = active_connections.lock().await;
                    if connections.is_empty() {
                        println!("No connected peers. Use: connect <id>");
                        continue;
                    }
                    connections.values().cloned().collect::<Vec<_>>()
                };

                for connection in connections {
                    if let Err(e) = transport.send(&connection, notice.as_bytes()).await {
                        eprintln!("Failed to notify peer: {e}");
                    }
                }

                println!(
                    "Shared {:?} (hash: {}) over P2P — peers downloading directly...",
                    file, hash
                );
            }

            Ok(Command::Branch { name, from }) => {
                if let Err(e) = execute_branch(state_file, db_path, &from, &name) {
                    eprintln!("Failed to create branch: {e}");
                }
            }

            Ok(Command::Edit {
                branch,
                in_point,
                out_point,
                clip_id,
            }) => {
                let target = clip_id.unwrap_or_else(|| {
                    Uuid::parse_str("11111111-1111-1111-1111-111111111111")
                        .unwrap_or_else(|_| Uuid::new_v4())
                });
                if let Err(e) = execute_edit_trim(state_file, &branch, target, in_point, out_point)
                {
                    eprintln!("Failed to record edit: {e}");
                }
            }

            Ok(Command::Sync) => {
                let state = CliState::load(state_file);
                let unmerged: Vec<EditOp> = state.branch_ops.values().flatten().cloned().collect();

                let connections = {
                    let connections = active_connections.lock().await;
                    connections.values().cloned().collect::<Vec<_>>()
                };

                if connections.is_empty() {
                    println!("No active P2P peers connected. Local merge applied.");
                    crate::state::execute_sync(state_file, None)?;
                } else {
                    let sync_payload = serde_json::to_string(&unmerged).unwrap_or_default();
                    let msg = format!("SKEIN_OP_SYNC:{}", sync_payload);

                    for connection in connections {
                        transport.send(&connection, msg.as_bytes()).await?;
                    }
                    println!(
                        "Broadcasted {} unmerged operations to all connected P2P peers over QUIC!",
                        unmerged.len()
                    );
                }
            }

            Ok(Command::RootHash { branch }) => {
                if let Err(e) = execute_root_hash(state_file, db_path, &branch) {
                    eprintln!("Failed to get root hash: {e}");
                }
            }

            Ok(Command::Id) => {
                print_statement::print_id(&transport.id());
            }

            Ok(Command::Help) => {
                print_statement::print_help();
            }

            Ok(Command::Quit) => {
                break;
            }
            Err(e) => {
                eprintln!("{e}");
            }
        }
        print_statement::print_prompt();
    }

    Ok(())
}
