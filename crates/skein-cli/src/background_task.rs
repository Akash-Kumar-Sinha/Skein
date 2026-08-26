use iroh_blobs::Hash;
use skein_edl::EditOp;
use skein_merge::{apply_merge_resolution, resolve_lww, three_way_merge};
use skein_store::{ChunkStore, RemoteChunkStore};
use std::path::{Path, PathBuf};

use crate::state::CliState;
use crate::{ActiveConnections, ActiveTransport, Connection, print_statement};

pub fn spawn_connection_listener(
    transport: ActiveTransport,
    active_connections: ActiveConnections,
) {
    tokio::spawn(async move {
        while let Ok(connection) = transport.accept().await {
            let remote_id = transport.remote_id(&connection);
            println!("\nAccepted connection from {:?}", remote_id);
            print_statement::print_prompt();
            active_connections
                .lock()
                .await
                .insert(remote_id, connection.clone());
            spawn_message_receiver(transport.clone(), connection);
        }
    });
}

pub fn spawn_message_receiver(transport: ActiveTransport, connection: Connection) {
    tokio::spawn(async move {
        let remote_id = transport.remote_id(&connection);
        let state_path = Path::new("skein_state.json");

        loop {
            match transport.receive(&connection).await {
                Ok(message) => {
                    let text = String::from_utf8_lossy(&message);

                    if let Some(hash_str) = text.strip_prefix("SKEIN_CHUNK_REQ:") {
                        if let Ok(hash) = hash_str.parse::<Hash>()
                            && let Ok(bytes) = transport.store().blobs().get_bytes(hash).await
                        {
                            let _ = transport.send(&connection, &bytes).await;
                        }
                        continue;
                    }

                    if let Some(ops_json) = text.strip_prefix("SKEIN_OP_SYNC:") {
                        if let Ok(remote_ops) = serde_json::from_str::<Vec<EditOp>>(ops_json) {
                            let mut state = CliState::load(state_path);
                            let local_ops: Vec<EditOp> =
                                state.branch_ops.values().flatten().cloned().collect();

                            let outcomes = three_way_merge(0, &local_ops, &remote_ops);
                            let resolved = apply_merge_resolution(&outcomes, resolve_lww);

                            for op in resolved {
                                if !state.base_ops.contains(&op) {
                                    state.base_ops.push(op);
                                }
                            }
                            for ops in state.branch_ops.values_mut() {
                                ops.clear();
                            }
                            let _ = state.save(state_path);

                            println!(
                                "\n< [P2P SYNC: Received and merged {} operations from peer!]",
                                remote_ops.len()
                            );

                            if let Ok(local_json) = serde_json::to_string(&local_ops) {
                                let reply_msg = format!("SKEIN_OP_REPLY:{}", local_json);
                                let _ = transport.send(&connection, reply_msg.as_bytes()).await;
                            }
                            print_statement::print_prompt();
                        }
                        continue;
                    }

                    if let Some(ops_json) = text.strip_prefix("SKEIN_OP_REPLY:") {
                        if let Ok(remote_ops) = serde_json::from_str::<Vec<EditOp>>(ops_json) {
                            let mut state = CliState::load(state_path);
                            let local_ops: Vec<EditOp> =
                                state.branch_ops.values().flatten().cloned().collect();

                            let outcomes = three_way_merge(0, &local_ops, &remote_ops);
                            let resolved = apply_merge_resolution(&outcomes, resolve_lww);

                            for op in resolved {
                                if !state.base_ops.contains(&op) {
                                    state.base_ops.push(op);
                                }
                            }
                            for ops in state.branch_ops.values_mut() {
                                ops.clear();
                            }
                            let _ = state.save(state_path);

                            println!(
                                "\n< [P2P SYNC: Sync complete! Both peers now converged to identical state.]"
                            );
                            print_statement::print_prompt();
                        }
                        continue;
                    }

                    if let Some(payload) = text.strip_prefix("SKEIN_FILE:") {
                        let parts: Vec<&str> = payload.split(':').collect();
                        let hash_str = parts.first().copied().unwrap_or("");
                        let filename = parts.get(1).copied().unwrap_or("download.bin");
                        let total_size: Option<u64> = parts.get(2).and_then(|s| s.parse().ok());

                        match hash_str.parse::<Hash>() {
                            Ok(hash) => {
                                let dest = PathBuf::from(filename);
                                let mut remote_store = RemoteChunkStore::new(
                                    transport.clone(),
                                    remote_id,
                                    transport.store(),
                                );
                                if let Some(size) = total_size {
                                    remote_store.set_total_size(size);
                                }

                                println!(
                                    "\n< [downloading P2P blob: {} (hash: {})]",
                                    filename, hash
                                );

                                match remote_store.export(hash, dest.clone()).await {
                                    Ok(()) => {
                                        println!("\n< [saved to {:?}]", dest);
                                        print_statement::print_prompt();
                                    }
                                    Err(e) => {
                                        eprintln!("\n< [download failed: {e:#}]");
                                        print_statement::print_prompt();
                                    }
                                }
                            }
                            Err(e) => eprintln!("< [invalid hash: {}]", e),
                        }
                    } else {
                        println!("\n< {}", text);
                        print_statement::print_prompt();
                    }
                }

                Err(e) => {
                    eprintln!("Connection {:?} closed: {e}", remote_id);
                    break;
                }
            }
        }
    });
}
