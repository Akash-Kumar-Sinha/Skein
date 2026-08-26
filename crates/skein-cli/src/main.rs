use clap::{Parser, Subcommand};
use skein_peer::{Conduit, Transport};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    background_task::spawn_connection_listener,
    error::CliError,
    run_command::run_command,
    state::{execute_branch, execute_edit_trim, execute_root_hash, execute_sync},
};

mod background_task;
mod error;
mod print_statement;
mod run_command;
mod state;

pub type ActiveTransport = Conduit;
pub type PeerId = <ActiveTransport as Transport>::PeerId;
pub type Connection = <ActiveTransport as Transport>::Connection;
pub type ActiveConnections = Arc<Mutex<HashMap<PeerId, Connection>>>;

#[derive(Parser, Debug)]
#[command(
    name = "skein-cli",
    about = "Skein P2P Collaborative Video Editing CLI"
)]
struct Args {
    #[arg(short, long, default_value = "skein_downloads")]
    store: PathBuf,

    #[arg(long, default_value = "skein_state.json")]
    state_file: PathBuf,

    #[arg(long, default_value = "skein_index.db")]
    db: PathBuf,

    #[arg(long, default_value = "iroh")]
    transport: String,

    #[arg(long, default_value = "127.0.0.1:0")]
    bind: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Branch {
        #[arg(long, default_value = "master")]
        from: String,
        #[arg(long)]
        name: String,
    },
    Edit {
        #[arg(long)]
        branch: String,
        #[arg(long)]
        trim: Option<Uuid>,
        #[arg(long = "in", default_value = "0")]
        in_point: u64,
        #[arg(long = "out", default_value = "1000")]
        out_point: u64,
    },
    Sync {
        #[arg(long = "with")]
        with_peer: Option<String>,
    },
    RootHash {
        #[arg(long)]
        branch: String,
    },
    Interactive,
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let args = Args::parse();

    match args.command {
        Some(Commands::Branch { from, name }) => {
            execute_branch(&args.state_file, &args.db, &from, &name)?;
        }
        Some(Commands::Edit {
            branch,
            trim,
            in_point,
            out_point,
        }) => {
            let target = trim.unwrap_or_else(|| {
                Uuid::parse_str("11111111-1111-1111-1111-111111111111")
                    .unwrap_or_else(|_| Uuid::new_v4())
            });
            execute_edit_trim(&args.state_file, &branch, target, in_point, out_point)?;
        }
        Some(Commands::Sync { with_peer }) => {
            execute_sync(&args.state_file, with_peer.as_deref())?;
        }
        Some(Commands::RootHash { branch }) => {
            execute_root_hash(&args.state_file, &args.db, &branch)?;
        }
        Some(Commands::Interactive) | None => {
            let transport = if args.transport.eq_ignore_ascii_case("tcp") {
                Conduit::tcp(&args.bind, &args.store).await?
            } else {
                Conduit::iroh(&args.store).await?
            };

            print_statement::print_banner(&transport.id());
            print_statement::print_prompt();

            let active_connections: ActiveConnections = Arc::new(Mutex::new(HashMap::new()));

            spawn_connection_listener(transport.clone(), active_connections.clone());

            run_command(transport, active_connections).await?;
        }
    }

    Ok(())
}
