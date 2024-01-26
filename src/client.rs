pub mod client_logic;
mod modules;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use client_logic::TorrentClient;
use modules::server_session::{ClientServerSession, TokioServerSocketWrapper};
use tracing::instrument;

use crate::{
    client_logic::client_main_loop,
    modules::{chunks::ChunkStates, files::SingleFileManager},
};

/// Simple torrent client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory to store files
    #[arg(short, long)]
    files_directory: PathBuf,

    /// Directory to store states for downloads
    #[arg(short, long)]
    states_directory: PathBuf,

    /// Address of the server to connect to
    #[arg(short, long)]
    server_address: String,
}

#[instrument]
async fn setup_client(args: &Args) -> anyhow::Result<TorrentClient> {
    let mut chunk_states = ChunkStates::new(&args.states_directory);

    let (server_session, load_result) = tokio::join!(
        ClientServerSession::connect(&args.server_address),
        chunk_states.load_from_directory()
    );

    load_result.context("Failed to load chunk states")?;
    let server_session = server_session.context("Failed to connect to server")?;

    Ok(TorrentClient::new(
        &args.files_directory,
        chunk_states,
        server_session,
    ))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    tracing::info!("Initializaing!");

    let client = match setup_client(&args).await {
        Ok(client) => client,
        Err(err) => {
            tracing::error!("Failed to setup client: {:?}", err);
            return;
        }
    };

    if let Err(e) = client_main_loop(client).await {
        tracing::error!("Error in main loop: {:?}", e);
        return;
    }

    tracing::info!("Done. cya later :)");
}
