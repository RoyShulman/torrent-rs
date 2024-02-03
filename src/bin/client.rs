use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;
use torrent_rs::{
    client_logic::{client_main_loop, TorrentClient},
    modules::{chunks::load_from_directory, connected_session::ConnectedSession},
};
use tracing::instrument;

/// Simple torrent client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory to store files
    #[arg(long)]
    files_directory: PathBuf,

    /// Directory to store states for downloads
    #[arg(long)]
    states_directory: PathBuf,

    /// Address of the server to connect to
    #[arg(long)]
    server_address: String,
}

#[instrument]
async fn setup_client(args: &Args) -> anyhow::Result<TorrentClient> {
    let (server_session, existing_chunk_states) = tokio::join!(
        ConnectedSession::connect(&args.server_address),
        load_from_directory(&args.states_directory)
    );

    let existing_chunk_states = existing_chunk_states.context("Failed to load chunk states")?;
    let server_session = server_session.context("Failed to connect to server")?;

    Ok(TorrentClient::new(
        &args.files_directory,
        &args.states_directory,
        existing_chunk_states,
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
