use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;
use tokio::{join, net::TcpListener, task::JoinHandle};
use torrent_rs::{
    client_logic::{client_main_loop, TorrentClient},
    modules::{
        chunks::load_from_directory,
        connected_session::{ConnectedSession, TokioSocketWrapper},
    },
    peer_listener::setup_peer_listener_task,
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

    ///
    /// The port to listen on for incoming connections from peers.
    #[arg(long)]
    peer_port: u16,
}

#[instrument]
async fn setup_client(args: &Args) -> anyhow::Result<TorrentClient> {
    let (server_session, existing_chunk_states, listener) = tokio::join!(
        ConnectedSession::connect(&args.server_address),
        load_from_directory(&args.states_directory),
        TcpListener::bind(format!("0.0.0.0:{}", args.peer_port)),
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

    let (peer_listener, client) = join!(
        setup_peer_listener_task(
            args.peer_port,
            &args.states_directory,
            &args.files_directory
        ),
        setup_client(&args)
    );

    let peer_listener_task = match peer_listener {
        Ok(peer_listener) => peer_listener,
        Err(err) => {
            tracing::error!("Failed to setup peer listener: {:?}", err);
            return;
        }
    };

    let client = match client {
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
