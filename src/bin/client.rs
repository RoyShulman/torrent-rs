use anyhow::Context;
use axum::ServiceExt;
use clap::Parser;
use std::{future::IntoFuture, path::PathBuf};
use tokio::{join, net::TcpListener, select, task::JoinHandle};
use torrent_rs::{
    client_logic::TorrentClient,
    client_rest_api::get_client_router,
    modules::{chunks::load_from_directory, connected_session::ConnectedSession},
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

    ///
    /// The port for the REST API to listen on.
    #[arg(long)]
    rest_api_port: u16,
}

#[instrument]
async fn setup_client(args: &Args) -> anyhow::Result<TorrentClient> {
    let (server_session, existing_chunk_states) = tokio::join!(
        ConnectedSession::connect(&args.server_address),
        load_from_directory(&args.states_directory),
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

    let listener =
        match tokio::net::TcpListener::bind(format!("127.0.0.1:{}", args.rest_api_port)).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!("Failed to bind to rest api port: {:?}", err);
                return;
            }
        };

    tracing::info!("API listening on: {}", args.rest_api_port);

    let client_api_router = get_client_router(client);
    let client_api_server = axum::serve(listener, client_api_router).into_future();

    select! {
        _ = peer_listener_task => {
            tracing::error!("Peer listener task failed");
        }
        _ = client_api_server => {
            tracing::error!("Client api server failed");
        }
    }

    tracing::info!("Done. cya later :)");
}
