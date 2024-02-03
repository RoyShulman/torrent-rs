use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use torrent_rs::{
    modules::connected_session::{ConnectedSession, TokioSocketWrapper},
    server_handlers::{self, handle_client_request, ConnectedClient},
};
use tracing::instrument;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Simple torrent client
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port for the server to listen on
    #[arg(short, long)]
    port: u16,

    /// Directory to store information about the avilable files and which peer holds them
    #[arg(short, long)]
    files_directory: String,
}

#[derive(Debug)]
struct Server {
    port: u16,
    listener: TcpListener,
    files_directory: PathBuf,
}

///
/// Sets up the server by binding to the given port
#[instrument(err(Debug))]
async fn setup_server(args: &Args) -> anyhow::Result<Server> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .context("failed to bind to port")?;

    Ok(Server {
        port: args.port,
        listener,
        files_directory: args.files_directory.clone().into(),
    })
}

///
/// Main loop for the server - accepts incoming connections and handles them
#[instrument(err(Debug))]
async fn server_main_loop(server: Server) -> anyhow::Result<()> {
    tracing::info!("Starting server main loop on port: {}", server.port);

    loop {
        let (socket, peer_addr) = server
            .listener
            .accept()
            .await
            .context("failed to accept connection")?;

        tracing::info!("Accepted connection from: {}", peer_addr);

        let files_directory = server.files_directory.clone();

        tokio::spawn(async move {
            let client = ConnectedClient {
                session: ConnectedSession::with_socket(TokioSocketWrapper::new(socket)),
                peer_addr,
            };

            if let Err(err) = connected_client_main_loop(client, files_directory).await {
                tracing::error!("Connected client main loop failed: {:?}", err);
            }
        });
    }
}

#[instrument(err(Debug))]
async fn connected_client_main_loop(
    mut connected_client: ConnectedClient,
    files_directory: PathBuf,
) -> anyhow::Result<()> {
    loop {
        let Some(message): Option<server_handlers::client_proto::RequestToServer> =
            connected_client
                .session
                .recv_msg()
                .await
                .context("failed to receive message")?
        else {
            tracing::info!("Client disconnected: {}", connected_client.peer_addr);
            return Ok(());
        };

        tracing::info!("Received message: {:?}", message);

        handle_client_request(message, &mut connected_client, &files_directory)
            .await
            .context("failed to handle client request")?;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                // Customize the layer here, for example, by omitting span events
                .with_span_events(fmt::format::FmtSpan::NONE) // Do not include span events
                .without_time(), // Example: also omitting timestamps
                                 // You can further customize the output format here
        )
        .init();
    let args = Args::parse();

    tracing::info!("Initializaing server on port: {}!", args.port);

    let server = match setup_server(&args).await {
        Ok(server) => server,
        Err(err) => {
            tracing::error!("Failed to setup server: {:?}", err);
            return;
        }
    };

    if let Err(err) = server_main_loop(server).await {
        tracing::error!("Server main loop failed: {:?}", err);
    }
}
