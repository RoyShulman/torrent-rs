use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::{net::TcpListener, task::JoinHandle};
use tracing::instrument;

use crate::{
    hash_utils::sha256_hash_chunk,
    modules::{
        chunks::SingleFileChunksState,
        connected_session::{ConnectedSession, TokioSocketWrapper},
        files::SingleChunkedFile,
    },
};

#[derive(Debug)]
struct ConnectedPeer {
    session: ConnectedSession<TokioSocketWrapper>,
    peer_addr: std::net::SocketAddr,
    states_directory: PathBuf,
    file_directory: PathBuf,
}

mod peer_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.peer.rs"));
}

///
/// A connection to a peer.
///
/// This will handle all the requests from the peer and send responses back.
/// Mostly used to send chunk responses about a single file.
/// TODO: This can probably be optimized since a session is about a single file, so
/// maybe we can read the state once into memory?
impl ConnectedPeer {
    #[instrument(err(Debug))]
    pub async fn serve_forever(mut self) -> anyhow::Result<()> {
        tracing::info!("Peer connected: {}", self.peer_addr);
        loop {
            let request = self
                .session
                .recv_msg::<peer_proto::PeerRequest>()
                .await
                .context("failed to receive message")?;

            let Some(request) = request else {
                tracing::info!("Peer disconnected");
                return Ok(());
            };

            let Some(request) = request.request else {
                anyhow::bail!("Received empty request");
            };

            let response = self.handle_peer_request(request).await;

            self.session
                .send_msg(&response)
                .await
                .context("failed to send response")?;
        }
    }

    async fn handle_peer_request(
        &mut self,
        request: peer_proto::peer_request::Request,
    ) -> peer_proto::PeerResponse {
        let response = match request {
            peer_proto::peer_request::Request::ListAvailableChunks(
                peer_proto::ListAvailableChunks { filename },
            ) => match self.handle_list_available_chunks(&filename).await {
                Ok(response) => Ok(peer_proto::PeerResponse {
                    response: Some(peer_proto::peer_response::Response::AvailableChunks(
                        response,
                    )),
                }),
                Err(e) => Err(e),
            },
            peer_proto::peer_request::Request::DownloadChunk(
                peer_proto::DownloadChunkRequest { filename, index },
            ) => match self.handle_download_chunk(&filename, index).await {
                Ok(response) => Ok(peer_proto::PeerResponse {
                    response: Some(peer_proto::peer_response::Response::ChunkResponse(response)),
                }),
                Err(e) => Err(e),
            },
        };

        match response {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("Error handling peer request: {:?}", e);
                peer_proto::PeerResponse {
                    response: Some(peer_proto::peer_response::Response::Error(
                        peer_proto::ErrorResponse {
                            message: e.to_string(),
                        },
                    )),
                }
            }
        }
    }

    #[instrument(err(Debug))]
    async fn handle_list_available_chunks(
        &self,
        filename: &str,
    ) -> anyhow::Result<peer_proto::AvailableChunksResponse> {
        let state_file = SingleFileChunksState::from_filename(&self.states_directory, filename)
            .await
            .context("failed to load state")?;

        Ok(peer_proto::AvailableChunksResponse {
            chunks: state_file.get_available_chunks(),
        })
    }

    #[instrument(err(Debug))]
    async fn handle_download_chunk(
        &self,
        filename: &str,
        chunk_index: u64,
    ) -> anyhow::Result<peer_proto::ChunkResponse> {
        let state_file = SingleFileChunksState::from_filename(&self.states_directory, filename)
            .await
            .context("failed to load state")?;

        if state_file.missing_chunks.contains(&chunk_index) {
            anyhow::bail!("Chunk is missing");
        }

        let chunked_file = SingleChunkedFile::new(self.file_directory.join(filename));
        let offset = state_file.get_chunk_offset(chunk_index);

        anyhow::ensure!(
            state_file.chunk_size < usize::MAX as u64,
            "chunk size: {} is too large (max: {})",
            state_file.chunk_size,
            usize::MAX
        );

        let chunk_data = chunked_file
            .read_chunk(offset, state_file.chunk_size as usize)
            .await
            .context("failed to read chunk data")?;

        let chunk_hash = sha256_hash_chunk(chunk_data.clone())
            .await
            .context("failed to hash chunk")?;

        Ok(peer_proto::ChunkResponse {
            buffer: chunk_data,
            chunk_index,
            sha256_hash: chunk_hash,
        })
    }
}

pub async fn setup_peer_listener_task(
    peer_port: u16,
    states_directory: &Path,
    files_directory: &Path,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", peer_port))
        .await
        .context("Failed to bind to peer listener port")?;
    tracing::info!("Listening to peer connections on port: {}", peer_port);

    let states_directory = states_directory.to_owned();
    let file_directory = files_directory.to_owned();
    Ok(tokio::spawn(async move {
        loop {
            let states_directory = states_directory.clone();
            let file_directory = file_directory.clone();
            let (socket, peer_addr) = listener
                .accept()
                .await
                .context("failed to accept connection")?;
            tokio::spawn(async move {
                let session = ConnectedSession::with_socket(TokioSocketWrapper::new(socket));
                let peer = ConnectedPeer {
                    session,
                    peer_addr,
                    states_directory,
                    file_directory,
                };
                if let Err(e) = peer.serve_forever().await {
                    tracing::error!("Error in peer connection: {:?}", e);
                }
            });
        }
    }))
}
