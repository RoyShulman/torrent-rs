use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use pin_project::pin_project;
use tokio::{
    sync::{oneshot, watch, Mutex},
    time::timeout,
};
use tracing::{instrument, Instrument};

use crate::{
    hash_utils::sha256_hash_chunk,
    modules::{
        chunks::{self, SingleFileChunksState},
        connected_session::{ConnectedSession, SocketWrapper},
        files::SingleChunkedFile,
    },
};

mod client_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.client.rs"));
}
mod server_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.server.rs"));
}
mod peer_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.peer.rs"));
}

const PEER_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const REQUEST_CHUNKS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const NO_PEERS_SLEEP_TIME: std::time::Duration = std::time::Duration::from_secs(5);
const REQUEST_SINGLE_CHUNK_DATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

pub struct DownloadFileResult {
    pub filename: String,
    pub result: DownloadFileResultInner,
}

#[derive(Debug)]
pub enum DownloadFileResultInner {
    Success,
    Error,
    Cancelled,
}

struct DownloadFilePeer<T> {
    session: ConnectedSession<T>,
    peer_addr: std::net::SocketAddrV4,
}

pub struct DownloadFileContextBuilder<T> {
    pub filename: String,
    pub server_session: Arc<Mutex<ConnectedSession<T>>>,
    pub num_chunks: u64,
    pub chunk_size: u64,
    pub file_size: u64,

    pub chunk_states_directory: PathBuf,
    pub data_files_directory: PathBuf,
    pub peer_listening_port: u16,
}

pub struct InitializedDownloadFileContext<T> {
    filename: String,
    server_session: Arc<Mutex<ConnectedSession<T>>>,
    downloaded_chunks: u64,
    peer_listener_port: u16,

    chunks_state: SingleFileChunksState,
    data_file: SingleChunkedFile,

    current_peers: Vec<DownloadFilePeer<T>>,
    last_peer_update: std::time::Instant,
    notified_server: bool,
}

impl<T> DownloadFileContextBuilder<T> {
    #[tracing::instrument(skip(self), fields(filename = %self.filename, num_chunks = self.num_chunks, chunk_size = self.chunk_size, file_size = self.file_size), err(Debug))]
    pub async fn build(self) -> anyhow::Result<InitializedDownloadFileContext<T>> {
        let chunks_state = chunks::find_or_create_chunk_state(
            &self.chunk_states_directory,
            &self.filename,
            self.num_chunks,
            self.chunk_size,
            self.file_size,
        )
        .await
        .context("failed to find or create chunk state")?;

        let data_file = SingleChunkedFile::new(self.data_files_directory.join(&self.filename));
        if chunks_state.get_available_chunks().is_empty() {
            // There was no previous attempt to download this file, we need to create the file
            data_file
                .create(self.file_size)
                .await
                .context("failed to create file")?;
        }

        Ok(InitializedDownloadFileContext {
            filename: self.filename,
            server_session: self.server_session,
            chunks_state,
            data_file,
            current_peers: Vec::new(),
            last_peer_update: std::time::Instant::now(),
            downloaded_chunks: 0,
            notified_server: false,
            peer_listener_port: self.peer_listening_port,
        })
    }
}

enum SingleLoopResult {
    Finished,
    NeedMoreChunks,
    Error,
}

#[derive(Debug)]
pub struct DownloadFileState {
    pub downloaded_chunks: u64,
    pub current_peers: Vec<String>,
}

#[derive(Debug)]
pub struct DownloadFileContext {
    pub cancel_sender: oneshot::Sender<()>,
    pub current_state: watch::Receiver<DownloadFileState>,
}

impl<T> InitializedDownloadFileContext<T>
where
    T: Send + 'static,
    T: SocketWrapper,
{
    #[tracing::instrument(skip(self), fields(filename = %self.filename, num_chunks = self.chunks_state.num_chunks, chunk_size = self.chunks_state.chunk_size))]
    pub fn start_download(
        mut self,
    ) -> (DownloadFileContext, oneshot::Receiver<DownloadFileResult>) {
        tracing::info!("starting download");
        let (cancel_sender, mut cancel_receiver) = oneshot::channel();
        let (result_sender, result_receiver) = oneshot::channel();
        let (mut state_sender, state_receiver) = watch::channel(DownloadFileState {
            downloaded_chunks: 0,
            current_peers: Vec::new(),
        });

        tokio::spawn(
            async move {
                loop {
                    tokio::select! {
                        _ = &mut cancel_receiver => {
                            let result = DownloadFileResult {
                                filename: self.filename,
                                result: DownloadFileResultInner::Cancelled,
                            };

                            if let Err(_) = result_sender.send(result) {
                                tracing::error!("Failed to send cancel result");
                            }
                            break;
                        }
                        single_loop_result = self.single_loop_iteration_wrapped(&mut state_sender) => {
                            match single_loop_result {
                                SingleLoopResult::Finished => {
                                    let result = DownloadFileResult {
                                        filename: self.filename,
                                        result: DownloadFileResultInner::Error,
                                    };

                                    tracing::info!("finished download");

                                    if let Err(_) = result_sender.send(result) {
                                        tracing::error!("Failed to send success result");
                                    }
                                    break;
                                }
                                SingleLoopResult::NeedMoreChunks => {
                                    // continue
                                }
                                SingleLoopResult::Error => {
                                    let result = DownloadFileResult {
                                        filename: self.filename,
                                        result: DownloadFileResultInner::Error,
                                    };

                                    tracing::warn!("Downloading filed");
                                    if let Err(_) = result_sender.send(result) {
                                        tracing::error!("Failed to send error result");
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            .in_current_span(),
        );

        (
            DownloadFileContext {
                cancel_sender,
                current_state: state_receiver,
            },
            result_receiver,
        )
    }

    #[tracing::instrument(skip(self))]
    async fn single_loop_iteration_wrapped(
        &mut self,
        state_sender: &mut watch::Sender<DownloadFileState>,
    ) -> SingleLoopResult {
        // sleep for 5 seconds just to make things more visible
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        match self.single_loop_iteration(state_sender).await {
            Ok(result) => {
                if let Err(e) = state_sender.send(DownloadFileState {
                    downloaded_chunks: self.downloaded_chunks,
                    current_peers: self
                        .current_peers
                        .iter()
                        .map(|x| x.peer_addr.to_string())
                        .collect(),
                }) {
                    tracing::error!("failed to send state update: {:?}", e);
                    return SingleLoopResult::Error;
                }

                result
            }
            Err(e) => {
                tracing::error!("single loop iteration failed: {:?}", e);
                SingleLoopResult::Error
            }
        }
    }

    ///
    /// A single iteration of the download loop.
    /// This will update the current peers if a timeout has passed, request chunks from peers, and write the chunks to the file.
    #[tracing::instrument(skip(self))]
    async fn single_loop_iteration(
        &mut self,
        state_sender: &mut watch::Sender<DownloadFileState>,
    ) -> anyhow::Result<SingleLoopResult> {
        tracing::trace!("single loop iteration");

        if self.chunks_state.missing_chunks.is_empty() {
            return Ok(SingleLoopResult::NeedMoreChunks);
        }

        if self.last_peer_update.elapsed() > PEER_UPDATE_INTERVAL {
            self.update_current_peers()
                .await
                .context("failed to update current peers")?;
            self.last_peer_update = std::time::Instant::now();
        }

        let chunks_for_peers = self.get_chunks_for_all_peers().await;
        let num_peers = chunks_for_peers.len();
        if num_peers == 0 {
            tracing::warn!("no peers available, sleeping in hopes of a better future");
            tokio::time::sleep(NO_PEERS_SLEEP_TIME).await;
            return Ok(SingleLoopResult::NeedMoreChunks);
        }

        let chunk_for_peer =
            get_next_chunk_for_peers(&self.chunks_state.missing_chunks, &chunks_for_peers);

        let downloaded_chunks = self.request_chunk_data_from_all_peers(chunk_for_peer).await;

        // This can probably be done in parallel, but we will do it sequentially for simplicity
        for chunk in downloaded_chunks {
            self.data_file
                .write_chunk(
                    &chunk.buffer,
                    chunk.chunk_index * self.chunks_state.chunk_size,
                )
                .await
                .context("failed to write chunk to file")?;
            self.chunks_state
                .update_new_chunk(chunk.chunk_index)
                .await
                .context("failed to mark chunk as downloaded")?;
            self.downloaded_chunks += 1;
        }

        if self.downloaded_chunks > 0 && !self.notified_server {
            // Update the server we also have chunks of the file
            let server_update_message = client_proto::UpdateNewAvailableFileOnClient {
                filename: self.filename.clone(),
                num_chunks: self.chunks_state.num_chunks,
                chunk_size: self.chunks_state.chunk_size,
                file_size: self.chunks_state.file_size,
                peer_port: self.peer_listener_port as u32,
            };

            let request = client_proto::RequestToServer {
                request: Some(
                    client_proto::request_to_server::Request::UpdateNewAvailableFileOnClient(
                        server_update_message,
                    ),
                ),
            };
            let mut server_session = self.server_session.lock().await;

            server_session
                .send_msg(&request)
                .await
                .context("failed to update new available file on client")?;

            self.notified_server = true;
        }

        if self.chunks_state.missing_chunks.is_empty() {
            Ok(SingleLoopResult::Finished)
        } else {
            Ok(SingleLoopResult::NeedMoreChunks)
        }
    }

    ///
    /// Send a request to each connected peer to get the list of chunks it has for the file.
    /// Notice this will drain the vector of current peers, to make it possible to send the request to each peer in parallel.
    /// This will then wait for all the responses and only refill the vector with the peers that responded.
    /// The peers that didn't will be dropped and thus the connection to them will also drop
    #[instrument(skip(self))]
    async fn get_chunks_for_all_peers(&mut self) -> HashMap<SocketAddrV4, HashSet<u64>> {
        let mut requests = FuturesUnordered::new();
        for peer in self.current_peers.drain(..) {
            let filename = self.filename.clone();
            requests.push(list_chunks_on_peer(peer, filename));
        }

        tracing::info!("requesting chunks from {} peers", requests.len());

        let mut chunks_for_peers = HashMap::new();
        let _ = timeout(REQUEST_CHUNKS_TIMEOUT, async {
            while let Some(response) = requests.next().await {
                let response = match response {
                    Ok(response) => response,
                    Err(e) => {
                        tracing::error!("failed to get chunks from peer: {:?}", e);
                        continue;
                    }
                };

                chunks_for_peers.insert(response.peer_connection.peer_addr, response.chunks);
                self.current_peers.push(response.peer_connection);
            }
        })
        .await;

        chunks_for_peers
    }

    ///
    /// Send a request to each connected peer we have chosen to download a chunk from with the chunk index.
    /// Notice this will drain the vector of current peers, to make it possible to send the request to each peer in parallel.
    /// This will then wait for all the responses and only refill the vector with the peers that responded.
    /// The peers that didn't will be dropped and thus the connection to them will also drop
    #[instrument(skip(self))]
    async fn request_chunk_data_from_all_peers(
        &mut self,
        mut chunks_for_peers: HashMap<SocketAddrV4, u64>,
    ) -> Vec<DownloadedChunk> {
        tracing::info!("requesting chunks from peers: {:?}", chunks_for_peers);
        let mut requests = FuturesUnordered::new();
        let mut no_requested_peers = Vec::new();
        for peer in self.current_peers.drain(..) {
            if let Some(chunk_index) = chunks_for_peers.remove(&peer.peer_addr) {
                let filename = self.filename.clone();
                requests.push(request_chunk_data_from_peer(peer, filename, chunk_index));
            } else {
                // This peer has no chunks we want, we will not request anything from it.
                // Yet we still want to keep the connection open for future requests.
                // We will add it back to the list of current peers later.
                no_requested_peers.push(peer);
            }
        }

        // Add back the peers that we didn't request anything from
        self.current_peers.extend(no_requested_peers);

        let mut downloaded_chunks = Vec::new();
        let _ = timeout(REQUEST_SINGLE_CHUNK_DATA_TIMEOUT, async {
            while let Some(response) = requests.next().await {
                let response = match response {
                    Ok(response) => response,
                    Err(e) => {
                        tracing::error!("failed to get chunk data from peer: {:?}", e);
                        continue;
                    }
                };

                self.current_peers.push(response.peer_connection);
                downloaded_chunks.push(response.chunk)
            }
        })
        .await;

        downloaded_chunks
    }

    ///
    /// Send a request to the server to get the list of peers for the currently downloading file.
    /// NOTE: We currently check if we should download from a peer only if he has a different port from us.
    /// This is a simple way to avoid downloading from ourselves, done for simplicity for now.
    #[tracing::instrument(skip(self))]
    async fn update_current_peers(&mut self) -> anyhow::Result<()> {
        tracing::info!("updating current peers");
        let request = client_proto::DownloadFile {
            filename: self.filename.clone(),
        };
        let request = client_proto::RequestToServer {
            request: Some(client_proto::request_to_server::Request::DownloadFile(
                request,
            )),
        };

        let mut server_session = self.server_session.lock().await;
        server_session
            .send_msg(&request)
            .await
            .context("failed to send download file request")?;

        let mut response: server_proto::DownloadFileResponse = server_session
            .recv_msg()
            .await
            .context("failed to receive download file response")?
            .context("empty response to download file request")?;

        response
            .peers
            .retain(|x| x.port != self.peer_listener_port as u32);

        let peers: HashSet<_> = response
            .peers
            .into_iter()
            .filter_map(|peer| {
                Some(SocketAddrV4::new(
                    peer.ip.into(),
                    peer.port.try_into().ok()?,
                ))
            })
            .collect();
        let current_peers: HashSet<_> = self.current_peers.iter().map(|x| x.peer_addr).collect();

        let peers_cloned = peers.clone();
        let to_remove: HashSet<_> = current_peers.difference(&peers_cloned).collect();
        let to_add: HashSet<_> = peers_cloned.difference(&current_peers).collect();

        // Remove all peers that no longer serve the file
        self.current_peers
            .retain(|peer| !to_remove.contains(&peer.peer_addr));

        let mut new_peer_connections = FuturesUnordered::new();
        // Add new peers
        for peer in peers {
            if !to_add.contains(&peer) {
                continue;
            }

            // This is a new peer that was added, we need to open a connection to it
            let future = FutureWithPeerAddr {
                peer_addr: peer,
                future: ConnectedSession::connect(peer),
            };
            new_peer_connections.push(future);
        }

        while let Some(peer_connection) = new_peer_connections.next().await {
            let FutureResultWithPeerAddr { peer_addr, result } = peer_connection;
            let peer_connection = match result {
                Ok(peer_connection) => peer_connection,
                Err(e) => {
                    tracing::error!("failed to connect to peer: {:?}", e);
                    continue;
                }
            };

            tracing::info!("connected to new peer: {}", peer_addr);
            self.current_peers.push(DownloadFilePeer {
                session: peer_connection,
                peer_addr,
            });
        }

        Ok(())
    }
}

#[pin_project]
struct FutureWithPeerAddr<F> {
    peer_addr: SocketAddrV4,
    #[pin]
    future: F,
}

struct FutureResultWithPeerAddr<T> {
    peer_addr: SocketAddrV4,
    result: T,
}

impl<F, T> Future for FutureWithPeerAddr<F>
where
    F: Future<Output = T>,
{
    type Output = FutureResultWithPeerAddr<T>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match this.future.poll(cx) {
            std::task::Poll::Ready(result) => std::task::Poll::Ready(FutureResultWithPeerAddr {
                peer_addr: *this.peer_addr,
                result,
            }),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

///
/// A basic implementation to get the next chunk to download for a peer.
/// We order the missing chunks by the number of peers that have them, to select the chunks that are less available first.
/// There is a small problem here. Assume we have 2 peers (1, 2) and 2 chunks (A, B). If peer A has chunks (0, 1) but B only has (0),
/// we will choose chunk 0 for A, but then find that no one has chunk 1. We should have chosen chunk 1 for B instead in this scenario.
/// We ignore this for simplicity.
fn get_next_chunk_for_peers(
    missing_chunks: &HashSet<u64>,
    all_chunks_by_peer: &HashMap<SocketAddrV4, HashSet<u64>>,
) -> HashMap<SocketAddrV4, u64> {
    let mut chunks_by_availability = missing_chunks
        .iter()
        .map(|chunk| {
            let num_peers = all_chunks_by_peer
                .values()
                .filter(|chunks| chunks.contains(chunk))
                .count();
            (chunk, num_peers)
        })
        .collect::<Vec<_>>();

    chunks_by_availability.sort_by_key(|(_, num_peers)| *num_peers);

    let mut chosen_chunk_for_peer = HashMap::new();
    for (chunk_index, num_peers) in chunks_by_availability {
        if chosen_chunk_for_peer.len() == all_chunks_by_peer.len() {
            // We have assigned a chunk to each peer
            break;
        }

        if num_peers == 0 {
            // No peers have this chunk - better luck next time
            continue;
        }

        for (peer, peer_chunks) in all_chunks_by_peer {
            if peer_chunks.contains(chunk_index) && !chosen_chunk_for_peer.contains_key(peer) {
                chosen_chunk_for_peer.insert(*peer, *chunk_index);
                break;
            }
        }
    }

    chosen_chunk_for_peer
}

struct ChunksOnPeerResponse<S> {
    peer_connection: DownloadFilePeer<S>,
    chunks: HashSet<u64>,
}

struct DownloadedChunk {
    buffer: Vec<u8>,
    chunk_index: u64,
}

struct ChunkResponse<S> {
    chunk: DownloadedChunk,
    peer_connection: DownloadFilePeer<S>,
}

///
/// Send a request to a peer to get the list of chunks it has for a file.
#[tracing::instrument(skip(peer_connection))]
async fn list_chunks_on_peer<S: SocketWrapper>(
    mut peer_connection: DownloadFilePeer<S>,
    filename: String,
) -> anyhow::Result<ChunksOnPeerResponse<S>> {
    tracing::info!(peer_addr=%peer_connection.peer_addr, "list chunks on peer");
    let request = peer_proto::ListAvailableChunks { filename };
    let request = peer_proto::PeerRequest {
        request: Some(peer_proto::peer_request::Request::ListAvailableChunks(
            request,
        )),
    };

    peer_connection
        .session
        .send_msg(&request)
        .await
        .context("failed to send list available chunks request")?;

    let response: peer_proto::PeerResponse = peer_connection
        .session
        .recv_msg()
        .await
        .context("failed to receive list available chunks response")?
        .context("empty response to list available chunks request")?;

    let response = response
        .response
        .context("invalid response to list available chunks request")?;

    let chunks = match response {
        peer_proto::peer_response::Response::AvailableChunks(available_chunks) => {
            available_chunks.chunks.into_iter().collect()
        }
        peer_proto::peer_response::Response::ChunkResponse(_) => {
            anyhow::bail!("peer returned chunk response which is unexpected since we sent a list available chunks request");
        }
        peer_proto::peer_response::Response::Error(error_response) => {
            anyhow::bail!("peer returned error: {}", error_response.message);
        }
    };

    Ok(ChunksOnPeerResponse {
        peer_connection,
        chunks,
    })
}

///
/// Request a chunk from a peer. This will send a request to the peer to get a chunk of the file and wait for a response.
/// When the chunk is received, it will validate the hash is correct.
#[tracing::instrument(skip(peer_connection))]
async fn request_chunk_data_from_peer<S: SocketWrapper>(
    mut peer_connection: DownloadFilePeer<S>,
    filename: String,
    chunk_index: u64,
) -> anyhow::Result<ChunkResponse<S>> {
    tracing::debug!("requesting chunk from peer");
    let request = peer_proto::DownloadChunkRequest {
        filename,
        index: chunk_index,
    };
    let request = peer_proto::PeerRequest {
        request: Some(peer_proto::peer_request::Request::DownloadChunk(request)),
    };

    peer_connection
        .session
        .send_msg(&request)
        .await
        .context("failed to send request chunk")?;

    let response: peer_proto::PeerResponse = peer_connection
        .session
        .recv_msg()
        .await
        .context("failed to receive chunk data")?
        .context("empty response to chunk data request")?;

    let response = response
        .response
        .context("invalid response to chunk data request")?;
    let chunk_response = match response {
        peer_proto::peer_response::Response::ChunkResponse(chunk_response) => chunk_response,
        peer_proto::peer_response::Response::Error(error_response) => {
            anyhow::bail!("peer returned error: {}", error_response.message);
        }
        peer_proto::peer_response::Response::AvailableChunks(_) => {
            anyhow::bail!("peer returned available chunks response which is unexpected since we sent a download chunk request");
        }
    };

    let expected_hash = sha256_hash_chunk(chunk_response.buffer.clone())
        .await
        .context("failed to hash chunk")?;
    if expected_hash != chunk_response.sha256_hash {
        anyhow::bail!("hash mismatch for chunk");
    }

    Ok(ChunkResponse {
        chunk: DownloadedChunk {
            buffer: chunk_response.buffer,
            chunk_index,
        },
        peer_connection,
    })
}
