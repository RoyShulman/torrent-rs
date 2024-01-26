use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use tokio::sync::RwLock;
use tracing::instrument;

use crate::modules::{
    chunks::ChunkStates,
    files::SingleFileManager,
    server_session::{ClientServerSession, TokioServerSocketWrapper},
};

mod client_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.client.rs"));
}
mod server_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.server.rs"));
}

#[derive(Debug)]
pub struct TorrentClient {
    file_directory: PathBuf,

    ///
    /// Shared state of chunks. Needs to be wrapped in an `Arc<RwLock<...>>` because
    /// when downloading files, a new task is created. Each task needs to be able to change
    /// the state of the chunks.
    chunk_states: Arc<RwLock<ChunkStates>>,
    server_session: ClientServerSession<TokioServerSocketWrapper>,
}

#[derive(Debug)]
struct Peer {
    ip: u32,
    port: u16,
}

impl TorrentClient {
    pub fn new<P: Into<PathBuf>>(
        file_directory: P,
        chunk_states: ChunkStates,
        server_session: ClientServerSession<TokioServerSocketWrapper>,
    ) -> Self {
        Self {
            file_directory: file_directory.into(),
            chunk_states: Arc::new(RwLock::new(chunk_states)),
            server_session,
        }
    }

    #[instrument(skip(self))]
    async fn list_files_from_server(&mut self) -> anyhow::Result<Vec<String>> {
        let message = client_proto::ListAvailableFilesRequest {};
        self.server_session
            .send_msg(&message)
            .await
            .context("failed to send list files request")?;

        let response: server_proto::ListAvailableFilesResponse = self
            .server_session
            .recv_msg()
            .await
            .context("failed to receive list files response")?;

        Ok(response.filenames)
    }

    #[instrument(skip(self))]
    async fn get_peers_to_download_file(&mut self, filename: &str) -> anyhow::Result<Vec<Peer>> {
        let message = client_proto::DownloadFile {
            filename: filename.to_string(),
        };
        self.server_session
            .send_msg(&message)
            .await
            .context("failed to send get peers request")?;

        let response: server_proto::DownloadFileResponse = self
            .server_session
            .recv_msg()
            .await
            .context("failed to receive get peers response")?;

        let peers = response
            .peers
            .into_iter()
            .filter_map(|p| {
                if p.port > u16::MAX as u32 {
                    tracing::warn!("invalid port received from server for ip {}", p.ip);
                    return None;
                }
                Some(Peer {
                    ip: p.ip,
                    port: p.port as u16,
                })
            })
            .collect();

        Ok(peers)
    }

    #[instrument(skip(self))]
    async fn download_file(&mut self, filename: &str) -> anyhow::Result<()> {
        let peers = self
            .get_peers_to_download_file(filename)
            .await
            .context("failed to get peers to download file")?;

        let single_file_manager = SingleFileManager::new(self.file_directory.join(filename));

        tokio::spawn(download_file_loop(
            peers,
            self.chunk_states.clone(),
            single_file_manager,
        ));

        Ok(())
    }
}

///
/// Main loop for downloading a file
#[instrument(skip(chunk_states))]
async fn download_file_loop(
    peers: Vec<Peer>,
    chunk_states: Arc<RwLock<ChunkStates>>,
    file_manager: SingleFileManager,
) {
}

///
/// Main loop of the client
///
///
#[instrument(skip(client))]
pub async fn client_main_loop(client: TorrentClient) -> anyhow::Result<()> {
    Ok(())
}
