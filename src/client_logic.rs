use anyhow::Context;
use futures::{stream::FuturesUnordered, StreamExt};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{oneshot, Mutex},
    time::timeout,
};
use tracing::instrument;

use crate::{
    client_rest_api::CurrentlyDowloadingFile,
    download_file::{DownloadFileContext, DownloadFileContextBuilder, DownloadFileResult},
    modules::{
        chunks::{
            determine_chunk_size, load_from_directory, DeterminedChunkSize, SingleFileChunksState,
        },
        connected_session::{ConnectedSession, TokioSocketWrapper},
        files::SingleChunkedFile,
    },
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
    states_directory: PathBuf,
    peer_port: u16,

    ///
    /// This needs to be a mutex because when sending messages to the server, we need the response to be in order
    /// and we can't have multiple threads sending messages at the same time.
    server_session: Arc<Mutex<ConnectedSession<TokioSocketWrapper>>>,
    //
    // A hashmap that stores all the currently downloading files.
    currently_downloading: HashMap<String, DownloadFileContext>,

    done_receivers: FuturesUnordered<oneshot::Receiver<DownloadFileResult>>,
}

impl TorrentClient {
    pub fn new<P: Into<PathBuf>>(
        file_directory: P,
        states_directory: P,
        server_session: ConnectedSession<TokioSocketWrapper>,
        peer_port: u16,
    ) -> Self {
        Self {
            file_directory: file_directory.into(),
            states_directory: states_directory.into(),
            server_session: Arc::new(Mutex::new(server_session)),
            currently_downloading: HashMap::new(),
            done_receivers: FuturesUnordered::new(),
            peer_port,
        }
    }

    ///
    /// Lists the locally stored chunks states files
    #[instrument(skip(self))]
    pub async fn list_local_files(&self) -> anyhow::Result<Vec<SingleFileChunksState>> {
        load_from_directory(&self.states_directory)
            .await
            .context("failed to list local files")
    }

    ///
    /// Registers a new file the client wants to share with the server and other clients.
    /// This will write the file to the files directory, and create a new chunk state file for it.
    /// Then, it will send the metadata to the server.
    #[instrument(skip(self, file_data), err(Debug))]
    pub async fn register_new_client_file(
        &mut self,
        filename: &str,
        file_data: Vec<u8>,
    ) -> anyhow::Result<()> {
        tracing::info!("registering new client file");
        let DeterminedChunkSize {
            num_chunks,
            chunk_size,
        } = determine_chunk_size(file_data.len() as u64);

        let chunk_state_file = SingleFileChunksState::new_existing_file(
            filename,
            &self.states_directory,
            num_chunks,
            chunk_size,
            file_data.len() as u64,
        );

        let server_update_message = client_proto::UpdateNewAvailableFileOnClient {
            filename: filename.to_string(),
            num_chunks,
            chunk_size,
            file_size: file_data.len() as u64,
        };

        let request = client_proto::RequestToServer {
            request: Some(
                client_proto::request_to_server::Request::UpdateNewAvailableFileOnClient(
                    server_update_message,
                ),
            ),
        };
        let mut server_session = self.server_session.lock().await;

        let (serialize_state_result, write_data_file_result, send_update_to_server_result) = tokio::join!(
            chunk_state_file.serialize_to_file(),
            SingleChunkedFile::with_existing_data(self.file_directory.join(filename), file_data),
            server_session.send_msg(&request)
        );

        serialize_state_result.context("failed to serialize chunk state")?;
        write_data_file_result.context("failed to write file data")?;
        send_update_to_server_result.context("failed to update new available file on client")?;

        Ok(())
    }

    ///
    /// List the files that are available on the server to download
    #[instrument(skip(self))]
    pub async fn list_files_from_server(
        &mut self,
    ) -> anyhow::Result<Vec<server_proto::list_available_files_response::SingleFile>> {
        let list_files = client_proto::ListAvailableFilesRequest {};
        let request = client_proto::RequestToServer {
            request: Some(client_proto::request_to_server::Request::ListAvailableFiles(list_files)),
        };
        let mut server_session = self.server_session.lock().await;
        server_session
            .send_msg(&request)
            .await
            .context("failed to send list files request")?;

        let response: server_proto::ListAvailableFilesResponse = server_session
            .recv_msg()
            .await
            .context("failed to receive list files response")?
            .context("empty response to list files request")?;

        Ok(response.files)
    }

    ///
    /// Deletes a file store locally on the client. Does the following:
    ///     - Sends a message to the server to remove the file from the server's list of available files
    ///     - Deletes the chunk state file
    ///     - Deletes the data file
    pub async fn delete_file(&mut self, filename: &str) -> anyhow::Result<()> {
        let request = client_proto::UpdateFileRemovedFromClient {
            filename: filename.to_string(),
        };
        let request = client_proto::RequestToServer {
            request: Some(
                client_proto::request_to_server::Request::UpdateFileRemovedFromClient(request),
            ),
        };

        let mut server_session = self.server_session.lock().await;
        server_session
            .send_msg(&request)
            .await
            .context("failed to send delete file request")?;

        let state_file = SingleFileChunksState::from_filename(&self.states_directory, filename)
            .await
            .context("failed to load chunk state file")?;
        state_file
            .delete_file()
            .await
            .context("failed to delete chunk state file")?;

        let data_file = SingleChunkedFile::new(&self.file_directory.join(filename));
        data_file
            .delete()
            .await
            .context("failed to delete data file")
    }

    ///
    /// Downloads a file from peers on the network. This will spawn a task that will download the file.
    pub async fn download_file(
        &mut self,
        filename: &str,
        num_chunks: u64,
        chunk_size: u64,
        file_size: u64,
    ) -> anyhow::Result<()> {
        if self.currently_downloading.contains_key(filename) {
            anyhow::bail!("{filename} is already being downloaded");
        }

        let context_builer = DownloadFileContextBuilder {
            filename: filename.to_string(),
            server_session: self.server_session.clone(),
            num_chunks,
            chunk_size,
            file_size,
            chunk_states_directory: self.states_directory.clone(),
            data_files_directory: self.file_directory.clone(),
            peer_port: self.peer_port,
        };

        let initialized = context_builer
            .build()
            .await
            .context("failed to initialize download file task")?;

        let (context, done_receiver) = initialized.start_download();

        self.done_receivers.push(done_receiver);

        self.currently_downloading
            .insert(filename.to_string(), context);

        Ok(())
    }

    ///
    /// Get the progress of all currently downloading tasks.
    /// If any finished, it will remove them from the list of currently downloading tasks.
    /// Otherwise, it will return the progress of how many chunks have been downloaded for each file.
    pub async fn get_downloading_progress(&mut self) -> Vec<CurrentlyDowloadingFile> {
        let mut completed_downloads = Vec::new();
        let _ = timeout(Duration::from_millis(10), async {
            while let Some(result) = self.done_receivers.next().await {
                match result {
                    Ok(result) => completed_downloads.push(result),
                    Err(_) => {
                        tracing::error!("download file task failed, sender was dropped");
                    }
                }
            }
        })
        .await;

        for completed in &completed_downloads {
            self.currently_downloading.remove(&completed.filename);
        }

        let mut currently_downloading = Vec::with_capacity(self.currently_downloading.len());

        for (filename, context) in &self.currently_downloading {
            let state = context.current_state.borrow();
            currently_downloading.push(CurrentlyDowloadingFile {
                filename: filename.clone(),
                peers: state.current_peers.clone(),
            });
        }

        currently_downloading
    }
}
