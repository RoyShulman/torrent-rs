use anyhow::Context;
use std::path::PathBuf;
use tokio::sync::Mutex;
use tracing::instrument;

use crate::modules::{
    chunks::{
        determine_chunk_size, load_from_directory, DeterminedChunkSize, SingleFileChunksState,
    },
    connected_session::{ConnectedSession, TokioSocketWrapper},
    files::SingleChunkedFile,
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

    ///
    /// This needs to be a mutex because when sending messages to the server, we need the response to be in order
    /// and we can't have multiple threads sending messages at the same time.
    server_session: Mutex<ConnectedSession<TokioSocketWrapper>>,
    //
    // A hashmap that stores all the currently downloading files.

    // currently_downloading: HashMap<String, CurrentlyDownloadingTask>,
}

impl TorrentClient {
    pub fn new<P: Into<PathBuf>>(
        file_directory: P,
        states_directory: P,
        server_session: ConnectedSession<TokioSocketWrapper>,
    ) -> Self {
        Self {
            file_directory: file_directory.into(),
            states_directory: states_directory.into(),
            server_session: Mutex::new(server_session),
            // currently_downloading: HashMap::new(),
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
        );

        let server_update_message = client_proto::UpdateNewAvailableFileOnClient {
            filename: filename.to_string(),
            num_chunks,
            chunk_size,
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
}
