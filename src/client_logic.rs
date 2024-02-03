use core::time;
use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use tokio::{
    sync::{oneshot, Mutex},
    time::timeout,
};
use tracing::instrument;

use crate::modules::{
    chunks::SingleFileChunksState,
    connected_session::{ConnectedSession, TokioSocketWrapper},
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
        existing_chunk_states: Vec<SingleFileChunksState>,
        server_session: ConnectedSession<TokioSocketWrapper>,
    ) -> Self {
        Self {
            file_directory: file_directory.into(),
            states_directory: states_directory.into(),
            server_session: Mutex::new(server_session),
            // currently_downloading: HashMap::new(),
        }
    }

    // #[instrument(skip(self))]
    // pub async fn continue_downloading_files(&mut self) -> anyhow::Result<()> {
    //     let files_to_continue_downloading: Vec<_> = self
    //         .chunk_states_communication
    //         .chunk_states
    //         .get_all_non_completed_files()
    //         .cloned()
    //         .collect();

    //     for filename in files_to_continue_downloading {
    //         self.continue_downloading_existing_file(&filename)
    //             .await
    //             .with_context(|| format!("failed to continue downloading file: {}", filename))?;
    //     }

    //     Ok(())
    // }

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

///
/// Main loop of the client
///
///
#[instrument(skip(client))]
pub async fn client_main_loop(mut client: TorrentClient) -> anyhow::Result<()> {
    tracing::info!("Starting client main loop");

    loop {
        let files = client.list_files_from_server().await?;
        tracing::info!("Available files: {:?}", files);

        tokio::time::sleep(time::Duration::from_secs(5)).await;
    }
}
