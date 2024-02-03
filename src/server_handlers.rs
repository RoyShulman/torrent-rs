use std::{collections::HashSet, path::Path};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::fs::{read_dir, read_to_string};
use tracing::instrument;

use crate::modules::connected_session::{ConnectedSession, TokioSocketWrapper};

pub mod client_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.client.rs"));
}
mod server_proto {
    include!(concat!(env!("OUT_DIR"), "/torrent.server.rs"));
}

#[derive(Debug)]
pub struct ConnectedClient {
    pub session: ConnectedSession<TokioSocketWrapper>,
    pub peer_addr: std::net::SocketAddr,
}

pub async fn handle_client_request(
    request: client_proto::RequestToServer,
    connected_client: &mut ConnectedClient,
    files_directory: &Path,
) -> anyhow::Result<()> {
    let Some(request) = request.request else {
        tracing::warn!("Received empty request");
        return Ok(());
    };

    match request {
        client_proto::request_to_server::Request::DownloadFile(_) => todo!(),
        client_proto::request_to_server::Request::ListAvailableFiles(_) => {
            handle_list_available_files(connected_client, files_directory)
                .await
                .context("failed to list available files")
        }
    }
}

///
/// Struct that represents an available file on the network.
/// The server keeps track of all the peers that have the file.
#[derive(Debug, Serialize, Deserialize)]
struct AvilableFile {
    filename: String,
    num_chunks: u64,
    chunk_size: u64,
    peers: HashSet<String>,
}

async fn get_available_files_response(
    files_directory: &Path,
) -> anyhow::Result<server_proto::ListAvailableFilesResponse> {
    let mut entries = read_dir(files_directory)
        .await
        .context("failed to read files directory")?;

    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await.context("failed to read entry")? {
        let path = entry.path();
        tracing::info!("Reading file: {:?}", path.extension());

        // skip non-json files
        if path.extension().map(|ext| ext != "json").unwrap_or(true) {
            continue;
        }

        let file_data = read_to_string(path)
            .await
            .context("failed to read file data")?;

        let file: AvilableFile =
            serde_json::from_str(&file_data).context("failed to deserialize file")?;
        files.push(file);
    }
    tracing::info!("Files: {:?}", files);

    let files = files
        .into_iter()
        .map(
            |file| server_proto::list_available_files_response::SingleFile {
                filename: file.filename,
                peers: file.peers.into_iter().collect(),
                chunk_size: file.chunk_size,
                num_chunks: file.num_chunks,
            },
        )
        .collect();

    Ok(server_proto::ListAvailableFilesResponse { files })
}

///
/// All the files that are available on the network are stored in this directory in json format.
#[instrument(err(Debug))]
async fn handle_list_available_files(
    connected_client: &mut ConnectedClient,
    files_directory: &Path,
) -> anyhow::Result<()> {
    tracing::info!("Handling list available files request");
    let response = get_available_files_response(files_directory).await?;
    tracing::info!("Sending list available files response: {:?}", response);

    connected_client
        .session
        .send_msg(&response)
        .await
        .context("failed to send list available files response")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_list_available_files() {
        let directory = tempdir().unwrap();
        let file1 = AvilableFile {
            filename: "file1".to_string(),
            num_chunks: 5,
            chunk_size: 10,
            peers: HashSet::from_iter(["127.0.0.1".to_string()]),
        };
        serde_json::to_writer(
            std::fs::File::create(directory.path().join("file1.json")).unwrap(),
            &file1,
        )
        .unwrap();

        let response = get_available_files_response(directory.path())
            .await
            .unwrap();

        assert_eq!(response.files.len(), 1);
        assert_eq!(response.files[0].filename, "file1");
        assert_eq!(response.files[0].peers, vec!["127.0.0.1".to_string()]);
    }

    #[tokio::test]
    async fn test_skip_non_json_files() {
        let directory = tempdir().unwrap();
        std::fs::write(directory.path().join("file1.txt"), "test").unwrap();

        let response = get_available_files_response(directory.path())
            .await
            .unwrap();

        assert_eq!(response.files.len(), 0);
    }
}
