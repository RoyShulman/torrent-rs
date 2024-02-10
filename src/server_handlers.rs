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
        client_proto::request_to_server::Request::UpdateNewAvailableFileOnClient(request) => {
            handle_new_file_on_client(request, files_directory, &connected_client.peer_addr)
                .await
                .context("failed to handle new file on client")
        }
        client_proto::request_to_server::Request::UpdateFileRemovedFromClient(request) => {
            handle_file_removed_from_client(request, files_directory, &connected_client.peer_addr)
                .await
                .context("failed to handle file removed from client")
        }
    }
}

#[instrument(err(Debug))]
async fn handle_file_removed_from_client(
    request: client_proto::UpdateFileRemovedFromClient,
    files_directory: &Path,
    peer_addr: &std::net::SocketAddr,
) -> anyhow::Result<()> {
    tracing::info!("Handling file removed from client request: {:?}", request);

    let metadata_file = files_directory.join(format!("{}.json", request.filename));

    if !metadata_file.exists() {
        return Ok(());
    }

    let file_data = read_to_string(&metadata_file)
        .await
        .context("failed to read file data")?;

    let mut file_data: AvailableFile =
        serde_json::from_str(&file_data).context("failed to deserialize file")?;
    file_data.peers.remove(&peer_addr.ip().to_string());
    if file_data.peers.is_empty() {
        tokio::fs::remove_file(metadata_file)
            .await
            .context("failed to remove metadata file")
    } else {
        let serialized = serde_json::to_string(&file_data).context("failed to serialize file")?;
        tokio::fs::write(metadata_file, serialized)
            .await
            .context("failed to write file data")
    }
}

///
/// Update the server with the new file that is available on the client.
/// This will check if metadata for the file already exists, and if it does will only add the peer address to the list of peers.
#[instrument(err(Debug))]
async fn handle_new_file_on_client(
    request: client_proto::UpdateNewAvailableFileOnClient,
    files_directory: &Path,
    peer_addr: &std::net::SocketAddr,
) -> anyhow::Result<()> {
    // TODO: there is a race here, if two clients add the same file at the same time, the file will be overwritten and we will lose the peers from one of the clients
    tracing::info!("Handling new file on client request: {:?}", request);

    let metadata_file = files_directory.join(format!("{}.json", request.filename));
    let mut file_data = if metadata_file.exists() {
        let file_data = read_to_string(&metadata_file)
            .await
            .context("failed to read file data")?;

        serde_json::from_str(&file_data).context("failed to deserialize file")?
    } else {
        AvailableFile {
            filename: request.filename,
            num_chunks: request.num_chunks,
            chunk_size: request.chunk_size,
            file_size: request.file_size,
            peers: HashSet::new(),
        }
    };

    file_data.peers.insert(peer_addr.ip().to_string());
    let serialized = serde_json::to_string(&file_data).context("failed to serialize file")?;
    tokio::fs::write(metadata_file, serialized)
        .await
        .context("failed to write file data")
}

///
/// Struct that represents an available file on the network.
/// The server keeps track of all the peers that have the file.
#[derive(Debug, Serialize, Deserialize)]
struct AvailableFile {
    filename: String,
    num_chunks: u64,
    chunk_size: u64,
    file_size: u64,
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

        // skip non-json files
        if path.extension().map(|ext| ext != "json").unwrap_or(true) {
            continue;
        }

        let file_data = read_to_string(path)
            .await
            .context("failed to read file data")?;

        let file: AvailableFile =
            serde_json::from_str(&file_data).context("failed to deserialize file")?;
        files.push(file);
    }

    let files = files
        .into_iter()
        .map(
            |file| server_proto::list_available_files_response::SingleFile {
                filename: file.filename,
                peers: file.peers.into_iter().collect(),
                chunk_size: file.chunk_size,
                num_chunks: file.num_chunks,
                file_size: file.file_size,
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
        let file1 = AvailableFile {
            filename: "file1".to_string(),
            num_chunks: 5,
            chunk_size: 10,
            file_size: 50,
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

    #[tokio::test]
    async fn test_handle_new_file_on_client_new_file() {
        let directory = tempdir().unwrap();
        let request = client_proto::UpdateNewAvailableFileOnClient {
            filename: "file1".to_string(),
            num_chunks: 5,
            chunk_size: 10,
            file_size: 50,
        };

        let peer_addr = "127.0.0.1:1".parse().unwrap();
        handle_new_file_on_client(request, directory.path(), &peer_addr)
            .await
            .unwrap();

        let file_data = read_to_string(directory.path().join("file1.json"))
            .await
            .unwrap();
        let file: AvailableFile = serde_json::from_str(&file_data).unwrap();
        assert_eq!(file.filename, "file1");
        assert_eq!(file.num_chunks, 5);
        assert_eq!(file.chunk_size, 10);
        assert_eq!(file.peers, HashSet::from_iter(["127.0.0.1".to_string()]));
    }

    #[tokio::test]
    async fn test_handle_new_file_on_client_existing_file() {
        let directory = tempdir().unwrap();
        let file = AvailableFile {
            filename: "file1".to_string(),
            num_chunks: 5,
            chunk_size: 10,
            file_size: 45,
            peers: HashSet::from_iter(["1.2.3.4".to_string()]),
        };

        serde_json::to_writer(
            std::fs::File::create(directory.path().join("file1.json")).unwrap(),
            &file,
        )
        .unwrap();

        let request = client_proto::UpdateNewAvailableFileOnClient {
            filename: "file1".to_string(),
            num_chunks: 5,
            chunk_size: 10,
            file_size: 47,
        };
        let peer_addr = "127.0.0.1:1".parse().unwrap();
        handle_new_file_on_client(request, directory.path(), &peer_addr)
            .await
            .unwrap();

        let file_data = read_to_string(directory.path().join("file1.json"))
            .await
            .unwrap();

        let file: AvailableFile = serde_json::from_str(&file_data).unwrap();
        assert_eq!(file.filename, "file1");
        assert_eq!(file.num_chunks, 5);
        assert_eq!(file.chunk_size, 10);
        assert_eq!(
            file.peers,
            HashSet::from_iter(["1.2.3.4".to_string(), "127.0.0.1".to_string()])
        );
    }
}
