use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_macros::debug_handler;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{fs::File, io::AsyncReadExt, sync::RwLock};
use tracing::instrument;

use crate::{client_logic::TorrentClient, modules::chunks::SingleFileChunksState};

type ClientState = Arc<RwLock<TorrentClient>>;

pub fn get_client_router(client: TorrentClient) -> Router {
    let router = Router::new()
        .route("/list_server_files", get(handle_list_server_files))
        .route("/list_local_files", get(handle_list_local_files))
        .route("/upload_new_client_file", post(handle_upload_file))
        .route("/healthcheck", get(handle_healthcheck))
        .route("/delete_file", post(handle_delete_file))
        .route("/download_file", post(handle_download_file))
        .route("/currently_downloading", get(handle_currently_downloading))
        .layer(DefaultBodyLimit::disable())
        .with_state(Arc::new(RwLock::new(client)));

    router
}

#[derive(Debug, Serialize)]
pub struct CurrentlyDowloadingFile {
    pub filename: String,
    pub peers: Vec<String>,
}

async fn handle_currently_downloading(
    State(client): State<ClientState>,
) -> Result<Json<Vec<CurrentlyDowloadingFile>>, ApiError> {
    let mut client = client.write().await;

    let currently_downloading = client.get_downloading_progress().await;

    Ok(currently_downloading.into())
}

async fn handle_healthcheck() -> Json<serde_json::Value> {
    json!({
        "status": "OK",
    })
    .into()
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoteFile {
    filename: String,
    num_chunks: u64,
    chunk_size: u64,
    file_size: u64,
    peers: Vec<String>,
}

async fn handle_list_local_files(
    State(client): State<ClientState>,
) -> Result<Json<Vec<SingleFileChunksState>>, ApiError> {
    tracing::info!("Handling list local files request");
    let client = client.read().await;

    let local_files = client
        .list_local_files()
        .await
        .context("failed to list local files")?;

    Ok(local_files.into())
}

#[debug_handler]
#[instrument(skip(client))]
async fn handle_list_server_files(
    State(client): State<ClientState>,
) -> Result<Json<Vec<RemoteFile>>, ApiError> {
    tracing::info!("Handling list server files request");
    let mut client = client.write().await;

    let remote_files = client
        .list_files_from_server()
        .await
        .context("failed to list files from server")?;
    let remote_files = remote_files
        .into_iter()
        .map(|x| RemoteFile {
            filename: x.filename,
            num_chunks: x.num_chunks,
            chunk_size: x.chunk_size,
            peers: x.peers,
            file_size: x.file_size,
        })
        .collect_vec();

    Ok(remote_files.into())
}

///
/// A handler for the upload file from a client. This make the file available to all other clients by
/// publishing it as available on the server.
/// Each multipart field is a file to be uploaded.
#[instrument(skip(client, multipart), err(Debug))]
async fn handle_upload_file(
    State(client): State<ClientState>,
    mut multipart: Multipart,
) -> Result<(), ApiError> {
    tracing::info!("Handling upload file request");
    while let Some(field) = multipart
        .next_field()
        .await
        .context("failed to read field")?
    {
        let filename = field.name().context("failed to read filename")?.to_string();
        let file_data = field.bytes().await.context("failed to read file data")?;
        client
            .write()
            .await
            .register_new_client_file(&filename, file_data.to_vec())
            .await
            .context("failed to register new client file")?;
    }

    Ok(())
}

#[derive(Deserialize)]
struct FilenamePostData {
    filename: String,
}

///
/// Remove a file stored on the client. This will also notify the server about the file removal,
/// so it won't advertise this client as a peer for this file anymore.
async fn handle_delete_file(
    State(client): State<ClientState>,
    Json(request): Json<FilenamePostData>,
) -> Result<(), ApiError> {
    tracing::info!("Handling delete file request");
    client
        .write()
        .await
        .delete_file(&request.filename)
        .await
        .context("failed to delete file")?;

    Ok(())
}

#[derive(Deserialize)]
struct DownloadFileRequest {
    filename: String,
    chunk_size: u64,
    num_chunks: u64,
    file_size: u64,
}

async fn handle_download_file(
    State(client): State<ClientState>,
    Json(request): Json<DownloadFileRequest>,
) -> Result<(), ApiError> {
    tracing::info!("Handling download file request");
    client
        .write()
        .await
        .download_file(
            &request.filename,
            request.chunk_size,
            request.num_chunks,
            request.file_size,
        )
        .await
        .context("failed to download file")?;

    Ok(())
}

#[derive(Debug)]
struct ApiError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {:?}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`.
impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
