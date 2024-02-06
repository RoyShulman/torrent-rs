use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use axum_macros::debug_handler;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::client_logic::TorrentClient;

type ClientState = Arc<RwLock<TorrentClient>>;

pub fn get_client_router(client: TorrentClient) -> Router {
    let router = Router::new()
        .route("/list_files", get(handle_list_files))
        .with_state(Arc::new(RwLock::new(client)));
    router
}

#[derive(Debug, Serialize, Deserialize)]
struct RemoteFile {
    filename: String,
    num_chunks: u64,
    chunk_size: u64,
    peers: Vec<String>,
}

#[debug_handler]
async fn handle_list_files(
    State(client): State<ClientState>,
) -> Result<Json<Vec<RemoteFile>>, ApiError> {
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
        })
        .collect_vec();

    Ok(remote_files.into())
}

struct ApiError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
