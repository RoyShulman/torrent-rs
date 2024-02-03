//!
//! Manages the state of available file chunks.
//!
//! When downloading or seeding a file, an entry is created in the chunks directory for the file.
//! This entry keeps track of the chunks that have been downloaded, so that the client can resume downloading
//! even if it is stopped and restarted. Each time a new chunk is downloaded, the file is updated.
//!
//! The module keeps track of the state of each file in a directory, so only upon initialization it reads the chunk states from disk.
//! It does however write the chunk states to disk every time a new chunk is downloaded.
//!
//! Each entry is a JSON file with the following structure:
//! ```json
//! {
//!    "filename": "file1",
//!    "num_chunks": 5,
//!    "chunk_size": 1024,
//!    "missing_chunks": [1, 2, 3],
//!    "chunk_states_directory": "/tmp/chunk_states"
//! }
//! ```
//!
//! This isn't very efficent, but it's simple and works for now.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;

///
/// For every downloading or seeding file, we keep track of the chunks that have been downloaded, as well as some metadata.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SingleFileChunksState {
    ///
    /// The name of the file.
    pub filename: String,
    ///
    /// The total number of chunks the file is split into.
    pub num_chunks: u64,
    ///
    /// A hashset of chunk numbers that are still missing and need to be downloaded.
    pub missing_chunks: HashSet<u64>,
    ///
    /// The size of each chunk.
    pub chunk_size: u64,
    ///
    /// The directory where this file is stored.
    chunk_states_directory: PathBuf,
}

impl SingleFileChunksState {
    #[tracing::instrument(err(Debug))]
    pub async fn from_filename(directory: &Path, filename: &str) -> anyhow::Result<Self> {
        let state_file = directory.join(format!("{filename}.json"));
        let content = read_to_string(&state_file)
            .await
            .context("failed to read state file")?;

        serde_json::from_str(&content).context("failed to deserialize state")
    }

    ///
    /// Creates a new state for a file that has not been downloaded yet.
    /// All chunks are considered missing.
    pub fn new_file_to_download<P: Into<PathBuf>>(
        filename: &str,
        chunk_states_directory: P,
        num_chunks: u64,
        chunk_size: u64,
    ) -> Self {
        Self {
            filename: filename.to_string(),
            num_chunks,
            missing_chunks: HashSet::from_iter(0..num_chunks),
            chunk_size,
            chunk_states_directory: chunk_states_directory.into(),
        }
    }

    ///
    /// Create a new state for a file that is created by the client and can be seeded.
    /// All chunks are considered available.
    pub fn new_existing_file<P: Into<PathBuf>>(
        filename: &str,
        chunk_states_directory: P,
        num_chunks: u64,
        chunk_size: u64,
    ) -> Self {
        Self {
            filename: filename.to_string(),
            num_chunks,
            missing_chunks: HashSet::new(),
            chunk_size,
            chunk_states_directory: chunk_states_directory.into(),
        }
    }

    ///
    /// Returns all the available chunk indexes for this file.
    /// This method is more costly than missing chunks because it has to iterate over all the missing chunks and returns the difference
    pub fn get_available_chunks(&self) -> Vec<u64> {
        let all_chunks = HashSet::from_iter(0..self.num_chunks);
        all_chunks
            .difference(&self.missing_chunks)
            .into_iter()
            .copied()
            .collect()
    }

    ///
    /// Serialize the given state to a JSON string and write it to the given path.
    #[tracing::instrument(skip(self), err(Debug))]
    async fn serialize_to_file(&self) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(self).context("failed to serialize state")?;

        tokio::fs::write(
            &self.chunk_states_directory.join(&self.filename),
            serialized,
        )
        .await
        .context("failed to write state to file")
    }

    ///
    /// Upon a new chunk being downloaded, updates the chunk state for the given file, and writes it to disk.
    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn update_new_chunk(&mut self, chunk_index: u64) -> anyhow::Result<()> {
        tracing::debug!("Got new chunk",);

        if !self.missing_chunks.remove(&chunk_index) {
            anyhow::bail!("chunk {} already downloaded", chunk_index);
        }

        self.serialize_to_file()
            .await
            .context("failed to write state in update new chunk")
    }

    pub fn get_chunk_offset(&self, chunk_index: u64) -> u64 {
        chunk_index * self.chunk_size
    }
}

///
/// Upon initialization, loads all chunk states from the chunks directory.
#[tracing::instrument(err(Debug), fields(chunks_dir = %chunks_dir.as_ref().display()))]
pub async fn load_from_directory<P: AsRef<Path>>(
    chunks_dir: P,
) -> anyhow::Result<Vec<SingleFileChunksState>> {
    tracing::info!(
        "loading chunk states from directory: {}",
        chunks_dir.as_ref().display()
    );
    let mut dir = tokio::fs::read_dir(chunks_dir)
        .await
        .context("failed to read chunks directory")?;

    let mut chunk_states = Vec::new();

    while let Some(entry) = dir.next_entry().await.context("failed to read entry")? {
        let path = entry.path();

        let content = match read_to_string(&path).await {
            Ok(content) => content,
            Err(err) => {
                tracing::warn!(path = %path.display(), "failed to read chunk state: {}", err);
                continue;
            }
        };

        let state = match serde_json::from_str(&content) {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(path = %path.display(), "failed to deserialize chunk state: {}", err);
                continue;
            }
        };

        chunk_states.push(state);
    }

    Ok(chunk_states)
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_get_missing_chunks() {
        let directory = tempdir().unwrap();
        let state = SingleFileChunksState {
            filename: "file1".to_string(),
            num_chunks: 5,
            missing_chunks: HashSet::from_iter([1, 2, 3]),
            chunk_size: 1024,
            chunk_states_directory: directory.path().to_path_buf(),
        };

        let missing_chunks = state.missing_chunks.into_iter().sorted().collect_vec();
        assert_eq!(missing_chunks, vec![1, 2, 3]);
    }

    #[test]
    fn test_create_new_file_to_download() {
        let directory = tempdir().unwrap();
        let state = SingleFileChunksState::new_file_to_download("file1", directory.path(), 5, 1024);
        let missing_chunks = state.missing_chunks.into_iter().sorted().collect_vec();
        assert_eq!(missing_chunks, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_create_new_existing_file() {
        let directory = tempdir().unwrap();
        let state = SingleFileChunksState::new_existing_file("file1", directory.path(), 5, 1024);
        let missing_chunks = state.missing_chunks.into_iter();
        assert_eq!(missing_chunks.count(), 0);
    }

    #[tokio::test]
    async fn test_update_new_downloaded_chunk() {
        let directory = tempdir().unwrap();
        let mut state =
            SingleFileChunksState::new_file_to_download("file1", directory.path(), 5, 1024);
        state.serialize_to_file().await.unwrap();
        state.update_new_chunk(1).await.unwrap();
        state.update_new_chunk(3).await.unwrap();

        let content = tokio::fs::read_to_string(directory.path().join("file1"))
            .await
            .unwrap();

        let state: SingleFileChunksState = serde_json::from_str(&content).unwrap();
        assert_eq!(
            state,
            SingleFileChunksState {
                filename: "file1".to_string(),
                num_chunks: 5,
                missing_chunks: HashSet::from_iter([0, 2, 4]),
                chunk_size: 1024,
                chunk_states_directory: directory.into_path(),
            }
        );
    }

    #[tokio::test]
    async fn test_load_from_directory() {
        let directory = tempdir().unwrap();
        tokio::fs::write(
            directory.path().join("file1"),
            format!(
                r#"{{
                "filename": "file1",
                "num_chunks": 5,
                "chunk_size": 1024,
                "missing_chunks": [1, 3],
                "chunk_states_directory": "{}"
            }}"#,
                directory.path().display(),
            ),
        )
        .await
        .unwrap();
        tokio::fs::write(
            directory.path().join("file2"),
            format!(
                r#"{{
                "filename": "file2",
                "num_chunks": 5,
                "chunk_size": 2048,
                "missing_chunks": [0],
                "chunk_states_directory": "{}"
            }}"#,
                directory.path().display(),
            ),
        )
        .await
        .unwrap();

        let states = load_from_directory(directory.path())
            .await
            .unwrap()
            .into_iter()
            .sorted_by(|a, b| a.filename.cmp(&b.filename))
            .collect_vec();

        assert_eq!(
            states,
            vec![
                SingleFileChunksState {
                    filename: "file1".to_string(),
                    num_chunks: 5,
                    missing_chunks: HashSet::from_iter([1, 3]),
                    chunk_size: 1024,
                    chunk_states_directory: directory.path().to_path_buf(),
                },
                SingleFileChunksState {
                    filename: "file2".to_string(),
                    num_chunks: 5,
                    missing_chunks: HashSet::from_iter([0]),
                    chunk_size: 2048,
                    chunk_states_directory: directory.path().to_path_buf(),
                }
            ]
        );
    }
}
