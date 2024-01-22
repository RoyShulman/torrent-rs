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
//!     "num_chunks": 5,
//!     "chunk_size": 1024,
//!    "downloaded_chunks": [1, 2, 3]
//! }
//!
//! This isn't very efficent, but it's simple and works for now.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;

///
/// This struct is used to keep track of the state of the file chunks.
#[derive(Debug, Serialize, Deserialize)]
struct FileChunksState {
    ///
    /// The name of the file.
    filename: String,
    ///
    /// The total number of chunks the file is split into.
    num_chunks: u64,
    ///
    /// A vector of chunk numbers that have been downloaded.
    downloaded_chunks: Vec<u64>,

    ///
    /// The size of each chunk.
    chunk_size: u64,
}

impl FileChunksState {
    ///
    /// Returns all the missing chunk indexes for this file.
    fn get_missing_chunks(&self) -> Vec<u64> {
        let downloaded_set = self
            .downloaded_chunks
            .iter()
            .collect::<std::collections::HashSet<_>>();

        let mut missing_chunks = vec![];

        for i in 0..self.num_chunks {
            if !downloaded_set.contains(&i) {
                missing_chunks.push(i);
            }
        }

        missing_chunks
    }
}

pub struct ChunkStates {
    ///
    /// A map of file names to their chunk states.
    states: HashMap<String, FileChunksState>,

    ///
    /// The directory where the chunk states are stored.
    chunks_dir: PathBuf,
}

impl ChunkStates {
    pub fn new<I: Into<PathBuf>>(chunks_dir: I) -> Self {
        Self {
            states: HashMap::new(),
            chunks_dir: chunks_dir.into(),
        }
    }

    ///
    /// Returns all the missing chunk indexes for the given file.
    fn get_missing_chunks(&self, filename: &str) -> anyhow::Result<Vec<u64>> {
        let Some(state) = self.states.get(filename) else {
            anyhow::bail!("no chunk state for file {}", filename);
        };

        Ok(state.get_missing_chunks())
    }

    ///
    /// Upon initialization, loads all chunk states from the chunks directory.
    /// This is used for efficiency, so that we don't have to read the chunk states from disk every time we need them.
    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn load_from_directory(&mut self) -> anyhow::Result<()> {
        let mut dir = tokio::fs::read_dir(&self.chunks_dir)
            .await
            .context("failed to read chunks directory")?;

        while let Some(entry) = dir.next_entry().await.context("failed to read entry")? {
            let path = entry.path();
            let filename = match path.file_name() {
                Some(filename) => filename.to_string_lossy().to_string(),
                None => continue,
            };

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

            self.states.insert(filename, state);
        }

        Ok(())
    }

    ///
    /// Creates a new chunk state file for the given file, initializing it with the given number of chunks and the chunk size.
    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn create_new_file(
        &mut self,
        filename: &str,
        num_chunks: u64,
        chunk_size: u64,
    ) -> anyhow::Result<()> {
        if self.states.contains_key(filename) {
            anyhow::bail!("chunk state for file {} already exists", filename);
        }

        let entry = self
            .states
            .entry(filename.to_string())
            .or_insert_with(|| FileChunksState {
                filename: filename.to_string(),
                num_chunks,
                downloaded_chunks: vec![],
                chunk_size,
            });

        write_state(&self.chunks_dir.join(filename), entry)
            .await
            .context("failed to write state in create file")
    }

    ///
    /// Upon a new chunk being downloaded, updates the chunk state for the given file, and writes it to disk.
    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn update_new_downloaded_chunk(
        &mut self,
        filename: &str,
        chunk_index: u64,
    ) -> anyhow::Result<()> {
        let entry = self
            .states
            .get_mut(filename)
            .context("no chunk state for file")?;

        if entry.downloaded_chunks.contains(&chunk_index) {
            anyhow::bail!("chunk {} already downloaded", chunk_index);
        }

        entry.downloaded_chunks.push(chunk_index);

        write_state(&self.chunks_dir.join(filename), entry)
            .await
            .context("failed to write state in update downloaded chunk")
    }
}

///
/// Serialize the given state to a JSON string and write it to the given path.
async fn write_state(path: &Path, state: &FileChunksState) -> anyhow::Result<()> {
    let serialized = serde_json::to_string(state).context("failed to serialize state")?;

    tokio::fs::write(path, serialized)
        .await
        .context("failed to write state to file")
}

#[cfg(test)]
mod tests {

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_get_missing_chunks_for_single_state() {
        let state = FileChunksState {
            filename: "file1".to_string(),
            num_chunks: 5,
            downloaded_chunks: vec![1, 2, 3],
            chunk_size: 1024,
        };

        let missing_chunks = state.get_missing_chunks();
        assert_eq!(missing_chunks, vec![0, 4]);
    }

    #[test]
    fn test_get_missing_chunks_non_existent_file() {
        let states = ChunkStates::new("chunks");
        states
            .get_missing_chunks("this_file_does_not_exist")
            .unwrap_err();
    }

    #[test]
    fn test_get_missing_chunks() {
        let mut states = ChunkStates::new("chunks");
        states.states.insert(
            "file1".to_string(),
            FileChunksState {
                filename: "file1".to_string(),
                num_chunks: 5,
                downloaded_chunks: vec![1, 2, 3],
                chunk_size: 1024,
            },
        );

        let missing_chunks = states.get_missing_chunks("file1").unwrap();
        assert_eq!(missing_chunks, vec![0, 4]);
    }

    #[tokio::test]
    async fn test_create_new_file() {
        let directory = tempdir().unwrap();
    }
}
