use std::path::PathBuf;

use anyhow::Context;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

#[derive(Debug)]
pub struct SingleFileManager {
    path: PathBuf,
}

impl SingleFileManager {
    pub fn new<T: Into<PathBuf>>(path: T) -> Self {
        Self { path: path.into() }
    }

    #[tracing::instrument(skip(self))]
    pub async fn read_chunk(
        &self,
        filename: &str,
        offset: u64,
        chunk_size: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let mut file = File::open(&self.path)
            .await
            .context("failed to open file")?;
        file.seek(io::SeekFrom::Start(offset))
            .await
            .context("failed to see to offset")?;

        let mut buffer = vec![0; chunk_size];
        let bytes_read = file
            .read(&mut buffer)
            .await
            .context("failed to read chunk")?;

        buffer.truncate(bytes_read);

        Ok(buffer)
    }

    #[tracing::instrument(skip(self))]
    pub async fn create(&self, filesize: u64) -> anyhow::Result<()> {
        tracing::info!("creating file {}", self.path.display());

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
            .await
            .context("failed to create file")?;

        file.set_len(filesize)
            .await
            .context("failed to set file size")?;

        Ok(())
    }

    #[tracing::instrument(skip(self, buffer))]
    pub async fn write_chunk(&self, buffer: &[u8], offset: u64) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .await
            .context("failed to open file")?;

        file.seek(io::SeekFrom::Start(offset))
            .await
            .context("failed to seek to offset")?;

        file.write_all(buffer)
            .await
            .context("failed to write chunk")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_write_chunk() {
        let directory = tempdir().unwrap();
        let filename = "file1";
        let file_path = directory.path().join(filename);
        let files_manager = SingleFileManager::new(file_path);

        files_manager.create(5).await.unwrap();

        let buffer = vec![1, 2, 3, 4, 5];
        let offset = 0;

        files_manager.write_chunk(&buffer, offset).await.unwrap();

        let mut file = File::open(directory.path().join(filename)).await.unwrap();
        let mut read_buffer = vec![0; buffer.len()];
        file.read_exact(&mut read_buffer).await.unwrap();

        assert_eq!(buffer, read_buffer);
    }

    #[tokio::test]
    async fn test_write_nonexistent_file() {
        let directory = tempdir().unwrap();
        let filename = "file1";
        let file_path = directory.path().join(filename);
        let files_manager = SingleFileManager::new(file_path);

        let buffer = vec![1, 2, 3, 4, 5];
        let offset = 0;

        let result = files_manager.write_chunk(&buffer, offset).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_chunk_offset() {
        let directory = tempdir().unwrap();
        let filename = "file1";
        let file_path = directory.path().join(filename);
        let files_manager = SingleFileManager::new(file_path);

        files_manager.create(10).await.unwrap();

        let buffer = vec![1, 2, 3, 4, 5];

        files_manager.write_chunk(&buffer, 5).await.unwrap();

        let mut file = File::open(directory.path().join(filename)).await.unwrap();
        let mut read_buffer = vec![0; 10];
        file.read_exact(&mut read_buffer).await.unwrap();

        assert_eq!(vec![0; 5], read_buffer[..5]);
        assert_eq!(buffer, read_buffer[5..]);
    }
}
