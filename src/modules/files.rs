use std::path::PathBuf;

use anyhow::Context;
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub struct FilesManager {
    directory: PathBuf,
}

impl FilesManager {
    pub fn new<T: Into<PathBuf>>(path: T) -> Self {
        Self {
            directory: path.into(),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn read_chunk(
        &self,
        filename: &str,
        offset: u64,
        chunk_size: usize,
    ) -> anyhow::Result<Vec<u8>> {
        let path = self.directory.join(filename);
        let mut file = File::open(path).await.context("failed to open file")?;
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
    pub async fn create_file(&self, filename: &str, filesize: u64) -> anyhow::Result<()> {
        let path = self.directory.join(filename);

        tracing::info!("creating file {}", path.display());

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .context("failed to create file")?;

        file.set_len(filesize)
            .await
            .context("failed to set file size")?;

        Ok(())
    }

    #[tracing::instrument(skip(self, buffer))]
    pub async fn write_chunk(
        &self,
        filename: &str,
        buffer: &[u8],
        offset: u64,
    ) -> anyhow::Result<()> {
        let path = self.directory.join(filename);

        let mut file = OpenOptions::new()
            .write(true)
            .open(path)
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
        let files_manager = FilesManager::new(directory.path());

        files_manager.create_file("file1", 5).await.unwrap();

        let filename = "file1";
        let buffer = vec![1, 2, 3, 4, 5];
        let offset = 0;

        files_manager
            .write_chunk(filename, &buffer, offset)
            .await
            .unwrap();

        let mut file = File::open(directory.path().join(filename)).await.unwrap();
        let mut read_buffer = vec![0; buffer.len()];
        file.read_exact(&mut read_buffer).await.unwrap();

        assert_eq!(buffer, read_buffer);
    }

    #[tokio::test]
    async fn test_write_nonexistent_file() {
        let directory = tempdir().unwrap();
        let files_manager = FilesManager::new(directory.path());

        let filename = "file1";
        let buffer = vec![1, 2, 3, 4, 5];
        let offset = 0;

        let result = files_manager.write_chunk(filename, &buffer, offset).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_chunk_offset() {
        let directory = tempdir().unwrap();
        let files_manager = FilesManager::new(directory.path());

        files_manager.create_file("file1", 10).await.unwrap();

        let filename = "file1";
        let buffer = vec![1, 2, 3, 4, 5];

        files_manager
            .write_chunk(filename, &buffer, 5)
            .await
            .unwrap();

        let mut file = File::open(directory.path().join(filename)).await.unwrap();
        let mut read_buffer = vec![0; 10];
        file.read_exact(&mut read_buffer).await.unwrap();

        assert_eq!(vec![0; 5], read_buffer[..5]);
        assert_eq!(buffer, read_buffer[5..]);
    }
}
