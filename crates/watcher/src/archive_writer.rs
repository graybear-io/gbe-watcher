use async_trait::async_trait;
use bytes::Bytes;

use crate::error::WatcherError;

/// Abstraction for writing archive files to cold storage.
#[async_trait]
pub trait ArchiveWriter: Send + Sync {
    async fn write(&self, path: &str, data: Bytes) -> Result<(), WatcherError>;
}

/// Filesystem-backed archive writer (for testing and local development).
pub struct FsArchiveWriter {
    base_dir: std::path::PathBuf,
}

impl FsArchiveWriter {
    pub fn new(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }
}

#[async_trait]
impl ArchiveWriter for FsArchiveWriter {
    async fn write(&self, path: &str, data: Bytes) -> Result<(), WatcherError> {
        let full_path = self.base_dir.join(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| WatcherError::Writer(e.to_string()))?;
        }
        tokio::fs::write(&full_path, &data)
            .await
            .map_err(|e| WatcherError::Writer(e.to_string()))?;
        Ok(())
    }
}
