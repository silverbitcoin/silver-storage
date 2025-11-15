//! Error types for storage operations

use thiserror::Error;

/// Storage error type
#[derive(Debug, Error)]
pub enum Error {
    /// Storage backend error (RocksDB)
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Data not found
    #[error("Data not found: {0}")]
    NotFound(String),

    /// Invalid data
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Core error from silver-core crate
    #[error("Core error: {0}")]
    Core(#[from] silver_core::Error),

    /// Bincode serialization error
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for storage operations
pub type Result<T> = std::result::Result<T, Error>;
