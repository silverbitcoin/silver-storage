//! Error types for storage operations

use thiserror::Error;

/// Storage error type
#[derive(Debug, Error)]
pub enum Error {
    /// Storage backend error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Data not found
    #[error("Data not found: {0}")]
    NotFound(String),

    /// Invalid data
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Core error
    #[error("Core error: {0}")]
    Core(#[from] silver_core::Error),
}

/// Result type for storage operations
pub type Result<T> = std::result::Result<T, Error>;
