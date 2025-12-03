//! Persistence layer error types and handling

use thiserror::Error;

/// Persistence layer error type
#[derive(Debug, Error)]
pub enum PersistenceError {
    /// Database operation failed
    #[error("Database error: {0}")]
    Database(String),

    /// Serialization/deserialization failed
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Data not found
    #[error("Data not found: {0}")]
    NotFound(String),

    /// Invalid data format or content
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Genesis block already exists
    #[error("Genesis block already exists")]
    GenesisBlockExists,

    /// Genesis block not found
    #[error("Genesis block not found")]
    GenesisBlockNotFound,

    /// Snapshot not found
    #[error("Snapshot not found: sequence {0}")]
    SnapshotNotFound(u64),

    /// Snapshot not finalized
    #[error("Snapshot not finalized: sequence {0}")]
    SnapshotNotFinalized(u64),

    /// Transaction not found
    #[error("Transaction not found: {0}")]
    TransactionNotFound(String),

    /// Validator state not found
    #[error("Validator state not found: {0}")]
    ValidatorNotFound(String),

    /// Atomic batch operation failed
    #[error("Batch operation failed: {0}")]
    BatchFailed(String),

    /// Flush operation failed
    #[error("Flush failed: {0}")]
    FlushFailed(String),

    /// Flush retry exhausted
    #[error("Flush retry exhausted after {attempts} attempts: {reason}")]
    FlushRetryExhausted {
        /// Number of retry attempts made
        attempts: u32,
        /// Reason for failure
        reason: String,
    },

    /// Recovery failed
    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    /// Consistency check failed
    #[error("Consistency check failed: {0}")]
    ConsistencyCheckFailed(String),

    /// Shutdown timeout
    #[error("Shutdown timeout after {seconds} seconds")]
    ShutdownTimeout {
        /// Number of seconds waited before timeout
        seconds: u64,
    },

    /// Partial write detected
    #[error("Partial write detected: {0}")]
    PartialWrite(String),

    /// Database corruption detected
    #[error("Database corruption detected: {0}")]
    Corruption(String),

    /// Insufficient disk space
    #[error("Insufficient disk space: {0}")]
    DiskFull(String),

    /// Permission denied
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Bincode serialization error
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    /// Storage layer error
    #[error("Storage error: {0}")]
    Storage(String),
}

/// Result type for persistence operations
pub type PersistenceResult<T> = std::result::Result<T, PersistenceError>;

impl From<crate::Error> for PersistenceError {
    fn from(err: crate::Error) -> Self {
        PersistenceError::Storage(err.to_string())
    }
}
