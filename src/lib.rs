//! # SilverBitcoin Storage
//!
//! Production-ready persistent storage for blockchain data using ParityDB.
//!
//! This crate provides:
//! - Object store with versioning
//! - Transaction and snapshot storage
//! - Event storage with indexing
//! - Write-ahead logging for crash recovery
//! - Data pruning and compression

#![warn(missing_docs, rust_2018_idioms)]
#![forbid(unsafe_code)]

pub mod db;
pub mod db_production;
pub mod error;
pub mod object_store;
pub mod token_store;
pub mod event_store;
pub mod block_store;
pub mod transaction_store;
pub mod snapshot_store;
pub mod mining_store;

pub use db::{ParityDatabase, DatabaseStatistics, CF_SNAPSHOTS};
pub use error::{Error, Result};
pub use object_store::ObjectStore;
pub use token_store::TokenStore;
pub use event_store::{EventStore, EventType, EventID, Event};
pub use block_store::{Block, BlockStore};
pub use transaction_store::{StoredTransaction, TransactionStore, ExecutionStatus, TransactionEffects};
pub use snapshot_store::SnapshotStore;
pub use mining_store::{MiningStore, DifficultyRecord, MiningRewardRecord, MinerStats};
