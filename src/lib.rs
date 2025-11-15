//! # SilverBitcoin Storage
//!
//! RocksDB-based persistent storage for blockchain data.
//!
//! This crate provides:
//! - Object store with versioning
//! - Transaction and snapshot storage
//! - Event storage with indexing
//! - Flexible attributes (dynamic fields)
//! - Write-ahead logging for crash recovery
//! - Data pruning and compression

#![warn(missing_docs, rust_2018_idioms)]
#![forbid(unsafe_code)]

mod error;
pub mod db;
pub mod object_store;
pub mod transaction_store;
pub mod snapshot_store;
pub mod event_store;
pub mod attributes;
pub mod cache;
pub mod recovery;

pub use error::{Error, Result};
pub use db::{RocksDatabase, CF_OBJECTS, CF_OWNER_INDEX, CF_TRANSACTIONS, CF_SNAPSHOTS, CF_EVENTS, CF_FLEXIBLE_ATTRIBUTES};
pub use object_store::ObjectStore;
pub use transaction_store::TransactionStore;
pub use snapshot_store::SnapshotStore;
pub use event_store::EventStore;
pub use attributes::AttributeStore;
pub use cache::ObjectCache;
pub use recovery::{RecoveryManager, RecoveryStats, PruningConfig, PruningStats, DatabaseHealth};
