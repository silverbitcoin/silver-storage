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

pub mod attributes;
pub mod block_store;
pub mod cache;
pub mod db;
mod error;
pub mod event_store;
pub mod object_store;
pub mod ownership;
pub mod persistence;
pub mod recovery;
pub mod snapshot_store;
pub mod transaction_store;
pub mod validator_store;

pub use attributes::AttributeStore;
pub use block_store::{Block, BlockStore};
pub use cache::ObjectCache;
pub use db::{
    RocksDatabase, CF_BLOCKS, CF_EVENTS, CF_FLEXIBLE_ATTRIBUTES, CF_OBJECTS, CF_OWNER_INDEX,
    CF_SNAPSHOTS, CF_TRANSACTIONS,
};
pub use error::{Error, Result};
pub use event_store::{Event, EventID, EventStore, EventType};
pub use object_store::ObjectStore;
pub use ownership::{
    ImmutableObjectManager, OwnershipManager, OwnershipTransferEvent, OwnershipTransferManager,
    SharedObjectManager, WrappedObjectManager,
};
pub use recovery::{DatabaseHealth, PruningConfig, PruningStats, RecoveryManager, RecoveryStats};
pub use snapshot_store::SnapshotStore;
pub use transaction_store::{
    ExecutionStatus, StoredTransaction, TransactionEffects, TransactionStore,
};
pub use validator_store::{
    PersistedDelegationRecord, PersistedRewardRecord, PersistedStakingRecord,
    PersistedValidatorSet, ValidatorStore,
};
