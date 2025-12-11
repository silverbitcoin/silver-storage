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
pub mod error;
pub mod object_store;
pub mod token_store;
pub mod event_store;
pub mod block_store;
pub mod transaction_store;
pub mod snapshot_store;
pub mod validator_store;
pub mod staking_store;

pub use db::{ParityDatabase, DatabaseStatistics};
pub use error::{Error, Result};
pub use object_store::ObjectStore;
pub use token_store::TokenStore;
pub use event_store::{EventStore, EventType, EventID, Event};
pub use block_store::{Block, BlockStore};
pub use transaction_store::{StoredTransaction, TransactionStore, ExecutionStatus, TransactionEffects};
pub use snapshot_store::SnapshotStore;
pub use validator_store::ValidatorStore;
pub use staking_store::{StakingStore, StakingRecord, DelegationRecord, RewardRecord, UnbondingRecord};
