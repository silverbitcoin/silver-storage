//! Blockchain persistence layer
//!
//! This module provides comprehensive persistence for blockchain state:
//! - Genesis block initialization and recovery
//! - Snapshot persistence and finalization
//! - Validator state persistence
//! - Transaction persistence with snapshot linkage
//! - Atomic batch operations
//! - Flush and durability guarantees
//! - State recovery and verification
//! - Graceful shutdown

pub mod atomic_batch;
pub mod db;
pub mod errors;
pub mod flush;
pub mod genesis;
pub mod keys;
pub mod models;
pub mod recovery;
pub mod shutdown;
pub mod snapshots;
pub mod traits;
pub mod transactions;
pub mod validator_state;

pub use atomic_batch::AtomicBatchManagerImpl;
pub use db::{
    PersistenceDatabase, CF_BLOCKS, CF_EVENTS, CF_FLEXIBLE_ATTRIBUTES, CF_OBJECTS,
    CF_OWNER_INDEX, CF_SNAPSHOTS, CF_TRANSACTIONS, PERSISTENCE_COLUMN_FAMILIES,
};
pub use errors::{PersistenceError, PersistenceResult};
pub use flush::FlushAndDurabilityManagerImpl;
pub use genesis::GenesisBlockManagerImpl;
pub use keys::{
    genesis_block_key, reward_record_key, shutdown_checkpoint_key, snapshot_certificate_key,
    snapshot_key, staking_record_key, tier_change_key, transaction_key,
    transaction_snapshot_mapping_key, unstaking_request_key, validator_state_key,
};
pub use models::{
    Block, GenesisConfig, Snapshot, SnapshotCertificate, StakingRecord, TierChange,
    Transaction, TransactionDigest, UnstakingRequest, ValidatorState, RewardRecord,
    ShutdownCheckpoint,
};
pub use recovery::StateRecoveryManagerImpl;
pub use shutdown::GracefulShutdownManagerImpl;
pub use snapshots::SnapshotPersistenceManagerImpl;
pub use transactions::TransactionPersistenceManagerImpl;
pub use validator_state::ValidatorStatePersistenceManagerImpl;
pub use traits::{
    AtomicBatchManager, FlushAndDurabilityManager, GenesisBlockManager,
    GracefulShutdownManager, SnapshotPersistenceManager, StateRecoveryManager,
    TransactionPersistenceManager, ValidatorStatePersistenceManager,
};
