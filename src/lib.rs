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

pub mod address_store;
pub mod advanced_indexing;
pub mod block_store;
pub mod db;
pub mod db_production;
pub mod error;
pub mod event_store;
pub mod event_store_persistent;
pub mod fee_store;
pub mod mempool_store;
pub mod mining_store;
pub mod network_store;
pub mod object_store;
pub mod snapshot_store;
pub mod token_store;
pub mod token_store_persistent;
pub mod transaction_store;
pub mod utxo_store;
pub mod wallet_store;

pub use address_store::{AddressInfo, AddressStore};
pub use advanced_indexing::{
    AdvancedIndexManager, ConfirmationIndex, FeeRangeIndex, ScriptTypeIndex, TimestampIndex,
};
pub use block_store::{Block, BlockStore};
pub use db::{DatabaseStatistics, ParityDatabase, CF_SNAPSHOTS};
pub use error::{Error, Result};
pub use event_store::{Event, EventID, EventStore, EventType};
pub use event_store_persistent::{EventStorePersistent, EventType as PersistentEventType};
pub use fee_store::{FeeEstimate, FeeHistoryPoint, FeeStats, FeeStore};
pub use mempool_store::{MempoolEntry, MempoolInfo, MempoolStore};
pub use mining_store::{DifficultyRecord, MinerStats, MiningRewardRecord, MiningStore};
pub use network_store::{BanEntry, NetworkStats, NetworkStore, PeerInfo};
pub use object_store::ObjectStore;
pub use snapshot_store::SnapshotStore;
pub use token_store::TokenStore;
pub use token_store_persistent::{
    TokenAllowance, TokenBalance, TokenEvent, TokenMetadata, TokenStorePersistent,
};
pub use transaction_store::{
    ExecutionStatus, StoredTransaction, TransactionEffects, TransactionStore,
};
pub use utxo_store::{UTXOSetInfo, UTXOStore, UTXO};
pub use wallet_store::{AccountInfo, KeyInfo, WalletInfo, WalletStore};
