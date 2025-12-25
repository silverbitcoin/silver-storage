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
pub mod utxo_store;
pub mod mempool_store;
pub mod address_store;
pub mod wallet_store;
pub mod network_store;
pub mod fee_store;
pub mod event_store_persistent;
pub mod token_store_persistent;
pub mod advanced_indexing;

pub use db::{ParityDatabase, DatabaseStatistics, CF_SNAPSHOTS};
pub use error::{Error, Result};
pub use object_store::ObjectStore;
pub use token_store::TokenStore;
pub use event_store::{EventStore, EventType, EventID, Event};
pub use block_store::{Block, BlockStore};
pub use transaction_store::{StoredTransaction, TransactionStore, ExecutionStatus, TransactionEffects};
pub use snapshot_store::SnapshotStore;
pub use mining_store::{MiningStore, DifficultyRecord, MiningRewardRecord, MinerStats};
pub use utxo_store::{UTXOStore, UTXO, UTXOSetInfo};
pub use mempool_store::{MempoolStore, MempoolEntry, MempoolInfo};
pub use address_store::{AddressStore, AddressInfo};
pub use wallet_store::{WalletStore, WalletInfo, AccountInfo, KeyInfo};
pub use network_store::{NetworkStore, PeerInfo, BanEntry, NetworkStats};
pub use fee_store::{FeeStore, FeeEstimate, FeeHistoryPoint, FeeStats};
pub use event_store_persistent::{EventStorePersistent, EventType as PersistentEventType};
pub use token_store_persistent::{TokenStorePersistent, TokenMetadata, TokenBalance, TokenAllowance, TokenEvent};
pub use advanced_indexing::{AdvancedIndexManager, TimestampIndex, FeeRangeIndex, ConfirmationIndex, ScriptTypeIndex};
