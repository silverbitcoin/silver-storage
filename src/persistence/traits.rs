//! Core traits for blockchain persistence
//!
//! This module defines the trait interfaces that all persistence managers must implement.

use super::models::*;
use super::errors::PersistenceResult;
use rocksdb::WriteBatch;

/// Genesis block manager trait
///
/// Responsible for initializing and managing the genesis block
pub trait GenesisBlockManager {
    /// Load genesis configuration from JSON file
    ///
    /// # Arguments
    /// * `path` - Path to genesis configuration JSON file
    ///
    /// # Returns
    /// Parsed genesis configuration
    fn load_genesis_config(path: &str) -> PersistenceResult<GenesisConfig>;

    /// Validate genesis configuration
    ///
    /// Checks all required fields and constraints:
    /// - chain_id not empty
    /// - validators not empty
    /// - initial_supply > 0
    /// - validator stakes >= 1,000,000 SBTC
    ///
    /// # Arguments
    /// * `config` - Genesis configuration to validate
    fn validate_genesis_config(config: &GenesisConfig) -> PersistenceResult<()>;

    /// Create genesis block from configuration
    ///
    /// Creates block with:
    /// - height: 0
    /// - hash: SHA3-256 of genesis config
    /// - parent_hash: all zeros
    /// - timestamp: genesis_time from config
    /// - transactions: empty
    /// - validator: all zeros
    /// - gas_used: 0
    /// - gas_limit: 0
    ///
    /// # Arguments
    /// * `config` - Genesis configuration
    fn create_genesis_block(config: &GenesisConfig) -> PersistenceResult<Block>;

    /// Persist genesis block to database
    ///
    /// Stores in CF_BLOCKS column family with key "genesis"
    ///
    /// # Arguments
    /// * `block` - Genesis block to persist
    /// * `db` - Database reference
    fn persist_genesis_block(block: &Block, db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Load genesis block from database
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(Some(block))` if genesis block exists
    /// - `Ok(None)` if genesis block doesn't exist
    /// - `Err` on database error
    fn load_genesis_block(db: &crate::RocksDatabase) -> PersistenceResult<Option<Block>>;
}

/// Snapshot persistence manager trait
///
/// Responsible for persisting and recovering finalized snapshots
pub trait SnapshotPersistenceManager {
    /// Persist snapshot to database
    ///
    /// Stores snapshot data without finalization certificate
    ///
    /// # Arguments
    /// * `snapshot` - Snapshot to persist
    /// * `db` - Database reference
    fn persist_snapshot(snapshot: &Snapshot, db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Persist finalized snapshot with certificate
    ///
    /// Atomically persists:
    /// - Snapshot data
    /// - Certificate with signatures
    /// - Flushes to disk
    ///
    /// # Arguments
    /// * `snapshot` - Finalized snapshot
    /// * `certificate` - Finalization certificate
    /// * `db` - Database reference
    fn persist_finalized_snapshot(
        snapshot: &Snapshot,
        certificate: &SnapshotCertificate,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Load snapshot by sequence number
    ///
    /// # Arguments
    /// * `sequence_number` - Snapshot sequence number
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(Some(snapshot))` if snapshot exists
    /// - `Ok(None)` if snapshot doesn't exist
    /// - `Err` on database error
    fn load_snapshot(
        sequence_number: u64,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<Option<Snapshot>>;

    /// Load latest finalized snapshot
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(Some(snapshot))` if any snapshot exists
    /// - `Ok(None)` if no snapshots exist
    /// - `Err` on database error
    fn load_latest_snapshot(db: &crate::RocksDatabase) -> PersistenceResult<Option<Snapshot>>;

    /// Load all finalized snapshots in order
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Returns
    /// Vector of snapshots in sequence order
    fn load_all_snapshots(db: &crate::RocksDatabase) -> PersistenceResult<Vec<Snapshot>>;

    /// Check if snapshot exists
    ///
    /// # Arguments
    /// * `sequence_number` - Snapshot sequence number
    /// * `db` - Database reference
    fn snapshot_exists(
        sequence_number: u64,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<bool>;
}

/// Validator state persistence manager trait
///
/// Responsible for persisting and recovering validator state
pub trait ValidatorStatePersistenceManager {
    /// Persist staking record
    ///
    /// # Arguments
    /// * `record` - Staking record to persist
    /// * `db` - Database reference
    fn persist_staking_record(
        record: &StakingRecord,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Persist tier change event
    ///
    /// # Arguments
    /// * `change` - Tier change to persist
    /// * `db` - Database reference
    fn persist_tier_change(
        change: &TierChange,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Persist unstaking request
    ///
    /// # Arguments
    /// * `request` - Unstaking request to persist
    /// * `db` - Database reference
    fn persist_unstaking_request(
        request: &UnstakingRequest,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Persist reward record
    ///
    /// # Arguments
    /// * `record` - Reward record to persist
    /// * `db` - Database reference
    fn persist_reward_record(
        record: &RewardRecord,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Load complete validator state
    ///
    /// Reconstructs validator state from all persisted records
    ///
    /// # Arguments
    /// * `validator_id` - Validator identifier
    /// * `db` - Database reference
    ///
    /// # Returns
    /// Complete validator state
    fn load_validator_state(
        validator_id: &str,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<ValidatorState>;

    /// Load all validator states
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Returns
    /// Vector of all validator states
    fn load_all_validator_state(db: &crate::RocksDatabase) -> PersistenceResult<Vec<ValidatorState>>;
}

/// Transaction persistence manager trait
///
/// Responsible for persisting transactions linked to snapshots
pub trait TransactionPersistenceManager {
    /// Persist single transaction
    ///
    /// Stores transaction with reference to containing snapshot and block
    ///
    /// # Arguments
    /// * `tx` - Transaction to persist
    /// * `snapshot_seq` - Containing snapshot sequence number
    /// * `block_num` - Containing block number
    /// * `db` - Database reference
    fn persist_transaction(
        tx: &Transaction,
        snapshot_seq: u64,
        block_num: u64,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Persist multiple transactions atomically
    ///
    /// All transactions are stored in a single batch
    ///
    /// # Arguments
    /// * `txs` - Transactions to persist
    /// * `snapshot_seq` - Containing snapshot sequence number
    /// * `block_num` - Containing block number
    /// * `db` - Database reference
    fn persist_transactions_batch(
        txs: &[Transaction],
        snapshot_seq: u64,
        block_num: u64,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()>;

    /// Load transaction by digest
    ///
    /// # Arguments
    /// * `digest` - Transaction digest (hex string)
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(Some(tx))` if transaction exists
    /// - `Ok(None)` if transaction doesn't exist
    /// - `Err` on database error
    fn load_transaction(
        digest: &str,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<Option<Transaction>>;

    /// Load all transactions in snapshot
    ///
    /// # Arguments
    /// * `sequence_number` - Snapshot sequence number
    /// * `db` - Database reference
    ///
    /// # Returns
    /// Vector of transactions in snapshot
    fn load_snapshot_transactions(
        sequence_number: u64,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<Vec<Transaction>>;

    /// Load all transactions in block
    ///
    /// # Arguments
    /// * `block_num` - Block number
    /// * `db` - Database reference
    ///
    /// # Returns
    /// Vector of transactions in block
    fn load_block_transactions(
        block_num: u64,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<Vec<Transaction>>;
}

/// Atomic batch manager trait
///
/// Responsible for atomic multi-step database operations
pub trait AtomicBatchManager {
    /// Create new write batch
    fn create_batch() -> WriteBatch;

    /// Add put operation to batch
    ///
    /// # Arguments
    /// * `batch` - Write batch
    /// * `cf_name` - Column family name
    /// * `key` - Key bytes
    /// * `value` - Value bytes
    fn add_put_to_batch(
        batch: &mut WriteBatch,
        cf_name: &str,
        key: &[u8],
        value: &[u8],
        db: &crate::RocksDatabase,
    );

    /// Add delete operation to batch
    ///
    /// # Arguments
    /// * `batch` - Write batch
    /// * `cf_name` - Column family name
    /// * `key` - Key bytes
    fn add_delete_to_batch(batch: &mut WriteBatch, cf_name: &str, key: &[u8], db: &crate::RocksDatabase);

    /// Write batch atomically
    ///
    /// All operations succeed or all fail
    ///
    /// # Arguments
    /// * `batch` - Write batch to apply
    /// * `db` - Database reference
    fn write_batch(batch: WriteBatch, db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Write batch atomically and flush to disk
    ///
    /// # Arguments
    /// * `batch` - Write batch to apply
    /// * `db` - Database reference
    fn write_batch_with_flush(batch: WriteBatch, db: &crate::RocksDatabase) -> PersistenceResult<()>;
}

/// Flush and durability manager trait
///
/// Responsible for ensuring data reaches disk
pub trait FlushAndDurabilityManager {
    /// Flush specific column family to disk
    ///
    /// # Arguments
    /// * `cf_name` - Column family name
    /// * `db` - Database reference
    fn flush_column_family(cf_name: &str, db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Flush all column families to disk
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn flush_all_column_families(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Flush write-ahead log to disk
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn flush_wal(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Force all data to disk with fsync
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn fsync_all(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Verify flush completed successfully
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn verify_flush_completed(db: &crate::RocksDatabase) -> PersistenceResult<()>;
}

/// State recovery manager trait
///
/// Responsible for recovering state on startup
pub trait StateRecoveryManager {
    /// Verify database integrity
    ///
    /// Checks for corruption and accessibility
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn verify_database_integrity(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Recover genesis block
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn recover_genesis_block(db: &crate::RocksDatabase) -> PersistenceResult<Block>;

    /// Recover all finalized snapshots
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn recover_snapshots(db: &crate::RocksDatabase) -> PersistenceResult<Vec<Snapshot>>;

    /// Recover all validator states
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn recover_validator_state(db: &crate::RocksDatabase) -> PersistenceResult<Vec<ValidatorState>>;

    /// Recover all transactions
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn recover_transactions(db: &crate::RocksDatabase) -> PersistenceResult<Vec<Transaction>>;

    /// Verify consistency across all data
    ///
    /// Checks:
    /// - Snapshot sequence numbers are sequential
    /// - Transaction digests match snapshot contents
    /// - Validator state matches snapshot metadata
    /// - Block numbers are sequential
    ///
    /// # Arguments
    /// * `genesis` - Genesis block
    /// * `snapshots` - All snapshots
    /// * `validators` - All validator states
    /// * `transactions` - All transactions
    fn verify_consistency(
        genesis: &Block,
        snapshots: &[Snapshot],
        validators: &[ValidatorState],
        transactions: &[Transaction],
    ) -> PersistenceResult<()>;
}

/// Graceful shutdown manager trait
///
/// Responsible for persisting state before shutdown
pub trait GracefulShutdownManager {
    /// Initiate graceful shutdown sequence
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn initiate_shutdown(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Stop accepting new transactions
    fn stop_accepting_transactions();

    /// Finalize any pending snapshots
    fn finalize_pending_snapshots() -> PersistenceResult<()>;

    /// Flush all in-memory data to disk
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn flush_all_data(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Create shutdown checkpoint for recovery
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn create_shutdown_checkpoint(db: &crate::RocksDatabase) -> PersistenceResult<()>;

    /// Close database cleanly
    ///
    /// # Arguments
    /// * `db` - Database reference
    fn close_database(db: &crate::RocksDatabase) -> PersistenceResult<()>;
}
