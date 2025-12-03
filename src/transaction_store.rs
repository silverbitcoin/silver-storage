//! Transaction storage with effects and indexing
//!
//! This module provides storage for finalized transactions with their execution effects.
//! Transactions are indexed by digest for efficient retrieval.

use crate::{
    db::{RocksDatabase, CF_TRANSACTIONS},
    Result,
};
use serde::{Deserialize, Serialize};
use silver_core::{Transaction, TransactionDigest};
use std::sync::Arc;
use tracing::{debug, info};

/// Transaction execution effects
///
/// Stores the results of transaction execution including
/// fuel used, status, and any error messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEffects {
    /// Transaction digest
    pub digest: TransactionDigest,

    /// Execution status
    pub status: ExecutionStatus,

    /// Fuel used during execution
    pub fuel_used: u64,

    /// Error message if execution failed
    pub error_message: Option<String>,

    /// Timestamp when transaction was executed (Unix milliseconds)
    pub timestamp: u64,
}

/// Transaction execution status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// Transaction executed successfully
    Success,

    /// Transaction execution failed
    Failed,

    /// Transaction is pending execution
    Pending,
}

/// Stored transaction with effects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTransaction {
    /// The transaction
    pub transaction: Transaction,

    /// Execution effects
    pub effects: TransactionEffects,
}

/// Transaction store for finalized transactions
///
/// Provides storage and retrieval of transactions with their execution effects.
pub struct TransactionStore {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
}

impl TransactionStore {
    /// Create a new transaction store
    ///
    /// # Arguments
    /// * `db` - Shared reference to the RocksDB database
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing TransactionStore");
        Self { db }
    }

    /// Store a finalized transaction with its effects
    ///
    /// # Arguments
    /// * `transaction` - The transaction to store
    /// * `effects` - Execution effects
    ///
    /// # Errors
    /// Returns error if serialization or database write fails
    pub fn store_transaction(
        &self,
        transaction: &Transaction,
        effects: TransactionEffects,
    ) -> Result<()> {
        let digest = transaction.digest();
        debug!("Storing transaction: {}", digest);

        // Create stored transaction
        let stored = StoredTransaction {
            transaction: transaction.clone(),
            effects,
        };

        // Serialize
        let stored_bytes = bincode::serialize(&stored)?;

        // Store by digest
        let key = self.make_transaction_key(&digest);
        self.db.put(CF_TRANSACTIONS, &key, &stored_bytes)?;

        debug!(
            "Transaction {} stored successfully ({} bytes)",
            digest,
            stored_bytes.len()
        );

        Ok(())
    }

    /// Get a transaction by digest
    ///
    /// # Arguments
    /// * `digest` - Transaction digest
    ///
    /// # Returns
    /// - `Ok(Some(stored_transaction))` if transaction exists
    /// - `Ok(None)` if transaction doesn't exist
    /// - `Err` on database or deserialization error
    pub fn get_transaction(&self, digest: &TransactionDigest) -> Result<Option<StoredTransaction>> {
        debug!("Retrieving transaction: {}", digest);

        let key = self.make_transaction_key(digest);
        let stored_bytes = self.db.get(CF_TRANSACTIONS, &key)?;

        match stored_bytes {
            Some(bytes) => {
                let stored: StoredTransaction = bincode::deserialize(&bytes)?;
                debug!("Transaction {} retrieved", digest);
                Ok(Some(stored))
            }
            None => {
                debug!("Transaction {} not found", digest);
                Ok(None)
            }
        }
    }

    /// Check if a transaction exists
    ///
    /// # Arguments
    /// * `digest` - Transaction digest
    pub fn exists(&self, digest: &TransactionDigest) -> Result<bool> {
        let key = self.make_transaction_key(digest);
        self.db.exists(CF_TRANSACTIONS, &key)
    }

    /// Get transaction effects only (without full transaction data)
    ///
    /// # Arguments
    /// * `digest` - Transaction digest
    ///
    /// # Returns
    /// - `Ok(Some(effects))` if transaction exists
    /// - `Ok(None)` if transaction doesn't exist
    pub fn get_effects(&self, digest: &TransactionDigest) -> Result<Option<TransactionEffects>> {
        self.get_transaction(digest)
            .map(|opt| opt.map(|stored| stored.effects))
    }

    /// Batch store multiple transactions
    ///
    /// All transactions are stored atomically.
    ///
    /// # Arguments
    /// * `transactions` - Slice of (transaction, effects) pairs
    ///
    /// # Errors
    /// Returns error if serialization or database write fails.
    /// On error, no transactions are stored (atomic operation).
    pub fn batch_store_transactions(
        &self,
        transactions: &[(Transaction, TransactionEffects)],
    ) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        info!("Batch storing {} transactions", transactions.len());

        // Create atomic batch
        let mut batch = self.db.batch();

        for (transaction, effects) in transactions {
            let digest = transaction.digest();

            let stored = StoredTransaction {
                transaction: transaction.clone(),
                effects: effects.clone(),
            };

            let stored_bytes = bincode::serialize(&stored)?;
            let key = self.make_transaction_key(&digest);

            self.db
                .batch_put(&mut batch, CF_TRANSACTIONS, &key, &stored_bytes)?;
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!(
            "Batch stored {} transactions successfully",
            transactions.len()
        );
        Ok(())
    }

    /// Get the total number of stored transactions (approximate)
    pub fn get_transaction_count(&self) -> Result<u64> {
        self.db.get_cf_key_count(CF_TRANSACTIONS)
    }

    /// Get the total size of transaction storage in bytes
    pub fn get_storage_size(&self) -> Result<u64> {
        self.db.get_cf_size(CF_TRANSACTIONS)
    }

    // ========== Private Helper Methods ==========

    /// Create transaction storage key
    ///
    /// Key format: transaction_digest (64 bytes)
    fn make_transaction_key(&self, digest: &TransactionDigest) -> Vec<u8> {
        digest.as_bytes().to_vec()
    }

    /// Get the latest block number (delegates to block metadata)
    pub fn get_latest_block_number(&self) -> Result<u64> {
        let metadata_key = b"latest_block_number".to_vec();

        match self.db.get(CF_TRANSACTIONS, &metadata_key)? {
            Some(bytes) => {
                if bytes.len() == 8 {
                    let number = u64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    Ok(number)
                } else {
                    Ok(0)
                }
            }
            None => Ok(0),
        }
    }

    /// Get block by number (delegates to block storage)
    pub fn get_block(&self, number: u64) -> Result<Option<crate::Block>> {
        let key = {
            let mut k = b"block:".to_vec();
            k.extend_from_slice(&number.to_le_bytes());
            k
        };

        match self.db.get(CF_TRANSACTIONS, &key)? {
            Some(bytes) => {
                let block: crate::Block = bincode::deserialize(&bytes)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }
}
