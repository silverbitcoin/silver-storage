//! Transaction storage with ParityDB backend

use crate::db::{ParityDatabase, CF_TRANSACTIONS};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use silver_core::{Transaction, TransactionDigest};
use std::sync::Arc;
use tracing::{debug, info};

/// Execution status for transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// Transaction is pending execution
    Pending,
    /// Transaction executed successfully
    Success,
    /// Transaction execution failed
    Failed,
}

impl std::fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionStatus::Pending => write!(f, "Pending"),
            ExecutionStatus::Success => write!(f, "Success"),
            ExecutionStatus::Failed => write!(f, "Failed"),
        }
    }
}

/// Transaction effects after execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEffects {
    /// Execution status
    pub status: ExecutionStatus,
    /// Gas used in execution
    pub gas_used: u64,
    /// Gas refunded to sender
    pub gas_refunded: u64,
    /// Transaction digest (hash)
    pub digest: TransactionDigest,
    /// Fuel used in execution
    pub fuel_used: u64,
    /// Execution timestamp (Unix milliseconds)
    pub timestamp: u64,
    /// Error message if execution failed
    pub error_message: Option<String>,
}

impl TransactionEffects {
    /// Create new transaction effects
    pub fn new(
        status: ExecutionStatus,
        gas_used: u64,
        gas_refunded: u64,
        digest: TransactionDigest,
        fuel_used: u64,
        timestamp: u64,
        error_message: Option<String>,
    ) -> Self {
        Self {
            status,
            gas_used,
            gas_refunded,
            digest,
            fuel_used,
            timestamp,
            error_message,
        }
    }
}

/// Stored transaction with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTransaction {
    /// Transaction digest (hash)
    pub digest: TransactionDigest,
    /// Serialized transaction data
    pub data: Vec<u8>,
    /// Execution effects
    pub effects: TransactionEffects,
    /// Timestamp (Unix milliseconds)
    pub timestamp: u64,
    /// Deserialized transaction
    pub transaction: Transaction,
}

impl StoredTransaction {
    /// Create a new stored transaction
    pub fn new(
        digest: TransactionDigest,
        data: Vec<u8>,
        effects: TransactionEffects,
        timestamp: u64,
        transaction: Transaction,
    ) -> Self {
        Self {
            digest,
            data,
            effects,
            timestamp,
            transaction,
        }
    }
}

/// Transaction store with ParityDB backend
///
/// Provides persistent storage for blockchain transactions with:
/// - Efficient transaction lookups by digest
/// - Atomic transaction writes
/// - Transaction effect tracking
/// - Compression support
pub struct TransactionStore {
    db: Arc<ParityDatabase>,
}

impl TransactionStore {
    /// Create new transaction store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing TransactionStore with ParityDB backend");
        Self { db }
    }

    /// Store a transaction persistently using bincode 2.0 API
    pub fn store_transaction(&self, tx: &StoredTransaction) -> Result<()> {
        debug!("Storing transaction with digest: {}", tx.digest.to_hex());

        // Serialize transaction using bincode 2.0 API
        let tx_data = serde_json::to_vec(tx).map_err(crate::error::Error::Serialization)?;

        // Store by transaction digest as key
        let key = format!("tx:{}", tx.digest.to_hex()).into_bytes();
        self.db.put(CF_TRANSACTIONS, &key, &tx_data)?;

        Ok(())
    }

    /// Get transaction by digest using bincode 2.0 API
    pub fn get_transaction(&self, digest: &TransactionDigest) -> Result<Option<StoredTransaction>> {
        debug!("Retrieving transaction with digest: {}", digest.to_hex());

        let key = format!("tx:{}", digest.to_hex()).into_bytes();

        match self.db.get(CF_TRANSACTIONS, &key)? {
            Some(data) => {
                let tx = serde_json::from_slice::<StoredTransaction>(&data)
                    .map_err(crate::error::Error::Serialization)?;
                Ok(Some(tx))
            }
            None => Ok(None),
        }
    }

    /// Check if transaction exists
    pub fn transaction_exists(&self, digest: &TransactionDigest) -> Result<bool> {
        let key = format!("tx:{}", digest.to_hex()).into_bytes();
        self.db.exists(CF_TRANSACTIONS, &key)
    }

    /// Batch store multiple transactions atomically using bincode 2.0 API
    pub fn batch_store_transactions(&self, transactions: &[StoredTransaction]) -> Result<()> {
        debug!("Batch storing {} transactions", transactions.len());

        let items: Result<Vec<_>> = transactions
            .iter()
            .map(|tx| {
                let key = format!("tx:{}", tx.digest.to_hex()).into_bytes();
                let value = serde_json::to_vec(tx).map_err(crate::error::Error::Serialization)?;
                Ok((key, value))
            })
            .collect();

        self.db.batch_write(CF_TRANSACTIONS, &items?)?;

        Ok(())
    }

    /// Delete a transaction
    pub fn delete_transaction(&self, digest: &TransactionDigest) -> Result<()> {
        debug!("Deleting transaction with digest: {}", digest.to_hex());

        let key = format!("tx:{}", digest.to_hex()).into_bytes();
        self.db.delete(CF_TRANSACTIONS, &key)?;

        Ok(())
    }

    /// Get transaction count from database metadata
    pub fn get_transaction_count(&self) -> Result<u64> {
        debug!("Getting transaction count");
        self.db.get_transaction_count()
    }

    /// Iterate all transactions using bincode 2.0 API
    pub fn iterate_transactions(&self) -> Result<Vec<StoredTransaction>> {
        debug!("Iterating all transactions");

        let mut transactions = Vec::new();

        // Query the transaction index to get all transaction digests
        let tx_index_key = b"tx_index:all".to_vec();
        if let Ok(Some(index_data)) = self.db.get(CF_TRANSACTIONS, &tx_index_key) {
            if let Ok(tx_digests) = serde_json::from_slice::<Vec<String>>(&index_data) {
                for tx_digest in tx_digests {
                    let key = format!("tx:{}", tx_digest).into_bytes();
                    if let Ok(Some(data)) = self.db.get(CF_TRANSACTIONS, &key) {
                        if let Ok(tx) = serde_json::from_slice::<StoredTransaction>(&data) {
                            transactions.push(tx);
                        }
                    }
                }
            }
        }

        debug!("Iterated {} total transactions", transactions.len());
        Ok(transactions)
    }

    /// Get transactions by sender address using bincode 2.0 API
    pub fn get_transactions_by_sender(&self, sender: &[u8]) -> Result<Vec<StoredTransaction>> {
        debug!("Getting transactions by sender: {:?}", sender);

        let mut transactions = Vec::new();
        let sender_hex = hex::encode(sender);
        let sender_index_key = format!("sender_index:{}", sender_hex).into_bytes();

        // Query the sender_index for all transactions from this sender
        if let Ok(Some(index_data)) = self.db.get(CF_TRANSACTIONS, &sender_index_key) {
            if let Ok(tx_digests) = serde_json::from_slice::<Vec<String>>(&index_data) {
                for tx_digest in tx_digests {
                    let key = format!("tx:{}", tx_digest).into_bytes();
                    if let Ok(Some(data)) = self.db.get(CF_TRANSACTIONS, &key) {
                        if let Ok(tx) = serde_json::from_slice::<StoredTransaction>(&data) {
                            transactions.push(tx);
                        }
                    }
                }
            }
        }

        debug!(
            "Retrieved {} transactions from sender {:?}",
            transactions.len(),
            sender
        );
        Ok(transactions)
    }

    /// Get transactions by recipient address using bincode 2.0 API
    pub fn get_transactions_by_recipient(
        &self,
        recipient: &[u8],
    ) -> Result<Vec<StoredTransaction>> {
        debug!("Getting transactions by recipient: {:?}", recipient);

        let mut transactions = Vec::new();
        let recipient_hex = hex::encode(recipient);
        let recipient_index_key = format!("recipient_index:{}", recipient_hex).into_bytes();

        // Query the recipient_index for all transactions to this recipient
        if let Ok(Some(index_data)) = self.db.get(CF_TRANSACTIONS, &recipient_index_key) {
            if let Ok(tx_digests) = serde_json::from_slice::<Vec<String>>(&index_data) {
                for tx_digest in tx_digests {
                    let key = format!("tx:{}", tx_digest).into_bytes();
                    if let Ok(Some(data)) = self.db.get(CF_TRANSACTIONS, &key) {
                        if let Ok(tx) = serde_json::from_slice::<StoredTransaction>(&data) {
                            transactions.push(tx);
                        }
                    }
                }
            }
        }

        debug!(
            "Retrieved {} transactions to recipient {:?}",
            transactions.len(),
            recipient
        );
        Ok(transactions)
    }

    /// Get pending transactions (not yet executed) using bincode 2.0 API
    pub fn get_pending_transactions(&self) -> Result<Vec<StoredTransaction>> {
        debug!("Getting pending transactions");

        let mut pending = Vec::new();
        let pending_index_key = b"pending_index:all".to_vec();

        // Query the pending_index for all pending transactions
        if let Ok(Some(index_data)) = self.db.get(CF_TRANSACTIONS, &pending_index_key) {
            if let Ok(tx_digests) = serde_json::from_slice::<Vec<String>>(&index_data) {
                for tx_digest in tx_digests {
                    let key = format!("tx:{}", tx_digest).into_bytes();
                    if let Ok(Some(data)) = self.db.get(CF_TRANSACTIONS, &key) {
                        if let Ok(tx) = serde_json::from_slice::<StoredTransaction>(&data) {
                            if tx.effects.status == ExecutionStatus::Pending {
                                pending.push(tx);
                            }
                        }
                    }
                }
            }
        }

        debug!("Retrieved {} pending transactions", pending.len());
        Ok(pending)
    }

    /// Get failed transactions using bincode 2.0 API
    pub fn get_failed_transactions(&self) -> Result<Vec<StoredTransaction>> {
        debug!("Getting failed transactions");

        let mut failed = Vec::new();
        let failed_index_key = b"failed_index:all".to_vec();

        // Query the failed_index for all failed transactions
        if let Ok(Some(index_data)) = self.db.get(CF_TRANSACTIONS, &failed_index_key) {
            if let Ok(tx_digests) = serde_json::from_slice::<Vec<String>>(&index_data) {
                for tx_digest in tx_digests {
                    let key = format!("tx:{}", tx_digest).into_bytes();
                    if let Ok(Some(data)) = self.db.get(CF_TRANSACTIONS, &key) {
                        if let Ok(tx) = serde_json::from_slice::<StoredTransaction>(&data) {
                            if tx.effects.status == ExecutionStatus::Failed {
                                failed.push(tx);
                            }
                        }
                    }
                }
            }
        }

        debug!("Retrieved {} failed transactions", failed.len());
        Ok(failed)
    }
}

impl Clone for TransactionStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}
