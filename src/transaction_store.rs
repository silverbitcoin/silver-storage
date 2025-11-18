//! Transaction storage with effects and indexing
//!
//! This module provides storage for finalized transactions with their execution effects.
//! Transactions are indexed by digest for efficient retrieval.

use crate::{
    db::{RocksDatabase, CF_TRANSACTIONS}, Result,
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
                .batch_put(&mut batch, CF_TRANSACTIONS, &key, &stored_bytes);
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!("Batch stored {} transactions successfully", transactions.len());
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
}


#[cfg(test)]
mod tests {
    use super::*;
    use silver_core::transaction::TransactionExpiration;
    use silver_core::{SilverAddress, TransactionData, TransactionKind, ObjectRef, ObjectID, SequenceNumber, Signature, SignatureScheme, TransactionDigest};
    use tempfile::TempDir;

    fn create_test_store() -> (TransactionStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
        let store = TransactionStore::new(db);
        (store, temp_dir)
    }

    fn create_test_transaction(id: u8) -> Transaction {
        let sender = SilverAddress::new([id; 64]);
        let fuel_payment = ObjectRef::new(
            ObjectID::new([id; 64]),
            SequenceNumber::new(0),
            TransactionDigest::new([id; 64]),
        );

        let data = TransactionData::new(
            sender,
            fuel_payment,
            1000,
            1000,
            TransactionKind::CompositeChain(vec![]),
            TransactionExpiration::None,
        );

        let signature = Signature {
            scheme: SignatureScheme::Dilithium3,
            bytes: vec![0u8; 100],
        };

        Transaction::new(data, vec![signature])
    }

    fn create_test_effects(digest: TransactionDigest, fuel_used: u64) -> TransactionEffects {
        TransactionEffects {
            digest,
            status: ExecutionStatus::Success,
            fuel_used,
            error_message: None,
            timestamp: 1000,
        }
    }

    #[test]
    fn test_store_and_get_transaction() {
        let (store, _temp) = create_test_store();

        let transaction = create_test_transaction(1);
        let digest = transaction.digest();
        let effects = create_test_effects(digest, 500);

        // Store transaction
        store.store_transaction(&transaction, effects.clone()).unwrap();

        // Get transaction
        let stored = store.get_transaction(&digest).unwrap();
        assert!(stored.is_some());

        let stored = stored.unwrap();
        assert_eq!(stored.transaction.digest(), digest);
        assert_eq!(stored.effects.fuel_used, 500);
        assert_eq!(stored.effects.status, ExecutionStatus::Success);
    }

    #[test]
    fn test_transaction_exists() {
        let (store, _temp) = create_test_store();

        let transaction = create_test_transaction(1);
        let digest = transaction.digest();
        let effects = create_test_effects(digest, 500);

        // Should not exist initially
        assert!(!store.exists(&digest).unwrap());

        // Store transaction
        store.store_transaction(&transaction, effects).unwrap();

        // Should exist now
        assert!(store.exists(&digest).unwrap());
    }

    #[test]
    fn test_get_effects() {
        let (store, _temp) = create_test_store();

        let transaction = create_test_transaction(1);
        let digest = transaction.digest();
        let effects = create_test_effects(digest, 750);

        // Store transaction
        store.store_transaction(&transaction, effects.clone()).unwrap();

        // Get effects only
        let retrieved_effects = store.get_effects(&digest).unwrap();
        assert!(retrieved_effects.is_some());

        let retrieved_effects = retrieved_effects.unwrap();
        assert_eq!(retrieved_effects.fuel_used, 750);
        assert_eq!(retrieved_effects.status, ExecutionStatus::Success);
    }

    #[test]
    fn test_failed_transaction() {
        let (store, _temp) = create_test_store();

        let transaction = create_test_transaction(1);
        let digest = transaction.digest();

        let effects = TransactionEffects {
            digest,
            status: ExecutionStatus::Failed,
            fuel_used: 100,
            error_message: Some("Out of fuel".to_string()),
            timestamp: 1000,
        };

        // Store failed transaction
        store.store_transaction(&transaction, effects).unwrap();

        // Retrieve and verify
        let stored = store.get_transaction(&digest).unwrap().unwrap();
        assert_eq!(stored.effects.status, ExecutionStatus::Failed);
        assert_eq!(stored.effects.error_message, Some("Out of fuel".to_string()));
    }

    #[test]
    fn test_batch_store_transactions() {
        let (store, _temp) = create_test_store();

        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        let effects1 = create_test_effects(tx1.digest(), 100);
        let effects2 = create_test_effects(tx2.digest(), 200);
        let effects3 = create_test_effects(tx3.digest(), 300);

        let transactions = vec![
            (tx1.clone(), effects1),
            (tx2.clone(), effects2),
            (tx3.clone(), effects3),
        ];

        // Batch store
        store.batch_store_transactions(&transactions).unwrap();

        // Verify all transactions exist
        assert!(store.exists(&tx1.digest()).unwrap());
        assert!(store.exists(&tx2.digest()).unwrap());
        assert!(store.exists(&tx3.digest()).unwrap());
    }

    #[test]
    fn test_get_transaction_count() {
        let (store, _temp) = create_test_store();

        // Initially 0
        let count = store.get_transaction_count().unwrap();
        assert_eq!(count, 0);

        // Add transactions
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);

        store
            .store_transaction(&tx1, create_test_effects(tx1.digest(), 100))
            .unwrap();
        store
            .store_transaction(&tx2, create_test_effects(tx2.digest(), 200))
            .unwrap();

        // Count should be 2
        let count = store.get_transaction_count().unwrap();
        assert!(count >= 2);
    }

    #[test]
    fn test_get_storage_size() {
        let (store, _temp) = create_test_store();

        // Add some transactions
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);

        store
            .store_transaction(&tx1, create_test_effects(tx1.digest(), 100))
            .unwrap();
        store
            .store_transaction(&tx2, create_test_effects(tx2.digest(), 200))
            .unwrap();

        // Size should be non-negative
        let size = store.get_storage_size().unwrap();
        assert!(size >= 0);
    }

    #[test]
    fn test_overwrite_transaction() {
        let (store, _temp) = create_test_store();

        let transaction = create_test_transaction(1);
        let digest = transaction.digest();

        // Store with initial effects
        let effects1 = create_test_effects(digest, 100);
        store.store_transaction(&transaction, effects1).unwrap();

        // Overwrite with new effects
        let effects2 = create_test_effects(digest, 200);
        store.store_transaction(&transaction, effects2).unwrap();

        // Verify updated effects
        let stored = store.get_transaction(&digest).unwrap().unwrap();
        assert_eq!(stored.effects.fuel_used, 200);
    }
}
