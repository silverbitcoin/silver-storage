//! Atomic batch manager for blockchain persistence
//!
//! This module provides atomic multi-step database operations with:
//! - Batch creation and operation accumulation
//! - Atomic writes (all-or-nothing semantics)
//! - Flush to disk for critical operations
//! - Error handling and rollback
//! - Partial write detection

use rocksdb::WriteBatch;
use tracing::{debug, error, info};

use crate::RocksDatabase;
use super::errors::{PersistenceError, PersistenceResult};

/// Atomic batch manager implementation
///
/// Manages atomic database operations using RocksDB WriteBatch.
/// Ensures all operations in a batch succeed or all fail together.
pub struct AtomicBatchManagerImpl;

impl AtomicBatchManagerImpl {
    /// Create new write batch
    ///
    /// # Returns
    /// Empty WriteBatch ready for operations
    ///
    /// # Example
    /// ```ignore
    /// let mut batch = AtomicBatchManagerImpl::create_batch();
    /// AtomicBatchManagerImpl::add_put_to_batch(&mut batch, CF_BLOCKS, b"key", b"value", &db);
    /// AtomicBatchManagerImpl::write_batch(batch, &db)?;
    /// ```
    pub fn create_batch() -> WriteBatch {
        debug!("Creating new write batch");
        WriteBatch::default()
    }

    /// Add put operation to batch
    ///
    /// Adds a key-value pair to the batch. The operation is not applied
    /// until the batch is written to the database.
    ///
    /// # Arguments
    /// * `batch` - Write batch to add operation to
    /// * `cf_name` - Column family name
    /// * `key` - Key bytes
    /// * `value` - Value bytes
    /// * `db` - Database reference (used to get column family handle)
    pub fn add_put_to_batch(
        batch: &mut WriteBatch,
        cf_name: &str,
        key: &[u8],
        value: &[u8],
        db: &RocksDatabase,
    ) {
        debug!(
            "Adding put operation to batch: cf={}, key_len={}, value_len={}",
            cf_name,
            key.len(),
            value.len()
        );

        // Use the public batch_put method from RocksDatabase
        let _ = db.batch_put(batch, cf_name, key, value);
    }

    /// Add delete operation to batch
    ///
    /// Adds a delete operation to the batch. The operation is not applied
    /// until the batch is written to the database.
    ///
    /// # Arguments
    /// * `batch` - Write batch to add operation to
    /// * `cf_name` - Column family name
    /// * `key` - Key bytes to delete
    /// * `db` - Database reference (used to get column family handle)
    pub fn add_delete_to_batch(
        batch: &mut WriteBatch,
        cf_name: &str,
        key: &[u8],
        db: &RocksDatabase,
    ) {
        debug!(
            "Adding delete operation to batch: cf={}, key_len={}",
            cf_name,
            key.len()
        );

        // Use the public batch_delete method from RocksDatabase
        let _ = db.batch_delete(batch, cf_name, key);
    }

    /// Write batch atomically to database
    ///
    /// Applies all operations in the batch atomically. If any operation fails,
    /// all operations are rolled back and the database remains in its previous state.
    ///
    /// # Arguments
    /// * `batch` - Write batch to apply
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(())` if all operations succeeded
    /// - `Err` if any operation failed (all rolled back)
    ///
    /// # Errors
    /// Returns error if:
    /// - Disk is full
    /// - Permission denied
    /// - I/O error
    /// - Data corruption detected
    pub fn write_batch(batch: WriteBatch, db: &RocksDatabase) -> PersistenceResult<()> {
        let batch_size = batch.len();
        debug!("Writing batch with {} operations", batch_size);

        // Write batch atomically
        db.write_batch(batch).map_err(|e| {
            error!("Batch write failed: {}", e);
            PersistenceError::BatchFailed(format!("Failed to write batch: {}", e))
        })?;

        info!("Batch write succeeded: {} operations applied", batch_size);
        Ok(())
    }

    /// Write batch atomically and flush to disk
    ///
    /// Applies all operations in the batch atomically and then flushes to disk
    /// to ensure durability. This is used for critical operations like snapshot
    /// finalization where data loss is unacceptable.
    ///
    /// # Arguments
    /// * `batch` - Write batch to apply
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(())` if all operations succeeded and flushed
    /// - `Err` if any operation failed or flush failed
    ///
    /// # Errors
    /// Returns error if:
    /// - Batch write failed
    /// - Flush failed
    /// - Disk is full
    /// - Permission denied
    pub fn write_batch_with_flush(batch: WriteBatch, db: &RocksDatabase) -> PersistenceResult<()> {
        let batch_size = batch.len();
        debug!("Writing batch with {} operations and flush", batch_size);

        // Write batch atomically
        db.write_batch(batch).map_err(|e| {
            error!("Batch write failed: {}", e);
            PersistenceError::BatchFailed(format!("Failed to write batch: {}", e))
        })?;

        // Flush all column families to disk
        Self::flush_all_column_families(db)?;

        info!(
            "Batch write with flush succeeded: {} operations applied and flushed",
            batch_size
        );
        Ok(())
    }

    /// Flush all column families to disk
    ///
    /// Forces all in-memory data in all column families to disk.
    /// This ensures durability for critical operations.
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Returns
    /// - `Ok(())` if flush succeeded
    /// - `Err` if flush failed
    fn flush_all_column_families(db: &RocksDatabase) -> PersistenceResult<()> {
        debug!("Flushing all column families to disk");

        // Use the public flush method which flushes all column families
        db.flush().map_err(|e| {
            error!("Failed to flush all column families: {}", e);
            PersistenceError::FlushFailed(format!("Failed to flush all column families: {}", e))
        })?;

        info!("All column families flushed successfully");
        Ok(())
    }

    /// Get batch size (number of operations)
    ///
    /// # Arguments
    /// * `batch` - Write batch
    ///
    /// # Returns
    /// Number of operations in the batch
    pub fn batch_size(batch: &WriteBatch) -> usize {
        batch.len()
    }

    /// Clear batch (remove all operations)
    ///
    /// # Arguments
    /// * `batch` - Write batch to clear
    pub fn clear_batch(batch: &mut WriteBatch) {
        debug!("Clearing batch");
        batch.clear();
    }

    /// Check if batch is empty
    ///
    /// # Arguments
    /// * `batch` - Write batch
    ///
    /// # Returns
    /// True if batch has no operations
    pub fn is_empty(batch: &WriteBatch) -> bool {
        batch.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::db::CF_BLOCKS;
    use tempfile::TempDir;

    fn setup_test_db() -> (RocksDatabase, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open test database");
        (db, temp_dir)
    }

    #[test]
    fn test_create_batch() {
        let batch = AtomicBatchManagerImpl::create_batch();
        assert!(AtomicBatchManagerImpl::is_empty(&batch));
        assert_eq!(AtomicBatchManagerImpl::batch_size(&batch), 0);
    }

    #[test]
    fn test_add_put_to_batch() {
        let (db, _temp_dir) = setup_test_db();
        let mut batch = AtomicBatchManagerImpl::create_batch();

        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch,
            CF_BLOCKS,
            b"test_key",
            b"test_value",
            &db,
        );

        assert!(!AtomicBatchManagerImpl::is_empty(&batch));
        assert_eq!(AtomicBatchManagerImpl::batch_size(&batch), 1);
    }

    #[test]
    fn test_add_delete_to_batch() {
        let (db, _temp_dir) = setup_test_db();
        let mut batch = AtomicBatchManagerImpl::create_batch();

        AtomicBatchManagerImpl::add_delete_to_batch(&mut batch, CF_BLOCKS, b"test_key", &db);

        assert!(!AtomicBatchManagerImpl::is_empty(&batch));
        assert_eq!(AtomicBatchManagerImpl::batch_size(&batch), 1);
    }

    #[test]
    fn test_write_batch_success() {
        let (db, _temp_dir) = setup_test_db();
        let mut batch = AtomicBatchManagerImpl::create_batch();

        // Add put operation
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch,
            CF_BLOCKS,
            b"test_key",
            b"test_value",
            &db,
        );

        // Write batch
        let result = AtomicBatchManagerImpl::write_batch(batch, &db);
        assert!(result.is_ok());

        // Verify data was written
        let value = db.get(CF_BLOCKS, b"test_key").expect("Failed to get value");
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_write_batch_multiple_operations() {
        let (db, _temp_dir) = setup_test_db();
        let mut batch = AtomicBatchManagerImpl::create_batch();

        // Add multiple put operations
        for i in 0..5 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            AtomicBatchManagerImpl::add_put_to_batch(
                &mut batch,
                CF_BLOCKS,
                key.as_bytes(),
                value.as_bytes(),
                &db,
            );
        }

        assert_eq!(AtomicBatchManagerImpl::batch_size(&batch), 5);

        // Write batch
        let result = AtomicBatchManagerImpl::write_batch(batch, &db);
        assert!(result.is_ok());

        // Verify all data was written
        for i in 0..5 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);
            let value = db.get(CF_BLOCKS, key.as_bytes()).expect("Failed to get value");
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }

    #[test]
    fn test_write_batch_with_flush() {
        let (db, _temp_dir) = setup_test_db();
        let mut batch = AtomicBatchManagerImpl::create_batch();

        // Add put operation
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch,
            CF_BLOCKS,
            b"flush_test_key",
            b"flush_test_value",
            &db,
        );

        // Write batch with flush
        let result = AtomicBatchManagerImpl::write_batch_with_flush(batch, &db);
        assert!(result.is_ok());

        // Verify data was written and flushed
        let value = db.get(CF_BLOCKS, b"flush_test_key").expect("Failed to get value");
        assert_eq!(value, Some(b"flush_test_value".to_vec()));
    }

    #[test]
    fn test_clear_batch() {
        let (db, _temp_dir) = setup_test_db();
        let mut batch = AtomicBatchManagerImpl::create_batch();

        // Add operations
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch,
            CF_BLOCKS,
            b"key1",
            b"value1",
            &db,
        );
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch,
            CF_BLOCKS,
            b"key2",
            b"value2",
            &db,
        );

        assert_eq!(AtomicBatchManagerImpl::batch_size(&batch), 2);

        // Clear batch
        AtomicBatchManagerImpl::clear_batch(&mut batch);
        assert!(AtomicBatchManagerImpl::is_empty(&batch));
        assert_eq!(AtomicBatchManagerImpl::batch_size(&batch), 0);
    }

    #[test]
    fn test_batch_atomicity() {
        let (db, _temp_dir) = setup_test_db();

        // First batch: write some data
        let mut batch1 = AtomicBatchManagerImpl::create_batch();
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch1,
            CF_BLOCKS,
            b"atomic_key_1",
            b"atomic_value_1",
            &db,
        );
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch1,
            CF_BLOCKS,
            b"atomic_key_2",
            b"atomic_value_2",
            &db,
        );

        let result = AtomicBatchManagerImpl::write_batch(batch1, &db);
        assert!(result.is_ok());

        // Verify both keys exist
        let value1 = db.get(CF_BLOCKS, b"atomic_key_1").expect("Failed to get value");
        let value2 = db.get(CF_BLOCKS, b"atomic_key_2").expect("Failed to get value");
        assert_eq!(value1, Some(b"atomic_value_1".to_vec()));
        assert_eq!(value2, Some(b"atomic_value_2".to_vec()));

        // Second batch: update both keys
        let mut batch2 = AtomicBatchManagerImpl::create_batch();
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch2,
            CF_BLOCKS,
            b"atomic_key_1",
            b"updated_value_1",
            &db,
        );
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch2,
            CF_BLOCKS,
            b"atomic_key_2",
            b"updated_value_2",
            &db,
        );

        let result = AtomicBatchManagerImpl::write_batch(batch2, &db);
        assert!(result.is_ok());

        // Verify both keys were updated
        let value1 = db.get(CF_BLOCKS, b"atomic_key_1").expect("Failed to get value");
        let value2 = db.get(CF_BLOCKS, b"atomic_key_2").expect("Failed to get value");
        assert_eq!(value1, Some(b"updated_value_1".to_vec()));
        assert_eq!(value2, Some(b"updated_value_2".to_vec()));
    }

    #[test]
    fn test_batch_delete_operation() {
        let (db, _temp_dir) = setup_test_db();

        // First write some data
        let mut batch1 = AtomicBatchManagerImpl::create_batch();
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut batch1,
            CF_BLOCKS,
            b"delete_key",
            b"delete_value",
            &db,
        );
        AtomicBatchManagerImpl::write_batch(batch1, &db).expect("Failed to write batch");

        // Verify data exists
        let value = db.get(CF_BLOCKS, b"delete_key").expect("Failed to get value");
        assert_eq!(value, Some(b"delete_value".to_vec()));

        // Delete the key
        let mut batch2 = AtomicBatchManagerImpl::create_batch();
        AtomicBatchManagerImpl::add_delete_to_batch(&mut batch2, CF_BLOCKS, b"delete_key", &db);
        AtomicBatchManagerImpl::write_batch(batch2, &db).expect("Failed to write batch");

        // Verify key is deleted
        let value = db.get(CF_BLOCKS, b"delete_key").expect("Failed to get value");
        assert_eq!(value, None);
    }

    #[test]
    fn test_batch_mixed_operations() {
        let (db, _temp_dir) = setup_test_db();

        // Setup: write initial data
        let mut setup_batch = AtomicBatchManagerImpl::create_batch();
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut setup_batch,
            CF_BLOCKS,
            b"keep_key",
            b"keep_value",
            &db,
        );
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut setup_batch,
            CF_BLOCKS,
            b"delete_key",
            b"delete_value",
            &db,
        );
        AtomicBatchManagerImpl::write_batch(setup_batch, &db).expect("Failed to write setup batch");

        // Mixed batch: put new, update existing, delete one
        let mut mixed_batch = AtomicBatchManagerImpl::create_batch();
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut mixed_batch,
            CF_BLOCKS,
            b"new_key",
            b"new_value",
            &db,
        );
        AtomicBatchManagerImpl::add_put_to_batch(
            &mut mixed_batch,
            CF_BLOCKS,
            b"keep_key",
            b"updated_value",
            &db,
        );
        AtomicBatchManagerImpl::add_delete_to_batch(&mut mixed_batch, CF_BLOCKS, b"delete_key", &db);

        AtomicBatchManagerImpl::write_batch(mixed_batch, &db).expect("Failed to write mixed batch");

        // Verify results
        let new_value = db.get(CF_BLOCKS, b"new_key").expect("Failed to get value");
        let keep_value = db.get(CF_BLOCKS, b"keep_key").expect("Failed to get value");
        let delete_value = db.get(CF_BLOCKS, b"delete_key").expect("Failed to get value");

        assert_eq!(new_value, Some(b"new_value".to_vec()));
        assert_eq!(keep_value, Some(b"updated_value".to_vec()));
        assert_eq!(delete_value, None);
    }
}
