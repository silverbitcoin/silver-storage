//! Flush and durability manager for blockchain persistence
//!
//! This module provides production-ready flush and durability operations:
//! - Flush specific column families to disk
//! - Flush all column families atomically
//! - Flush write-ahead log
//! - Force data to disk with fsync
//! - Verify flush completion
//! - Retry logic with exponential backoff
//! - Comprehensive error handling and logging

use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::errors::{PersistenceError, PersistenceResult};
use super::traits::FlushAndDurabilityManager;
use super::db::PERSISTENCE_COLUMN_FAMILIES;

/// Flush and durability manager implementation
///
/// Provides production-ready flush operations with:
/// - Exponential backoff retry logic
/// - Comprehensive error handling
/// - Detailed logging
/// - Durability guarantees
pub struct FlushAndDurabilityManagerImpl;

impl FlushAndDurabilityManager for FlushAndDurabilityManagerImpl {
    /// Flush specific column family to disk
    ///
    /// # Arguments
    /// * `cf_name` - Column family name
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if flush fails after retries
    fn flush_column_family(cf_name: &str, db: &crate::RocksDatabase) -> PersistenceResult<()> {
        debug!("Flushing column family: {}", cf_name);

        // Validate column family name
        if !PERSISTENCE_COLUMN_FAMILIES.contains(&cf_name) {
            error!("Invalid column family: {}", cf_name);
            return Err(PersistenceError::Database(format!(
                "Invalid column family: {}",
                cf_name
            )));
        }

        // Flush with retry logic
        Self::flush_with_retry(|| {
            db.flush().map_err(|e| {
                error!("Failed to flush column family {}: {}", cf_name, e);
                PersistenceError::FlushFailed(format!(
                    "Failed to flush column family {}: {}",
                    cf_name, e
                ))
            })
        })?;

        debug!("Column family {} flushed successfully", cf_name);
        Ok(())
    }

    /// Flush all column families to disk
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if flush fails after retries
    fn flush_all_column_families(db: &crate::RocksDatabase) -> PersistenceResult<()> {
        info!("Flushing all column families to disk");

        // Flush with retry logic
        Self::flush_with_retry(|| {
            db.flush().map_err(|e| {
                error!("Failed to flush all column families: {}", e);
                PersistenceError::FlushFailed(format!(
                    "Failed to flush all column families: {}",
                    e
                ))
            })
        })?;

        info!("All column families flushed successfully");
        Ok(())
    }

    /// Flush write-ahead log to disk
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if flush fails after retries
    fn flush_wal(db: &crate::RocksDatabase) -> PersistenceResult<()> {
        debug!("Flushing write-ahead log to disk");

        // Flush with retry logic
        Self::flush_with_retry(|| {
            db.flush_wal().map_err(|e| {
                error!("Failed to flush WAL: {}", e);
                PersistenceError::FlushFailed(format!("Failed to flush WAL: {}", e))
            })
        })?;

        debug!("Write-ahead log flushed successfully");
        Ok(())
    }

    /// Force all data to disk with fsync
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if fsync fails after retries
    fn fsync_all(db: &crate::RocksDatabase) -> PersistenceResult<()> {
        info!("Forcing all data to disk with fsync");

        // Flush with retry logic
        Self::flush_with_retry(|| {
            db.fsync().map_err(|e| {
                error!("Failed to fsync: {}", e);
                PersistenceError::FlushFailed(format!("Failed to fsync: {}", e))
            })
        })?;

        info!("All data forced to disk successfully");
        Ok(())
    }

    /// Verify flush completed successfully
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if verification fails
    fn verify_flush_completed(db: &crate::RocksDatabase) -> PersistenceResult<()> {
        debug!("Verifying flush completion");

        // Check database integrity
        db.verify_integrity().map_err(|e| {
            error!("Flush verification failed: {}", e);
            PersistenceError::FlushFailed(format!("Flush verification failed: {}", e))
        })?;

        debug!("Flush verification successful");
        Ok(())
    }
}

impl FlushAndDurabilityManagerImpl {
    /// Flush with exponential backoff retry logic
    ///
    /// Retries up to 5 times with exponential backoff:
    /// - Attempt 1: immediate
    /// - Attempt 2: 10ms delay
    /// - Attempt 3: 20ms delay
    /// - Attempt 4: 40ms delay
    /// - Attempt 5: 80ms delay
    ///
    /// # Arguments
    /// * `operation` - Closure that performs the flush operation
    ///
    /// # Errors
    /// Returns error if all retry attempts fail
    fn flush_with_retry<F>(mut operation: F) -> PersistenceResult<()>
    where
        F: FnMut() -> PersistenceResult<()>,
    {
        const MAX_RETRIES: u32 = 5;
        const INITIAL_BACKOFF_MS: u64 = 10;

        for attempt in 1..=MAX_RETRIES {
            match operation() {
                Ok(()) => {
                    if attempt > 1 {
                        debug!("Flush succeeded on attempt {}", attempt);
                    }
                    return Ok(());
                }
                Err(e) => {
                    if attempt == MAX_RETRIES {
                        error!(
                            "Flush failed after {} attempts: {}",
                            MAX_RETRIES, e
                        );
                        return Err(PersistenceError::FlushRetryExhausted {
                            attempts: MAX_RETRIES,
                            reason: e.to_string(),
                        });
                    }

                    // Calculate exponential backoff
                    let backoff_ms = INITIAL_BACKOFF_MS * (2_u64.pow(attempt - 1));
                    warn!(
                        "Flush attempt {} failed: {}. Retrying in {}ms",
                        attempt, e, backoff_ms
                    );

                    // Sleep before retry
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                }
            }
        }

        // This should never be reached due to the loop logic
        Err(PersistenceError::FlushFailed(
            "Flush retry logic error".to_string(),
        ))
    }

    /// Flush all critical data for shutdown
    ///
    /// Performs comprehensive flush sequence:
    /// 1. Flush all column families
    /// 2. Flush write-ahead log
    /// 3. Force data to disk with fsync
    /// 4. Verify flush completed
    ///
    /// # Arguments
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if any flush step fails
    pub fn flush_all_critical(db: &crate::RocksDatabase) -> PersistenceResult<()> {
        info!("Performing critical flush sequence for shutdown");

        // Step 1: Flush all column families
        Self::flush_all_column_families(db)?;

        // Step 2: Flush write-ahead log
        Self::flush_wal(db)?;

        // Step 3: Force data to disk with fsync
        Self::fsync_all(db)?;

        // Step 4: Verify flush completed
        Self::verify_flush_completed(db)?;

        info!("Critical flush sequence completed successfully");
        Ok(())
    }

    /// Flush specific column families
    ///
    /// # Arguments
    /// * `cf_names` - Vector of column family names to flush
    /// * `db` - Database reference
    ///
    /// # Errors
    /// Returns error if any flush fails
    pub fn flush_specific_cfs(
        cf_names: &[&str],
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()> {
        debug!("Flushing {} column families", cf_names.len());

        for cf_name in cf_names {
            Self::flush_column_family(cf_name, db)?;
        }

        debug!("All specified column families flushed successfully");
        Ok(())
    }

    /// Flush with timeout
    ///
    /// Attempts to flush within specified timeout duration
    ///
    /// # Arguments
    /// * `db` - Database reference
    /// * `timeout` - Maximum time to wait for flush
    ///
    /// # Errors
    /// Returns error if flush fails or timeout exceeded
    pub fn flush_with_timeout(
        db: &crate::RocksDatabase,
        timeout: Duration,
    ) -> PersistenceResult<()> {
        debug!("Flushing with timeout: {:?}", timeout);

        let start = std::time::Instant::now();

        // Perform flush
        Self::flush_all_column_families(db)?;

        let elapsed = start.elapsed();
        if elapsed > timeout {
            warn!(
                "Flush completed but exceeded timeout: {:?} > {:?}",
                elapsed, timeout
            );
            return Err(PersistenceError::FlushFailed(format!(
                "Flush exceeded timeout: {:?} > {:?}",
                elapsed, timeout
            )));
        }

        debug!("Flush completed within timeout: {:?}", elapsed);
        Ok(())
    }

    /// Get flush statistics
    ///
    /// Returns information about flush operations
    ///
    /// # Returns
    /// String containing flush statistics
    pub fn get_flush_stats(db: &crate::RocksDatabase) -> Option<String> {
        db.get_statistics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::db::{CF_BLOCKS, CF_SNAPSHOTS, CF_TRANSACTIONS};
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, crate::RocksDatabase) {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = crate::RocksDatabase::open(temp_dir.path())
            .expect("Failed to open test database");
        (temp_dir, db)
    }

    #[test]
    fn test_flush_column_family() {
        let (_temp_dir, db) = create_test_db();

        // Put some data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Flush column family
        FlushAndDurabilityManagerImpl::flush_column_family(CF_BLOCKS, &db)
            .expect("Failed to flush column family");

        // Verify data is still there
        let value = db.get(CF_BLOCKS, b"test_key")
            .expect("Failed to get value");
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_flush_all_column_families() {
        let (_temp_dir, db) = create_test_db();

        // Put data in multiple column families
        db.put(CF_BLOCKS, b"block_key", b"block_value")
            .expect("Failed to put in CF_BLOCKS");
        db.put(CF_SNAPSHOTS, b"snapshot_key", b"snapshot_value")
            .expect("Failed to put in CF_SNAPSHOTS");
        db.put(CF_TRANSACTIONS, b"tx_key", b"tx_value")
            .expect("Failed to put in CF_TRANSACTIONS");

        // Flush all
        FlushAndDurabilityManagerImpl::flush_all_column_families(&db)
            .expect("Failed to flush all column families");

        // Verify all data is still there
        assert_eq!(
            db.get(CF_BLOCKS, b"block_key").expect("Failed to get"),
            Some(b"block_value".to_vec())
        );
        assert_eq!(
            db.get(CF_SNAPSHOTS, b"snapshot_key").expect("Failed to get"),
            Some(b"snapshot_value".to_vec())
        );
        assert_eq!(
            db.get(CF_TRANSACTIONS, b"tx_key").expect("Failed to get"),
            Some(b"tx_value".to_vec())
        );
    }

    #[test]
    fn test_flush_wal() {
        let (_temp_dir, db) = create_test_db();

        // Put some data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Flush WAL
        FlushAndDurabilityManagerImpl::flush_wal(&db)
            .expect("Failed to flush WAL");

        // Verify data is still there
        let value = db.get(CF_BLOCKS, b"test_key")
            .expect("Failed to get value");
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_fsync_all() {
        let (_temp_dir, db) = create_test_db();

        // Put some data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Fsync all
        FlushAndDurabilityManagerImpl::fsync_all(&db)
            .expect("Failed to fsync");

        // Verify data is still there
        let value = db.get(CF_BLOCKS, b"test_key")
            .expect("Failed to get value");
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_verify_flush_completed() {
        let (_temp_dir, db) = create_test_db();

        // Put some data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Flush
        FlushAndDurabilityManagerImpl::flush_all_column_families(&db)
            .expect("Failed to flush");

        // Verify flush completed
        FlushAndDurabilityManagerImpl::verify_flush_completed(&db)
            .expect("Failed to verify flush");
    }

    #[test]
    fn test_flush_all_critical() {
        let (_temp_dir, db) = create_test_db();

        // Put data in multiple column families
        db.put(CF_BLOCKS, b"block_key", b"block_value")
            .expect("Failed to put in CF_BLOCKS");
        db.put(CF_SNAPSHOTS, b"snapshot_key", b"snapshot_value")
            .expect("Failed to put in CF_SNAPSHOTS");

        // Perform critical flush
        FlushAndDurabilityManagerImpl::flush_all_critical(&db)
            .expect("Failed to perform critical flush");

        // Verify all data is still there
        assert_eq!(
            db.get(CF_BLOCKS, b"block_key").expect("Failed to get"),
            Some(b"block_value".to_vec())
        );
        assert_eq!(
            db.get(CF_SNAPSHOTS, b"snapshot_key").expect("Failed to get"),
            Some(b"snapshot_value".to_vec())
        );
    }

    #[test]
    fn test_flush_specific_cfs() {
        let (_temp_dir, db) = create_test_db();

        // Put data in multiple column families
        db.put(CF_BLOCKS, b"block_key", b"block_value")
            .expect("Failed to put in CF_BLOCKS");
        db.put(CF_SNAPSHOTS, b"snapshot_key", b"snapshot_value")
            .expect("Failed to put in CF_SNAPSHOTS");
        db.put(CF_TRANSACTIONS, b"tx_key", b"tx_value")
            .expect("Failed to put in CF_TRANSACTIONS");

        // Flush only specific column families
        FlushAndDurabilityManagerImpl::flush_specific_cfs(&[CF_BLOCKS, CF_SNAPSHOTS], &db)
            .expect("Failed to flush specific CFs");

        // Verify all data is still there
        assert_eq!(
            db.get(CF_BLOCKS, b"block_key").expect("Failed to get"),
            Some(b"block_value".to_vec())
        );
        assert_eq!(
            db.get(CF_SNAPSHOTS, b"snapshot_key").expect("Failed to get"),
            Some(b"snapshot_value".to_vec())
        );
        assert_eq!(
            db.get(CF_TRANSACTIONS, b"tx_key").expect("Failed to get"),
            Some(b"tx_value".to_vec())
        );
    }

    #[test]
    fn test_flush_with_timeout() {
        let (_temp_dir, db) = create_test_db();

        // Put some data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Flush with generous timeout
        FlushAndDurabilityManagerImpl::flush_with_timeout(&db, Duration::from_secs(10))
            .expect("Failed to flush with timeout");

        // Verify data is still there
        let value = db.get(CF_BLOCKS, b"test_key")
            .expect("Failed to get value");
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_flush_invalid_column_family() {
        let (_temp_dir, db) = create_test_db();

        // Try to flush invalid column family
        let result = FlushAndDurabilityManagerImpl::flush_column_family("invalid_cf", &db);
        assert!(result.is_err());
    }

    #[test]
    fn test_flush_with_large_data() {
        let (_temp_dir, db) = create_test_db();

        // Put large amount of data
        for i in 0..1000 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).repeat(100).into_bytes();
            db.put(CF_BLOCKS, &key, &value)
                .expect("Failed to put value");
        }

        // Flush all
        FlushAndDurabilityManagerImpl::flush_all_column_families(&db)
            .expect("Failed to flush all column families");

        // Verify some data is still there
        let value = db.get(CF_BLOCKS, b"key_500")
            .expect("Failed to get value");
        assert!(value.is_some());
    }

    #[test]
    fn test_flush_multiple_times() {
        let (_temp_dir, db) = create_test_db();

        // Put data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Flush multiple times
        for _ in 0..5 {
            FlushAndDurabilityManagerImpl::flush_all_column_families(&db)
                .expect("Failed to flush");
        }

        // Verify data is still there
        let value = db.get(CF_BLOCKS, b"test_key")
            .expect("Failed to get value");
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_get_flush_stats() {
        let (_temp_dir, db) = create_test_db();

        // Put some data
        db.put(CF_BLOCKS, b"test_key", b"test_value")
            .expect("Failed to put value");

        // Get flush stats
        let stats = FlushAndDurabilityManagerImpl::get_flush_stats(&db);
        // Stats may or may not be available depending on RocksDB configuration
        // Just verify the function doesn't panic
        let _ = stats;
    }
}
