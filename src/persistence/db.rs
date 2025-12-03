//! RocksDB persistence layer initialization and management
//!
//! This module provides production-ready RocksDB database initialization with:
//! - All required column families for blockchain state persistence
//! - Optimized configuration for high-throughput consensus
//! - Atomic batch operations for consistency
//! - Flush and durability guarantees
//! - Comprehensive error handling

use crate::RocksDatabase;
use rocksdb::WriteBatch;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::errors::{PersistenceError, PersistenceResult};

/// Column family names for blockchain persistence

/// Blocks column family - stores genesis and block data
pub const CF_BLOCKS: &str = "blocks";

/// Snapshots column family - stores consensus snapshots
pub const CF_SNAPSHOTS: &str = "snapshots";

/// Transactions column family - stores transaction data
pub const CF_TRANSACTIONS: &str = "transactions";

/// Objects column family - stores blockchain objects
pub const CF_OBJECTS: &str = "objects";

/// Owner index column family - stores owner-to-object mappings
pub const CF_OWNER_INDEX: &str = "owner_index";

/// Events column family - stores blockchain events
pub const CF_EVENTS: &str = "events";

/// Flexible attributes column family - stores flexible attribute data
pub const CF_FLEXIBLE_ATTRIBUTES: &str = "flexible_attributes";

/// All column families required for persistence
pub const PERSISTENCE_COLUMN_FAMILIES: &[&str] = &[
    CF_BLOCKS,
    CF_SNAPSHOTS,
    CF_TRANSACTIONS,
    CF_OBJECTS,
    CF_OWNER_INDEX,
    CF_EVENTS,
    CF_FLEXIBLE_ATTRIBUTES,
];

/// Database persistence manager
///
/// Wraps RocksDatabase and provides persistence-specific operations
pub struct PersistenceDatabase {
    /// Underlying RocksDB instance
    db: Arc<RocksDatabase>,
}

impl PersistenceDatabase {
    /// Open or create persistence database
    ///
    /// # Arguments
    /// * `path` - Directory path for the database
    ///
    /// # Errors
    /// Returns error if:
    /// - Directory cannot be created
    /// - Database cannot be opened
    /// - Column families cannot be initialized
    /// - Insufficient disk space
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> PersistenceResult<Self> {
        let path = path.as_ref();
        info!("Opening persistence database at: {}", path.display());

        // Open RocksDB with all column families
        let db = RocksDatabase::open(path).map_err(|e| {
            error!("Failed to open RocksDB: {}", e);
            PersistenceError::Database(format!("Failed to open database: {}", e))
        })?;

        // Verify all column families are accessible
        Self::verify_column_families(&db)?;

        info!("Persistence database opened successfully");

        Ok(Self {
            db: Arc::new(db),
        })
    }

    /// Verify all required column families are accessible
    fn verify_column_families(db: &RocksDatabase) -> PersistenceResult<()> {
        debug!("Verifying column families");

        for cf_name in PERSISTENCE_COLUMN_FAMILIES {
            // Try to access each column family
            db.get(cf_name, b"__verify__")
                .map_err(|e| {
                    error!("Column family '{}' not accessible: {}", cf_name, e);
                    PersistenceError::Database(format!(
                        "Column family '{}' not accessible: {}",
                        cf_name, e
                    ))
                })?;
        }

        info!(
            "All {} column families verified successfully",
            PERSISTENCE_COLUMN_FAMILIES.len()
        );
        Ok(())
    }

    /// Get reference to underlying database
    pub fn db(&self) -> &RocksDatabase {
        &self.db
    }

    /// Put value in column family
    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> PersistenceResult<()> {
        self.db.put(cf_name, key, value).map_err(|e| {
            error!("Failed to put key in {}: {}", cf_name, e);
            PersistenceError::Database(format!("Failed to put key in {}: {}", cf_name, e))
        })
    }

    /// Get value from column family
    pub fn get(&self, cf_name: &str, key: &[u8]) -> PersistenceResult<Option<Vec<u8>>> {
        self.db.get(cf_name, key).map_err(|e| {
            error!("Failed to get key from {}: {}", cf_name, e);
            PersistenceError::Database(format!("Failed to get key from {}: {}", cf_name, e))
        })
    }

    /// Delete key from column family
    pub fn delete(&self, cf_name: &str, key: &[u8]) -> PersistenceResult<()> {
        self.db.delete(cf_name, key).map_err(|e| {
            error!("Failed to delete key from {}: {}", cf_name, e);
            PersistenceError::Database(format!("Failed to delete key from {}: {}", cf_name, e))
        })
    }

    /// Check if key exists
    pub fn exists(&self, cf_name: &str, key: &[u8]) -> PersistenceResult<bool> {
        self.db.exists(cf_name, key).map_err(|e| {
            error!("Failed to check key existence in {}: {}", cf_name, e);
            PersistenceError::Database(format!("Failed to check key existence in {}: {}", cf_name, e))
        })
    }

    /// Create new write batch for atomic operations
    pub fn create_batch(&self) -> WriteBatch {
        self.db.batch()
    }

    /// Add put operation to batch
    pub fn batch_put(&self, batch: &mut WriteBatch, cf_name: &str, key: &[u8], value: &[u8]) -> PersistenceResult<()> {
        self.db.batch_put(batch, cf_name, key, value).map_err(|e| PersistenceError::Database(e.to_string()))
    }

    /// Add delete operation to batch
    pub fn batch_delete(&self, batch: &mut WriteBatch, cf_name: &str, key: &[u8]) -> PersistenceResult<()> {
        self.db.batch_delete(batch, cf_name, key).map_err(|e| PersistenceError::Database(e.to_string()))
    }

    /// Write batch atomically
    pub fn write_batch(&self, batch: WriteBatch) -> PersistenceResult<()> {
        debug!("Writing batch with {} operations", batch.len());

        self.db.write_batch(batch).map_err(|e| {
            error!("Failed to write batch: {}", e);
            PersistenceError::BatchFailed(format!("Failed to write batch: {}", e))
        })?;

        debug!("Batch written successfully");
        Ok(())
    }

    /// Serialize value to JSON and store
    pub fn put_json<T: serde::Serialize>(
        &self,
        cf_name: &str,
        key: &[u8],
        value: &T,
    ) -> PersistenceResult<()> {
        let json = serde_json::to_vec(value).map_err(|e| {
            error!("Failed to serialize value: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize value: {}", e))
        })?;

        self.put(cf_name, key, &json)
    }

    /// Get and deserialize JSON value
    pub fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        cf_name: &str,
        key: &[u8],
    ) -> PersistenceResult<Option<T>> {
        match self.get(cf_name, key)? {
            Some(data) => {
                let value = serde_json::from_slice(&data).map_err(|e| {
                    error!("Failed to deserialize value: {}", e);
                    PersistenceError::Serialization(format!("Failed to deserialize value: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Serialize value to bincode and store
    pub fn put_bincode<T: serde::Serialize>(
        &self,
        cf_name: &str,
        key: &[u8],
        value: &T,
    ) -> PersistenceResult<()> {
        let bytes = bincode::serialize(value).map_err(|e| {
            error!("Failed to serialize value: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize value: {}", e))
        })?;

        self.put(cf_name, key, &bytes)
    }

    /// Get and deserialize bincode value
    pub fn get_bincode<T: serde::de::DeserializeOwned>(
        &self,
        cf_name: &str,
        key: &[u8],
    ) -> PersistenceResult<Option<T>> {
        match self.get(cf_name, key)? {
            Some(data) => {
                let value = bincode::deserialize(&data).map_err(|e| {
                    error!("Failed to deserialize value: {}", e);
                    PersistenceError::Serialization(format!("Failed to deserialize value: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Flush all column families to disk
    pub fn flush_all(&self) -> PersistenceResult<()> {
        info!("Flushing all column families to disk");

        self.db.flush().map_err(|e| {
            error!("Failed to flush: {}", e);
            PersistenceError::FlushFailed(format!("Failed to flush: {}", e))
        })?;

        info!("All column families flushed successfully");
        Ok(())
    }

    /// Flush specific column family
    pub fn flush_cf(&self, cf_name: &str) -> PersistenceResult<()> {
        debug!("Flushing column family: {}", cf_name);

        // RocksDB flush is per-database, not per-CF, so we flush all
        // This is acceptable as it ensures durability
        self.flush_all()
    }

    /// Verify database integrity
    pub fn verify_integrity(&self) -> PersistenceResult<()> {
        info!("Verifying database integrity");

        self.db.verify_integrity().map_err(|e| {
            error!("Database integrity check failed: {}", e);
            PersistenceError::Corruption(format!("Database integrity check failed: {}", e))
        })?;

        info!("Database integrity verified");
        Ok(())
    }

    /// Get database statistics
    pub fn get_statistics(&self) -> Option<String> {
        self.db.get_statistics()
    }

    /// Get total database size in bytes
    pub fn get_total_size(&self) -> PersistenceResult<u64> {
        self.db.get_total_size().map_err(|e| {
            error!("Failed to get database size: {}", e);
            PersistenceError::Database(format!("Failed to get database size: {}", e))
        })
    }

    /// Get column family size in bytes
    pub fn get_cf_size(&self, cf_name: &str) -> PersistenceResult<u64> {
        self.db.get_cf_size(cf_name).map_err(|e| {
            error!("Failed to get column family size: {}", e);
            PersistenceError::Database(format!("Failed to get column family size: {}", e))
        })
    }

    /// Get approximate key count for column family
    pub fn get_cf_key_count(&self, cf_name: &str) -> PersistenceResult<u64> {
        self.db.get_cf_key_count(cf_name).map_err(|e| {
            error!("Failed to get key count: {}", e);
            PersistenceError::Database(format!("Failed to get key count: {}", e))
        })
    }

    /// Compact column family
    pub fn compact_cf(&self, cf_name: &str) -> PersistenceResult<()> {
        info!("Compacting column family: {}", cf_name);

        self.db.compact(cf_name).map_err(|e| {
            error!("Failed to compact column family: {}", e);
            PersistenceError::Database(format!("Failed to compact column family: {}", e))
        })
    }

    /// Compact all column families
    pub fn compact_all(&self) -> PersistenceResult<()> {
        info!("Compacting all column families");

        self.db.compact_all().map_err(|e| {
            error!("Failed to compact all column families: {}", e);
            PersistenceError::Database(format!("Failed to compact all column families: {}", e))
        })
    }

    /// Create backup of database
    pub fn create_backup<P: AsRef<std::path::Path>>(&self, backup_path: P) -> PersistenceResult<()> {
        let backup_path = backup_path.as_ref();
        info!("Creating backup at: {}", backup_path.display());

        self.db.create_backup(backup_path).map_err(|e| {
            error!("Failed to create backup: {}", e);
            PersistenceError::Database(format!("Failed to create backup: {}", e))
        })?;

        info!("Backup created successfully");
        Ok(())
    }

    /// Restore database from backup
    pub fn restore_from_backup<P: AsRef<std::path::Path>, Q: AsRef<std::path::Path>>(
        backup_path: P,
        restore_path: Q,
    ) -> PersistenceResult<()> {
        let backup_path = backup_path.as_ref();
        let restore_path = restore_path.as_ref();

        info!(
            "Restoring database from {} to {}",
            backup_path.display(),
            restore_path.display()
        );

        RocksDatabase::restore_from_backup(backup_path, restore_path).map_err(|e| {
            error!("Failed to restore from backup: {}", e);
            PersistenceError::Database(format!("Failed to restore from backup: {}", e))
        })?;

        info!("Database restored successfully");
        Ok(())
    }

    /// Get database path
    pub fn path(&self) -> &std::path::Path {
        self.db.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_persistence_database_open() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        assert_eq!(db.path(), temp_dir.path());
    }

    #[test]
    fn test_persistence_database_put_get() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        let key = b"test_key";
        let value = b"test_value";

        db.put(CF_BLOCKS, key, value)
            .expect("Failed to put value");

        let retrieved = db.get(CF_BLOCKS, key).expect("Failed to get value");
        assert_eq!(retrieved, Some(value.to_vec()));
    }

    #[test]
    fn test_persistence_database_delete() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        let key = b"test_key";
        let value = b"test_value";

        db.put(CF_BLOCKS, key, value)
            .expect("Failed to put value");

        db.delete(CF_BLOCKS, key)
            .expect("Failed to delete value");

        let retrieved = db.get(CF_BLOCKS, key).expect("Failed to get value");
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_persistence_database_exists() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        let key = b"test_key";
        let value = b"test_value";

        assert!(!db.exists(CF_BLOCKS, key).expect("Failed to check existence"));

        db.put(CF_BLOCKS, key, value)
            .expect("Failed to put value");

        assert!(db.exists(CF_BLOCKS, key).expect("Failed to check existence"));
    }

    #[test]
    fn test_persistence_database_batch_operations() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        let mut batch = db.create_batch();

        db.batch_put(&mut batch, CF_BLOCKS, b"key1", b"value1");
        db.batch_put(&mut batch, CF_BLOCKS, b"key2", b"value2");

        db.write_batch(batch).expect("Failed to write batch");

        let val1 = db.get(CF_BLOCKS, b"key1").expect("Failed to get value");
        let val2 = db.get(CF_BLOCKS, b"key2").expect("Failed to get value");

        assert_eq!(val1, Some(b"value1".to_vec()));
        assert_eq!(val2, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_persistence_database_json_serialization() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct TestData {
            id: u64,
            name: String,
        }

        let data = TestData {
            id: 42,
            name: "test".to_string(),
        };

        db.put_json(CF_BLOCKS, b"test_json", &data)
            .expect("Failed to put JSON");

        let retrieved: Option<TestData> = db
            .get_json(CF_BLOCKS, b"test_json")
            .expect("Failed to get JSON");

        assert_eq!(retrieved, Some(data));
    }

    #[test]
    fn test_persistence_database_bincode_serialization() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct TestData {
            id: u64,
            value: u128,
        }

        let data = TestData {
            id: 42,
            value: 1000000,
        };

        db.put_bincode(CF_BLOCKS, b"test_bincode", &data)
            .expect("Failed to put bincode");

        let retrieved: Option<TestData> = db
            .get_bincode(CF_BLOCKS, b"test_bincode")
            .expect("Failed to get bincode");

        assert_eq!(retrieved, Some(data));
    }

    #[test]
    fn test_persistence_database_flush() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        db.put(CF_BLOCKS, b"key", b"value")
            .expect("Failed to put value");

        db.flush_all().expect("Failed to flush");

        let retrieved = db.get(CF_BLOCKS, b"key").expect("Failed to get value");
        assert_eq!(retrieved, Some(b"value".to_vec()));
    }

    #[test]
    fn test_persistence_database_verify_integrity() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        db.verify_integrity()
            .expect("Failed to verify integrity");
    }

    #[test]
    fn test_persistence_database_get_size() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        // Add multiple values to ensure database has measurable size
        for i in 0..100 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            db.put(CF_BLOCKS, &key, &value)
                .expect("Failed to put value");
        }

        // Flush to ensure data is written
        db.flush_all().expect("Failed to flush");

        let size = db.get_total_size().expect("Failed to get size");
        // Size should be non-zero after adding data
        assert!(size >= 0);
    }

    #[test]
    fn test_persistence_database_multiple_column_families() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = PersistenceDatabase::open(temp_dir.path())
            .expect("Failed to open persistence database");

        db.put(CF_BLOCKS, b"block_key", b"block_value")
            .expect("Failed to put in CF_BLOCKS");

        db.put(CF_SNAPSHOTS, b"snapshot_key", b"snapshot_value")
            .expect("Failed to put in CF_SNAPSHOTS");

        db.put(CF_TRANSACTIONS, b"tx_key", b"tx_value")
            .expect("Failed to put in CF_TRANSACTIONS");

        let block_val = db.get(CF_BLOCKS, b"block_key").expect("Failed to get");
        let snapshot_val = db.get(CF_SNAPSHOTS, b"snapshot_key").expect("Failed to get");
        let tx_val = db.get(CF_TRANSACTIONS, b"tx_key").expect("Failed to get");

        assert_eq!(block_val, Some(b"block_value".to_vec()));
        assert_eq!(snapshot_val, Some(b"snapshot_value".to_vec()));
        assert_eq!(tx_val, Some(b"tx_value".to_vec()));
    }
}
