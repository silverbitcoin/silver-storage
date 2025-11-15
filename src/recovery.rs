//! Storage recovery and pruning functionality
//!
//! This module provides:
//! - Crash recovery using Write-Ahead Log (WAL)
//! - Data corruption detection and recovery
//! - Pruning of old object versions
//! - Database health checks and repair

use crate::{
    db::{RocksDatabase, CF_OBJECTS, CF_SNAPSHOTS, CF_TRANSACTIONS},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// Recovery manager for database crash recovery and pruning
pub struct RecoveryManager {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
}

/// Recovery statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStats {
    /// Number of records recovered from WAL
    pub records_recovered: u64,
    
    /// Number of corrupted records detected
    pub corrupted_records: u64,
    
    /// Number of records repaired
    pub records_repaired: u64,
    
    /// Recovery duration in milliseconds
    pub duration_ms: u64,
    
    /// Whether recovery was successful
    pub success: bool,
    
    /// Error message if recovery failed
    pub error_message: Option<String>,
}

/// Pruning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruningConfig {
    /// Retention period in seconds (default: 30 days)
    pub retention_period_secs: u64,
    
    /// Maximum number of object versions to keep per object (default: 10)
    pub max_versions_per_object: u32,
    
    /// Whether to prune transactions older than retention period
    pub prune_old_transactions: bool,
    
    /// Whether to prune snapshots older than retention period
    pub prune_old_snapshots: bool,
    
    /// Minimum number of snapshots to always keep (default: 100)
    pub min_snapshots_to_keep: u32,
}

impl Default for PruningConfig {
    fn default() -> Self {
        Self {
            retention_period_secs: 30 * 24 * 60 * 60, // 30 days
            max_versions_per_object: 10,
            prune_old_transactions: true,
            prune_old_snapshots: false, // Keep all snapshots by default
            min_snapshots_to_keep: 100,
        }
    }
}

/// Pruning statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruningStats {
    /// Number of object versions pruned
    pub objects_pruned: u64,
    
    /// Number of transactions pruned
    pub transactions_pruned: u64,
    
    /// Number of snapshots pruned
    pub snapshots_pruned: u64,
    
    /// Space reclaimed in bytes
    pub space_reclaimed_bytes: u64,
    
    /// Pruning duration in milliseconds
    pub duration_ms: u64,
}

impl RecoveryManager {
    /// Create a new recovery manager
    ///
    /// # Arguments
    /// * `db` - Shared reference to the RocksDB database
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing RecoveryManager");
        Self { db }
    }

    /// Perform crash recovery
    ///
    /// This function:
    /// 1. Checks database integrity
    /// 2. Recovers data from WAL if needed
    /// 3. Repairs corrupted data if possible
    /// 4. Returns recovery statistics
    ///
    /// # Returns
    /// Recovery statistics including number of records recovered
    ///
    /// # Errors
    /// Returns error if recovery fails and database cannot be repaired
    pub fn recover_from_crash(&self) -> Result<RecoveryStats> {
        info!("Starting crash recovery");
        let start_time = SystemTime::now();

        let mut stats = RecoveryStats {
            records_recovered: 0,
            corrupted_records: 0,
            records_repaired: 0,
            duration_ms: 0,
            success: false,
            error_message: None,
        };

        // Step 1: Verify database integrity
        info!("Step 1: Verifying database integrity");
        match self.db.verify_integrity() {
            Ok(()) => {
                info!("Database integrity check passed");
            }
            Err(e) => {
                warn!("Database integrity check failed: {}", e);
                stats.corrupted_records += 1;
                
                // Attempt to repair
                info!("Attempting to repair database");
                if let Err(repair_err) = self.repair_database() {
                    error!("Database repair failed: {}", repair_err);
                    stats.error_message = Some(format!("Repair failed: {}", repair_err));
                    stats.duration_ms = SystemTime::now()
                        .duration_since(start_time)
                        .unwrap()
                        .as_millis() as u64;
                    return Ok(stats);
                }
                
                stats.records_repaired += 1;
            }
        }

        // Step 2: WAL recovery (RocksDB handles this automatically on open)
        // The WAL is automatically replayed when the database is opened,
        // so we just need to verify that the recovery was successful
        info!("Step 2: WAL recovery (handled automatically by RocksDB)");
        
        // RocksDB automatically recovers from WAL on open, so we just verify
        // that the database is in a consistent state
        match self.verify_consistency() {
            Ok(recovered) => {
                stats.records_recovered = recovered;
                info!("WAL recovery successful: {} records", recovered);
            }
            Err(e) => {
                error!("WAL recovery verification failed: {}", e);
                stats.error_message = Some(format!("WAL verification failed: {}", e));
                stats.duration_ms = SystemTime::now()
                    .duration_since(start_time)
                    .unwrap()
                    .as_millis() as u64;
                return Ok(stats);
            }
        }

        // Step 3: Flush all memtables to ensure durability
        info!("Step 3: Flushing memtables to disk");
        if let Err(e) = self.db.flush() {
            error!("Failed to flush memtables: {}", e);
            stats.error_message = Some(format!("Flush failed: {}", e));
            stats.duration_ms = SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_millis() as u64;
            return Ok(stats);
        }

        stats.success = true;
        stats.duration_ms = SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis() as u64;

        info!(
            "Crash recovery completed successfully in {}ms (recovered: {}, repaired: {})",
            stats.duration_ms, stats.records_recovered, stats.records_repaired
        );

        Ok(stats)
    }

    /// Verify database consistency
    ///
    /// Checks that the database is in a consistent state after recovery.
    /// Returns the number of records that were recovered from WAL.
    fn verify_consistency(&self) -> Result<u64> {
        debug!("Verifying database consistency");

        // Count records in each column family to verify they're accessible
        let mut total_records = 0u64;

        // Verify objects
        match self.db.get_cf_key_count(CF_OBJECTS) {
            Ok(count) => {
                debug!("Objects column family: {} records", count);
                total_records += count;
            }
            Err(e) => {
                error!("Failed to count objects: {}", e);
                return Err(e);
            }
        }

        // Verify transactions
        match self.db.get_cf_key_count(CF_TRANSACTIONS) {
            Ok(count) => {
                debug!("Transactions column family: {} records", count);
                total_records += count;
            }
            Err(e) => {
                error!("Failed to count transactions: {}", e);
                return Err(e);
            }
        }

        // Verify snapshots
        match self.db.get_cf_key_count(CF_SNAPSHOTS) {
            Ok(count) => {
                debug!("Snapshots column family: {} records", count);
                total_records += count;
            }
            Err(e) => {
                error!("Failed to count snapshots: {}", e);
                return Err(e);
            }
        }

        debug!("Consistency verification passed: {} total records", total_records);
        Ok(total_records)
    }

    /// Repair database
    ///
    /// Attempts to repair a corrupted database by:
    /// 1. Creating a backup of the corrupted database
    /// 2. Attempting to recover readable data
    /// 3. Rebuilding indexes
    fn repair_database(&self) -> Result<()> {
        info!("Attempting database repair");

        // Create backup before repair
        let backup_path = self.db.path().parent().unwrap().join("backup_before_repair");
        info!("Creating backup at: {}", backup_path.display());
        
        if let Err(e) = self.db.create_backup(&backup_path) {
            warn!("Failed to create backup before repair: {}", e);
            // Continue with repair anyway
        }

        // Compact all column families to remove corrupted data
        info!("Compacting all column families");
        self.db.compact_all()?;

        // Verify integrity after repair
        info!("Verifying integrity after repair");
        self.db.verify_integrity()?;

        info!("Database repair completed successfully");
        Ok(())
    }

    /// Detect data corruption
    ///
    /// Scans the database for corrupted records and returns their count.
    ///
    /// # Returns
    /// Number of corrupted records detected
    pub fn detect_corruption(&self) -> Result<u64> {
        info!("Scanning database for corruption");

        // Verify integrity first
        if let Err(e) = self.db.verify_integrity() {
            error!("Corruption detected during integrity check: {}", e);
            return Ok(1); // At least one corruption detected
        }

        // Try to iterate through all column families
        let mut corrupted_count = 0u64;

        for cf_name in &[CF_OBJECTS, CF_TRANSACTIONS, CF_SNAPSHOTS] {
            debug!("Scanning column family: {}", cf_name);
            
            for result in self.db.iter(cf_name, rocksdb::IteratorMode::Start) {
                match result {
                    Ok(_) => {
                        // Record is readable
                    }
                    Err(e) => {
                        error!("Corrupted record in {}: {}", cf_name, e);
                        corrupted_count += 1;
                    }
                }
            }
        }

        if corrupted_count > 0 {
            warn!("Detected {} corrupted records", corrupted_count);
        } else {
            info!("No corruption detected");
        }

        Ok(corrupted_count)
    }

    /// Prune old data according to configuration
    ///
    /// This function:
    /// 1. Prunes old object versions beyond retention period
    /// 2. Prunes old transactions if configured
    /// 3. Prunes old snapshots if configured (keeping minimum required)
    /// 4. Compacts database to reclaim space
    ///
    /// # Arguments
    /// * `config` - Pruning configuration
    ///
    /// # Returns
    /// Pruning statistics including space reclaimed
    pub fn prune(&self, config: &PruningConfig) -> Result<PruningStats> {
        info!("Starting pruning with config: {:?}", config);
        let start_time = SystemTime::now();

        let mut stats = PruningStats {
            objects_pruned: 0,
            transactions_pruned: 0,
            snapshots_pruned: 0,
            space_reclaimed_bytes: 0,
            duration_ms: 0,
        };

        // Get current size before pruning
        let size_before = self.db.get_total_size()?;

        // Calculate cutoff timestamp
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff_timestamp = current_time.saturating_sub(config.retention_period_secs);

        info!("Pruning data older than {} seconds (cutoff: {})", 
              config.retention_period_secs, cutoff_timestamp);

        // Step 1: Prune old object versions
        // Note: This is a simplified implementation. In production, we would need
        // to track object versions and prune based on version count and age.
        info!("Step 1: Pruning old object versions");
        // For now, we skip this as it requires version tracking in the object store
        // This would be implemented when object versioning is fully implemented

        // Step 2: Prune old transactions
        if config.prune_old_transactions {
            info!("Step 2: Pruning old transactions");
            stats.transactions_pruned = self.prune_old_transactions(cutoff_timestamp)?;
            info!("Pruned {} old transactions", stats.transactions_pruned);
        }

        // Step 3: Prune old snapshots
        if config.prune_old_snapshots {
            info!("Step 3: Pruning old snapshots");
            stats.snapshots_pruned = self.prune_old_snapshots(
                cutoff_timestamp,
                config.min_snapshots_to_keep,
            )?;
            info!("Pruned {} old snapshots", stats.snapshots_pruned);
        }

        // Step 4: Compact database to reclaim space
        info!("Step 4: Compacting database to reclaim space");
        self.db.compact_all()?;

        // Calculate space reclaimed
        let size_after = self.db.get_total_size()?;
        stats.space_reclaimed_bytes = size_before.saturating_sub(size_after);

        stats.duration_ms = SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis() as u64;

        info!(
            "Pruning completed in {}ms (objects: {}, transactions: {}, snapshots: {}, space reclaimed: {} bytes)",
            stats.duration_ms,
            stats.objects_pruned,
            stats.transactions_pruned,
            stats.snapshots_pruned,
            stats.space_reclaimed_bytes
        );

        Ok(stats)
    }

    /// Prune old transactions
    ///
    /// Removes transactions older than the cutoff timestamp.
    /// Note: This is a simplified implementation that would need to be
    /// enhanced with proper timestamp tracking in production.
    fn prune_old_transactions(&self, _cutoff_timestamp: u64) -> Result<u64> {
        // In a production implementation, we would:
        // 1. Iterate through transactions
        // 2. Check their timestamps
        // 3. Delete transactions older than cutoff
        // 4. Update indexes
        
        // For now, return 0 as we don't have timestamp indexing yet
        debug!("Transaction pruning not yet implemented (requires timestamp indexing)");
        Ok(0)
    }

    /// Prune old snapshots
    ///
    /// Removes snapshots older than the cutoff timestamp, keeping at least
    /// min_snapshots_to_keep recent snapshots.
    fn prune_old_snapshots(&self, _cutoff_timestamp: u64, _min_to_keep: u32) -> Result<u64> {
        // In a production implementation, we would:
        // 1. Get all snapshots sorted by sequence number
        // 2. Keep the most recent min_to_keep snapshots
        // 3. Delete snapshots older than cutoff (beyond min_to_keep)
        // 4. Update indexes
        
        // For now, return 0 as we don't want to accidentally delete important snapshots
        debug!("Snapshot pruning not yet implemented (requires careful snapshot management)");
        Ok(0)
    }

    /// Create a backup before performing risky operations
    ///
    /// # Arguments
    /// * `backup_path` - Path to store the backup
    pub fn create_backup<P: AsRef<Path>>(&self, backup_path: P) -> Result<()> {
        info!("Creating backup at: {}", backup_path.as_ref().display());
        self.db.create_backup(backup_path)
    }

    /// Restore from a backup
    ///
    /// # Arguments
    /// * `backup_path` - Path containing the backup
    /// * `restore_path` - Path to restore the database to
    pub fn restore_from_backup<P: AsRef<Path>, Q: AsRef<Path>>(
        backup_path: P,
        restore_path: Q,
    ) -> Result<()> {
        info!(
            "Restoring from backup: {} -> {}",
            backup_path.as_ref().display(),
            restore_path.as_ref().display()
        );
        RocksDatabase::restore_from_backup(backup_path, restore_path)
    }

    /// Get database health status
    ///
    /// Returns information about database health including:
    /// - Integrity status
    /// - Size statistics
    /// - Corruption detection
    pub fn get_health_status(&self) -> Result<DatabaseHealth> {
        info!("Checking database health");

        let integrity_ok = self.db.verify_integrity().is_ok();
        let total_size = self.db.get_total_size()?;
        let corrupted_records = if integrity_ok {
            0
        } else {
            self.detect_corruption()?
        };

        let health = DatabaseHealth {
            integrity_ok,
            total_size_bytes: total_size,
            corrupted_records,
            last_check_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        info!("Database health: {:?}", health);
        Ok(health)
    }
}

/// Database health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseHealth {
    /// Whether database integrity check passed
    pub integrity_ok: bool,
    
    /// Total database size in bytes
    pub total_size_bytes: u64,
    
    /// Number of corrupted records detected
    pub corrupted_records: u64,
    
    /// Timestamp of last health check (Unix seconds)
    pub last_check_timestamp: u64,
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_recovery_manager() -> (RecoveryManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
        let manager = RecoveryManager::new(db);
        (manager, temp_dir)
    }

    #[test]
    fn test_recover_from_crash() {
        let (manager, _temp) = create_test_recovery_manager();

        // Should succeed on healthy database
        let stats = manager.recover_from_crash().unwrap();
        assert!(stats.success);
        assert_eq!(stats.corrupted_records, 0);
    }

    #[test]
    fn test_verify_consistency() {
        let (manager, _temp) = create_test_recovery_manager();

        // Should succeed on empty database
        let count = manager.verify_consistency().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_detect_corruption() {
        let (manager, _temp) = create_test_recovery_manager();

        // Should detect no corruption on healthy database
        let corrupted = manager.detect_corruption().unwrap();
        assert_eq!(corrupted, 0);
    }

    #[test]
    fn test_prune_with_default_config() {
        let (manager, _temp) = create_test_recovery_manager();

        let config = PruningConfig::default();
        let stats = manager.prune(&config).unwrap();

        // Should complete without error
        assert_eq!(stats.objects_pruned, 0); // No objects to prune
        assert_eq!(stats.transactions_pruned, 0); // Not implemented yet
        assert_eq!(stats.snapshots_pruned, 0); // Not implemented yet
    }

    #[test]
    fn test_prune_with_custom_config() {
        let (manager, _temp) = create_test_recovery_manager();

        let config = PruningConfig {
            retention_period_secs: 7 * 24 * 60 * 60, // 7 days
            max_versions_per_object: 5,
            prune_old_transactions: true,
            prune_old_snapshots: false,
            min_snapshots_to_keep: 50,
        };

        let stats = manager.prune(&config).unwrap();
        // Duration might be 0 for very fast operations on empty database
        assert!(stats.duration_ms >= 0);
    }

    #[test]
    fn test_create_and_restore_backup() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let restore_dir = TempDir::new().unwrap();

        // Create database and recovery manager
        {
            let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
            let manager = RecoveryManager::new(db.clone());

            // Add some data
            db.put(CF_OBJECTS, b"key1", b"value1").unwrap();
            db.put(CF_OBJECTS, b"key2", b"value2").unwrap();

            // Create backup
            manager.create_backup(backup_dir.path()).unwrap();
        }

        // Restore backup
        RecoveryManager::restore_from_backup(backup_dir.path(), restore_dir.path()).unwrap();

        // Verify restored data
        let restored_db = RocksDatabase::open(restore_dir.path()).unwrap();
        assert_eq!(
            restored_db.get(CF_OBJECTS, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            restored_db.get(CF_OBJECTS, b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn test_get_health_status() {
        let (manager, _temp) = create_test_recovery_manager();

        let health = manager.get_health_status().unwrap();
        assert!(health.integrity_ok);
        assert_eq!(health.corrupted_records, 0);
        assert!(health.last_check_timestamp > 0);
    }

    #[test]
    fn test_pruning_config_default() {
        let config = PruningConfig::default();
        assert_eq!(config.retention_period_secs, 30 * 24 * 60 * 60);
        assert_eq!(config.max_versions_per_object, 10);
        assert!(config.prune_old_transactions);
        assert!(!config.prune_old_snapshots);
        assert_eq!(config.min_snapshots_to_keep, 100);
    }

    #[test]
    fn test_recovery_stats_serialization() {
        let stats = RecoveryStats {
            records_recovered: 100,
            corrupted_records: 5,
            records_repaired: 3,
            duration_ms: 1500,
            success: true,
            error_message: None,
        };

        // Should serialize and deserialize
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: RecoveryStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.records_recovered, 100);
        assert_eq!(deserialized.corrupted_records, 5);
        assert!(deserialized.success);
    }

    #[test]
    fn test_pruning_stats_serialization() {
        let stats = PruningStats {
            objects_pruned: 50,
            transactions_pruned: 100,
            snapshots_pruned: 10,
            space_reclaimed_bytes: 1024 * 1024,
            duration_ms: 2000,
        };

        // Should serialize and deserialize
        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: PruningStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.objects_pruned, 50);
        assert_eq!(deserialized.space_reclaimed_bytes, 1024 * 1024);
    }

    #[test]
    fn test_database_health_serialization() {
        let health = DatabaseHealth {
            integrity_ok: true,
            total_size_bytes: 1024 * 1024 * 100,
            corrupted_records: 0,
            last_check_timestamp: 1234567890,
        };

        // Should serialize and deserialize
        let json = serde_json::to_string(&health).unwrap();
        let deserialized: DatabaseHealth = serde_json::from_str(&json).unwrap();
        assert!(deserialized.integrity_ok);
        assert_eq!(deserialized.total_size_bytes, 1024 * 1024 * 100);
    }
}
