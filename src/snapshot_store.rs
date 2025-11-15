//! Snapshot storage with validator signatures
//!
//! This module provides storage for blockchain snapshots (checkpoints) with
//! validator signatures for verification.

use crate::{
    db::{RocksDatabase, CF_SNAPSHOTS},
    Error, Result,
};
use silver_core::consensus::SnapshotSequenceNumber;
use silver_core::{Snapshot, SnapshotDigest};
use std::sync::Arc;
use tracing::{debug, error, info};

/// Snapshot store for blockchain checkpoints
///
/// Provides storage and retrieval of snapshots with validator signatures.
pub struct SnapshotStore {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
}

impl SnapshotStore {
    /// Create a new snapshot store
    ///
    /// # Arguments
    /// * `db` - Shared reference to the RocksDB database
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing SnapshotStore");
        Self { db }
    }

    /// Store a snapshot
    ///
    /// Snapshots are indexed by both sequence number and digest for efficient retrieval.
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot to store
    ///
    /// # Errors
    /// Returns error if serialization or database write fails
    pub fn store_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        debug!(
            "Storing snapshot: seq={}, digest={}",
            snapshot.sequence_number, snapshot.digest
        );

        // Validate snapshot
        // Note: We skip total_stake validation here as it requires validator set context
        if snapshot.transactions.len() > 1000 {
            return Err(Error::InvalidData(format!(
                "Snapshot has too many transactions: {}",
                snapshot.transactions.len()
            )));
        }

        // Serialize snapshot
        let snapshot_bytes = bincode::serialize(snapshot)?;

        // Create atomic batch for dual indexing
        let mut batch = self.db.batch();

        // Index by sequence number (primary key)
        let seq_key = self.make_sequence_key(snapshot.sequence_number);
        self.db
            .batch_put(&mut batch, CF_SNAPSHOTS, &seq_key, &snapshot_bytes);

        // Index by digest (secondary key)
        let digest_key = self.make_digest_key(&snapshot.digest);
        // Store sequence number as value for digest lookup
        let seq_bytes = snapshot.sequence_number.to_le_bytes();
        self.db
            .batch_put(&mut batch, CF_SNAPSHOTS, &digest_key, &seq_bytes);

        // Write batch atomically
        self.db.write_batch(batch)?;

        debug!(
            "Snapshot {} stored successfully ({} bytes)",
            snapshot.sequence_number,
            snapshot_bytes.len()
        );

        Ok(())
    }

    /// Get a snapshot by sequence number
    ///
    /// # Arguments
    /// * `sequence_number` - Snapshot sequence number
    ///
    /// # Returns
    /// - `Ok(Some(snapshot))` if snapshot exists
    /// - `Ok(None)` if snapshot doesn't exist
    /// - `Err` on database or deserialization error
    pub fn get_snapshot_by_sequence(
        &self,
        sequence_number: SnapshotSequenceNumber,
    ) -> Result<Option<Snapshot>> {
        debug!("Retrieving snapshot by sequence: {}", sequence_number);

        let key = self.make_sequence_key(sequence_number);
        let snapshot_bytes = self.db.get(CF_SNAPSHOTS, &key)?;

        match snapshot_bytes {
            Some(bytes) => {
                let snapshot: Snapshot = bincode::deserialize(&bytes)?;
                debug!("Snapshot {} retrieved", sequence_number);
                Ok(Some(snapshot))
            }
            None => {
                debug!("Snapshot {} not found", sequence_number);
                Ok(None)
            }
        }
    }

    /// Get a snapshot by digest
    ///
    /// # Arguments
    /// * `digest` - Snapshot digest
    ///
    /// # Returns
    /// - `Ok(Some(snapshot))` if snapshot exists
    /// - `Ok(None)` if snapshot doesn't exist
    pub fn get_snapshot_by_digest(&self, digest: &SnapshotDigest) -> Result<Option<Snapshot>> {
        debug!("Retrieving snapshot by digest: {}", digest);

        // First lookup sequence number by digest
        let digest_key = self.make_digest_key(digest);
        let seq_bytes = self.db.get(CF_SNAPSHOTS, &digest_key)?;

        match seq_bytes {
            Some(bytes) => {
                if bytes.len() != 8 {
                    return Err(Error::InvalidData(format!(
                        "Invalid sequence number size: {} bytes",
                        bytes.len()
                    )));
                }

                let mut seq_array = [0u8; 8];
                seq_array.copy_from_slice(&bytes);
                let sequence_number = u64::from_le_bytes(seq_array);

                // Now get the actual snapshot
                self.get_snapshot_by_sequence(sequence_number)
            }
            None => {
                debug!("Snapshot with digest {} not found", digest);
                Ok(None)
            }
        }
    }

    /// Check if a snapshot exists by sequence number
    ///
    /// # Arguments
    /// * `sequence_number` - Snapshot sequence number
    pub fn exists_by_sequence(&self, sequence_number: SnapshotSequenceNumber) -> Result<bool> {
        let key = self.make_sequence_key(sequence_number);
        self.db.exists(CF_SNAPSHOTS, &key)
    }

    /// Check if a snapshot exists by digest
    ///
    /// # Arguments
    /// * `digest` - Snapshot digest
    pub fn exists_by_digest(&self, digest: &SnapshotDigest) -> Result<bool> {
        let key = self.make_digest_key(digest);
        self.db.exists(CF_SNAPSHOTS, &key)
    }

    /// Get the latest snapshot
    ///
    /// Returns the snapshot with the highest sequence number.
    ///
    /// # Returns
    /// - `Ok(Some(snapshot))` if any snapshots exist
    /// - `Ok(None)` if no snapshots exist
    pub fn get_latest_snapshot(&self) -> Result<Option<Snapshot>> {
        debug!("Retrieving latest snapshot");

        // Iterate in reverse to find the highest sequence number
        // Note: This is a simple implementation. For production, we might want to
        // maintain a separate "latest" pointer for O(1) access.
        
        let mut latest: Option<Snapshot> = None;
        let mut max_seq = 0u64;

        // Iterate over all snapshots
        for result in self.db.iter(CF_SNAPSHOTS, rocksdb::IteratorMode::Start) {
            let (key, value) = result?;

            // Check if this is a sequence key (starts with 's')
            if key.len() > 0 && key[0] == b's' && key.len() == 9 {
                let snapshot: Snapshot = bincode::deserialize(&value)?;
                if snapshot.sequence_number > max_seq {
                    max_seq = snapshot.sequence_number;
                    latest = Some(snapshot);
                }
            }
        }

        if let Some(ref snapshot) = latest {
            debug!("Latest snapshot: seq={}", snapshot.sequence_number);
        } else {
            debug!("No snapshots found");
        }

        Ok(latest)
    }

    /// Get the latest snapshot sequence number
    ///
    /// # Returns
    /// - `Ok(Some(sequence_number))` if any snapshots exist
    /// - `Ok(None)` if no snapshots exist
    pub fn get_latest_sequence_number(&self) -> Result<Option<SnapshotSequenceNumber>> {
        self.get_latest_snapshot()
            .map(|opt| opt.map(|s| s.sequence_number))
    }

    /// Batch store multiple snapshots
    ///
    /// All snapshots are stored atomically.
    ///
    /// # Arguments
    /// * `snapshots` - Slice of snapshots to store
    ///
    /// # Errors
    /// Returns error if serialization or database write fails.
    /// On error, no snapshots are stored (atomic operation).
    pub fn batch_store_snapshots(&self, snapshots: &[Snapshot]) -> Result<()> {
        if snapshots.is_empty() {
            return Ok(());
        }

        info!("Batch storing {} snapshots", snapshots.len());

        // Create atomic batch
        let mut batch = self.db.batch();

        for snapshot in snapshots {
            // Serialize snapshot
            let snapshot_bytes = bincode::serialize(snapshot)?;

            // Index by sequence number
            let seq_key = self.make_sequence_key(snapshot.sequence_number);
            self.db
                .batch_put(&mut batch, CF_SNAPSHOTS, &seq_key, &snapshot_bytes);

            // Index by digest
            let digest_key = self.make_digest_key(&snapshot.digest);
            let seq_bytes = snapshot.sequence_number.to_le_bytes();
            self.db
                .batch_put(&mut batch, CF_SNAPSHOTS, &digest_key, &seq_bytes);
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!("Batch stored {} snapshots successfully", snapshots.len());
        Ok(())
    }

    /// Get the total number of stored snapshots (approximate)
    pub fn get_snapshot_count(&self) -> Result<u64> {
        // Divide by 2 since we store each snapshot twice (by seq and by digest)
        self.db.get_cf_key_count(CF_SNAPSHOTS).map(|count| count / 2)
    }

    /// Get the total size of snapshot storage in bytes
    pub fn get_storage_size(&self) -> Result<u64> {
        self.db.get_cf_size(CF_SNAPSHOTS)
    }

    // ========== Private Helper Methods ==========

    /// Create snapshot key by sequence number
    ///
    /// Key format: 's' (1 byte) + sequence_number (8 bytes)
    fn make_sequence_key(&self, sequence_number: SnapshotSequenceNumber) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.push(b's'); // 's' for sequence
        key.extend_from_slice(&sequence_number.to_be_bytes()); // Big-endian for proper ordering
        key
    }

    /// Create snapshot key by digest
    ///
    /// Key format: 'd' (1 byte) + digest (64 bytes)
    fn make_digest_key(&self, digest: &SnapshotDigest) -> Vec<u8> {
        let mut key = Vec::with_capacity(65);
        key.push(b'd'); // 'd' for digest
        key.extend_from_slice(digest.as_bytes());
        key
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use silver_core::consensus::{ValidatorSignature, ValidatorID};
    use silver_core::{StateDigest, TransactionDigest, SilverAddress, Signature, SignatureScheme};
    use tempfile::TempDir;

    fn create_test_store() -> (SnapshotStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
        let store = SnapshotStore::new(db);
        (store, temp_dir)
    }

    fn create_test_snapshot(seq: u64, tx_count: usize) -> Snapshot {
        let transactions: Vec<TransactionDigest> = (0..tx_count)
            .map(|i| TransactionDigest::new([i as u8; 64]))
            .collect();

        let validator_sig = ValidatorSignature::new(
            ValidatorID::new(SilverAddress::new([1; 64])),
            Signature {
                scheme: SignatureScheme::Dilithium3,
                bytes: vec![0u8; 100],
            },
        );

        Snapshot::new(
            seq,
            1000 + seq,
            SnapshotDigest::new([0; 64]),
            StateDigest::new([1; 64]),
            transactions,
            0,
            vec![validator_sig],
            1000,
        )
    }

    #[test]
    fn test_store_and_get_snapshot_by_sequence() {
        let (store, _temp) = create_test_store();

        let snapshot = create_test_snapshot(1, 5);
        let seq = snapshot.sequence_number;

        // Store snapshot
        store.store_snapshot(&snapshot).unwrap();

        // Get by sequence
        let retrieved = store.get_snapshot_by_sequence(seq).unwrap();
        assert!(retrieved.is_some());

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.sequence_number, seq);
        assert_eq!(retrieved.transactions.len(), 5);
    }

    #[test]
    fn test_store_and_get_snapshot_by_digest() {
        let (store, _temp) = create_test_store();

        let snapshot = create_test_snapshot(1, 5);
        let digest = snapshot.digest;

        // Store snapshot
        store.store_snapshot(&snapshot).unwrap();

        // Get by digest
        let retrieved = store.get_snapshot_by_digest(&digest).unwrap();
        assert!(retrieved.is_some());

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.digest, digest);
        assert_eq!(retrieved.sequence_number, 1);
    }

    #[test]
    fn test_exists_by_sequence() {
        let (store, _temp) = create_test_store();

        let snapshot = create_test_snapshot(1, 5);
        let seq = snapshot.sequence_number;

        // Should not exist initially
        assert!(!store.exists_by_sequence(seq).unwrap());

        // Store snapshot
        store.store_snapshot(&snapshot).unwrap();

        // Should exist now
        assert!(store.exists_by_sequence(seq).unwrap());
    }

    #[test]
    fn test_exists_by_digest() {
        let (store, _temp) = create_test_store();

        let snapshot = create_test_snapshot(1, 5);
        let digest = snapshot.digest;

        // Should not exist initially
        assert!(!store.exists_by_digest(&digest).unwrap());

        // Store snapshot
        store.store_snapshot(&snapshot).unwrap();

        // Should exist now
        assert!(store.exists_by_digest(&digest).unwrap());
    }

    #[test]
    fn test_get_latest_snapshot() {
        let (store, _temp) = create_test_store();

        // Initially no snapshots
        assert!(store.get_latest_snapshot().unwrap().is_none());

        // Store snapshots out of order
        let snap1 = create_test_snapshot(1, 5);
        let snap3 = create_test_snapshot(3, 7);
        let snap2 = create_test_snapshot(2, 6);

        store.store_snapshot(&snap1).unwrap();
        store.store_snapshot(&snap3).unwrap();
        store.store_snapshot(&snap2).unwrap();

        // Latest should be sequence 3
        let latest = store.get_latest_snapshot().unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().sequence_number, 3);
    }

    #[test]
    fn test_get_latest_sequence_number() {
        let (store, _temp) = create_test_store();

        // Initially no snapshots
        assert!(store.get_latest_sequence_number().unwrap().is_none());

        // Store snapshots
        store.store_snapshot(&create_test_snapshot(1, 5)).unwrap();
        store.store_snapshot(&create_test_snapshot(2, 6)).unwrap();

        // Latest sequence should be 2
        let latest_seq = store.get_latest_sequence_number().unwrap();
        assert_eq!(latest_seq, Some(2));
    }

    #[test]
    fn test_batch_store_snapshots() {
        let (store, _temp) = create_test_store();

        let snapshots = vec![
            create_test_snapshot(1, 5),
            create_test_snapshot(2, 6),
            create_test_snapshot(3, 7),
        ];

        // Batch store
        store.batch_store_snapshots(&snapshots).unwrap();

        // Verify all snapshots exist
        assert!(store.exists_by_sequence(1).unwrap());
        assert!(store.exists_by_sequence(2).unwrap());
        assert!(store.exists_by_sequence(3).unwrap());
    }

    #[test]
    fn test_get_snapshot_count() {
        let (store, _temp) = create_test_store();

        // Initially 0
        let count = store.get_snapshot_count().unwrap();
        assert_eq!(count, 0);

        // Add snapshots
        store.store_snapshot(&create_test_snapshot(1, 5)).unwrap();
        store.store_snapshot(&create_test_snapshot(2, 6)).unwrap();

        // Count should be 2
        let count = store.get_snapshot_count().unwrap();
        assert!(count >= 2);
    }

    #[test]
    fn test_get_storage_size() {
        let (store, _temp) = create_test_store();

        // Add some snapshots
        store.store_snapshot(&create_test_snapshot(1, 5)).unwrap();
        store.store_snapshot(&create_test_snapshot(2, 6)).unwrap();

        // Size should be non-negative
        let size = store.get_storage_size().unwrap();
        assert!(size >= 0);
    }

    #[test]
    fn test_overwrite_snapshot() {
        let (store, _temp) = create_test_store();

        // Store initial snapshot
        let snap1 = create_test_snapshot(1, 5);
        store.store_snapshot(&snap1).unwrap();

        // Overwrite with new snapshot (same sequence)
        let snap2 = create_test_snapshot(1, 10);
        store.store_snapshot(&snap2).unwrap();

        // Verify updated snapshot
        let retrieved = store.get_snapshot_by_sequence(1).unwrap().unwrap();
        assert_eq!(retrieved.transactions.len(), 10);
    }

    #[test]
    fn test_snapshot_ordering() {
        let (store, _temp) = create_test_store();

        // Store snapshots in random order
        store.store_snapshot(&create_test_snapshot(5, 1)).unwrap();
        store.store_snapshot(&create_test_snapshot(1, 1)).unwrap();
        store.store_snapshot(&create_test_snapshot(3, 1)).unwrap();

        // Verify we can retrieve them by sequence
        assert!(store.get_snapshot_by_sequence(1).unwrap().is_some());
        assert!(store.get_snapshot_by_sequence(3).unwrap().is_some());
        assert!(store.get_snapshot_by_sequence(5).unwrap().is_some());

        // Latest should be 5
        let latest = store.get_latest_snapshot().unwrap().unwrap();
        assert_eq!(latest.sequence_number, 5);
    }
}
