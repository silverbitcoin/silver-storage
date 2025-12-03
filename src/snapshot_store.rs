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
use tracing::{debug, info, warn};

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
            .batch_put(&mut batch, CF_SNAPSHOTS, &seq_key, &snapshot_bytes)?;

        // Index by digest (secondary key)
        let digest_key = self.make_digest_key(&snapshot.digest);
        // Store sequence number as value for digest lookup
        let seq_bytes = snapshot.sequence_number.to_le_bytes();
        self.db
            .batch_put(&mut batch, CF_SNAPSHOTS, &digest_key, &seq_bytes)?;

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

        // First, try to get the latest pointer for O(1) access
        let latest_key = b"latest_snapshot";
        match self.db.get(CF_SNAPSHOTS, latest_key) {
            Ok(Some(value)) => {
                // Deserialize and return the latest snapshot
                match bincode::deserialize::<Snapshot>(&value) {
                    Ok(snapshot) => {
                        debug!("Retrieved latest snapshot: sequence={}", snapshot.sequence_number);
                        return Ok(Some(snapshot));
                    }
                    Err(e) => {
                        warn!("Failed to deserialize latest snapshot pointer: {}", e);
                        // Fall through to manual search
                    }
                }
            }
            Ok(None) => {
                // No latest pointer, fall through to manual search
            }
            Err(e) => {
                warn!("Failed to get latest snapshot pointer: {}", e);
                // Fall through to manual search
            }
        }

        // Manual search: iterate in reverse to find the highest sequence number
        let mut latest: Option<Snapshot> = None;
        let mut max_seq = 0u64;

        // Iterate over all snapshots in reverse order
        for result in self.db.iter(CF_SNAPSHOTS, rocksdb::IteratorMode::End) {
            let (key, value) = result?;

            // Check if this is a sequence key (starts with 's')
            if key.len() > 0 && key[0] == b's' && key.len() == 9 {
                let snapshot: Snapshot = bincode::deserialize(&value)?;
                if snapshot.sequence_number > max_seq {
                    max_seq = snapshot.sequence_number;
                    latest = Some(snapshot.clone());
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
                .batch_put(&mut batch, CF_SNAPSHOTS, &seq_key, &snapshot_bytes)?;

            // Index by digest
            let digest_key = self.make_digest_key(&snapshot.digest);
            let seq_bytes = snapshot.sequence_number.to_le_bytes();
            self.db
                .batch_put(&mut batch, CF_SNAPSHOTS, &digest_key, &seq_bytes)?;
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!("Batch stored {} snapshots successfully", snapshots.len());
        Ok(())
    }

    /// Get the total number of stored snapshots (approximate)
    pub fn get_snapshot_count(&self) -> Result<u64> {
        // Divide by 2 since we store each snapshot twice (by seq and by digest)
        self.db
            .get_cf_key_count(CF_SNAPSHOTS)
            .map(|count| count / 2)
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
