//! Snapshot storage for blockchain state with ParityDB backend

use crate::db::{ParityDatabase, CF_SNAPSHOTS};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

/// Snapshot of blockchain state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot sequence number
    pub sequence_number: u64,
    /// State root hash (as Vec for serialization)
    pub state_root: Vec<u8>,
    /// Timestamp (Unix milliseconds)
    pub timestamp: u64,
    /// Validator set at this snapshot
    pub validator_set: Vec<Vec<u8>>,
    /// Previous snapshot digest (as Vec for serialization)
    pub previous_snapshot_digest: Vec<u8>,
}

impl Snapshot {
    /// Create a new snapshot
    pub fn new(
        sequence_number: u64,
        state_root: Vec<u8>,
        timestamp: u64,
        validator_set: Vec<Vec<u8>>,
        previous_snapshot_digest: Vec<u8>,
    ) -> Self {
        Self {
            sequence_number,
            state_root,
            timestamp,
            validator_set,
            previous_snapshot_digest,
        }
    }
}

/// Snapshot store with ParityDB backend
///
/// Provides persistent storage for blockchain snapshots with:
/// - Efficient snapshot lookups by sequence number
/// - Atomic snapshot writes
/// - Chain integrity verification
/// - Compression support
pub struct SnapshotStore {
    db: Arc<ParityDatabase>,
}

impl SnapshotStore {
    /// Create new snapshot store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing SnapshotStore with ParityDB backend");
        Self { db }
    }

    /// Store a snapshot persistently using bincode 2.0 API
    pub fn store_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        debug!("Storing snapshot #{}", snapshot.sequence_number);
        
        // Serialize snapshot using bincode 2.0 API
        let snapshot_data = serde_json::to_vec(snapshot)
            .map_err(crate::error::Error::Serialization)?;
        
        // Store by sequence number as key
        let key = format!("snapshot:{}", snapshot.sequence_number).into_bytes();
        self.db.put(CF_SNAPSHOTS, &key, &snapshot_data)?;
        
        // Also store latest snapshot pointer
        let latest_key = b"snapshot:latest".to_vec();
        self.db.put(CF_SNAPSHOTS, &latest_key, &snapshot_data)?;
        
        Ok(())
    }

    /// Get snapshot by sequence number using bincode 2.0 API
    pub fn get_snapshot(&self, sequence_number: u64) -> Result<Option<Snapshot>> {
        debug!("Retrieving snapshot #{}", sequence_number);
        
        let key = format!("snapshot:{}", sequence_number).into_bytes();
        
        match self.db.get(CF_SNAPSHOTS, &key)? {
            Some(data) => {
                let snapshot = serde_json::from_slice::<Snapshot>(&data)
                    .map_err(crate::error::Error::Serialization)?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    /// Get latest snapshot using bincode 2.0 API
    pub fn get_latest_snapshot(&self) -> Result<Option<Snapshot>> {
        debug!("Retrieving latest snapshot");
        
        let key = b"snapshot:latest".to_vec();
        
        match self.db.get(CF_SNAPSHOTS, &key)? {
            Some(data) => {
                let snapshot = serde_json::from_slice::<Snapshot>(&data)
                    .map_err(crate::error::Error::Serialization)?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    /// Check if snapshot exists
    pub fn snapshot_exists(&self, sequence_number: u64) -> Result<bool> {
        let key = format!("snapshot:{}", sequence_number).into_bytes();
        self.db.exists(CF_SNAPSHOTS, &key)
    }

    /// Delete a snapshot
    pub fn delete_snapshot(&self, sequence_number: u64) -> Result<()> {
        debug!("Deleting snapshot #{}", sequence_number);
        
        let key = format!("snapshot:{}", sequence_number).into_bytes();
        self.db.delete(CF_SNAPSHOTS, &key)?;
        
        Ok(())
    }

    /// Verify snapshot chain integrity
    pub fn verify_chain_integrity(&self) -> Result<bool> {
        debug!("Verifying snapshot chain integrity");
        
        // Get latest snapshot
        if let Some(latest) = self.get_latest_snapshot()? {
            // Verify chain by walking backwards
            let mut current_seq = latest.sequence_number;
            
            while current_seq > 0 {
                if let Some(snapshot) = self.get_snapshot(current_seq)? {
                    if current_seq > 0 {
                        if let Some(prev) = self.get_snapshot(current_seq - 1)? {
                            // Verify previous digest matches
                            if snapshot.previous_snapshot_digest != prev.state_root {
                                return Ok(false);
                            }
                        }
                    }
                    current_seq -= 1;
                } else {
                    // Gap in sequence
                    return Ok(false);
                }
            }
            
            Ok(true)
        } else {
            Ok(true) // Empty chain is valid
        }
    }

    /// Prune old snapshots keeping only recent ones
    pub fn prune_old_snapshots(&self, keep_count: usize) -> Result<u64> {
        debug!("Pruning old snapshots, keeping {}", keep_count);
        
        if let Some(latest) = self.get_latest_snapshot()? {
            let latest_seq = latest.sequence_number;
            let mut removed_count = 0u64;
            
            // Delete snapshots older than keep_count
            if latest_seq > keep_count as u64 {
                let cutoff = latest_seq - keep_count as u64;
                for seq in 0..cutoff {
                    if self.snapshot_exists(seq)? {
                        self.delete_snapshot(seq)?;
                        removed_count += 1;
                    }
                }
            }
            
            Ok(removed_count)
        } else {
            Ok(0)
        }
    }
}

impl Clone for SnapshotStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}
