//! Snapshot persistence manager
//!
//! This module provides production-ready snapshot persistence with:
//! - Atomic finalized snapshot persistence with certificates
//! - Snapshot retrieval by sequence number
//! - Latest snapshot queries
//! - Snapshot iteration
//! - Existence checks
//! - Full durability guarantees

use super::db::CF_SNAPSHOTS;
use super::errors::{PersistenceError, PersistenceResult};
use super::keys::{snapshot_certificate_key, snapshot_key};
use super::models::{Snapshot, SnapshotCertificate};
use super::traits::SnapshotPersistenceManager;
use crate::RocksDatabase;
use std::collections::BTreeMap;
use tracing::{debug, error, info, warn};

/// Snapshot persistence manager implementation
pub struct SnapshotPersistenceManagerImpl;

impl SnapshotPersistenceManager for SnapshotPersistenceManagerImpl {
    fn persist_snapshot(snapshot: &Snapshot, db: &RocksDatabase) -> PersistenceResult<()> {
        debug!(
            "Persisting snapshot with sequence number: {}",
            snapshot.sequence_number
        );

        // Validate snapshot before persistence
        Self::validate_snapshot(snapshot)?;

        // Serialize snapshot to JSON
        let snapshot_json = serde_json::to_vec(snapshot).map_err(|e| {
            error!("Failed to serialize snapshot: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize snapshot: {}", e))
        })?;

        // Generate key for this snapshot
        let key = snapshot_key(snapshot.sequence_number);

        // Store in database
        db.put(CF_SNAPSHOTS, &key, &snapshot_json)
            .map_err(|e| {
                error!(
                    "Failed to persist snapshot {}: {}",
                    snapshot.sequence_number, e
                );
                PersistenceError::Database(format!(
                    "Failed to persist snapshot {}: {}",
                    snapshot.sequence_number, e
                ))
            })?;

        info!(
            "Snapshot {} persisted successfully ({} bytes)",
            snapshot.sequence_number,
            snapshot_json.len()
        );

        Ok(())
    }

    fn persist_finalized_snapshot(
        snapshot: &Snapshot,
        certificate: &SnapshotCertificate,
        db: &RocksDatabase,
    ) -> PersistenceResult<()> {
        info!(
            "Persisting finalized snapshot with sequence number: {}",
            snapshot.sequence_number
        );

        // Validate snapshot and certificate
        Self::validate_snapshot(snapshot)?;
        Self::validate_certificate(certificate)?;

        // Verify certificate matches snapshot
        if certificate.sequence_number != snapshot.sequence_number {
            error!(
                "Certificate sequence {} does not match snapshot sequence {}",
                certificate.sequence_number, snapshot.sequence_number
            );
            return Err(PersistenceError::InvalidData(
                "Certificate sequence does not match snapshot sequence".to_string(),
            ));
        }

        // Verify quorum was achieved
        if !certificate.quorum_achieved {
            warn!(
                "Snapshot {} finalized without quorum",
                snapshot.sequence_number
            );
        }

        // Serialize snapshot and certificate
        let snapshot_json = serde_json::to_vec(snapshot).map_err(|e| {
            error!("Failed to serialize snapshot: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize snapshot: {}", e))
        })?;

        let certificate_json = serde_json::to_vec(certificate).map_err(|e| {
            error!("Failed to serialize certificate: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize certificate: {}", e))
        })?;

        // Generate keys
        let snapshot_key_bytes = snapshot_key(snapshot.sequence_number);
        let certificate_key_bytes = snapshot_certificate_key(snapshot.sequence_number);

        // Create atomic batch for both operations
        let mut batch = db.batch();

        db.batch_put(
            &mut batch,
            CF_SNAPSHOTS,
            &snapshot_key_bytes,
            &snapshot_json,
        )?;
        db.batch_put(
            &mut batch,
            CF_SNAPSHOTS,
            &certificate_key_bytes,
            &certificate_json,
        )?;

        // Write batch atomically
        db.write_batch(batch).map_err(|e| {
            error!(
                "Failed to write finalized snapshot batch {}: {}",
                snapshot.sequence_number, e
            );
            PersistenceError::BatchFailed(format!(
                "Failed to write finalized snapshot batch: {}",
                e
            ))
        })?;

        // Flush to disk for durability
        db.flush().map_err(|e| {
            error!("Failed to flush finalized snapshot {}: {}", snapshot.sequence_number, e);
            PersistenceError::FlushFailed(format!(
                "Failed to flush finalized snapshot: {}",
                e
            ))
        })?;

        info!(
            "Finalized snapshot {} persisted and flushed successfully (snapshot: {} bytes, certificate: {} bytes)",
            snapshot.sequence_number,
            snapshot_json.len(),
            certificate_json.len()
        );

        Ok(())
    }

    fn load_snapshot(
        sequence_number: u64,
        db: &RocksDatabase,
    ) -> PersistenceResult<Option<Snapshot>> {
        debug!("Loading snapshot with sequence number: {}", sequence_number);

        let key = snapshot_key(sequence_number);

        // Try to get snapshot from database
        let snapshot_data = db.get(CF_SNAPSHOTS, &key).map_err(|e| {
            error!("Failed to load snapshot {}: {}", sequence_number, e);
            PersistenceError::Database(format!("Failed to load snapshot {}: {}", sequence_number, e))
        })?;

        match snapshot_data {
            Some(data) => {
                // Deserialize snapshot
                let snapshot: Snapshot = serde_json::from_slice(&data).map_err(|e| {
                    error!("Failed to deserialize snapshot {}: {}", sequence_number, e);
                    PersistenceError::Serialization(format!(
                        "Failed to deserialize snapshot {}: {}",
                        sequence_number, e
                    ))
                })?;

                // Validate deserialized snapshot
                Self::validate_snapshot(&snapshot)?;

                debug!("Snapshot {} loaded successfully", sequence_number);
                Ok(Some(snapshot))
            }
            None => {
                debug!("Snapshot {} not found", sequence_number);
                Ok(None)
            }
        }
    }

    fn load_latest_snapshot(db: &RocksDatabase) -> PersistenceResult<Option<Snapshot>> {
        debug!("Loading latest snapshot");

        // Get all snapshots and find the one with highest sequence number
        let snapshots = Self::load_all_snapshots(db)?;

        if snapshots.is_empty() {
            debug!("No snapshots found");
            return Ok(None);
        }

        // Snapshots are already sorted by sequence number, so last one is latest
        let latest = snapshots.last().cloned();

        if let Some(ref snapshot) = latest {
            info!("Latest snapshot is sequence {}", snapshot.sequence_number);
        }

        Ok(latest)
    }

    fn load_all_snapshots(db: &RocksDatabase) -> PersistenceResult<Vec<Snapshot>> {
        debug!("Loading all snapshots");

        // Use iterator to get all snapshots
        let mut snapshots = BTreeMap::new();

        // Iterate through all keys in CF_SNAPSHOTS starting from beginning
        let iter = db.iter(CF_SNAPSHOTS, rocksdb::IteratorMode::Start);

        for result in iter {
            match result {
                Ok((key, value)) => {
                    // Only process snapshot keys (not certificate keys)
                    let key_str = String::from_utf8_lossy(&key);

                    if key_str.starts_with("snapshot:") && !key_str.starts_with("snapshot_cert:") {
                        // Extract sequence number from key
                        if let Some(seq_str) = key_str.strip_prefix("snapshot:") {
                            if let Ok(sequence_number) = seq_str.parse::<u64>() {
                                // Deserialize snapshot
                                match serde_json::from_slice::<Snapshot>(&value) {
                                    Ok(snapshot) => {
                                        // Validate snapshot
                                        if let Err(e) = Self::validate_snapshot(&snapshot) {
                                            warn!(
                                                "Invalid snapshot {} during load: {}",
                                                sequence_number, e
                                            );
                                            continue;
                                        }
                                        snapshots.insert(sequence_number, snapshot);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to deserialize snapshot {}: {}",
                                            sequence_number, e
                                        );
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Iterator error while loading snapshots: {}", e);
                    continue;
                }
            }
        }

        // Convert to sorted vector
        let result: Vec<Snapshot> = snapshots.into_values().collect();

        info!("Loaded {} snapshots", result.len());

        Ok(result)
    }

    fn snapshot_exists(
        sequence_number: u64,
        db: &RocksDatabase,
    ) -> PersistenceResult<bool> {
        debug!("Checking if snapshot {} exists", sequence_number);

        let key = snapshot_key(sequence_number);

        let exists = db.exists(CF_SNAPSHOTS, &key).map_err(|e| {
            error!("Failed to check snapshot existence: {}", e);
            PersistenceError::Database(format!("Failed to check snapshot existence: {}", e))
        })?;

        debug!("Snapshot {} exists: {}", sequence_number, exists);

        Ok(exists)
    }
}

impl SnapshotPersistenceManagerImpl {
    /// Validate snapshot data integrity
    fn validate_snapshot(snapshot: &Snapshot) -> PersistenceResult<()> {
        // Validate sequence number is not zero (genesis is block, not snapshot)
        if snapshot.sequence_number == 0 {
            return Err(PersistenceError::InvalidData(
                "Snapshot sequence number cannot be 0".to_string(),
            ));
        }

        // Validate timestamp is reasonable (not zero)
        if snapshot.timestamp == 0 {
            return Err(PersistenceError::InvalidData(
                "Snapshot timestamp cannot be 0".to_string(),
            ));
        }

        // Validate state root is not all zeros (should be actual hash)
        if snapshot.state_root == [0u8; 32] {
            return Err(PersistenceError::InvalidData(
                "Snapshot state root cannot be all zeros".to_string(),
            ));
        }

        // Validate signatures and stake weights match
        if snapshot.signatures.len() != snapshot.stake_weights.len() {
            return Err(PersistenceError::InvalidData(
                "Snapshot signatures and stake weights length mismatch".to_string(),
            ));
        }

        // Validate at least one signature (for finalized snapshots)
        if snapshot.signatures.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Snapshot must have at least one signature".to_string(),
            ));
        }

        // Validate transaction digests are not empty (should have at least one)
        if snapshot.transaction_digests.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Snapshot must have at least one transaction".to_string(),
            ));
        }

        Ok(())
    }

    /// Validate certificate data integrity
    fn validate_certificate(certificate: &SnapshotCertificate) -> PersistenceResult<()> {
        // Validate sequence number is not zero
        if certificate.sequence_number == 0 {
            return Err(PersistenceError::InvalidData(
                "Certificate sequence number cannot be 0".to_string(),
            ));
        }

        // Validate finalization timestamp is reasonable
        if certificate.finalized_at == 0 {
            return Err(PersistenceError::InvalidData(
                "Certificate finalization timestamp cannot be 0".to_string(),
            ));
        }

        // Validate signatures and stake weights match
        if certificate.signatures.len() != certificate.stake_weights.len() {
            return Err(PersistenceError::InvalidData(
                "Certificate signatures and stake weights length mismatch".to_string(),
            ));
        }

        // Validate at least one signature
        if certificate.signatures.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Certificate must have at least one signature".to_string(),
            ));
        }

        // Validate total stake weight is not zero
        if certificate.total_stake_weight == 0 {
            return Err(PersistenceError::InvalidData(
                "Certificate total stake weight cannot be 0".to_string(),
            ));
        }

        // Validate sum of stake weights equals total
        let sum: u128 = certificate.stake_weights.iter().sum();
        if sum != certificate.total_stake_weight {
            return Err(PersistenceError::InvalidData(
                "Certificate stake weights sum does not match total".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksDatabase;
    use tempfile::TempDir;

    fn create_test_snapshot(sequence_number: u64) -> Snapshot {
        Snapshot {
            sequence_number,
            timestamp: 1000 + sequence_number * 100,
            state_root: [1u8; 32],
            previous_snapshot_digest: if sequence_number > 1 {
                [2u8; 32]
            } else {
                [0u8; 32]
            },
            transaction_digests: vec![[3u8; 32]],
            cycle: sequence_number / 10,
            signatures: vec![super::super::models::Signature {
                validator_id: "validator_1".to_string(),
                signature: vec![1, 2, 3, 4, 5],
            }],
            stake_weights: vec![1000000],
        }
    }

    fn create_test_certificate(sequence_number: u64) -> SnapshotCertificate {
        SnapshotCertificate {
            sequence_number,
            finalized_at: 2000 + sequence_number * 100,
            signatures: vec![super::super::models::Signature {
                validator_id: "validator_1".to_string(),
                signature: vec![1, 2, 3, 4, 5],
            }],
            stake_weights: vec![1000000],
            total_stake_weight: 1000000,
            quorum_achieved: true,
        }
    }

    #[test]
    fn test_persist_snapshot() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let snapshot = create_test_snapshot(1);

        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");

        // Verify it was stored
        let loaded = SnapshotPersistenceManagerImpl::load_snapshot(1, &db)
            .expect("Failed to load snapshot");

        assert!(loaded.is_some());
        let loaded_snapshot = loaded.unwrap();
        assert_eq!(loaded_snapshot.sequence_number, 1);
        assert_eq!(loaded_snapshot.timestamp, 1100);
    }

    #[test]
    fn test_persist_finalized_snapshot() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let snapshot = create_test_snapshot(1);
        let certificate = create_test_certificate(1);

        SnapshotPersistenceManagerImpl::persist_finalized_snapshot(&snapshot, &certificate, &db)
            .expect("Failed to persist finalized snapshot");

        // Verify snapshot was stored
        let loaded_snapshot = SnapshotPersistenceManagerImpl::load_snapshot(1, &db)
            .expect("Failed to load snapshot");
        assert!(loaded_snapshot.is_some());

        // Verify certificate was stored
        let key = snapshot_certificate_key(1);
        let cert_data = db.get(CF_SNAPSHOTS, &key).expect("Failed to get certificate");
        assert!(cert_data.is_some());
    }

    #[test]
    fn test_load_snapshot_not_found() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let loaded = SnapshotPersistenceManagerImpl::load_snapshot(999, &db)
            .expect("Failed to load snapshot");

        assert!(loaded.is_none());
    }

    #[test]
    fn test_load_latest_snapshot() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist multiple snapshots
        for i in 1..=5 {
            let snapshot = create_test_snapshot(i);
            SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
                .expect("Failed to persist snapshot");
        }

        let latest = SnapshotPersistenceManagerImpl::load_latest_snapshot(&db)
            .expect("Failed to load latest snapshot");

        assert!(latest.is_some());
        assert_eq!(latest.unwrap().sequence_number, 5);
    }

    #[test]
    fn test_load_latest_snapshot_empty() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let latest = SnapshotPersistenceManagerImpl::load_latest_snapshot(&db)
            .expect("Failed to load latest snapshot");

        assert!(latest.is_none());
    }

    #[test]
    fn test_load_all_snapshots() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist multiple snapshots
        for i in 1..=5 {
            let snapshot = create_test_snapshot(i);
            SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
                .expect("Failed to persist snapshot");
        }

        let snapshots = SnapshotPersistenceManagerImpl::load_all_snapshots(&db)
            .expect("Failed to load all snapshots");

        assert_eq!(snapshots.len(), 5);

        // Verify they are in order
        for (i, snapshot) in snapshots.iter().enumerate() {
            assert_eq!(snapshot.sequence_number, (i + 1) as u64);
        }
    }

    #[test]
    fn test_load_all_snapshots_empty() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let snapshots = SnapshotPersistenceManagerImpl::load_all_snapshots(&db)
            .expect("Failed to load all snapshots");

        assert_eq!(snapshots.len(), 0);
    }

    #[test]
    fn test_snapshot_exists() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let snapshot = create_test_snapshot(1);

        assert!(!SnapshotPersistenceManagerImpl::snapshot_exists(1, &db)
            .expect("Failed to check existence"));

        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");

        assert!(SnapshotPersistenceManagerImpl::snapshot_exists(1, &db)
            .expect("Failed to check existence"));
    }

    #[test]
    fn test_validate_snapshot_invalid_sequence() {
        let mut snapshot = create_test_snapshot(1);
        snapshot.sequence_number = 0;

        let result = SnapshotPersistenceManagerImpl::validate_snapshot(&snapshot);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_snapshot_invalid_timestamp() {
        let mut snapshot = create_test_snapshot(1);
        snapshot.timestamp = 0;

        let result = SnapshotPersistenceManagerImpl::validate_snapshot(&snapshot);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_snapshot_invalid_state_root() {
        let mut snapshot = create_test_snapshot(1);
        snapshot.state_root = [0u8; 32];

        let result = SnapshotPersistenceManagerImpl::validate_snapshot(&snapshot);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_snapshot_mismatched_signatures_weights() {
        let mut snapshot = create_test_snapshot(1);
        snapshot.stake_weights.push(500000);

        let result = SnapshotPersistenceManagerImpl::validate_snapshot(&snapshot);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_snapshot_no_signatures() {
        let mut snapshot = create_test_snapshot(1);
        snapshot.signatures.clear();

        let result = SnapshotPersistenceManagerImpl::validate_snapshot(&snapshot);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_snapshot_no_transactions() {
        let mut snapshot = create_test_snapshot(1);
        snapshot.transaction_digests.clear();

        let result = SnapshotPersistenceManagerImpl::validate_snapshot(&snapshot);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_invalid_sequence() {
        let mut certificate = create_test_certificate(1);
        certificate.sequence_number = 0;

        let result = SnapshotPersistenceManagerImpl::validate_certificate(&certificate);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_invalid_timestamp() {
        let mut certificate = create_test_certificate(1);
        certificate.finalized_at = 0;

        let result = SnapshotPersistenceManagerImpl::validate_certificate(&certificate);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_mismatched_signatures_weights() {
        let mut certificate = create_test_certificate(1);
        certificate.stake_weights.push(500000);

        let result = SnapshotPersistenceManagerImpl::validate_certificate(&certificate);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_no_signatures() {
        let mut certificate = create_test_certificate(1);
        certificate.signatures.clear();

        let result = SnapshotPersistenceManagerImpl::validate_certificate(&certificate);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_zero_total_stake() {
        let mut certificate = create_test_certificate(1);
        certificate.total_stake_weight = 0;

        let result = SnapshotPersistenceManagerImpl::validate_certificate(&certificate);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_certificate_mismatched_total_stake() {
        let mut certificate = create_test_certificate(1);
        certificate.total_stake_weight = 2000000; // Doesn't match sum of weights

        let result = SnapshotPersistenceManagerImpl::validate_certificate(&certificate);
        assert!(result.is_err());
    }

    #[test]
    fn test_persist_finalized_snapshot_mismatched_sequence() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let snapshot = create_test_snapshot(1);
        let certificate = create_test_certificate(2); // Different sequence

        let result =
            SnapshotPersistenceManagerImpl::persist_finalized_snapshot(&snapshot, &certificate, &db);

        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_snapshots_ordering() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist snapshots in random order
        for i in [3, 1, 4, 2, 5] {
            let snapshot = create_test_snapshot(i);
            SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
                .expect("Failed to persist snapshot");
        }

        let snapshots = SnapshotPersistenceManagerImpl::load_all_snapshots(&db)
            .expect("Failed to load all snapshots");

        // Verify they are returned in order
        assert_eq!(snapshots.len(), 5);
        for (i, snapshot) in snapshots.iter().enumerate() {
            assert_eq!(snapshot.sequence_number, (i + 1) as u64);
        }
    }

    #[test]
    fn test_snapshot_round_trip() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let original = create_test_snapshot(42);

        SnapshotPersistenceManagerImpl::persist_snapshot(&original, &db)
            .expect("Failed to persist snapshot");

        let loaded = SnapshotPersistenceManagerImpl::load_snapshot(42, &db)
            .expect("Failed to load snapshot")
            .expect("Snapshot not found");

        assert_eq!(loaded.sequence_number, original.sequence_number);
        assert_eq!(loaded.timestamp, original.timestamp);
        assert_eq!(loaded.state_root, original.state_root);
        assert_eq!(loaded.cycle, original.cycle);
        assert_eq!(loaded.signatures.len(), original.signatures.len());
        assert_eq!(loaded.stake_weights, original.stake_weights);
    }
}
