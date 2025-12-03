//! Integration tests for complete blockchain persistence workflows
//!
//! These tests verify the complete lifecycle of blockchain state persistence:
//! - Genesis block initialization
//! - Snapshot creation and finalization
//! - Validator state changes
//! - Transaction persistence
//! - Graceful shutdown and recovery
//! - Error scenarios and recovery

use silver_storage::persistence::*;
use silver_storage::persistence::models::{
    ValidatorTier, Signature, TransactionStatus, NetworkConfig, AccountConfig, 
    ConsensusConfig, FuelConfig, ValidatorConfig
};
use silver_storage::RocksDatabase;
use std::fs;
use tempfile::TempDir;

/// Test helper to create a temporary database
fn create_test_db() -> (TempDir, RocksDatabase) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_db");
    fs::create_dir_all(&db_path).expect("Failed to create db directory");

    let db = RocksDatabase::open(&db_path).expect("Failed to open test database");

    (temp_dir, db)
}

/// Test helper to create a valid genesis configuration
fn create_test_genesis_config() -> GenesisConfig {
    GenesisConfig {
        chain_id: "test-chain".to_string(),
        genesis_time: 1000,
        protocol_version: "1.0.0".to_string(),
        validators: vec![
            ValidatorConfig {
                address: "validator1".to_string(),
                protocol_pubkey: "pubkey1".to_string(),
                network_pubkey: "netkey1".to_string(),
                worker_pubkey: "workkey1".to_string(),
                stake_amount: 2_000_000_000_000_000, // 2M SBTC
                network_address: "127.0.0.1:8001".to_string(),
                p2p_address: "127.0.0.1:9001".to_string(),
                name: "Validator 1".to_string(),
                commission_rate: 0.05,
            },
            ValidatorConfig {
                address: "validator2".to_string(),
                protocol_pubkey: "pubkey2".to_string(),
                network_pubkey: "netkey2".to_string(),
                worker_pubkey: "workkey2".to_string(),
                stake_amount: 2_000_000_000_000_000, // 2M SBTC
                network_address: "127.0.0.1:8002".to_string(),
                p2p_address: "127.0.0.1:9002".to_string(),
                name: "Validator 2".to_string(),
                commission_rate: 0.05,
            },
        ],
        initial_supply: 100_000_000_000_000_000, // 100M SBTC
        initial_accounts: vec![AccountConfig {
            address: "account1".to_string(),
            balance: 1_000_000_000_000_000, // 1M SBTC
        }],
        consensus_config: ConsensusConfig {
            timeout_ms: 5000,
            min_validators: 2,
        },
        fuel_config: FuelConfig {
            base_cost: 100,
            max_per_block: 1_000_000,
        },
        network_config: NetworkConfig {
            name: "test-network".to_string(),
            network_id: 1,
        },
    }
}

/// Test helper to create a test snapshot
fn create_test_snapshot(sequence: u64) -> Snapshot {
    Snapshot {
        sequence_number: sequence,
        timestamp: 2000 + (sequence * 1000),
        state_root: [sequence as u8; 32],
        previous_snapshot_digest: if sequence > 0 {
            [(sequence - 1) as u8; 32]
        } else {
            [0u8; 32]
        },
        transaction_digests: vec![[sequence as u8; 32]],
        cycle: sequence,
        signatures: vec![Signature {
            validator_id: "validator1".to_string(),
            signature: vec![sequence as u8; 64],
        }],
        stake_weights: vec![2_000_000_000_000_000],
    }
}

/// Test helper to create a test transaction
fn create_test_transaction(digest_seed: u8) -> Transaction {
    Transaction {
        digest: [digest_seed; 32],
        sender: "account1".to_string(),
        recipient: "account2".to_string(),
        amount: 1_000_000_000,
        fuel_used: 500,
        status: TransactionStatus::Success,
        effects: vec![],
    }
}

#[test]
fn test_complete_workflow_genesis_to_recovery() {
    let (_temp_dir, db) = create_test_db();

    // Step 1: Create and persist genesis block
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");

    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Verify genesis block was persisted
    let loaded_genesis = GenesisBlockManagerImpl::load_genesis_block(&db)
        .expect("Failed to load genesis block")
        .expect("Genesis block not found");

    assert_eq!(loaded_genesis.number, 0);
    assert_eq!(loaded_genesis.hash, genesis_block.hash);

    // Step 2: Create and persist multiple snapshots
    let snapshot1 = create_test_snapshot(1);
    let snapshot2 = create_test_snapshot(2);
    let snapshot3 = create_test_snapshot(3);

    SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot1, &db)
        .expect("Failed to persist snapshot 1");
    SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot2, &db)
        .expect("Failed to persist snapshot 2");
    SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot3, &db)
        .expect("Failed to persist snapshot 3");

    // Step 3: Verify snapshots can be retrieved
    let loaded_snapshot1 = SnapshotPersistenceManagerImpl::load_snapshot(1, &db)
        .expect("Failed to load snapshot 1")
        .expect("Snapshot 1 not found");

    assert_eq!(loaded_snapshot1.sequence_number, 1);
    assert_eq!(loaded_snapshot1.state_root, [1u8; 32]);

    // Step 4: Persist transactions linked to snapshots
    let tx1 = create_test_transaction(1);
    let tx2 = create_test_transaction(2);

    TransactionPersistenceManagerImpl::persist_transaction(&tx1, 1, 0, &db)
        .expect("Failed to persist transaction 1");
    TransactionPersistenceManagerImpl::persist_transaction(&tx2, 2, 0, &db)
        .expect("Failed to persist transaction 2");

    // Step 5: Verify transactions can be retrieved
    let tx1_digest_hex = hex::encode(tx1.digest);
    let loaded_tx1 = TransactionPersistenceManagerImpl::load_transaction(&tx1_digest_hex, &db)
        .expect("Failed to load transaction 1")
        .expect("Transaction 1 not found");

    assert_eq!(loaded_tx1.sender, "account1");
    assert_eq!(loaded_tx1.amount, 1_000_000_000);

    // Step 6: Verify snapshot transactions
    let snapshot_txs = TransactionPersistenceManagerImpl::load_snapshot_transactions(1, &db)
        .expect("Failed to load snapshot transactions");

    assert!(!snapshot_txs.is_empty());

    // Step 7: Recovery verification
    let recovered_genesis = StateRecoveryManagerImpl::recover_genesis_block(&db)
        .expect("Failed to recover genesis block");

    assert_eq!(recovered_genesis.number, 0);

    let recovered_snapshots = StateRecoveryManagerImpl::recover_snapshots(&db)
        .expect("Failed to recover snapshots");

    assert_eq!(recovered_snapshots.len(), 3);
    assert_eq!(recovered_snapshots[0].sequence_number, 1);
    assert_eq!(recovered_snapshots[2].sequence_number, 3);
}

#[test]
fn test_multiple_snapshots_and_transactions() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create 5 snapshots with multiple transactions each
    for snapshot_seq in 1..=5 {
        let snapshot = create_test_snapshot(snapshot_seq);
        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");

        // Add 3 transactions per snapshot
        for tx_idx in 0..3 {
            let mut tx = create_test_transaction((snapshot_seq * 10 + tx_idx) as u8);
            tx.digest = [(snapshot_seq * 10 + tx_idx) as u8; 32];

            TransactionPersistenceManagerImpl::persist_transaction(&tx, snapshot_seq, 0, &db)
                .expect("Failed to persist transaction");
        }
    }

    // Verify all snapshots exist
    let all_snapshots = SnapshotPersistenceManagerImpl::load_all_snapshots(&db)
        .expect("Failed to load all snapshots");

    assert_eq!(all_snapshots.len(), 5);

    // Verify latest snapshot
    let latest = SnapshotPersistenceManagerImpl::load_latest_snapshot(&db)
        .expect("Failed to load latest snapshot")
        .expect("No latest snapshot found");

    assert_eq!(latest.sequence_number, 5);

    // Verify transactions per snapshot
    for snapshot_seq in 1..=5 {
        let txs = TransactionPersistenceManagerImpl::load_snapshot_transactions(snapshot_seq, &db)
            .expect("Failed to load snapshot transactions");

        assert_eq!(txs.len(), 3);
    }
}

#[test]
fn test_validator_state_changes_across_snapshots() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create snapshots
    for snapshot_seq in 1..=3 {
        let snapshot = create_test_snapshot(snapshot_seq);
        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");
    }

    // Persist staking records for validator1
    let staking_record1 = StakingRecord {
        validator_id: "validator1".to_string(),
        amount: 1_000_000_000_000_000,
        tier: ValidatorTier::Silver,
        timestamp: 1000,
        transaction_digest: [1u8; 32],
    };

    ValidatorStatePersistenceManagerImpl::persist_staking_record(&staking_record1, &db)
        .expect("Failed to persist staking record 1");

    // Persist tier change
    let tier_change = TierChange {
        validator_id: "validator1".to_string(),
        old_tier: ValidatorTier::Silver,
        new_tier: ValidatorTier::Gold,
        new_stake: 2_000_000_000_000_000,
        cycle: 2,
        timestamp: 2000,
    };

    ValidatorStatePersistenceManagerImpl::persist_tier_change(&tier_change, &db)
        .expect("Failed to persist tier change");

    // Persist reward record
    let reward_record = RewardRecord {
        validator_id: "validator1".to_string(),
        cycle: 2,
        amount: 100_000_000_000,
        stake_weight: 2_000_000_000_000_000,
        participation_rate: 100,
        timestamp: 2500,
    };

    ValidatorStatePersistenceManagerImpl::persist_reward_record(&reward_record, &db)
        .expect("Failed to persist reward record");

    // Recover validator state
    let validator_states = StateRecoveryManagerImpl::recover_validator_state(&db)
        .expect("Failed to recover validator state");

    // Verify validator1 state was recovered
    let validator1_state = validator_states
        .iter()
        .find(|v| v.validator_id == "validator1")
        .expect("Validator1 state not found");

    assert_eq!(validator1_state.stake, 2_000_000_000_000_000);
    assert_eq!(validator1_state.tier, ValidatorTier::Gold);
    assert!(!validator1_state.deposits.is_empty());
    assert!(!validator1_state.rewards.is_empty());
}

#[test]
fn test_graceful_shutdown_and_recovery() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create snapshots
    for snapshot_seq in 1..=3 {
        let snapshot = create_test_snapshot(snapshot_seq);
        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");
    }

    // Create shutdown checkpoint
    let checkpoint = ShutdownCheckpoint {
        latest_snapshot_sequence: 3,
        latest_block_number: 0,
        validator_count: 2,
        total_stake: 4_000_000_000_000_000,
        timestamp: 3000,
    };

    // Persist checkpoint (simulating graceful shutdown)
    let checkpoint_key = shutdown_checkpoint_key();
    let checkpoint_bytes = bincode::serialize(&checkpoint).expect("Failed to serialize checkpoint");
    db.put(CF_SNAPSHOTS, &checkpoint_key, &checkpoint_bytes)
        .expect("Failed to persist checkpoint");

    // Flush to disk
    db.flush().expect("Failed to flush database");

    // Simulate recovery
    let checkpoint_bytes = db
        .get(CF_SNAPSHOTS, &checkpoint_key)
        .expect("Failed to get checkpoint")
        .expect("Checkpoint not found");

    let recovered_checkpoint: ShutdownCheckpoint =
        bincode::deserialize(&checkpoint_bytes).expect("Failed to deserialize checkpoint");

    assert_eq!(recovered_checkpoint.latest_snapshot_sequence, 3);
    assert_eq!(recovered_checkpoint.validator_count, 2);

    // Verify all snapshots are still there
    let snapshots = SnapshotPersistenceManagerImpl::load_all_snapshots(&db)
        .expect("Failed to load snapshots");

    assert_eq!(snapshots.len(), 3);
}

#[test]
fn test_atomic_batch_operations() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create a snapshot with certificate (atomic operation)
    let snapshot = create_test_snapshot(1);
    let certificate = SnapshotCertificate {
        sequence_number: 1,
        finalized_at: 2000,
        signatures: vec![Signature {
            validator_id: "validator1".to_string(),
            signature: vec![1u8; 64],
        }],
        stake_weights: vec![2_000_000_000_000_000],
        total_stake_weight: 2_000_000_000_000_000,
        quorum_achieved: true,
    };

    // Persist finalized snapshot (atomic)
    SnapshotPersistenceManagerImpl::persist_finalized_snapshot(&snapshot, &certificate, &db)
        .expect("Failed to persist finalized snapshot");

    // Verify both snapshot and certificate were persisted
    let loaded_snapshot = SnapshotPersistenceManagerImpl::load_snapshot(1, &db)
        .expect("Failed to load snapshot")
        .expect("Snapshot not found");

    assert_eq!(loaded_snapshot.sequence_number, 1);

    // Verify certificate was persisted
    let cert_key = snapshot_certificate_key(1);
    let cert_bytes = db
        .get(CF_SNAPSHOTS, &cert_key)
        .expect("Failed to get certificate")
        .expect("Certificate not found");

    let loaded_cert: SnapshotCertificate =
        bincode::deserialize(&cert_bytes).expect("Failed to deserialize certificate");

    assert_eq!(loaded_cert.sequence_number, 1);
    assert!(loaded_cert.quorum_achieved);
}

#[test]
fn test_database_integrity_verification() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Verify database integrity
    let integrity_result = StateRecoveryManagerImpl::verify_database_integrity(&db);

    assert!(integrity_result.is_ok(), "Database integrity check failed");
}

#[test]
fn test_consistency_verification() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create snapshots
    for snapshot_seq in 1..=3 {
        let snapshot = create_test_snapshot(snapshot_seq);
        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");
    }

    // Recover all data
    let genesis = StateRecoveryManagerImpl::recover_genesis_block(&db)
        .expect("Failed to recover genesis");
    let snapshots = StateRecoveryManagerImpl::recover_snapshots(&db)
        .expect("Failed to recover snapshots");
    let validators = StateRecoveryManagerImpl::recover_validator_state(&db)
        .expect("Failed to recover validators");
    let transactions = StateRecoveryManagerImpl::recover_transactions(&db)
        .expect("Failed to recover transactions");

    // Verify consistency
    let consistency_result =
        StateRecoveryManagerImpl::verify_consistency(&genesis, &snapshots, &validators, &transactions);

    assert!(
        consistency_result.is_ok(),
        "Consistency verification failed: {:?}",
        consistency_result
    );
}

#[test]
fn test_unfinalised_snapshots_not_persisted() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create an unfinalised snapshot (no certificate)
    let snapshot = create_test_snapshot(1);
    SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
        .expect("Failed to persist snapshot");

    // Verify snapshot exists
    let loaded = SnapshotPersistenceManagerImpl::load_snapshot(1, &db)
        .expect("Failed to load snapshot")
        .expect("Snapshot not found");

    assert_eq!(loaded.sequence_number, 1);

    // Verify certificate does NOT exist
    let cert_key = snapshot_certificate_key(1);
    let cert_result = db.get(CF_SNAPSHOTS, &cert_key).expect("Failed to get certificate");

    assert!(cert_result.is_none(), "Certificate should not exist for unfinalised snapshot");
}

#[test]
fn test_transaction_snapshot_linkage() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create snapshots
    let snapshot1 = create_test_snapshot(1);
    let snapshot2 = create_test_snapshot(2);

    SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot1, &db)
        .expect("Failed to persist snapshot 1");
    SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot2, &db)
        .expect("Failed to persist snapshot 2");

    // Create transactions for each snapshot
    let tx1 = create_test_transaction(1);
    let tx2 = create_test_transaction(2);

    TransactionPersistenceManagerImpl::persist_transaction(&tx1, 1, 0, &db)
        .expect("Failed to persist tx1");
    TransactionPersistenceManagerImpl::persist_transaction(&tx2, 2, 0, &db)
        .expect("Failed to persist tx2");

    // Verify transactions are linked to correct snapshots
    let snapshot1_txs = TransactionPersistenceManagerImpl::load_snapshot_transactions(1, &db)
        .expect("Failed to load snapshot 1 transactions");

    let snapshot2_txs = TransactionPersistenceManagerImpl::load_snapshot_transactions(2, &db)
        .expect("Failed to load snapshot 2 transactions");

    assert_eq!(snapshot1_txs.len(), 1);
    assert_eq!(snapshot2_txs.len(), 1);
    assert_eq!(snapshot1_txs[0].digest, tx1.digest);
    assert_eq!(snapshot2_txs[0].digest, tx2.digest);
}

#[test]
fn test_error_recovery_scenario() {
    let (_temp_dir, db) = create_test_db();

    // Try to load genesis when none exists
    let result = GenesisBlockManagerImpl::load_genesis_block(&db)
        .expect("Failed to query database");

    assert!(result.is_none(), "Genesis block should not exist initially");

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Try to load non-existent snapshot
    let result = SnapshotPersistenceManagerImpl::load_snapshot(999, &db)
        .expect("Failed to query database");

    assert!(result.is_none(), "Non-existent snapshot should return None");

    // Try to load non-existent transaction
    let nonexistent_digest = hex::encode([255u8; 32]);
    let result = TransactionPersistenceManagerImpl::load_transaction(&nonexistent_digest, &db)
        .expect("Failed to query database");

    assert!(result.is_none(), "Non-existent transaction should return None");
}

#[test]
fn test_snapshot_sequence_ordering() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Create snapshots in random order
    let snapshots = vec![
        create_test_snapshot(3),
        create_test_snapshot(1),
        create_test_snapshot(4),
        create_test_snapshot(2),
    ];

    for snapshot in snapshots {
        SnapshotPersistenceManagerImpl::persist_snapshot(&snapshot, &db)
            .expect("Failed to persist snapshot");
    }

    // Load all snapshots
    let loaded = SnapshotPersistenceManagerImpl::load_all_snapshots(&db)
        .expect("Failed to load snapshots");

    // Verify they are in sequence order
    assert_eq!(loaded.len(), 4);
    for (idx, snapshot) in loaded.iter().enumerate() {
        assert_eq!(snapshot.sequence_number, (idx + 1) as u64);
    }
}

#[test]
fn test_validator_state_recovery_completeness() {
    let (_temp_dir, db) = create_test_db();

    // Create genesis
    let genesis_config = create_test_genesis_config();
    let genesis_block = GenesisBlockManagerImpl::create_genesis_block(&genesis_config)
        .expect("Failed to create genesis block");
    GenesisBlockManagerImpl::persist_genesis_block(&genesis_block, &db)
        .expect("Failed to persist genesis block");

    // Persist complete validator state
    let staking_record = StakingRecord {
        validator_id: "validator1".to_string(),
        amount: 1_000_000_000_000_000,
        tier: ValidatorTier::Silver,
        timestamp: 1000,
        transaction_digest: [1u8; 32],
    };

    let tier_change = TierChange {
        validator_id: "validator1".to_string(),
        old_tier: ValidatorTier::Silver,
        new_tier: ValidatorTier::Gold,
        new_stake: 2_000_000_000_000_000,
        cycle: 1,
        timestamp: 1500,
    };

    let reward = RewardRecord {
        validator_id: "validator1".to_string(),
        cycle: 1,
        amount: 50_000_000_000,
        stake_weight: 2_000_000_000_000_000,
        participation_rate: 100,
        timestamp: 2000,
    };

    let unstaking = UnstakingRequest {
        validator_id: "validator1".to_string(),
        amount: 500_000_000_000_000,
        request_timestamp: 2500,
        unbonding_completion_timestamp: 2500 + 604_800_000, // 7 days
    };

    ValidatorStatePersistenceManagerImpl::persist_staking_record(&staking_record, &db)
        .expect("Failed to persist staking record");
    ValidatorStatePersistenceManagerImpl::persist_tier_change(&tier_change, &db)
        .expect("Failed to persist tier change");
    ValidatorStatePersistenceManagerImpl::persist_reward_record(&reward, &db)
        .expect("Failed to persist reward");
    ValidatorStatePersistenceManagerImpl::persist_unstaking_request(&unstaking, &db)
        .expect("Failed to persist unstaking request");

    // Recover validator state
    let validators = StateRecoveryManagerImpl::recover_validator_state(&db)
        .expect("Failed to recover validator state");

    // Verify completeness
    assert!(!validators.is_empty());
    let validator1 = validators
        .iter()
        .find(|v| v.validator_id == "validator1")
        .expect("Validator1 not found");

    assert!(!validator1.deposits.is_empty());
    assert_eq!(validator1.tier, ValidatorTier::Gold);
    assert!(!validator1.rewards.is_empty());
    assert!(!validator1.unbonding_requests.is_empty());
}
