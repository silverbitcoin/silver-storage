//! Integration tests for MiningStore with real ParityDB backend
//! Tests the production-grade implementation of mining reward tracking

use silver_storage::db::ParityDatabase;
use silver_storage::mining_store::{MiningStore, MiningRewardRecord, DifficultyRecord, MinerStats};
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test database
fn create_test_db() -> (TempDir, Arc<ParityDatabase>) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let db = ParityDatabase::new(temp_dir.path()).expect("Failed to create database");
    (temp_dir, Arc::new(db))
}

#[test]
fn test_store_and_retrieve_mining_reward() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];
    let record = MiningRewardRecord::new(
        0,                    // chain_id
        100,                  // block_height
        miner_address.clone(),
        50_000_000_000,       // base_reward
        1_000_000,            // transaction_fees
        1000,                 // timestamp
    );

    // Store the reward
    store.store_reward(&record).expect("Failed to store reward");

    // Retrieve the reward
    let retrieved = store
        .get_reward(0, 100, &miner_address)
        .expect("Failed to retrieve reward");

    assert!(retrieved.is_some());
    let retrieved_record = retrieved.unwrap();
    assert_eq!(retrieved_record.chain_id, 0);
    assert_eq!(retrieved_record.block_height, 100);
    assert_eq!(retrieved_record.total_reward, 50_001_000_000);
}

#[test]
fn test_store_and_retrieve_difficulty() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let record = DifficultyRecord::new(
        0,                    // chain_id
        1_000_000,            // difficulty
        500_000,              // previous_difficulty
        100,                  // block_height
        1000,                 // timestamp
        2.0,                  // adjustment_factor
    );

    // Store the difficulty
    store.store_difficulty(&record).expect("Failed to store difficulty");

    // Retrieve the latest difficulty
    let retrieved = store
        .get_latest_difficulty(0)
        .expect("Failed to retrieve difficulty");

    assert!(retrieved.is_some());
    let retrieved_record = retrieved.unwrap();
    assert_eq!(retrieved_record.chain_id, 0);
    assert_eq!(retrieved_record.difficulty, 1_000_000);
    assert_eq!(retrieved_record.adjustment_factor, 2.0);
}

#[test]
fn test_store_and_retrieve_miner_stats() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];
    let mut stats = MinerStats::new(miner_address.clone());
    stats.blocks_mined = 10;
    stats.total_rewards = 500_000_000_000;

    // Store the stats
    store.store_miner_stats(&stats).expect("Failed to store stats");

    // Retrieve the stats
    let retrieved = store
        .get_miner_stats(&miner_address)
        .expect("Failed to retrieve stats");

    assert!(retrieved.is_some());
    let retrieved_stats = retrieved.unwrap();
    assert_eq!(retrieved_stats.blocks_mined, 10);
    assert_eq!(retrieved_stats.total_rewards, 500_000_000_000);
}

#[test]
fn test_update_miner_stats_after_block() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];
    let reward = 50_001_000_000u128;

    // Update stats for first block
    store
        .update_miner_stats_after_block(&miner_address, reward)
        .expect("Failed to update stats");

    let stats = store
        .get_miner_stats(&miner_address)
        .expect("Failed to retrieve stats")
        .expect("Stats should exist");

    assert_eq!(stats.blocks_mined, 1);
    assert_eq!(stats.total_rewards, reward);

    // Update stats for second block
    store
        .update_miner_stats_after_block(&miner_address, reward)
        .expect("Failed to update stats");

    let stats = store
        .get_miner_stats(&miner_address)
        .expect("Failed to retrieve stats")
        .expect("Stats should exist");

    assert_eq!(stats.blocks_mined, 2);
    assert_eq!(stats.total_rewards, reward * 2);
}

#[test]
fn test_get_total_blocks_by_miner() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];

    // Initially should be 0
    let blocks = store
        .get_total_blocks_by_miner(&miner_address)
        .expect("Failed to get total blocks");
    assert_eq!(blocks, 0);

    // Update stats
    store
        .update_miner_stats_after_block(&miner_address, 50_001_000_000)
        .expect("Failed to update stats");

    let blocks = store
        .get_total_blocks_by_miner(&miner_address)
        .expect("Failed to get total blocks");
    assert_eq!(blocks, 1);
}

#[test]
fn test_get_total_rewards_for_chain_empty() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    // For a new chain with no rewards, should return 0
    let total = store
        .get_total_rewards_for_chain(0)
        .expect("Failed to get total rewards");
    assert_eq!(total, 0);
}

#[test]
fn test_get_total_rewards_for_chain_with_rewards() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];
    let chain_id = 0u32;

    // Store multiple rewards for the same chain
    for block_height in 0..5 {
        let record = MiningRewardRecord::new(
            chain_id,
            block_height,
            miner_address.clone(),
            50_000_000_000,
            1_000_000,
            1000 + block_height as u64,
        );
        store.store_reward(&record).expect("Failed to store reward");
    }

    // Get total rewards - this tests the real database query implementation
    let total = store
        .get_total_rewards_for_chain(chain_id)
        .expect("Failed to get total rewards");

    // Each reward is 50_001_000_000, and we have 5 rewards
    // Expected: 5 * 50_001_000_000 = 250_005_000_000
    assert_eq!(total, 250_005_000_000);
}

#[test]
fn test_get_total_rewards_for_different_chains() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];

    // Store rewards for chain 0
    for block_height in 0..3 {
        let record = MiningRewardRecord::new(
            0,
            block_height,
            miner_address.clone(),
            50_000_000_000,
            1_000_000,
            1000,
        );
        store.store_reward(&record).expect("Failed to store reward");
    }

    // Store rewards for chain 1
    for block_height in 0..2 {
        let record = MiningRewardRecord::new(
            1,
            block_height,
            miner_address.clone(),
            100_000_000_000,
            2_000_000,
            1000,
        );
        store.store_reward(&record).expect("Failed to store reward");
    }

    // Get total rewards for chain 0
    let total_chain_0 = store
        .get_total_rewards_for_chain(0)
        .expect("Failed to get total rewards for chain 0");
    assert_eq!(total_chain_0, 3 * 50_001_000_000);

    // Get total rewards for chain 1
    let total_chain_1 = store
        .get_total_rewards_for_chain(1)
        .expect("Failed to get total rewards for chain 1");
    assert_eq!(total_chain_1, 2 * 100_002_000_000);
}

#[test]
fn test_mining_store_clone() {
    let (_temp_dir, db) = create_test_db();
    let store1 = MiningStore::new(db);
    let store2 = store1.clone();

    let miner_address = vec![1u8; 20];
    let record = MiningRewardRecord::new(
        0,
        100,
        miner_address.clone(),
        50_000_000_000,
        1_000_000,
        1000,
    );

    // Store with store1
    store1.store_reward(&record).expect("Failed to store reward");

    // Retrieve with store2 (should work because they share the same database)
    let retrieved = store2
        .get_reward(0, 100, &miner_address)
        .expect("Failed to retrieve reward");

    assert!(retrieved.is_some());
}

#[test]
fn test_difficulty_at_specific_height() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    // Store difficulties at different heights
    for height in 0..5 {
        let record = DifficultyRecord::new(
            0,
            1_000_000 + (height as u64 * 100_000),
            1_000_000 + ((height - 1) as u64 * 100_000),
            height,
            1000 + height as u64,
            1.1,
        );
        store.store_difficulty(&record).expect("Failed to store difficulty");
    }

    // Retrieve difficulty at specific height
    let retrieved = store
        .get_difficulty_at_height(0, 2)
        .expect("Failed to retrieve difficulty");

    assert!(retrieved.is_some());
    let record = retrieved.unwrap();
    assert_eq!(record.block_height, 2);
    assert_eq!(record.difficulty, 1_200_000);
}

#[test]
fn test_large_reward_values() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];
    let large_reward = u128::MAX / 2; // Very large reward value

    let record = MiningRewardRecord::new(
        0,
        100,
        miner_address.clone(),
        large_reward,
        large_reward,
        1000,
    );

    store.store_reward(&record).expect("Failed to store reward");

    let retrieved = store
        .get_reward(0, 100, &miner_address)
        .expect("Failed to retrieve reward");

    assert!(retrieved.is_some());
    let retrieved_record = retrieved.unwrap();
    // Should use saturating_add to prevent overflow
    assert_eq!(retrieved_record.total_reward, large_reward.saturating_add(large_reward));
}

#[test]
fn test_reward_saturation_on_overflow() {
    let (_temp_dir, db) = create_test_db();
    let store = MiningStore::new(db);

    let miner_address = vec![1u8; 20];

    // Store a reward with values that would overflow if not handled
    let record = MiningRewardRecord::new(
        0,
        100,
        miner_address.clone(),
        u128::MAX - 1000,
        2000,
        1000,
    );

    store.store_reward(&record).expect("Failed to store reward");

    let retrieved = store
        .get_reward(0, 100, &miner_address)
        .expect("Failed to retrieve reward");

    assert!(retrieved.is_some());
    let retrieved_record = retrieved.unwrap();
    // Should saturate at u128::MAX due to saturating_add in MiningRewardRecord::new
    assert_eq!(retrieved_record.total_reward, u128::MAX);
}
