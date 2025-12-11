//! ParityDB Integration Demo
//!
//! This example demonstrates the production-ready ParityDB integration
//! for SilverBitcoin blockchain storage.

use silver_storage::{
    ParityDatabase, BlockStore, TransactionStore, SnapshotStore, ValidatorStore,
    ObjectStore, Block, StoredTransaction, TransactionEffects, ExecutionStatus,
};
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SilverBitcoin ParityDB Integration Demo\n");

    // Create temporary directory for database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("demo_db");
    std::fs::create_dir_all(&db_path)?;

    println!("📁 Database Path: {:?}\n", db_path);

    // Initialize ParityDB
    println!("🔧 Initializing ParityDB...");
    let db = Arc::new(ParityDatabase::open(&db_path)?);
    println!("✅ ParityDB initialized successfully\n");

    // Verify database access
    println!("🔍 Verifying database access...");
    db.verify_read_write_access()?;
    println!("✅ Read/write access verified\n");

    // Initialize storage layers
    println!("📦 Initializing storage layers...");
    let block_store = Arc::new(BlockStore::new(Arc::clone(&db)));
    let transaction_store = Arc::new(TransactionStore::new(Arc::clone(&db)));
    let snapshot_store = Arc::new(SnapshotStore::new(Arc::clone(&db)));
    let validator_store = Arc::new(ValidatorStore::new(Arc::clone(&db)));
    let object_store = Arc::new(ObjectStore::new(Arc::clone(&db)));
    println!("✅ All storage layers initialized\n");

    // Test Block Storage
    println!("📝 Testing Block Storage...");
    let genesis_block = Block::new(
        0,
        [0u8; 64],
        [0u8; 64],
        1000,
        vec![],
        vec![0u8; 20], // validator address
        0, // gas_used
        0, // gas_limit
    );
    block_store.store_block(&genesis_block)?;
    println!("  ✓ Stored genesis block #0");

    let retrieved_block = block_store.get_block(0)?;
    assert!(retrieved_block.is_some(), "Block not found!");
    println!("  ✓ Retrieved genesis block #0");

    let latest = block_store.get_latest_block()?;
    assert!(latest.is_some(), "Latest block not found!");
    println!("  ✓ Retrieved latest block\n");

    // Test Transaction Storage
    println!("📝 Testing Transaction Storage...");
    let tx_digest = [1u8; 64];
    let stored_tx = StoredTransaction::new(
        silver_core::TransactionDigest::new(tx_digest),
        vec![1, 2, 3, 4, 5],
        TransactionEffects {
            status: ExecutionStatus::Success,
            gas_used: 1000,
            gas_refunded: 100,
        },
        2000,
    );
    transaction_store.store_transaction(&stored_tx)?;
    println!("  ✓ Stored transaction");

    let retrieved_tx = transaction_store.get_transaction(&silver_core::TransactionDigest::new(tx_digest))?;
    assert!(retrieved_tx.is_some(), "Transaction not found!");
    println!("  ✓ Retrieved transaction\n");

    // Test Snapshot Storage
    println!("📝 Testing Snapshot Storage...");
    let snapshot = silver_storage::snapshot_store::Snapshot::new(
        0,
        vec![0u8; 32],
        3000,
        vec![],
        vec![],
    );
    snapshot_store.store_snapshot(&snapshot)?;
    println!("  ✓ Stored snapshot #0");

    let retrieved_snapshot = snapshot_store.get_snapshot(0)?;
    assert!(retrieved_snapshot.is_some(), "Snapshot not found!");
    println!("  ✓ Retrieved snapshot #0");

    let latest_snapshot = snapshot_store.get_latest_snapshot()?;
    assert!(latest_snapshot.is_some(), "Latest snapshot not found!");
    println!("  ✓ Retrieved latest snapshot\n");

    // Test Database Statistics
    println!("📊 Database Statistics:");
    let stats = db.get_stats()?;
    println!("  Path: {:?}", stats.path);
    println!("  Columns: {}", stats.column_count);
    println!("  Total Reads: {}", stats.total_reads);
    println!("  Total Writes: {}", stats.total_writes);
    println!("  Total Deletes: {}", stats.total_deletes);
    println!("  Bytes Written: {}\n", stats.total_bytes_written);

    // Test Verification Methods
    println!("🔐 Testing Verification Methods...");
    db.verify_column_family("blocks")?;
    println!("  ✓ Column family verification passed");

    db.check_corruption_markers()?;
    println!("  ✓ Corruption check passed");

    db.verify_key_value_consistency()?;
    println!("  ✓ Key-value consistency check passed\n");

    println!("✨ All tests passed successfully!");
    println!("🎉 ParityDB integration is production-ready!\n");

    Ok(())
}
