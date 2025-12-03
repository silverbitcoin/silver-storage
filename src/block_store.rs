//! Block storage with indexing
//!
//! This module provides storage for blockchain blocks with efficient retrieval.

use crate::{
    db::{RocksDatabase, CF_BLOCKS},
    Result,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

/// Block information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    /// Block number (height)
    pub number: u64,

    /// Block hash
    pub hash: Vec<u8>,

    /// Parent block hash
    pub parent_hash: Vec<u8>,

    /// Block creation timestamp (Unix milliseconds)
    pub timestamp: u64,

    /// Transaction digests in this block
    pub transactions: Vec<Vec<u8>>,

    /// Validator who created this block
    pub validator: Vec<u8>,

    /// Gas used in this block
    pub gas_used: u64,

    /// Gas limit for this block
    pub gas_limit: u64,
}

impl Block {
    /// Create a new block
    pub fn new(
        number: u64,
        hash: Vec<u8>,
        parent_hash: Vec<u8>,
        timestamp: u64,
        transactions: Vec<Vec<u8>>,
        validator: Vec<u8>,
        gas_used: u64,
        gas_limit: u64,
    ) -> Self {
        Self {
            number,
            hash,
            parent_hash,
            timestamp,
            transactions,
            validator,
            gas_used,
            gas_limit,
        }
    }

    /// Get block size in bytes
    pub fn size_bytes(&self) -> usize {
        std::mem::size_of::<u64>()
            + self.hash.len()
            + self.parent_hash.len()
            + std::mem::size_of::<u64>()
            + self.transactions.iter().map(|t| t.len()).sum::<usize>()
            + self.validator.len()
            + std::mem::size_of::<u64>() * 2
    }
}

/// Block store for blockchain blocks
pub struct BlockStore {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
}

impl BlockStore {
    /// Create a new block store
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing BlockStore");
        Self { db }
    }

    /// Store a block
    pub fn store_block(&self, block: &Block) -> Result<()> {
        debug!("Storing block: {}", block.number);

        let block_bytes = bincode::serialize(block)?;
        let key = self.make_block_key(block.number);

        self.db.put(CF_BLOCKS, &key, &block_bytes)?;

        // Update latest block number
        self.set_latest_block_number(block.number)?;

        // Store hash index for fast lookup
        let hash_array: [u8; 64] = if block.hash.len() == 64 {
            let mut arr = [0u8; 64];
            arr.copy_from_slice(&block.hash);
            arr
        } else {
            // Hash is not 64 bytes, skip indexing
            return Ok(());
        };

        self.store_block_hash_index(&hash_array, block.number)?;

        debug!(
            "Block {} stored successfully ({} bytes)",
            block.number,
            block_bytes.len()
        );
        Ok(())
    }

    /// Get a block by number
    pub fn get_block(&self, number: u64) -> Result<Option<Block>> {
        debug!("Retrieving block: {}", number);

        let key = self.make_block_key(number);
        let block_bytes = self.db.get(CF_BLOCKS, &key)?;

        match block_bytes {
            Some(bytes) => {
                let block: Block = bincode::deserialize(&bytes)?;
                debug!("Block {} retrieved", number);
                Ok(Some(block))
            }
            None => {
                debug!("Block {} not found", number);
                Ok(None)
            }
        }
    }

    /// Get the latest block number
    pub fn get_latest_block_number(&self) -> Result<u64> {
        debug!("Retrieving latest block number");

        // Try to get the latest block number from metadata
        let metadata_key = b"latest_block_number".to_vec();

        match self.db.get(CF_BLOCKS, &metadata_key)? {
            Some(bytes) => {
                if bytes.len() == 8 {
                    let number = u64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    debug!("Latest block number: {}", number);
                    Ok(number)
                } else {
                    debug!("Invalid metadata format, returning 0");
                    Ok(0)
                }
            }
            None => {
                debug!("No blocks stored yet");
                Ok(0)
            }
        }
    }

    /// Set the latest block number (called after storing a block)
    pub fn set_latest_block_number(&self, number: u64) -> Result<()> {
        debug!("Setting latest block number to: {}", number);

        let metadata_key = b"latest_block_number".to_vec();
        let number_bytes = number.to_le_bytes().to_vec();

        self.db.put(CF_BLOCKS, &metadata_key, &number_bytes)?;

        debug!("Latest block number set to: {}", number);
        Ok(())
    }

    /// Get block by hash
    pub fn get_block_by_hash(&self, hash: &[u8; 64]) -> Result<Option<Block>> {
        debug!("Retrieving block by hash");

        // Create hash index key
        let hash_key = {
            let mut key = b"hash:".to_vec();
            key.extend_from_slice(hash);
            key
        };

        match self.db.get(CF_BLOCKS, &hash_key)? {
            Some(bytes) => {
                let block_number = u64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);

                // Retrieve the actual block
                self.get_block(block_number)
            }
            None => {
                debug!("Block with hash not found");
                Ok(None)
            }
        }
    }

    /// Store hash index for a block
    pub fn store_block_hash_index(&self, hash: &[u8; 64], block_number: u64) -> Result<()> {
        debug!("Storing hash index for block {}", block_number);

        let hash_key = {
            let mut key = b"hash:".to_vec();
            key.extend_from_slice(hash);
            key
        };

        let number_bytes = block_number.to_le_bytes().to_vec();
        self.db.put(CF_BLOCKS, &hash_key, &number_bytes)?;

        Ok(())
    }

    /// Batch store multiple blocks
    pub fn batch_store_blocks(&self, blocks: &[Block]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        info!("Batch storing {} blocks", blocks.len());

        let mut batch = self.db.batch();

        for block in blocks {
            let block_bytes = bincode::serialize(block)?;
            let key = self.make_block_key(block.number);

            self.db.batch_put(&mut batch, CF_BLOCKS, &key, &block_bytes)?;
        }

        self.db.write_batch(batch)?;

        info!("Batch stored {} blocks successfully", blocks.len());
        Ok(())
    }

    // ========== Private Helper Methods ==========

    /// Create block storage key
    fn make_block_key(&self, number: u64) -> Vec<u8> {
        number.to_le_bytes().to_vec()
    }
}
