//! Block storage with ParityDB backend

use crate::db::{ParityDatabase, CF_BLOCKS};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use silver_core::TransactionDigest;
use std::sync::Arc;
use tracing::{debug, info};

/// Block data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    /// Block number (sequence number)
    pub number: u64,
    /// Block hash (256-bit) - stored as Vec for serialization
    #[serde(with = "serde_arrays")]
    pub hash: [u8; 64],
    /// Parent block hash - stored as Vec for serialization
    #[serde(with = "serde_arrays")]
    pub parent_hash: [u8; 64],
    /// Timestamp (Unix milliseconds)
    pub timestamp: u64,
    /// Transactions in block
    pub transactions: Vec<TransactionDigest>,
    /// Validator address (20 bytes for Ethereum compatibility)
    pub validator: Vec<u8>,
    /// Gas used in block
    pub gas_used: u64,
    /// Gas limit for block
    pub gas_limit: u64,
}

// Helper module for serializing fixed-size arrays
mod serde_arrays {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(data: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(data)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<u8> = serde::de::Deserialize::deserialize(deserializer)?;
        let mut array = [0u8; 64];
        if vec.len() != 64 {
            return Err(serde::de::Error::custom("Invalid array length"));
        }
        array.copy_from_slice(&vec);
        Ok(array)
    }
}

impl Block {
    /// Create a new block
    pub fn new(
        number: u64,
        hash: [u8; 64],
        parent_hash: [u8; 64],
        timestamp: u64,
        transactions: Vec<TransactionDigest>,
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
}

/// Block store with ParityDB backend
///
/// Provides persistent storage for blockchain blocks with:
/// - Efficient block lookups by number
/// - Atomic block writes
/// - Compression support
pub struct BlockStore {
    db: Arc<ParityDatabase>,
}

impl BlockStore {
    /// Create new block store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing BlockStore with ParityDB backend");
        Self { db }
    }

    /// Store a block persistently
    pub fn store_block(&self, block: &Block) -> Result<()> {
        debug!("Storing block #{} with hash {:?}", block.number, block.hash);
        
        // Serialize block
        let block_data = bincode::serialize(block)?;
        
        // Store by block number as key
        let key = format!("block:{}", block.number).into_bytes();
        self.db.put(CF_BLOCKS, &key, &block_data)?;
        
        // Also store latest block pointer
        let latest_key = b"block:latest".to_vec();
        self.db.put(CF_BLOCKS, &latest_key, &block_data)?;
        
        Ok(())
    }

    /// Get block by number
    pub fn get_block(&self, number: u64) -> Result<Option<Block>> {
        debug!("Retrieving block #{}", number);
        
        let key = format!("block:{}", number).into_bytes();
        
        match self.db.get(CF_BLOCKS, &key)? {
            Some(data) => {
                let block = bincode::deserialize(&data)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }

    /// Get latest block
    pub fn get_latest_block(&self) -> Result<Option<Block>> {
        debug!("Retrieving latest block");
        
        let key = b"block:latest".to_vec();
        
        match self.db.get(CF_BLOCKS, &key)? {
            Some(data) => {
                let block = bincode::deserialize(&data)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }

    /// Get blocks by hash
    pub fn get_blocks_by_hash(&self, hash: &str) -> Result<Vec<Block>> {
        debug!("Retrieving blocks by hash: {}", hash);
        
        // Validate hash format (should be 64 hex characters for SHA256)
        if hash.len() != 64 {
            return Ok(Vec::new());
        }
        
        // Convert hex string to bytes
        let hash_bytes = match hex::decode(hash) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(Vec::new()),
        };
        
        if hash_bytes.len() != 32 {
            return Ok(Vec::new());
        }
        
        // Query by hash index in ParityDB
        let hash_index_key = format!("block_hash_index:{}", hash).into_bytes();
        
        match self.db.get(CF_BLOCKS, &hash_index_key)? {
            Some(block_number_bytes) => {
                // Decode block number from index
                if block_number_bytes.len() != 8 {
                    return Ok(Vec::new());
                }
                
                let block_number = u64::from_le_bytes([
                    block_number_bytes[0], block_number_bytes[1],
                    block_number_bytes[2], block_number_bytes[3],
                    block_number_bytes[4], block_number_bytes[5],
                    block_number_bytes[6], block_number_bytes[7],
                ]);
                
                // Retrieve the actual block
                match self.get_block(block_number)? {
                    Some(block) => Ok(vec![block]),
                    None => Ok(Vec::new()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    /// Check if block exists
    pub fn block_exists(&self, number: u64) -> Result<bool> {
        let key = format!("block:{}", number).into_bytes();
        self.db.exists(CF_BLOCKS, &key)
    }

    /// Get block by hash
    pub fn get_block_by_hash(&self, hash: &[u8; 64]) -> Result<Option<Block>> {
        debug!("Retrieving block by hash");
        
        let key = format!("block:hash:{}", hex::encode(&hash[..])).into_bytes();
        
        match self.db.get(CF_BLOCKS, &key)? {
            Some(data) => {
                let block = bincode::deserialize(&data)?;
                Ok(Some(block))
            }
            None => Ok(None),
        }
    }

    /// Get latest block number
    pub fn get_latest_block_number(&self) -> Result<u64> {
        debug!("Retrieving latest block number");
        
        if let Some(block) = self.get_latest_block()? {
            Ok(block.number)
        } else {
            Ok(0)
        }
    }
}

impl Clone for BlockStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}
