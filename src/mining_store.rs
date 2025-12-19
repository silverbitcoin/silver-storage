//! Mining state persistence with ParityDB backend
//! Tracks mining difficulty, rewards, and miner statistics

use crate::db::{ParityDatabase, CF_OBJECTS};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

/// Mining difficulty record per chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DifficultyRecord {
    /// Chain ID
    pub chain_id: u32,
    /// Current difficulty
    pub difficulty: u64,
    /// Previous difficulty
    pub previous_difficulty: u64,
    /// Block height when adjusted
    pub block_height: u64,
    /// Adjustment timestamp
    pub timestamp: u64,
    /// Adjustment factor
    pub adjustment_factor: f64,
}

impl DifficultyRecord {
    /// Creates a new difficulty record with the specified parameters
    pub fn new(
        chain_id: u32,
        difficulty: u64,
        previous_difficulty: u64,
        block_height: u64,
        timestamp: u64,
        adjustment_factor: f64,
    ) -> Self {
        Self {
            chain_id,
            difficulty,
            previous_difficulty,
            block_height,
            timestamp,
            adjustment_factor,
        }
    }
}

/// Mining reward record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiningRewardRecord {
    /// Chain ID
    pub chain_id: u32,
    /// Block height
    pub block_height: u64,
    /// Miner address
    pub miner_address: Vec<u8>,
    /// Base reward (in satoshis)
    pub base_reward: u128,
    /// Transaction fees
    pub transaction_fees: u128,
    /// Total reward
    pub total_reward: u128,
    /// Reward timestamp
    pub timestamp: u64,
}

impl MiningRewardRecord {
    /// Creates a new mining reward record with the specified parameters
    pub fn new(
        chain_id: u32,
        block_height: u64,
        miner_address: Vec<u8>,
        base_reward: u128,
        transaction_fees: u128,
        timestamp: u64,
    ) -> Self {
        let total_reward = base_reward + transaction_fees;
        Self {
            chain_id,
            block_height,
            miner_address,
            base_reward,
            transaction_fees,
            total_reward,
            timestamp,
        }
    }
}

/// Miner statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinerStats {
    /// Miner address
    pub miner_address: Vec<u8>,
    /// Total blocks mined
    pub blocks_mined: u64,
    /// Total rewards earned (in satoshis)
    pub total_rewards: u128,
    /// Average block time (seconds)
    pub average_block_time: f64,
    /// Last block timestamp
    pub last_block_timestamp: u64,
    /// First block timestamp
    pub first_block_timestamp: u64,
}

impl MinerStats {
    /// Creates a new miner statistics record for the specified miner address
    pub fn new(miner_address: Vec<u8>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            miner_address,
            blocks_mined: 0,
            total_rewards: 0,
            average_block_time: 0.0,
            last_block_timestamp: now,
            first_block_timestamp: now,
        }
    }
}

/// Mining store with ParityDB backend
pub struct MiningStore {
    db: Arc<ParityDatabase>,
}

impl MiningStore {
    /// Create new mining store
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing MiningStore with ParityDB backend");
        Self { db }
    }

    /// Store difficulty record
    pub fn store_difficulty(&self, record: &DifficultyRecord) -> Result<()> {
        debug!(
            "Storing difficulty record for chain {} at height {}",
            record.chain_id, record.block_height
        );

        let data = bincode::serialize(record)?;
        let key = format!("difficulty:{}:{}", record.chain_id, record.block_height).into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;

        // Also store latest difficulty for chain
        let latest_key = format!("difficulty:latest:{}", record.chain_id).into_bytes();
        self.db.put(CF_OBJECTS, &latest_key, &data)?;

        Ok(())
    }

    /// Get latest difficulty for chain
    pub fn get_latest_difficulty(&self, chain_id: u32) -> Result<Option<DifficultyRecord>> {
        debug!("Retrieving latest difficulty for chain {}", chain_id);

        let key = format!("difficulty:latest:{}", chain_id).into_bytes();

        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let record = bincode::deserialize(&data)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Get difficulty at specific height
    pub fn get_difficulty_at_height(
        &self,
        chain_id: u32,
        block_height: u64,
    ) -> Result<Option<DifficultyRecord>> {
        debug!(
            "Retrieving difficulty for chain {} at height {}",
            chain_id, block_height
        );

        let key = format!("difficulty:{}:{}", chain_id, block_height).into_bytes();

        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let record = bincode::deserialize(&data)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Store mining reward
    pub fn store_reward(&self, record: &MiningRewardRecord) -> Result<()> {
        debug!(
            "Storing mining reward for chain {} at height {}",
            record.chain_id, record.block_height
        );

        let data = bincode::serialize(record)?;
        let key = format!(
            "reward:{}:{}:{}",
            record.chain_id, record.block_height, hex::encode(&record.miner_address)
        )
        .into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;

        Ok(())
    }

    /// Get reward for block
    pub fn get_reward(
        &self,
        chain_id: u32,
        block_height: u64,
        miner_address: &[u8],
    ) -> Result<Option<MiningRewardRecord>> {
        debug!(
            "Retrieving reward for chain {} at height {}",
            chain_id, block_height
        );

        let key = format!(
            "reward:{}:{}:{}",
            chain_id,
            block_height,
            hex::encode(miner_address)
        )
        .into_bytes();

        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let record = bincode::deserialize(&data)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Store miner statistics
    pub fn store_miner_stats(&self, stats: &MinerStats) -> Result<()> {
        debug!("Storing miner stats for {}", hex::encode(&stats.miner_address));

        let data = bincode::serialize(stats)?;
        let key = format!("miner_stats:{}", hex::encode(&stats.miner_address)).into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;

        Ok(())
    }

    /// Get miner statistics
    pub fn get_miner_stats(&self, miner_address: &[u8]) -> Result<Option<MinerStats>> {
        debug!("Retrieving miner stats for {}", hex::encode(miner_address));

        let key = format!("miner_stats:{}", hex::encode(miner_address)).into_bytes();

        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let stats = bincode::deserialize(&data)?;
                Ok(Some(stats))
            }
            None => Ok(None),
        }
    }

    /// Update miner statistics after block
    pub fn update_miner_stats_after_block(
        &self,
        miner_address: &[u8],
        reward: u128,
    ) -> Result<()> {
        debug!("Updating miner stats after block for {}", hex::encode(miner_address));

        let mut stats = self
            .get_miner_stats(miner_address)?
            .unwrap_or_else(|| MinerStats::new(miner_address.to_vec()));

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        stats.blocks_mined += 1;
        stats.total_rewards += reward;

        if stats.blocks_mined > 1 {
            let time_diff = now - stats.last_block_timestamp;
            stats.average_block_time =
                (stats.average_block_time * (stats.blocks_mined - 1) as f64 + time_diff as f64)
                    / stats.blocks_mined as f64;
        }

        stats.last_block_timestamp = now;

        self.store_miner_stats(&stats)?;
        Ok(())
    }

    /// Get total mining rewards for chain
    pub fn get_total_rewards_for_chain(&self, chain_id: u32) -> Result<u128> {
        debug!("Calculating total rewards for chain {}", chain_id);

        // This would require iterating through all rewards for the chain
        // For now, return 0 as a placeholder for the iteration logic
        // In production, this would use a proper index or aggregation
        Ok(0)
    }

    /// Get total blocks mined by miner
    pub fn get_total_blocks_by_miner(&self, miner_address: &[u8]) -> Result<u64> {
        debug!("Getting total blocks mined by {}", hex::encode(miner_address));

        match self.get_miner_stats(miner_address)? {
            Some(stats) => Ok(stats.blocks_mined),
            None => Ok(0),
        }
    }
}

impl Clone for MiningStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_difficulty_record_creation() {
        let record = DifficultyRecord::new(0, 1_000_000, 500_000, 100, 1000, 2.0);
        assert_eq!(record.chain_id, 0);
        assert_eq!(record.difficulty, 1_000_000);
        assert_eq!(record.adjustment_factor, 2.0);
    }

    #[test]
    fn test_mining_reward_record_creation() {
        let record = MiningRewardRecord::new(
            0,
            100,
            vec![1u8; 20],
            50_000_000_000,
            1_000_000,
            1000,
        );
        assert_eq!(record.chain_id, 0);
        assert_eq!(record.block_height, 100);
        assert_eq!(record.total_reward, 50_001_000_000);
    }

    #[test]
    fn test_miner_stats_creation() {
        let stats = MinerStats::new(vec![1u8; 20]);
        assert_eq!(stats.blocks_mined, 0);
        assert_eq!(stats.total_rewards, 0);
    }

    #[test]
    fn test_miner_stats_update() {
        let mut stats = MinerStats::new(vec![1u8; 20]);
        stats.blocks_mined = 5;
        stats.total_rewards = 250_000_000_000;
        assert_eq!(stats.blocks_mined, 5);
    }
}
