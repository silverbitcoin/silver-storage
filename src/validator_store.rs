//! Validator state persistence layer
//!
//! This module provides production-ready persistence for:
//! - Validator set state
//! - Staking records
//! - Delegation records
//! - Reward history
//! - Validator set change events
//!
//! All data is persisted to RocksDB with:
//! - Atomic writes
//! - Compression
//! - Bloom filters
//! - Full recovery support

use crate::{Error, RocksDatabase};
use silver_core::{Error as CoreError, Result as CoreResult, ValidatorID};
use silver_consensus::{
    StakingManager, DelegationManager, ValidatorSet, ValidatorSetChangeEvent,
    SnapshotManager, SnapshotCertificate,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, error, info, warn};

/// Validator store for persistence
pub struct ValidatorStore {
    /// RocksDB database
    db: RocksDatabase,
    
    /// Column family for validator set state
    cf_validator_set: String,
    
    /// Column family for staking records
    cf_staking: String,
    
    /// Column family for delegation records
    cf_delegation: String,
    
    /// Column family for reward history
    cf_rewards: String,
    
    /// Column family for validator set changes
    cf_changes: String,
}

/// Persisted validator set state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedValidatorSet {
    /// Current cycle
    pub cycle: u64,
    
    /// Total validators
    pub validator_count: usize,
    
    /// Total stake
    pub total_stake: u64,
    
    /// Timestamp
    pub timestamp: u64,
}

/// Persisted staking record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedStakingRecord {
    /// Validator ID
    pub validator_id: String,
    
    /// Active stake
    pub active_stake: u64,
    
    /// Unbonding stake
    pub unbonding_stake: u64,
    
    /// Total stake
    pub total_stake: u64,
    
    /// Current tier
    pub tier: String,
    
    /// Timestamp
    pub timestamp: u64,
}

/// Persisted delegation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDelegationRecord {
    /// Delegator address
    pub delegator: String,
    
    /// Validator ID
    pub validator_id: String,
    
    /// Delegated amount
    pub amount: u64,
    
    /// Accumulated rewards
    pub accumulated_rewards: u64,
    
    /// Timestamp
    pub timestamp: u64,
}

/// Persisted reward record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedRewardRecord {
    /// Validator ID
    pub validator_id: String,
    
    /// Cycle
    pub cycle: u64,
    
    /// Reward amount
    pub reward_amount: u64,
    
    /// Stake weight
    pub stake_weight: f64,
    
    /// Participation rate
    pub participation_rate: f64,
    
    /// Timestamp
    pub timestamp: u64,
}

impl ValidatorStore {
    /// Open or create validator store
    pub fn open<P: AsRef<Path>>(path: P) -> CoreResult<Self> {
        let db = RocksDatabase::open(&path).map_err(|e| {
            CoreError::InvalidData(format!("Failed to open validator store: {}", e))
        })?;

        info!("Opened validator store at: {}", path.as_ref().display());

        Ok(Self {
            db,
            cf_validator_set: "validator_set".to_string(),
            cf_staking: "staking".to_string(),
            cf_delegation: "delegation".to_string(),
            cf_rewards: "rewards".to_string(),
            cf_changes: "changes".to_string(),
        })
    }

    /// Save validator set state
    pub fn save_validator_set(
        &self,
        cycle: u64,
        validator_count: usize,
        total_stake: u64,
    ) -> CoreResult<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = PersistedValidatorSet {
            cycle,
            validator_count,
            total_stake,
            timestamp,
        };

        let key = format!("validator_set:{}", cycle);
        let value = serde_json::to_vec(&state).map_err(|e| {
            CoreError::InvalidData(format!("Failed to serialize validator set: {}", e))
        })?;

        self.db.put(&self.cf_validator_set, key.as_bytes(), &value)
            .map_err(|e| CoreError::InvalidData(format!("Failed to save validator set: {}", e)))?;

        debug!("Saved validator set for cycle {} (count: {}, stake: {})", cycle, validator_count, total_stake);

        Ok(())
    }

    /// Load validator set state for cycle
    pub fn load_validator_set(&self, cycle: u64) -> CoreResult<Option<PersistedValidatorSet>> {
        let key = format!("validator_set:{}", cycle);
        
        match self.db.get(&self.cf_validator_set, key.as_bytes()) {
            Ok(Some(value)) => {
                let state = serde_json::from_slice(&value).map_err(|e| {
                    CoreError::InvalidData(format!("Failed to deserialize validator set: {}", e))
                })?;
                Ok(Some(state))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CoreError::InvalidData(format!("Failed to load validator set: {}", e))),
        }
    }

    /// Get latest validator set state
    pub fn get_latest_validator_set(&self) -> CoreResult<Option<PersistedValidatorSet>> {
        // Iterate from end to find latest
        let iter = self.db.iter(&self.cf_validator_set, rocksdb::IteratorMode::End);
        
        for (key, value) in iter {
            if let Ok(state) = serde_json::from_slice::<PersistedValidatorSet>(&value) {
                return Ok(Some(state));
            }
        }

        Ok(None)
    }

    /// Save staking record
    pub fn save_staking_record(
        &self,
        validator_id: &ValidatorID,
        active_stake: u64,
        unbonding_stake: u64,
        total_stake: u64,
        tier: &str,
    ) -> CoreResult<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let record = PersistedStakingRecord {
            validator_id: validator_id.to_string(),
            active_stake,
            unbonding_stake,
            total_stake,
            tier: tier.to_string(),
            timestamp,
        };

        let key = format!("staking:{}:{}", validator_id, timestamp);
        let value = serde_json::to_vec(&record).map_err(|e| {
            CoreError::InvalidData(format!("Failed to serialize staking record: {}", e))
        })?;

        self.db.put(&self.cf_staking, key.as_bytes(), &value)
            .map_err(|e| CoreError::InvalidData(format!("Failed to save staking record: {}", e)))?;

        debug!("Saved staking record for validator {} (active: {}, total: {})", validator_id, active_stake, total_stake);

        Ok(())
    }

    /// Get staking history for validator
    pub fn get_staking_history(&self, validator_id: &ValidatorID, limit: usize) -> CoreResult<Vec<PersistedStakingRecord>> {
        let prefix = format!("staking:{}:", validator_id);
        let mut records = Vec::new();

        let iter = self.db.iter(&self.cf_staking, rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Reverse));

        for (key, value) in iter {
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                break;
            }

            if let Ok(record) = serde_json::from_slice::<PersistedStakingRecord>(&value) {
                records.push(record);
                if records.len() >= limit {
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Save delegation record
    pub fn save_delegation_record(
        &self,
        delegator: &str,
        validator_id: &ValidatorID,
        amount: u64,
        accumulated_rewards: u64,
    ) -> CoreResult<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let record = PersistedDelegationRecord {
            delegator: delegator.to_string(),
            validator_id: validator_id.to_string(),
            amount,
            accumulated_rewards,
            timestamp,
        };

        let key = format!("delegation:{}:{}:{}", delegator, validator_id, timestamp);
        let value = serde_json::to_vec(&record).map_err(|e| {
            CoreError::InvalidData(format!("Failed to serialize delegation record: {}", e))
        })?;

        self.db.put(&self.cf_delegation, key.as_bytes(), &value)
            .map_err(|e| CoreError::InvalidData(format!("Failed to save delegation record: {}", e)))?;

        debug!("Saved delegation record: {} -> {} (amount: {})", delegator, validator_id, amount);

        Ok(())
    }

    /// Get delegation history for delegator
    pub fn get_delegation_history(&self, delegator: &str, limit: usize) -> CoreResult<Vec<PersistedDelegationRecord>> {
        let prefix = format!("delegation:{}:", delegator);
        let mut records = Vec::new();

        let iter = self.db.iter(&self.cf_delegation, rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Reverse));

        for (key, value) in iter {
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                break;
            }

            if let Ok(record) = serde_json::from_slice::<PersistedDelegationRecord>(&value) {
                records.push(record);
                if records.len() >= limit {
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Save reward record
    pub fn save_reward_record(
        &self,
        validator_id: &ValidatorID,
        cycle: u64,
        reward_amount: u64,
        stake_weight: f64,
        participation_rate: f64,
    ) -> CoreResult<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let record = PersistedRewardRecord {
            validator_id: validator_id.to_string(),
            cycle,
            reward_amount,
            stake_weight,
            participation_rate,
            timestamp,
        };

        let key = format!("rewards:{}:{}", validator_id, cycle);
        let value = serde_json::to_vec(&record).map_err(|e| {
            CoreError::InvalidData(format!("Failed to serialize reward record: {}", e))
        })?;

        self.db.put(&self.cf_rewards, key.as_bytes(), &value)
            .map_err(|e| CoreError::InvalidData(format!("Failed to save reward record: {}", e)))?;

        debug!("Saved reward record for validator {} cycle {} (amount: {})", validator_id, cycle, reward_amount);

        Ok(())
    }

    /// Get reward history for validator
    pub fn get_reward_history(&self, validator_id: &ValidatorID, limit: usize) -> CoreResult<Vec<PersistedRewardRecord>> {
        let prefix = format!("rewards:{}:", validator_id);
        let mut records = Vec::new();

        let iter = self.db.iter(&self.cf_rewards, rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Reverse));

        for (key, value) in iter {
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                break;
            }

            if let Ok(record) = serde_json::from_slice::<PersistedRewardRecord>(&value) {
                records.push(record);
                if records.len() >= limit {
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Save validator set change event
    pub fn save_change_event(&self, event: &ValidatorSetChangeEvent) -> CoreResult<()> {
        let key = format!("changes:{}:{}", event.cycle, event.timestamp);
        let value = serde_json::to_vec(event).map_err(|e| {
            CoreError::InvalidData(format!("Failed to serialize change event: {}", e))
        })?;

        self.db.put(&self.cf_changes, key.as_bytes(), &value)
            .map_err(|e| CoreError::InvalidData(format!("Failed to save change event: {}", e)))?;

        debug!("Saved change event for cycle {} (added: {}, removed: {})", 
            event.cycle, event.added.len(), event.removed.len());

        Ok(())
    }

    /// Get change events for cycle
    pub fn get_cycle_changes(&self, cycle: u64) -> CoreResult<Vec<ValidatorSetChangeEvent>> {
        let prefix = format!("changes:{}:", cycle);
        let mut events = Vec::new();

        let iter = self.db.iter(&self.cf_changes, rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward));

        for (key, value) in iter {
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with(&prefix) {
                break;
            }

            if let Ok(event) = serde_json::from_slice::<ValidatorSetChangeEvent>(&value) {
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Get all change events
    pub fn get_all_changes(&self) -> CoreResult<Vec<ValidatorSetChangeEvent>> {
        let mut events = Vec::new();

        let iter = self.db.iter(&self.cf_changes, rocksdb::IteratorMode::Start);

        for (_, value) in iter {
            if let Ok(event) = serde_json::from_slice::<ValidatorSetChangeEvent>(&value) {
                events.push(event);
            }
        }

        Ok(events)
    }

    /// Flush all pending writes to disk
    pub fn flush(&self) -> CoreResult<()> {
        self.db.flush()
            .map_err(|e| CoreError::InvalidData(format!("Failed to flush: {}", e)))
    }

    /// Compact database
    pub fn compact(&self) -> CoreResult<()> {
        self.db.compact()
            .map_err(|e| CoreError::InvalidData(format!("Failed to compact: {}", e)))
    }

    /// Get database statistics
    pub fn get_stats(&self) -> CoreResult<String> {
        self.db.get_stats()
            .map_err(|e| CoreError::InvalidData(format!("Failed to get stats: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silver_core::SilverAddress;
    use tempfile::TempDir;

    fn create_test_validator_id(id: u8) -> ValidatorID {
        ValidatorID::new(SilverAddress::new([id; 64]))
    }

    #[test]
    fn test_validator_store_creation() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();
        assert!(store.get_latest_validator_set().is_ok());
    }

    #[test]
    fn test_save_and_load_validator_set() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();

        store.save_validator_set(0, 3, 300_000_000).unwrap();
        
        let loaded = store.load_validator_set(0).unwrap();
        assert!(loaded.is_some());
        
        let state = loaded.unwrap();
        assert_eq!(state.cycle, 0);
        assert_eq!(state.validator_count, 3);
        assert_eq!(state.total_stake, 300_000_000);
    }

    #[test]
    fn test_staking_records() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();

        let validator_id = create_test_validator_id(1);
        
        store.save_staking_record(&validator_id, 50_000, 0, 50_000, "Silver").unwrap();
        store.save_staking_record(&validator_id, 100_000, 0, 100_000, "Gold").unwrap();

        let history = store.get_staking_history(&validator_id, 10).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].total_stake, 100_000); // Latest first
    }

    #[test]
    fn test_delegation_records() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();

        let validator_id = create_test_validator_id(1);
        let delegator = "0x1234567890abcdef";

        store.save_delegation_record(delegator, &validator_id, 1000, 100).unwrap();
        store.save_delegation_record(delegator, &validator_id, 2000, 200).unwrap();

        let history = store.get_delegation_history(delegator, 10).unwrap();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_reward_records() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();

        let validator_id = create_test_validator_id(1);

        store.save_reward_record(&validator_id, 0, 1000, 0.5, 0.95).unwrap();
        store.save_reward_record(&validator_id, 1, 1500, 0.5, 0.98).unwrap();

        let history = store.get_reward_history(&validator_id, 10).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].cycle, 1); // Latest first
    }

    #[test]
    fn test_change_events() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();

        let event = ValidatorSetChangeEvent::new(
            0,
            vec![create_test_validator_id(1)],
            vec![],
            1,
            50_000,
        );

        store.save_change_event(&event).unwrap();

        let changes = store.get_cycle_changes(0).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].added.len(), 1);
    }

    #[test]
    fn test_flush_and_compact() {
        let temp_dir = TempDir::new().unwrap();
        let store = ValidatorStore::open(temp_dir.path()).unwrap();

        store.save_validator_set(0, 3, 300_000_000).unwrap();
        
        assert!(store.flush().is_ok());
        assert!(store.compact().is_ok());
    }
}
