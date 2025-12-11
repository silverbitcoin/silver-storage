//! Validator state persistence with ParityDB backend

use crate::db::{ParityDatabase, CF_OBJECTS};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Staking record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedStakingRecord {
    /// Validator address
    pub validator: [u8; 32],
    /// Staked amount
    pub amount: u128,
    /// Staking timestamp
    pub timestamp: u64,
}

/// Delegation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDelegationRecord {
    /// Delegator address
    pub delegator: [u8; 32],
    /// Validator address
    pub validator: [u8; 32],
    /// Delegated amount
    pub amount: u128,
    /// Delegation timestamp
    pub timestamp: u64,
}

/// Reward record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedRewardRecord {
    /// Validator address
    pub validator: [u8; 32],
    /// Reward amount
    pub amount: u128,
    /// Reward timestamp
    pub timestamp: u64,
}

/// Validator set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedValidatorSet {
    /// Validators
    pub validators: Vec<[u8; 32]>,
    /// Epoch number
    pub epoch: u64,
    /// Timestamp
    pub timestamp: u64,
}

/// Validator store with ParityDB backend
///
/// Provides persistent storage for validator state including:
/// - Staking records
/// - Delegation records
/// - Reward records
/// - Validator sets
pub struct ValidatorStore {
    db: Arc<ParityDatabase>,
}

impl ValidatorStore {
    /// Create new validator store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing ValidatorStore with ParityDB backend");
        Self { db }
    }

    /// Record staking persistently
    pub fn record_staking(&self, record: &PersistedStakingRecord) -> Result<()> {
        debug!("Recording staking for validator: {:?}", record.validator);
        
        let data = bincode::serialize(record)?;
        let key = format!("staking:{}:{}", hex::encode(&record.validator[..]), record.timestamp).into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        Ok(())
    }

    /// Record delegation persistently
    pub fn record_delegation(&self, record: &PersistedDelegationRecord) -> Result<()> {
        debug!("Recording delegation from {:?} to {:?}", record.delegator, record.validator);
        
        let data = bincode::serialize(record)?;
        let key = format!("delegation:{}:{}:{}", hex::encode(&record.delegator[..]), hex::encode(&record.validator[..]), record.timestamp).into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        Ok(())
    }

    /// Record reward persistently
    pub fn record_reward(&self, record: &PersistedRewardRecord) -> Result<()> {
        debug!("Recording reward for validator: {:?}", record.validator);
        
        let data = bincode::serialize(record)?;
        let key = format!("reward:{}:{}", hex::encode(&record.validator[..]), record.timestamp).into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        Ok(())
    }

    /// Set validator set persistently
    pub fn set_validator_set(&self, validator_set: &PersistedValidatorSet) -> Result<()> {
        debug!("Setting validator set for epoch {}", validator_set.epoch);
        
        let data = bincode::serialize(validator_set)?;
        let key = format!("validator_set:{}", validator_set.epoch).into_bytes();
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        // Also store as latest
        let latest_key = b"validator_set:latest".to_vec();
        self.db.put(CF_OBJECTS, &latest_key, &data)?;
        
        Ok(())
    }

    /// Get current validator set
    pub fn get_current_validator_set(&self) -> Result<Option<PersistedValidatorSet>> {
        debug!("Retrieving current validator set");
        
        let key = b"validator_set:latest".to_vec();
        
        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let validator_set = bincode::deserialize(&data)?;
                Ok(Some(validator_set))
            }
            None => Ok(None),
        }
    }

    /// Get validator set by epoch
    pub fn get_validator_set(&self, epoch: u64) -> Result<Option<PersistedValidatorSet>> {
        debug!("Retrieving validator set for epoch {}", epoch);
        
        let key = format!("validator_set:{}", epoch).into_bytes();
        
        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let validator_set = bincode::deserialize(&data)?;
                Ok(Some(validator_set))
            }
            None => Ok(None),
        }
    }

    /// Calculate total stake for validator (self-stake + delegations)
    pub fn get_total_stake(&self, validator: &[u8; 32]) -> Result<u128> {
        debug!("Calculating total stake for validator: {:?}", validator);
        
        // Get self-stake
        let self_stake_key = format!("validator_stake:{}", hex::encode(&validator[..])).into_bytes();
        let self_stake = match self.db.get(crate::db::CF_OBJECTS, &self_stake_key)? {
            Some(data) => {
                let record: PersistedStakingRecord = bincode::deserialize(&data)?;
                record.amount
            }
            None => 0,
        };
        
        // Get delegations from persistent storage
        let mut total_delegations = 0u128;
        
        // Query all delegation records for this validator
        // Delegations are stored with key format: "delegation:{validator_hex}:{delegator_hex}"
        let validator_hex = hex::encode(&validator[..]);
        
        // Since ParityDB doesn't have prefix iteration in the current API,
        // we maintain a separate index of delegations per validator
        // Query the delegation index for this validator
        let delegation_index_key = format!("delegation_index:{}", validator_hex).into_bytes();
        
        if let Ok(Some(delegations_data)) = self.db.get(CF_OBJECTS, &delegation_index_key) {
            // Deserialize the list of delegator addresses
            if let Ok(delegators) = bincode::deserialize::<Vec<[u8; 32]>>(&delegations_data) {
                for delegator in delegators {
                    let delegation_key = format!(
                        "delegation:{}:{}",
                        validator_hex,
                        hex::encode(&delegator[..])
                    ).into_bytes();
                    
                    if let Ok(Some(record_data)) = self.db.get(CF_OBJECTS, &delegation_key) {
                        if let Ok(record) = bincode::deserialize::<PersistedDelegationRecord>(&record_data) {
                            total_delegations = total_delegations.saturating_add(record.amount);
                        }
                    }
                }
            }
        }
        
        Ok(self_stake.saturating_add(total_delegations))
    }

    /// Calculate total rewards for validator
    pub fn get_total_rewards(&self, validator: &[u8; 32]) -> Result<u128> {
        debug!("Calculating total rewards for validator: {:?}", validator);
        
        let mut total_rewards = 0u128;
        let validator_hex = hex::encode(&validator[..]);
        let reward_index_key = format!("reward_index:{}", validator_hex).into_bytes();
        
        // Query the reward index for all rewards for this validator
        if let Ok(Some(index_data)) = self.db.get(crate::db::CF_OBJECTS, &reward_index_key) {
            if let Ok(reward_ids) = bincode::deserialize::<Vec<String>>(&index_data) {
                for reward_id in reward_ids {
                    let key = format!("reward:{}:{}", validator_hex, reward_id).into_bytes();
                    if let Ok(Some(data)) = self.db.get(crate::db::CF_OBJECTS, &key) {
                        if let Ok(record) = bincode::deserialize::<PersistedRewardRecord>(&data) {
                            total_rewards = total_rewards.saturating_add(record.amount);
                        }
                    }
                }
            }
        }
        
        debug!("Total rewards for validator {:?}: {}", validator, total_rewards);
        Ok(total_rewards)
    }

    /// Get all validators with their total stakes
    pub fn get_all_validators_with_stakes(&self) -> Result<HashMap<[u8; 32], u128>> {
        debug!("Retrieving all validators with stakes");
        
        let mut validators_stakes = HashMap::new();
        let validator_index_key = b"validator_index:all".to_vec();
        
        // Query the validator index for all validators
        if let Ok(Some(index_data)) = self.db.get(crate::db::CF_OBJECTS, &validator_index_key) {
            if let Ok(validator_addrs) = bincode::deserialize::<Vec<[u8; 32]>>(&index_data) {
                for validator in validator_addrs {
                    if let Ok(Some(stake)) = self.get_validator_stake(&validator) {
                        validators_stakes.insert(validator, stake);
                    }
                }
            }
        }
        
        debug!("Retrieved {} validators with stakes", validators_stakes.len());
        Ok(validators_stakes)
    }

    /// Get validator stake
    pub fn get_validator_stake(&self, validator: &[u8; 32]) -> Result<Option<u128>> {
        debug!("Getting validator stake: {:?}", validator);
        
        let key = format!("validator_stake:{}", hex::encode(&validator[..])).into_bytes();
        match self.db.get(crate::db::CF_OBJECTS, &key)? {
            Some(data) => {
                let record: PersistedStakingRecord = bincode::deserialize(&data)?;
                Ok(Some(record.amount))
            }
            None => Ok(None),
        }
    }

    /// Set validator stake
    pub fn set_validator_stake(&self, validator: &[u8; 32], amount: u128) -> Result<()> {
        debug!("Setting validator stake: {:?} = {}", validator, amount);
        
        let record = PersistedStakingRecord {
            validator: *validator,
            amount,
            timestamp: chrono::Local::now().timestamp() as u64,
        };
        
        let data = bincode::serialize(&record)?;
        let key = format!("validator_stake:{}", hex::encode(&validator[..])).into_bytes();
        self.db.put(crate::db::CF_OBJECTS, &key, &data)?;
        
        Ok(())
    }

    /// Get delegations for validator
    pub fn get_delegations_for_validator(&self, validator: &[u8; 32]) -> Result<Vec<PersistedDelegationRecord>> {
        debug!("Getting delegations for validator: {:?}", validator);
        
        let mut delegations = Vec::new();
        let validator_hex = hex::encode(&validator[..]);
        let validator_delegation_index_key = format!("delegation_index:validator:{}", validator_hex).into_bytes();
        
        // Query the delegation index for all delegations to this validator
        if let Ok(Some(index_data)) = self.db.get(crate::db::CF_OBJECTS, &validator_delegation_index_key) {
            if let Ok(delegator_addrs) = bincode::deserialize::<Vec<[u8; 32]>>(&index_data) {
                for delegator in delegator_addrs {
                    let key = format!(
                        "delegation:{}:{}",
                        validator_hex,
                        hex::encode(&delegator[..])
                    ).into_bytes();
                    if let Ok(Some(data)) = self.db.get(crate::db::CF_OBJECTS, &key) {
                        if let Ok(record) = bincode::deserialize::<PersistedDelegationRecord>(&data) {
                            delegations.push(record);
                        }
                    }
                }
            }
        }
        
        debug!("Retrieved {} delegations for validator {:?}", delegations.len(), validator);
        Ok(delegations)
    }

    /// Get delegations by delegator
    pub fn get_delegations_by_delegator(&self, delegator: &[u8; 32]) -> Result<Vec<PersistedDelegationRecord>> {
        debug!("Getting delegations by delegator: {:?}", delegator);
        
        let mut delegations = Vec::new();
        let delegator_hex = hex::encode(&delegator[..]);
        let delegator_delegation_index_key = format!("delegation_index:delegator:{}", delegator_hex).into_bytes();
        
        // Query the delegator index for all delegations from this delegator
        if let Ok(Some(index_data)) = self.db.get(crate::db::CF_OBJECTS, &delegator_delegation_index_key) {
            if let Ok(validator_addrs) = bincode::deserialize::<Vec<[u8; 32]>>(&index_data) {
                for validator in validator_addrs {
                    let key = format!(
                        "delegation:{}:{}",
                        hex::encode(&validator[..]),
                        delegator_hex
                    ).into_bytes();
                    if let Ok(Some(data)) = self.db.get(crate::db::CF_OBJECTS, &key) {
                        if let Ok(record) = bincode::deserialize::<PersistedDelegationRecord>(&data) {
                            delegations.push(record);
                        }
                    }
                }
            }
        }
        
        debug!("Retrieved {} delegations from delegator {:?}", delegations.len(), delegator);
        Ok(delegations)
    }
}

impl Clone for ValidatorStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}
