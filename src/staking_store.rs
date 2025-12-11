//! Staking and delegation store with ParityDB backend
//!
//! Provides persistent storage for:
//! - Staking records (validator stakes)
//! - Delegation records (delegator -> validator)
//! - Reward records (earned rewards)
//! - Unbonding records (pending unbonding)

use crate::db::{ParityDatabase, CF_OBJECTS};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Staking record for validator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakingRecord {
    /// Validator address (32 bytes)
    pub validator: [u8; 32],
    /// Staked amount in SBTC (smallest unit)
    pub amount: u128,
    /// Staking timestamp (Unix seconds)
    pub timestamp: u64,
    /// Epoch when staking started
    pub epoch: u64,
}

/// Delegation record from delegator to validator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationRecord {
    /// Delegator address (32 bytes)
    pub delegator: [u8; 32],
    /// Validator address (32 bytes)
    pub validator: [u8; 32],
    /// Delegated amount in SBTC
    pub amount: u128,
    /// Delegation timestamp (Unix seconds)
    pub timestamp: u64,
    /// Epoch when delegation started
    pub epoch: u64,
    /// Accumulated rewards
    pub accumulated_rewards: u128,
    /// Status: "active", "unbonding", "inactive"
    pub status: String,
}

/// Reward record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardRecord {
    /// Validator or delegator address
    pub recipient: [u8; 32],
    /// Reward amount in SBTC
    pub amount: u128,
    /// Reward timestamp (Unix seconds)
    pub timestamp: u64,
    /// Epoch when reward was earned
    pub epoch: u64,
    /// Reward type: "participation", "commission", "delegation"
    pub reward_type: String,
}

/// Unbonding record for pending unbonding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnbondingRecord {
    /// Delegator address
    pub delegator: [u8; 32],
    /// Validator address
    pub validator: [u8; 32],
    /// Amount being unbonded
    pub amount: u128,
    /// Unbonding start timestamp
    pub start_timestamp: u64,
    /// Unbonding completion timestamp
    pub completion_timestamp: u64,
    /// Epoch when unbonding started
    pub epoch: u64,
}

/// Staking store with ParityDB backend
pub struct StakingStore {
    db: Arc<ParityDatabase>,
}

impl StakingStore {
    /// Create new staking store
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing StakingStore with ParityDB backend");
        Self { db }
    }

    /// Record staking persistently
    pub fn record_staking(&self, record: &StakingRecord) -> Result<()> {
        debug!("Recording staking for validator: {:?}, amount: {}", 
               hex::encode(&record.validator[..16]), record.amount);
        
        let data = bincode::serialize(record)?;
        let key = format!(
            "staking:{}:{}",
            hex::encode(&record.validator[..]),
            record.timestamp
        ).into_bytes();
        
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        // Also store latest staking record for quick lookup
        let latest_key = format!("staking:latest:{}", hex::encode(&record.validator[..])).into_bytes();
        self.db.put(CF_OBJECTS, &latest_key, &data)?;
        
        info!("Staking recorded: validator={}, amount={}", 
              hex::encode(&record.validator[..16]), record.amount);
        
        Ok(())
    }

    /// Record delegation persistently
    pub fn record_delegation(&self, record: &DelegationRecord) -> Result<()> {
        debug!("Recording delegation from {:?} to {:?}, amount: {}", 
               hex::encode(&record.delegator[..16]), 
               hex::encode(&record.validator[..16]), 
               record.amount);
        
        let data = bincode::serialize(record)?;
        let key = format!(
            "delegation:{}:{}:{}",
            hex::encode(&record.delegator[..]),
            hex::encode(&record.validator[..]),
            record.timestamp
        ).into_bytes();
        
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        // Also store latest delegation record for quick lookup
        let latest_key = format!(
            "delegation:latest:{}:{}",
            hex::encode(&record.delegator[..]),
            hex::encode(&record.validator[..])
        ).into_bytes();
        self.db.put(CF_OBJECTS, &latest_key, &data)?;
        
        info!("Delegation recorded: delegator={}, validator={}, amount={}", 
              hex::encode(&record.delegator[..16]), 
              hex::encode(&record.validator[..16]), 
              record.amount);
        
        Ok(())
    }

    /// Get latest delegation for delegator and validator
    pub fn get_delegation(&self, delegator: &[u8; 32], validator: &[u8; 32]) -> Result<Option<DelegationRecord>> {
        let key = format!(
            "delegation:latest:{}:{}",
            hex::encode(delegator),
            hex::encode(validator)
        ).into_bytes();
        
        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let record: DelegationRecord = bincode::deserialize(&data)?;
                debug!("Retrieved delegation: delegator={}, validator={}, amount={}", 
                       hex::encode(&delegator[..16]), 
                       hex::encode(&validator[..16]), 
                       record.amount);
                Ok(Some(record))
            },
            None => {
                debug!("No delegation found for delegator={}, validator={}", 
                       hex::encode(&delegator[..16]), 
                       hex::encode(&validator[..16]));
                Ok(None)
            }
        }
    }

    /// Get latest staking record for validator
    pub fn get_staking(&self, validator: &[u8; 32]) -> Result<Option<StakingRecord>> {
        let key = format!("staking:latest:{}", hex::encode(validator)).into_bytes();
        
        match self.db.get(CF_OBJECTS, &key)? {
            Some(data) => {
                let record: StakingRecord = bincode::deserialize(&data)?;
                debug!("Retrieved staking: validator={}, amount={}", 
                       hex::encode(&validator[..16]), record.amount);
                Ok(Some(record))
            },
            None => {
                debug!("No staking found for validator={}", hex::encode(&validator[..16]));
                Ok(None)
            }
        }
    }

    /// Record reward persistently
    pub fn record_reward(&self, record: &RewardRecord) -> Result<()> {
        debug!("Recording reward for recipient: {:?}, amount: {}, type: {}", 
               hex::encode(&record.recipient[..16]), record.amount, record.reward_type);
        
        let data = bincode::serialize(record)?;
        let key = format!(
            "reward:{}:{}:{}",
            hex::encode(&record.recipient[..]),
            record.timestamp,
            record.reward_type
        ).into_bytes();
        
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        info!("Reward recorded: recipient={}, amount={}, type={}", 
              hex::encode(&record.recipient[..16]), record.amount, record.reward_type);
        
        Ok(())
    }

    /// Get total rewards for recipient
    pub fn get_total_rewards(&self, recipient: &[u8; 32]) -> Result<u128> {
        // In production, this would iterate through all reward records
        // For now, return 0 (would be implemented with proper indexing)
        debug!("Getting total rewards for recipient={}", hex::encode(&recipient[..16]));
        Ok(0)
    }

    /// Record unbonding
    pub fn record_unbonding(&self, record: &UnbondingRecord) -> Result<()> {
        debug!("Recording unbonding: delegator={}, validator={}, amount={}", 
               hex::encode(&record.delegator[..16]), 
               hex::encode(&record.validator[..16]), 
               record.amount);
        
        let data = bincode::serialize(record)?;
        let key = format!(
            "unbonding:{}:{}:{}",
            hex::encode(&record.delegator[..]),
            hex::encode(&record.validator[..]),
            record.start_timestamp
        ).into_bytes();
        
        self.db.put(CF_OBJECTS, &key, &data)?;
        
        info!("Unbonding recorded: delegator={}, validator={}, amount={}", 
              hex::encode(&record.delegator[..16]), 
              hex::encode(&record.validator[..16]), 
              record.amount);
        
        Ok(())
    }

    /// Get pending unbonding for delegator and validator
    pub fn get_pending_unbonding(&self, delegator: &[u8; 32], validator: &[u8; 32]) -> Result<Vec<UnbondingRecord>> {
        // In production, this would query all unbonding records
        // For now, return empty vector
        debug!("Getting pending unbonding for delegator={}, validator={}", 
               hex::encode(&delegator[..16]), 
               hex::encode(&validator[..16]));
        Ok(Vec::new())
    }

    /// Update delegation status
    pub fn update_delegation_status(
        &self,
        delegator: &[u8; 32],
        validator: &[u8; 32],
        new_status: &str,
    ) -> Result<()> {
        // Get current delegation
        if let Some(mut delegation) = self.get_delegation(delegator, validator)? {
            delegation.status = new_status.to_string();
            self.record_delegation(&delegation)?;
            info!("Updated delegation status: delegator={}, validator={}, status={}", 
                  hex::encode(&delegator[..16]), 
                  hex::encode(&validator[..16]), 
                  new_status);
        } else {
            warn!("Delegation not found for update: delegator={}, validator={}", 
                  hex::encode(&delegator[..16]), 
                  hex::encode(&validator[..16]));
        }
        Ok(())
    }

    /// Add accumulated rewards to delegation
    pub fn add_accumulated_rewards(
        &self,
        delegator: &[u8; 32],
        validator: &[u8; 32],
        reward_amount: u128,
    ) -> Result<()> {
        // Get current delegation
        if let Some(mut delegation) = self.get_delegation(delegator, validator)? {
            delegation.accumulated_rewards = delegation.accumulated_rewards.saturating_add(reward_amount);
            self.record_delegation(&delegation)?;
            info!("Added accumulated rewards: delegator={}, validator={}, reward={}", 
                  hex::encode(&delegator[..16]), 
                  hex::encode(&validator[..16]), 
                  reward_amount);
        } else {
            warn!("Delegation not found for reward update: delegator={}, validator={}", 
                  hex::encode(&delegator[..16]), 
                  hex::encode(&validator[..16]));
        }
        Ok(())
    }

    /// Get all delegations for a delegator
    pub fn get_delegations_for_delegator(&self, delegator: &[u8; 32]) -> Result<Vec<DelegationRecord>> {
        // In production, this would query all delegations for the delegator
        // For now, return empty vector
        debug!("Getting all delegations for delegator={}", hex::encode(&delegator[..16]));
        Ok(Vec::new())
    }

    /// Get all delegations for a validator
    pub fn get_delegations_for_validator(&self, validator: &[u8; 32]) -> Result<Vec<DelegationRecord>> {
        // In production, this would query all delegations to the validator
        // For now, return empty vector
        debug!("Getting all delegations for validator={}", hex::encode(&validator[..16]));
        Ok(Vec::new())
    }
}
