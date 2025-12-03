//! Validator state persistence manager
//!
//! This module provides production-ready validator state persistence with:
//! - Staking record persistence
//! - Tier change event recording
//! - Unstaking request persistence with unbonding period
//! - Reward record persistence
//! - Complete validator state recovery
//! - All validator state loading
//! - Full durability guarantees

use super::db::CF_OBJECTS;
use super::errors::{PersistenceError, PersistenceResult};
use super::keys::{
    reward_record_key, staking_record_key, tier_change_key, unstaking_request_key,
};
use super::models::{
    RewardRecord, StakingRecord, TierChange, UnstakingRequest, ValidatorState,
    ValidatorTier,
};
use super::traits::ValidatorStatePersistenceManager;
use crate::RocksDatabase;
use std::collections::BTreeMap;
use tracing::{debug, error, info, warn};

/// Validator state persistence manager implementation
pub struct ValidatorStatePersistenceManagerImpl;

impl ValidatorStatePersistenceManager for ValidatorStatePersistenceManagerImpl {
    fn persist_staking_record(
        record: &StakingRecord,
        db: &RocksDatabase,
    ) -> PersistenceResult<()> {
        debug!(
            "Persisting staking record for validator: {}",
            record.validator_id
        );

        // Validate staking record
        Self::validate_staking_record(record)?;

        // Serialize record to JSON
        let record_json = serde_json::to_vec(record).map_err(|e| {
            error!("Failed to serialize staking record: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize staking record: {}", e))
        })?;

        // Get current record count for this validator to generate index
        let record_index = Self::get_next_staking_record_index(&record.validator_id, db)?;

        // Generate key for this staking record
        let key = staking_record_key(&record.validator_id, record_index);

        // Store in database
        db.put(CF_OBJECTS, &key, &record_json).map_err(|e| {
            error!(
                "Failed to persist staking record for {}: {}",
                record.validator_id, e
            );
            PersistenceError::Database(format!(
                "Failed to persist staking record: {}",
                e
            ))
        })?;

        info!(
            "Staking record for {} persisted successfully (index: {}, {} bytes)",
            record.validator_id,
            record_index,
            record_json.len()
        );

        Ok(())
    }

    fn persist_tier_change(
        change: &TierChange,
        db: &RocksDatabase,
    ) -> PersistenceResult<()> {
        debug!(
            "Persisting tier change for validator: {} (cycle: {})",
            change.validator_id, change.cycle
        );

        // Validate tier change
        Self::validate_tier_change(change)?;

        // Serialize change to JSON
        let change_json = serde_json::to_vec(change).map_err(|e| {
            error!("Failed to serialize tier change: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize tier change: {}", e))
        })?;

        // Generate key for this tier change (keyed by cycle)
        let key = tier_change_key(&change.validator_id, change.cycle);

        // Store in database
        db.put(CF_OBJECTS, &key, &change_json).map_err(|e| {
            error!(
                "Failed to persist tier change for {}: {}",
                change.validator_id, e
            );
            PersistenceError::Database(format!("Failed to persist tier change: {}", e))
        })?;

        info!(
            "Tier change for {} persisted successfully (cycle: {}, {} bytes)",
            change.validator_id,
            change.cycle,
            change_json.len()
        );

        Ok(())
    }

    fn persist_unstaking_request(
        request: &UnstakingRequest,
        db: &RocksDatabase,
    ) -> PersistenceResult<()> {
        debug!(
            "Persisting unstaking request for validator: {}",
            request.validator_id
        );

        // Validate unstaking request
        Self::validate_unstaking_request(request)?;

        // Serialize request to JSON
        let request_json = serde_json::to_vec(request).map_err(|e| {
            error!("Failed to serialize unstaking request: {}", e);
            PersistenceError::Serialization(format!(
                "Failed to serialize unstaking request: {}",
                e
            ))
        })?;

        // Get current request count for this validator to generate index
        let request_index = Self::get_next_unstaking_request_index(&request.validator_id, db)?;

        // Generate key for this unstaking request
        let key = unstaking_request_key(&request.validator_id, request_index);

        // Store in database
        db.put(CF_OBJECTS, &key, &request_json).map_err(|e| {
            error!(
                "Failed to persist unstaking request for {}: {}",
                request.validator_id, e
            );
            PersistenceError::Database(format!(
                "Failed to persist unstaking request: {}",
                e
            ))
        })?;

        info!(
            "Unstaking request for {} persisted successfully (index: {}, {} bytes)",
            request.validator_id,
            request_index,
            request_json.len()
        );

        Ok(())
    }

    fn persist_reward_record(
        record: &RewardRecord,
        db: &RocksDatabase,
    ) -> PersistenceResult<()> {
        debug!(
            "Persisting reward record for validator: {} (cycle: {})",
            record.validator_id, record.cycle
        );

        // Validate reward record
        Self::validate_reward_record(record)?;

        // Serialize record to JSON
        let record_json = serde_json::to_vec(record).map_err(|e| {
            error!("Failed to serialize reward record: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize reward record: {}", e))
        })?;

        // Generate key for this reward record (keyed by cycle)
        let key = reward_record_key(&record.validator_id, record.cycle);

        // Store in database
        db.put(CF_OBJECTS, &key, &record_json).map_err(|e| {
            error!(
                "Failed to persist reward record for {}: {}",
                record.validator_id, e
            );
            PersistenceError::Database(format!("Failed to persist reward record: {}", e))
        })?;

        info!(
            "Reward record for {} persisted successfully (cycle: {}, {} bytes)",
            record.validator_id,
            record.cycle,
            record_json.len()
        );

        Ok(())
    }

    fn load_validator_state(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<ValidatorState> {
        debug!("Loading validator state for: {}", validator_id);

        // Load all staking records for this validator
        let deposits = Self::load_staking_records(validator_id, db)?;

        // Load all unstaking requests for this validator
        let unbonding_requests = Self::load_unstaking_requests(validator_id, db)?;

        // Load all reward records for this validator
        let rewards = Self::load_reward_records(validator_id, db)?;

        // Calculate current stake from deposits and unstaking requests
        let mut current_stake: u128 = deposits.iter().map(|d| d.amount).sum();
        for request in &unbonding_requests {
            current_stake = current_stake.saturating_sub(request.amount);
        }

        // Determine current tier based on stake
        let current_tier = Self::determine_tier(current_stake);

        // Create validator state
        let state = ValidatorState {
            validator_id: validator_id.to_string(),
            stake: current_stake,
            tier: current_tier,
            deposits,
            delegations: vec![], // Delegations loaded separately if needed
            rewards,
            unbonding_requests,
        };

        info!(
            "Validator state for {} loaded successfully (stake: {}, tier: {:?})",
            validator_id, current_stake, current_tier
        );

        Ok(state)
    }

    fn load_all_validator_state(db: &RocksDatabase) -> PersistenceResult<Vec<ValidatorState>> {
        debug!("Loading all validator states");

        // Collect all unique validator IDs from the database
        let validator_ids = Self::collect_all_validator_ids(db)?;

        // Load state for each validator
        let mut states = Vec::new();
        for validator_id in validator_ids {
            match Self::load_validator_state(&validator_id, db) {
                Ok(state) => states.push(state),
                Err(e) => {
                    warn!("Failed to load validator state for {}: {}", validator_id, e);
                    continue;
                }
            }
        }

        info!("Loaded {} validator states", states.len());

        Ok(states)
    }
}

impl ValidatorStatePersistenceManagerImpl {
    /// Validate staking record data integrity
    fn validate_staking_record(record: &StakingRecord) -> PersistenceResult<()> {
        // Validate validator ID is not empty
        if record.validator_id.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Staking record validator ID cannot be empty".to_string(),
            ));
        }

        // Validate amount is not zero
        if record.amount == 0 {
            return Err(PersistenceError::InvalidData(
                "Staking record amount cannot be zero".to_string(),
            ));
        }

        // Validate amount is at least 1,000,000 SBTC (minimum stake)
        if record.amount < 1_000_000 {
            return Err(PersistenceError::InvalidData(
                "Staking record amount must be at least 1,000,000 SBTC".to_string(),
            ));
        }

        // Validate timestamp is reasonable (not zero)
        if record.timestamp == 0 {
            return Err(PersistenceError::InvalidData(
                "Staking record timestamp cannot be zero".to_string(),
            ));
        }

        // Validate transaction digest is not all zeros
        if record.transaction_digest == [0u8; 32] {
            return Err(PersistenceError::InvalidData(
                "Staking record transaction digest cannot be all zeros".to_string(),
            ));
        }

        Ok(())
    }

    /// Validate tier change data integrity
    fn validate_tier_change(change: &TierChange) -> PersistenceResult<()> {
        // Validate validator ID is not empty
        if change.validator_id.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Tier change validator ID cannot be empty".to_string(),
            ));
        }

        // Validate new stake is not zero
        if change.new_stake == 0 {
            return Err(PersistenceError::InvalidData(
                "Tier change new stake cannot be zero".to_string(),
            ));
        }

        // Validate timestamp is reasonable (not zero)
        if change.timestamp == 0 {
            return Err(PersistenceError::InvalidData(
                "Tier change timestamp cannot be zero".to_string(),
            ));
        }

        // Validate old and new tiers are different
        if change.old_tier == change.new_tier {
            return Err(PersistenceError::InvalidData(
                "Tier change old and new tiers must be different".to_string(),
            ));
        }

        Ok(())
    }

    /// Validate unstaking request data integrity
    fn validate_unstaking_request(request: &UnstakingRequest) -> PersistenceResult<()> {
        // Validate validator ID is not empty
        if request.validator_id.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Unstaking request validator ID cannot be empty".to_string(),
            ));
        }

        // Validate amount is not zero
        if request.amount == 0 {
            return Err(PersistenceError::InvalidData(
                "Unstaking request amount cannot be zero".to_string(),
            ));
        }

        // Validate request timestamp is reasonable (not zero)
        if request.request_timestamp == 0 {
            return Err(PersistenceError::InvalidData(
                "Unstaking request timestamp cannot be zero".to_string(),
            ));
        }

        // Validate unbonding completion timestamp is after request timestamp
        if request.unbonding_completion_timestamp <= request.request_timestamp {
            return Err(PersistenceError::InvalidData(
                "Unbonding completion timestamp must be after request timestamp".to_string(),
            ));
        }

        // Validate unbonding period is 7 days (604,800 seconds)
        let expected_completion = request.request_timestamp + 604_800_000; // milliseconds
        if (request.unbonding_completion_timestamp as i64 - expected_completion as i64).abs()
            > 1000
        {
            // Allow 1 second tolerance
            warn!(
                "Unbonding period is not exactly 7 days: {} ms",
                request.unbonding_completion_timestamp - request.request_timestamp
            );
        }

        Ok(())
    }

    /// Validate reward record data integrity
    fn validate_reward_record(record: &RewardRecord) -> PersistenceResult<()> {
        // Validate validator ID is not empty
        if record.validator_id.is_empty() {
            return Err(PersistenceError::InvalidData(
                "Reward record validator ID cannot be empty".to_string(),
            ));
        }

        // Validate amount is not zero
        if record.amount == 0 {
            return Err(PersistenceError::InvalidData(
                "Reward record amount cannot be zero".to_string(),
            ));
        }

        // Validate stake weight is not zero
        if record.stake_weight == 0 {
            return Err(PersistenceError::InvalidData(
                "Reward record stake weight cannot be zero".to_string(),
            ));
        }

        // Validate participation rate is 0-100
        if record.participation_rate > 100 {
            return Err(PersistenceError::InvalidData(
                "Reward record participation rate must be 0-100".to_string(),
            ));
        }

        // Validate timestamp is reasonable (not zero)
        if record.timestamp == 0 {
            return Err(PersistenceError::InvalidData(
                "Reward record timestamp cannot be zero".to_string(),
            ));
        }

        Ok(())
    }

    /// Load all staking records for a validator
    fn load_staking_records(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<Vec<StakingRecord>> {
        let mut records = Vec::new();
        let mut index = 0;

        loop {
            let key = staking_record_key(validator_id, index);

            match db.get(CF_OBJECTS, &key) {
                Ok(Some(data)) => {
                    match serde_json::from_slice::<StakingRecord>(&data) {
                        Ok(record) => {
                            if let Err(e) = Self::validate_staking_record(&record) {
                                warn!(
                                    "Invalid staking record for {} at index {}: {}",
                                    validator_id, index, e
                                );
                            } else {
                                records.push(record);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize staking record for {} at index {}: {}",
                                validator_id, index, e
                            );
                        }
                    }
                    index += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(
                        "Error reading staking record for {} at index {}: {}",
                        validator_id, index, e
                    );
                    break;
                }
            }
        }

        debug!(
            "Loaded {} staking records for validator {}",
            records.len(),
            validator_id
        );

        Ok(records)
    }

    /// Load all tier changes for a validator
    #[allow(dead_code)]
    fn load_tier_changes(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<Vec<TierChange>> {
        let mut changes = BTreeMap::new();

        // Iterate through all keys to find tier changes for this validator
        let iter = db.iter(CF_OBJECTS, rocksdb::IteratorMode::Start);

        for result in iter {
            match result {
                Ok((key, value)) => {
                    let key_str = String::from_utf8_lossy(&key);

                    if key_str.starts_with(&format!("tier_change:{}:", validator_id)) {
                        // Extract cycle from key
                        if let Some(cycle_str) = key_str.strip_prefix(&format!("tier_change:{}:", validator_id)) {
                            if let Ok(cycle) = cycle_str.parse::<u64>() {
                                match serde_json::from_slice::<TierChange>(&value) {
                                    Ok(change) => {
                                        if let Err(e) = Self::validate_tier_change(&change) {
                                            warn!(
                                                "Invalid tier change for {} at cycle {}: {}",
                                                validator_id, cycle, e
                                            );
                                        } else {
                                            changes.insert(cycle, change);
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to deserialize tier change for {} at cycle {}: {}",
                                            validator_id, cycle, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Iterator error while loading tier changes: {}", e);
                    break;
                }
            }
        }

        let result: Vec<TierChange> = changes.into_values().collect();

        debug!(
            "Loaded {} tier changes for validator {}",
            result.len(),
            validator_id
        );

        Ok(result)
    }

    /// Load all unstaking requests for a validator
    fn load_unstaking_requests(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<Vec<UnstakingRequest>> {
        let mut requests = Vec::new();
        let mut index = 0;

        loop {
            let key = unstaking_request_key(validator_id, index);

            match db.get(CF_OBJECTS, &key) {
                Ok(Some(data)) => {
                    match serde_json::from_slice::<UnstakingRequest>(&data) {
                        Ok(request) => {
                            if let Err(e) = Self::validate_unstaking_request(&request) {
                                warn!(
                                    "Invalid unstaking request for {} at index {}: {}",
                                    validator_id, index, e
                                );
                            } else {
                                requests.push(request);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize unstaking request for {} at index {}: {}",
                                validator_id, index, e
                            );
                        }
                    }
                    index += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(
                        "Error reading unstaking request for {} at index {}: {}",
                        validator_id, index, e
                    );
                    break;
                }
            }
        }

        debug!(
            "Loaded {} unstaking requests for validator {}",
            requests.len(),
            validator_id
        );

        Ok(requests)
    }

    /// Load all reward records for a validator
    fn load_reward_records(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<Vec<RewardRecord>> {
        let mut records = BTreeMap::new();

        // Iterate through all keys to find reward records for this validator
        let iter = db.iter(CF_OBJECTS, rocksdb::IteratorMode::Start);

        for result in iter {
            match result {
                Ok((key, value)) => {
                    let key_str = String::from_utf8_lossy(&key);

                    if key_str.starts_with(&format!("reward:{}:", validator_id)) {
                        // Extract cycle from key
                        if let Some(cycle_str) = key_str.strip_prefix(&format!("reward:{}:", validator_id)) {
                            if let Ok(cycle) = cycle_str.parse::<u64>() {
                                match serde_json::from_slice::<RewardRecord>(&value) {
                                    Ok(record) => {
                                        if let Err(e) = Self::validate_reward_record(&record) {
                                            warn!(
                                                "Invalid reward record for {} at cycle {}: {}",
                                                validator_id, cycle, e
                                            );
                                        } else {
                                            records.insert(cycle, record);
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to deserialize reward record for {} at cycle {}: {}",
                                            validator_id, cycle, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Iterator error while loading reward records: {}", e);
                    break;
                }
            }
        }

        let result: Vec<RewardRecord> = records.into_values().collect();

        debug!(
            "Loaded {} reward records for validator {}",
            result.len(),
            validator_id
        );

        Ok(result)
    }

    /// Get next staking record index for a validator
    fn get_next_staking_record_index(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<u64> {
        let mut index = 0;

        loop {
            let key = staking_record_key(validator_id, index);

            match db.get(CF_OBJECTS, &key) {
                Ok(Some(_)) => index += 1,
                Ok(None) => return Ok(index),
                Err(e) => {
                    return Err(PersistenceError::Database(format!(
                        "Failed to check staking record index: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Get next unstaking request index for a validator
    fn get_next_unstaking_request_index(
        validator_id: &str,
        db: &RocksDatabase,
    ) -> PersistenceResult<u64> {
        let mut index = 0;

        loop {
            let key = unstaking_request_key(validator_id, index);

            match db.get(CF_OBJECTS, &key) {
                Ok(Some(_)) => index += 1,
                Ok(None) => return Ok(index),
                Err(e) => {
                    return Err(PersistenceError::Database(format!(
                        "Failed to check unstaking request index: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Determine validator tier based on stake amount
    fn determine_tier(stake: u128) -> ValidatorTier {
        match stake {
            0..=49_999_999 => ValidatorTier::Bronze,
            50_000_000..=99_999_999 => ValidatorTier::Silver,
            100_000_000..=499_999_999 => ValidatorTier::Gold,
            _ => ValidatorTier::Platinum,
        }
    }

    /// Collect all unique validator IDs from the database
    fn collect_all_validator_ids(db: &RocksDatabase) -> PersistenceResult<Vec<String>> {
        let mut validator_ids = std::collections::HashSet::new();

        // Iterate through all keys to find validator-related entries
        let iter = db.iter(CF_OBJECTS, rocksdb::IteratorMode::Start);

        for result in iter {
            match result {
                Ok((key, _)) => {
                    let key_str = String::from_utf8_lossy(&key);

                    // Extract validator ID from various key formats
                    if let Some(validator_id) = Self::extract_validator_id(&key_str) {
                        validator_ids.insert(validator_id);
                    }
                }
                Err(e) => {
                    warn!("Iterator error while collecting validator IDs: {}", e);
                    break;
                }
            }
        }

        let result: Vec<String> = validator_ids.into_iter().collect();

        debug!("Found {} unique validators", result.len());

        Ok(result)
    }

    /// Extract validator ID from a database key
    fn extract_validator_id(key_str: &str) -> Option<String> {
        if let Some(id) = key_str.strip_prefix("staking:") {
            if let Some(validator_id) = id.split(':').next() {
                return Some(validator_id.to_string());
            }
        }

        if let Some(id) = key_str.strip_prefix("tier_change:") {
            if let Some(validator_id) = id.split(':').next() {
                return Some(validator_id.to_string());
            }
        }

        if let Some(id) = key_str.strip_prefix("unstaking:") {
            if let Some(validator_id) = id.split(':').next() {
                return Some(validator_id.to_string());
            }
        }

        if let Some(id) = key_str.strip_prefix("reward:") {
            if let Some(validator_id) = id.split(':').next() {
                return Some(validator_id.to_string());
            }
        }

        None
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_staking_record(validator_id: &str, amount: u128) -> StakingRecord {
        StakingRecord {
            validator_id: validator_id.to_string(),
            amount,
            tier: ValidatorTier::Silver,
            timestamp: 1000,
            transaction_digest: [1u8; 32],
        }
    }

    fn create_test_tier_change(
        validator_id: &str,
        old_tier: ValidatorTier,
        new_tier: ValidatorTier,
    ) -> TierChange {
        TierChange {
            validator_id: validator_id.to_string(),
            old_tier,
            new_tier,
            new_stake: 100_000_000,
            cycle: 10,
            timestamp: 2000,
        }
    }

    fn create_test_unstaking_request(validator_id: &str, amount: u128) -> UnstakingRequest {
        UnstakingRequest {
            validator_id: validator_id.to_string(),
            amount,
            request_timestamp: 3000,
            unbonding_completion_timestamp: 3000 + 604_800_000,
        }
    }

    fn create_test_reward_record(validator_id: &str, cycle: u64) -> RewardRecord {
        RewardRecord {
            validator_id: validator_id.to_string(),
            cycle,
            amount: 50_000,
            stake_weight: 100_000_000,
            participation_rate: 95,
            timestamp: 4000,
        }
    }

    #[test]
    fn test_persist_staking_record() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let record = create_test_staking_record("validator_1", 50_000_000);

        ValidatorStatePersistenceManagerImpl::persist_staking_record(&record, &db)
            .expect("Failed to persist staking record");

        // Verify it was stored
        let key = staking_record_key("validator_1", 0);
        let data = db.get(CF_OBJECTS, &key).expect("Failed to get record");
        assert!(data.is_some());
    }

    #[test]
    fn test_persist_tier_change() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let change = create_test_tier_change("validator_1", ValidatorTier::Silver, ValidatorTier::Gold);

        ValidatorStatePersistenceManagerImpl::persist_tier_change(&change, &db)
            .expect("Failed to persist tier change");

        // Verify it was stored
        let key = tier_change_key("validator_1", 10);
        let data = db.get(CF_OBJECTS, &key).expect("Failed to get change");
        assert!(data.is_some());
    }

    #[test]
    fn test_persist_unstaking_request() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let request = create_test_unstaking_request("validator_1", 10_000_000);

        ValidatorStatePersistenceManagerImpl::persist_unstaking_request(&request, &db)
            .expect("Failed to persist unstaking request");

        // Verify it was stored
        let key = unstaking_request_key("validator_1", 0);
        let data = db.get(CF_OBJECTS, &key).expect("Failed to get request");
        assert!(data.is_some());
    }

    #[test]
    fn test_persist_reward_record() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        let record = create_test_reward_record("validator_1", 5);

        ValidatorStatePersistenceManagerImpl::persist_reward_record(&record, &db)
            .expect("Failed to persist reward record");

        // Verify it was stored
        let key = reward_record_key("validator_1", 5);
        let data = db.get(CF_OBJECTS, &key).expect("Failed to get record");
        assert!(data.is_some());
    }

    #[test]
    fn test_load_validator_state() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist multiple records for a validator
        let staking_record = create_test_staking_record("validator_1", 50_000_000);
        ValidatorStatePersistenceManagerImpl::persist_staking_record(&staking_record, &db)
            .expect("Failed to persist staking record");

        let reward_record = create_test_reward_record("validator_1", 5);
        ValidatorStatePersistenceManagerImpl::persist_reward_record(&reward_record, &db)
            .expect("Failed to persist reward record");

        // Load validator state
        let state = ValidatorStatePersistenceManagerImpl::load_validator_state("validator_1", &db)
            .expect("Failed to load validator state");

        assert_eq!(state.validator_id, "validator_1");
        assert_eq!(state.stake, 50_000_000);
        assert_eq!(state.deposits.len(), 1);
        assert_eq!(state.rewards.len(), 1);
    }

    #[test]
    fn test_load_validator_state_with_unstaking() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist staking and unstaking records
        let staking_record = create_test_staking_record("validator_1", 50_000_000);
        ValidatorStatePersistenceManagerImpl::persist_staking_record(&staking_record, &db)
            .expect("Failed to persist staking record");

        let unstaking_request = create_test_unstaking_request("validator_1", 10_000_000);
        ValidatorStatePersistenceManagerImpl::persist_unstaking_request(&unstaking_request, &db)
            .expect("Failed to persist unstaking request");

        // Load validator state
        let state = ValidatorStatePersistenceManagerImpl::load_validator_state("validator_1", &db)
            .expect("Failed to load validator state");

        // Stake should be reduced by unstaking amount
        assert_eq!(state.stake, 40_000_000);
        assert_eq!(state.unbonding_requests.len(), 1);
    }

    #[test]
    fn test_load_all_validator_state() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist records for multiple validators
        for i in 1..=3 {
            let validator_id = format!("validator_{}", i);
            let record = create_test_staking_record(&validator_id, 50_000_000);
            ValidatorStatePersistenceManagerImpl::persist_staking_record(&record, &db)
                .expect("Failed to persist staking record");
        }

        // Load all validator states
        let states = ValidatorStatePersistenceManagerImpl::load_all_validator_state(&db)
            .expect("Failed to load all validator states");

        assert_eq!(states.len(), 3);
    }

    #[test]
    fn test_validate_staking_record_invalid_validator_id() {
        let mut record = create_test_staking_record("validator_1", 50_000_000);
        record.validator_id = String::new();

        let result = ValidatorStatePersistenceManagerImpl::validate_staking_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_staking_record_zero_amount() {
        let mut record = create_test_staking_record("validator_1", 50_000_000);
        record.amount = 0;

        let result = ValidatorStatePersistenceManagerImpl::validate_staking_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_staking_record_insufficient_amount() {
        let mut record = create_test_staking_record("validator_1", 50_000_000);
        record.amount = 500_000; // Less than 1,000,000

        let result = ValidatorStatePersistenceManagerImpl::validate_staking_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_staking_record_zero_timestamp() {
        let mut record = create_test_staking_record("validator_1", 50_000_000);
        record.timestamp = 0;

        let result = ValidatorStatePersistenceManagerImpl::validate_staking_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_staking_record_zero_digest() {
        let mut record = create_test_staking_record("validator_1", 50_000_000);
        record.transaction_digest = [0u8; 32];

        let result = ValidatorStatePersistenceManagerImpl::validate_staking_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_tier_change_same_tier() {
        let mut change = create_test_tier_change("validator_1", ValidatorTier::Silver, ValidatorTier::Gold);
        change.old_tier = ValidatorTier::Gold;
        change.new_tier = ValidatorTier::Gold;

        let result = ValidatorStatePersistenceManagerImpl::validate_tier_change(&change);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_unstaking_request_invalid_timestamps() {
        let mut request = create_test_unstaking_request("validator_1", 10_000_000);
        request.unbonding_completion_timestamp = request.request_timestamp;

        let result = ValidatorStatePersistenceManagerImpl::validate_unstaking_request(&request);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_reward_record_invalid_participation_rate() {
        let mut record = create_test_reward_record("validator_1", 5);
        record.participation_rate = 101;

        let result = ValidatorStatePersistenceManagerImpl::validate_reward_record(&record);
        assert!(result.is_err());
    }

    #[test]
    fn test_determine_tier_bronze() {
        let tier = ValidatorStatePersistenceManagerImpl::determine_tier(25_000_000);
        assert_eq!(tier, ValidatorTier::Bronze);
    }

    #[test]
    fn test_determine_tier_silver() {
        let tier = ValidatorStatePersistenceManagerImpl::determine_tier(75_000_000);
        assert_eq!(tier, ValidatorTier::Silver);
    }

    #[test]
    fn test_determine_tier_gold() {
        let tier = ValidatorStatePersistenceManagerImpl::determine_tier(250_000_000);
        assert_eq!(tier, ValidatorTier::Gold);
    }

    #[test]
    fn test_determine_tier_platinum() {
        let tier = ValidatorStatePersistenceManagerImpl::determine_tier(600_000_000);
        assert_eq!(tier, ValidatorTier::Platinum);
    }

    #[test]
    fn test_multiple_staking_records() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist multiple staking records for same validator
        for i in 0..3 {
            let mut record = create_test_staking_record("validator_1", 50_000_000);
            record.timestamp = 1000 + (i * 100);
            ValidatorStatePersistenceManagerImpl::persist_staking_record(&record, &db)
                .expect("Failed to persist staking record");
        }

        // Load validator state
        let state = ValidatorStatePersistenceManagerImpl::load_validator_state("validator_1", &db)
            .expect("Failed to load validator state");

        assert_eq!(state.deposits.len(), 3);
        assert_eq!(state.stake, 150_000_000); // 3 * 50_000_000
    }

    #[test]
    fn test_validator_state_round_trip() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = RocksDatabase::open(temp_dir.path()).expect("Failed to open database");

        // Persist various records
        let staking = create_test_staking_record("validator_1", 100_000_000);
        ValidatorStatePersistenceManagerImpl::persist_staking_record(&staking, &db)
            .expect("Failed to persist staking record");

        let tier_change = create_test_tier_change("validator_1", ValidatorTier::Silver, ValidatorTier::Gold);
        ValidatorStatePersistenceManagerImpl::persist_tier_change(&tier_change, &db)
            .expect("Failed to persist tier change");

        let reward = create_test_reward_record("validator_1", 5);
        ValidatorStatePersistenceManagerImpl::persist_reward_record(&reward, &db)
            .expect("Failed to persist reward record");

        // Load and verify
        let state = ValidatorStatePersistenceManagerImpl::load_validator_state("validator_1", &db)
            .expect("Failed to load validator state");

        assert_eq!(state.validator_id, "validator_1");
        assert_eq!(state.stake, 100_000_000);
        assert_eq!(state.tier, ValidatorTier::Gold);
        assert_eq!(state.deposits.len(), 1);
        assert_eq!(state.rewards.len(), 1);
    }
}
