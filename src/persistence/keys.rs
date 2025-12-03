//! Database key formatting functions
//!
//! This module provides functions to format keys for different data types
//! in a consistent and queryable manner.

/// Format key for genesis block
///
/// Genesis block is stored with a fixed key since there's only one per chain
pub fn genesis_block_key() -> Vec<u8> {
    b"genesis".to_vec()
}

/// Format key for snapshot by sequence number
///
/// # Arguments
/// * `sequence_number` - Snapshot sequence number
pub fn snapshot_key(sequence_number: u64) -> Vec<u8> {
    format!("snapshot:{:020}", sequence_number).into_bytes()
}

/// Format key for snapshot certificate by sequence number
///
/// # Arguments
/// * `sequence_number` - Snapshot sequence number
pub fn snapshot_certificate_key(sequence_number: u64) -> Vec<u8> {
    format!("snapshot_cert:{:020}", sequence_number).into_bytes()
}

/// Format key for transaction by digest
///
/// # Arguments
/// * `digest` - Transaction digest (hex string)
pub fn transaction_key(digest: &str) -> Vec<u8> {
    format!("tx:{}", digest).into_bytes()
}

/// Format key for transaction-snapshot mapping
///
/// Maps a transaction digest to its containing snapshot sequence number
///
/// # Arguments
/// * `digest` - Transaction digest (hex string)
pub fn transaction_snapshot_mapping_key(digest: &str) -> Vec<u8> {
    format!("tx_snapshot:{}", digest).into_bytes()
}

/// Format key for validator state by validator ID
///
/// # Arguments
/// * `validator_id` - Validator identifier
pub fn validator_state_key(validator_id: &str) -> Vec<u8> {
    format!("validator:{}", validator_id).into_bytes()
}

/// Format key for staking record
///
/// # Arguments
/// * `validator_id` - Validator identifier
/// * `record_index` - Index of the staking record for this validator
pub fn staking_record_key(validator_id: &str, record_index: u64) -> Vec<u8> {
    format!("staking:{}:{:020}", validator_id, record_index).into_bytes()
}

/// Format key for tier change event
///
/// # Arguments
/// * `validator_id` - Validator identifier
/// * `cycle` - Cycle number when tier changed
pub fn tier_change_key(validator_id: &str, cycle: u64) -> Vec<u8> {
    format!("tier_change:{}:{:020}", validator_id, cycle).into_bytes()
}

/// Format key for unstaking request
///
/// # Arguments
/// * `validator_id` - Validator identifier
/// * `request_index` - Index of the unstaking request for this validator
pub fn unstaking_request_key(validator_id: &str, request_index: u64) -> Vec<u8> {
    format!("unstaking:{}:{:020}", validator_id, request_index).into_bytes()
}

/// Format key for reward record
///
/// # Arguments
/// * `validator_id` - Validator identifier
/// * `cycle` - Cycle number for the reward
pub fn reward_record_key(validator_id: &str, cycle: u64) -> Vec<u8> {
    format!("reward:{}:{:020}", validator_id, cycle).into_bytes()
}

/// Format key for account balance
///
/// # Arguments
/// * `address` - Account address
pub fn account_balance_key(address: &str) -> Vec<u8> {
    format!("balance:{}", address).into_bytes()
}

/// Format key for shutdown checkpoint
///
/// Stores the latest shutdown checkpoint for recovery
pub fn shutdown_checkpoint_key() -> Vec<u8> {
    b"checkpoint:latest".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis_block_key() {
        let key = genesis_block_key();
        assert_eq!(key, b"genesis");
    }

    #[test]
    fn test_snapshot_key_formatting() {
        let key = snapshot_key(1);
        assert_eq!(key, b"snapshot:00000000000000000001");

        let key = snapshot_key(12345);
        assert_eq!(key, b"snapshot:00000000000000012345");
    }

    #[test]
    fn test_snapshot_certificate_key_formatting() {
        let key = snapshot_certificate_key(1);
        assert_eq!(key, b"snapshot_cert:00000000000000000001");
    }

    #[test]
    fn test_transaction_key_formatting() {
        let digest = "abc123def456";
        let key = transaction_key(digest);
        assert_eq!(key, b"tx:abc123def456");
    }

    #[test]
    fn test_transaction_snapshot_mapping_key() {
        let digest = "abc123def456";
        let key = transaction_snapshot_mapping_key(digest);
        assert_eq!(key, b"tx_snapshot:abc123def456");
    }

    #[test]
    fn test_validator_state_key() {
        let validator_id = "validator_1";
        let key = validator_state_key(validator_id);
        assert_eq!(key, b"validator:validator_1");
    }

    #[test]
    fn test_staking_record_key() {
        let validator_id = "validator_1";
        let key = staking_record_key(validator_id, 1);
        assert_eq!(key, b"staking:validator_1:00000000000000000001");
    }

    #[test]
    fn test_tier_change_key() {
        let validator_id = "validator_1";
        let key = tier_change_key(validator_id, 100);
        assert_eq!(key, b"tier_change:validator_1:00000000000000000100");
    }

    #[test]
    fn test_unstaking_request_key() {
        let validator_id = "validator_1";
        let key = unstaking_request_key(validator_id, 5);
        assert_eq!(key, b"unstaking:validator_1:00000000000000000005");
    }

    #[test]
    fn test_reward_record_key() {
        let validator_id = "validator_1";
        let key = reward_record_key(validator_id, 50);
        assert_eq!(key, b"reward:validator_1:00000000000000000050");
    }

    #[test]
    fn test_account_balance_key() {
        let address = "0x1234567890abcdef";
        let key = account_balance_key(address);
        assert_eq!(key, b"balance:0x1234567890abcdef");
    }

    #[test]
    fn test_shutdown_checkpoint_key() {
        let key = shutdown_checkpoint_key();
        assert_eq!(key, b"checkpoint:latest");
    }
}
