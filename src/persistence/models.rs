//! Data models for blockchain persistence
//!
//! This module defines the core data structures that are persisted to the database.

use serde::{Deserialize, Serialize};

/// Transaction digest (hash)
pub type TransactionDigest = [u8; 32];

/// Genesis configuration loaded from JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisConfig {
    /// Chain identifier
    pub chain_id: String,

    /// Genesis timestamp (Unix seconds)
    pub genesis_time: u64,

    /// Protocol version
    pub protocol_version: String,

    /// Initial validators
    pub validators: Vec<ValidatorConfig>,

    /// Initial supply in SBTC (smallest unit)
    pub initial_supply: u128,

    /// Initial accounts
    pub initial_accounts: Vec<AccountConfig>,

    /// Consensus configuration
    pub consensus_config: ConsensusConfig,

    /// Fuel configuration
    pub fuel_config: FuelConfig,

    /// Network configuration
    pub network_config: NetworkConfig,
}

/// Validator configuration in genesis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// Validator address
    pub address: String,

    /// Protocol public key
    pub protocol_pubkey: String,

    /// Network public key
    pub network_pubkey: String,

    /// Worker public key
    pub worker_pubkey: String,

    /// Initial stake amount
    pub stake_amount: u128,

    /// Network address
    pub network_address: String,

    /// P2P address
    pub p2p_address: String,

    /// Validator name
    pub name: String,

    /// Commission rate (0-1)
    pub commission_rate: f64,
}

/// Account configuration in genesis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    /// Account address
    pub address: String,

    /// Initial balance
    pub balance: u128,

    /// Account description (for explorer display)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    /// Consensus timeout in milliseconds
    pub timeout_ms: u64,

    /// Minimum validators required
    pub min_validators: u32,
}

/// Fuel configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuelConfig {
    /// Base fuel cost per transaction
    pub base_cost: u64,

    /// Maximum fuel per block
    pub max_per_block: u64,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Network name
    pub name: String,

    /// Network ID
    pub network_id: u32,
}

/// Genesis block (height 0)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    /// Block number (height)
    pub number: u64,

    /// Block hash (SHA3-256)
    pub hash: [u8; 32],

    /// Parent block hash (all zeros for genesis)
    pub parent_hash: [u8; 32],

    /// Block timestamp (Unix milliseconds)
    pub timestamp: u64,

    /// Transaction digests in this block
    pub transactions: Vec<TransactionDigest>,

    /// Validator that proposed this block
    pub validator: [u8; 32],

    /// Gas used in this block
    pub gas_used: u64,

    /// Gas limit for this block
    pub gas_limit: u64,
}

/// Snapshot - consensus checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot sequence number
    pub sequence_number: u64,

    /// Snapshot timestamp (Unix milliseconds)
    pub timestamp: u64,

    /// State root hash (SHA3-256)
    pub state_root: [u8; 32],

    /// Previous snapshot digest
    pub previous_snapshot_digest: [u8; 32],

    /// Transaction digests in this snapshot
    pub transaction_digests: Vec<TransactionDigest>,

    /// Consensus cycle number
    pub cycle: u64,

    /// Validator signatures
    pub signatures: Vec<Signature>,

    /// Stake weights of signers
    pub stake_weights: Vec<u128>,
}

/// Snapshot certificate - proof of finalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCertificate {
    /// Snapshot sequence number
    pub sequence_number: u64,

    /// Finalization timestamp (Unix milliseconds)
    pub finalized_at: u64,

    /// Validator signatures
    pub signatures: Vec<Signature>,

    /// Stake weights of signers
    pub stake_weights: Vec<u128>,

    /// Total stake weight
    pub total_stake_weight: u128,

    /// Quorum achieved (2/3+ of total stake)
    pub quorum_achieved: bool,
}

/// Validator signature
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    /// Validator ID
    pub validator_id: String,

    /// Signature bytes
    pub signature: Vec<u8>,
}

/// Transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction digest (hash)
    pub digest: TransactionDigest,

    /// Sender address
    pub sender: String,

    /// Recipient address
    pub recipient: String,

    /// Amount transferred
    pub amount: u128,

    /// Fuel used
    pub fuel_used: u64,

    /// Transaction status
    pub status: TransactionStatus,

    /// Transaction effects
    pub effects: Vec<Effect>,
}

/// Transaction status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Transaction succeeded
    Success,

    /// Transaction failed
    Failed,

    /// Transaction pending
    Pending,
}

/// Transaction effect
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Effect {
    /// Effect type
    pub effect_type: String,

    /// Effect data
    pub data: Vec<u8>,
}

/// Validator state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorState {
    /// Validator ID
    pub validator_id: String,

    /// Current stake amount
    pub stake: u128,

    /// Current tier
    pub tier: ValidatorTier,

    /// Staking records
    pub deposits: Vec<StakingRecord>,

    /// Delegations
    pub delegations: Vec<Delegation>,

    /// Reward records
    pub rewards: Vec<RewardRecord>,

    /// Unbonding requests
    pub unbonding_requests: Vec<UnstakingRequest>,
}

/// Validator tier
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValidatorTier {
    /// Bronze: 10K-50K SBTC
    Bronze,

    /// Silver: 50K-100K SBTC
    Silver,

    /// Gold: 100K-500K SBTC
    Gold,

    /// Platinum: 500K+ SBTC
    Platinum,
}

/// Staking record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakingRecord {
    /// Validator ID
    pub validator_id: String,

    /// Stake amount
    pub amount: u128,

    /// Tier at time of deposit
    pub tier: ValidatorTier,

    /// Deposit timestamp (Unix milliseconds)
    pub timestamp: u64,

    /// Transaction digest
    pub transaction_digest: TransactionDigest,
}

/// Tier change event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierChange {
    /// Validator ID
    pub validator_id: String,

    /// Old tier
    pub old_tier: ValidatorTier,

    /// New tier
    pub new_tier: ValidatorTier,

    /// New stake amount
    pub new_stake: u128,

    /// Cycle number
    pub cycle: u64,

    /// Timestamp (Unix milliseconds)
    pub timestamp: u64,
}

/// Unstaking request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnstakingRequest {
    /// Validator ID
    pub validator_id: String,

    /// Unstake amount
    pub amount: u128,

    /// Request timestamp (Unix milliseconds)
    pub request_timestamp: u64,

    /// Unbonding completion timestamp (Unix milliseconds)
    /// 7 days (604,800 seconds) after request
    pub unbonding_completion_timestamp: u64,
}

/// Reward record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardRecord {
    /// Validator ID
    pub validator_id: String,

    /// Consensus cycle number
    pub cycle: u64,

    /// Reward amount
    pub amount: u128,

    /// Stake weight during cycle
    pub stake_weight: u128,

    /// Participation rate (0-100)
    pub participation_rate: u8,

    /// Reward timestamp (Unix milliseconds)
    pub timestamp: u64,
}

/// Delegation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delegation {
    /// Delegator address
    pub delegator: String,

    /// Delegated amount
    pub amount: u128,

    /// Delegation timestamp (Unix milliseconds)
    pub timestamp: u64,
}

/// Shutdown checkpoint for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownCheckpoint {
    /// Latest snapshot sequence number
    pub latest_snapshot_sequence: u64,

    /// Latest block number
    pub latest_block_number: u64,

    /// Number of validators in the set
    pub validator_count: u64,

    /// Total stake weight
    pub total_stake: u128,

    /// Checkpoint timestamp (Unix seconds)
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_creation() {
        let block = Block {
            number: 0,
            hash: [0u8; 32],
            parent_hash: [0u8; 32],
            timestamp: 1000,
            transactions: vec![],
            validator: [0u8; 32],
            gas_used: 0,
            gas_limit: 0,
        };

        assert_eq!(block.number, 0);
        assert_eq!(block.timestamp, 1000);
    }

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot {
            sequence_number: 1,
            timestamp: 2000,
            state_root: [1u8; 32],
            previous_snapshot_digest: [0u8; 32],
            transaction_digests: vec![],
            cycle: 0,
            signatures: vec![],
            stake_weights: vec![],
        };

        assert_eq!(snapshot.sequence_number, 1);
        assert_eq!(snapshot.cycle, 0);
    }

    #[test]
    fn test_validator_tier_ordering() {
        assert_eq!(ValidatorTier::Bronze, ValidatorTier::Bronze);
        assert_ne!(ValidatorTier::Bronze, ValidatorTier::Silver);
    }

    #[test]
    fn test_transaction_status() {
        assert_eq!(TransactionStatus::Success, TransactionStatus::Success);
        assert_ne!(TransactionStatus::Success, TransactionStatus::Failed);
    }
}
