//! Genesis block manager implementation
//!
//! This module implements the GenesisBlockManager trait for initializing
//! and managing the genesis block - the first block (height 0) that initializes
//! the blockchain with initial validators and state.

use crate::persistence::errors::{PersistenceError, PersistenceResult};
use crate::persistence::keys::genesis_block_key;
use crate::persistence::models::*;
use crate::persistence::traits::GenesisBlockManager;
use crate::persistence::CF_BLOCKS;
use sha3::{Digest, Sha3_256};
use std::fs;
use std::path::Path;
use tracing::{debug, error, info};

/// Genesis block manager implementation
pub struct GenesisBlockManagerImpl;

impl GenesisBlockManager for GenesisBlockManagerImpl {
    fn load_genesis_config(path: &str) -> PersistenceResult<GenesisConfig> {
        info!("Loading genesis configuration from: {}", path);

        let path = Path::new(path);
        if !path.exists() {
            error!("Genesis configuration file not found: {}", path.display());
            return Err(PersistenceError::InvalidData(format!(
                "Genesis configuration file not found: {}",
                path.display()
            )));
        }

        let content = fs::read_to_string(path).map_err(|e| {
            error!("Failed to read genesis configuration file: {}", e);
            PersistenceError::Io(e)
        })?;

        let config: GenesisConfig = serde_json::from_str(&content).map_err(|e| {
            error!("Failed to parse genesis configuration JSON: {}", e);
            PersistenceError::Json(e)
        })?;

        info!(
            "Genesis configuration loaded successfully: chain_id={}, validators={}",
            config.chain_id,
            config.validators.len()
        );

        Ok(config)
    }

    fn validate_genesis_config(config: &GenesisConfig) -> PersistenceResult<()> {
        debug!("Validating genesis configuration");

        // Validate chain_id is not empty
        if config.chain_id.is_empty() {
            error!("Genesis configuration validation failed: chain_id is empty");
            return Err(PersistenceError::InvalidData(
                "chain_id must not be empty".to_string(),
            ));
        }

        // Validate validators list is not empty
        if config.validators.is_empty() {
            error!("Genesis configuration validation failed: validators list is empty");
            return Err(PersistenceError::InvalidData(
                "validators list must not be empty".to_string(),
            ));
        }

        // Validate initial_supply > 0
        if config.initial_supply == 0 {
            error!("Genesis configuration validation failed: initial_supply is zero");
            return Err(PersistenceError::InvalidData(
                "initial_supply must be greater than 0".to_string(),
            ));
        }

        // Validate each validator has sufficient stake (>= 1,000,000 SBTC)
        const MIN_VALIDATOR_STAKE: u128 = 1_000_000_000_000_000; // 1,000,000 SBTC in smallest units

        for (idx, validator) in config.validators.iter().enumerate() {
            if validator.stake_amount < MIN_VALIDATOR_STAKE {
                error!(
                    "Genesis configuration validation failed: validator {} has insufficient stake: {}",
                    idx, validator.stake_amount
                );
                return Err(PersistenceError::InvalidData(format!(
                    "Validator {} has insufficient stake: {} (minimum: {})",
                    idx, validator.stake_amount, MIN_VALIDATOR_STAKE
                )));
            }

            if validator.address.is_empty() {
                error!(
                    "Genesis configuration validation failed: validator {} has empty address",
                    idx
                );
                return Err(PersistenceError::InvalidData(format!(
                    "Validator {} has empty address",
                    idx
                )));
            }

            if validator.protocol_pubkey.is_empty() {
                error!(
                    "Genesis configuration validation failed: validator {} has empty protocol_pubkey",
                    idx
                );
                return Err(PersistenceError::InvalidData(format!(
                    "Validator {} has empty protocol_pubkey",
                    idx
                )));
            }

            if validator.network_pubkey.is_empty() {
                error!(
                    "Genesis configuration validation failed: validator {} has empty network_pubkey",
                    idx
                );
                return Err(PersistenceError::InvalidData(format!(
                    "Validator {} has empty network_pubkey",
                    idx
                )));
            }

            if validator.worker_pubkey.is_empty() {
                error!(
                    "Genesis configuration validation failed: validator {} has empty worker_pubkey",
                    idx
                );
                return Err(PersistenceError::InvalidData(format!(
                    "Validator {} has empty worker_pubkey",
                    idx
                )));
            }

            if validator.commission_rate < 0.0 || validator.commission_rate > 1.0 {
                error!(
                    "Genesis configuration validation failed: validator {} has invalid commission_rate: {}",
                    idx, validator.commission_rate
                );
                return Err(PersistenceError::InvalidData(format!(
                    "Validator {} has invalid commission_rate: {} (must be 0-1)",
                    idx, validator.commission_rate
                )));
            }
        }

        info!("Genesis configuration validation passed");
        Ok(())
    }

    fn create_genesis_block(config: &GenesisConfig) -> PersistenceResult<Block> {
        debug!("Creating genesis block from configuration");

        // Validate configuration first
        Self::validate_genesis_config(config)?;

        // Compute genesis block hash as SHA3-256 of serialized config
        let config_json = serde_json::to_string(config).map_err(|e| {
            error!("Failed to serialize genesis config for hashing: {}", e);
            PersistenceError::Serialization(format!(
                "Failed to serialize genesis config for hashing: {}",
                e
            ))
        })?;

        let mut hasher = Sha3_256::new();
        hasher.update(config_json.as_bytes());
        let hash_result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&hash_result[..32]);

        // Convert genesis_time from Unix seconds to Unix milliseconds
        let timestamp = config.genesis_time * 1000;

        let genesis_block = Block {
            number: 0,
            hash,
            parent_hash: [0u8; 32], // All zeros for genesis
            timestamp,
            transactions: vec![], // No transactions in genesis block
            validator: [0u8; 32], // All zeros (no single validator for genesis)
            gas_used: 0,
            gas_limit: 0,
        };

        info!(
            "Genesis block created: hash={}, timestamp={}",
            hex::encode(hash),
            timestamp
        );

        Ok(genesis_block)
    }

    fn persist_genesis_block(
        block: &Block,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()> {
        info!("Persisting genesis block to database");

        let key = genesis_block_key();

        // Serialize block to JSON
        let block_json = serde_json::to_vec(block).map_err(|e| {
            error!("Failed to serialize genesis block: {}", e);
            PersistenceError::Serialization(format!("Failed to serialize genesis block: {}", e))
        })?;

        // Store in CF_BLOCKS column family
        db.put(CF_BLOCKS, &key, &block_json)
            .map_err(|e| {
                error!("Failed to persist genesis block: {}", e);
                PersistenceError::Database(format!("Failed to persist genesis block: {}", e))
            })?;

        info!(
            "Genesis block persisted successfully: hash={}",
            hex::encode(block.hash)
        );

        Ok(())
    }

    fn load_genesis_block(db: &crate::RocksDatabase) -> PersistenceResult<Option<Block>> {
        debug!("Loading genesis block from database");

        let key = genesis_block_key();

        match db.get(CF_BLOCKS, &key).map_err(|e| {
            error!("Failed to load genesis block: {}", e);
            PersistenceError::Database(format!("Failed to load genesis block: {}", e))
        })? {
            Some(data) => {
                let block: Block = serde_json::from_slice(&data).map_err(|e| {
                    error!("Failed to deserialize genesis block: {}", e);
                    PersistenceError::Serialization(format!(
                        "Failed to deserialize genesis block: {}",
                        e
                    ))
                })?;

                info!(
                    "Genesis block loaded successfully: hash={}",
                    hex::encode(block.hash)
                );

                Ok(Some(block))
            }
            None => {
                debug!("Genesis block not found in database");
                Ok(None)
            }
        }
    }
}

/// Helper functions for genesis block management
impl GenesisBlockManagerImpl {
    /// Persist genesis state (validators and initial accounts)
    pub fn persist_genesis_state(
        config: &GenesisConfig,
        db: &crate::RocksDatabase,
    ) -> PersistenceResult<()> {
        use crate::persistence::keys::{validator_state_key, account_balance_key};
        use crate::persistence::CF_OBJECTS;

        info!("Persisting genesis state: validators and initial accounts");

        // Persist validator states
        for (idx, validator_config) in config.validators.iter().enumerate() {
            let validator_state = ValidatorState {
                validator_id: validator_config.address.clone(),
                stake: validator_config.stake_amount,
                tier: Self::calculate_validator_tier(validator_config.stake_amount),
                deposits: vec![StakingRecord {
                    validator_id: validator_config.address.clone(),
                    amount: validator_config.stake_amount,
                    tier: Self::calculate_validator_tier(validator_config.stake_amount),
                    timestamp: config.genesis_time * 1000,
                    transaction_digest: [0u8; 32],
                }],
                delegations: vec![],
                rewards: vec![],
                unbonding_requests: vec![],
            };

            let key = validator_state_key(&validator_config.address);
            let state_json = serde_json::to_vec(&validator_state).map_err(|e| {
                error!("Failed to serialize validator state: {}", e);
                PersistenceError::Serialization(format!(
                    "Failed to serialize validator state: {}",
                    e
                ))
            })?;

            db.put(CF_OBJECTS, &key, &state_json).map_err(|e| {
                error!("Failed to persist validator state: {}", e);
                PersistenceError::Database(format!("Failed to persist validator state: {}", e))
            })?;

            info!(
                "Validator {} persisted: stake={}",
                idx, validator_config.stake_amount
            );
        }

        // Persist initial account balances
        for (idx, account_config) in config.initial_accounts.iter().enumerate() {
            let key = account_balance_key(&account_config.address);
            let balance_json = serde_json::to_vec(&account_config.balance).map_err(|e| {
                error!("Failed to serialize account balance: {}", e);
                PersistenceError::Serialization(format!(
                    "Failed to serialize account balance: {}",
                    e
                ))
            })?;

            db.put(CF_OBJECTS, &key, &balance_json).map_err(|e| {
                error!("Failed to persist account balance: {}", e);
                PersistenceError::Database(format!("Failed to persist account balance: {}", e))
            })?;

            info!(
                "Account {} persisted: balance={}",
                idx, account_config.balance
            );
        }

        info!(
            "Genesis state persisted: {} validators, {} accounts",
            config.validators.len(),
            config.initial_accounts.len()
        );

        Ok(())
    }

    /// Calculate validator tier based on stake amount
    fn calculate_validator_tier(stake: u128) -> ValidatorTier {
        const BRONZE_THRESHOLD: u128 = 10_000_000_000_000_000; // 10K SBTC
        const SILVER_THRESHOLD: u128 = 50_000_000_000_000_000; // 50K SBTC
        const GOLD_THRESHOLD: u128 = 100_000_000_000_000_000; // 100K SBTC

        if stake >= GOLD_THRESHOLD {
            ValidatorTier::Gold
        } else if stake >= SILVER_THRESHOLD {
            ValidatorTier::Silver
        } else if stake >= BRONZE_THRESHOLD {
            ValidatorTier::Bronze
        } else {
            ValidatorTier::Bronze
        }
    }
}

/// Parse ISO 8601 timestamp to Unix milliseconds
///
/// Handles formats like "2024-12-01T00:00:00Z"
#[allow(dead_code)]
fn parse_iso8601_to_millis(timestamp_str: &str) -> PersistenceResult<u64> {
    // Try parsing with chrono if available, otherwise use a simple parser
    use chrono::DateTime;

    let dt = DateTime::parse_from_rfc3339(timestamp_str).map_err(|e| {
        error!("Failed to parse ISO 8601 timestamp: {}", e);
        PersistenceError::InvalidData(format!("Failed to parse ISO 8601 timestamp: {}", e))
    })?;

    let millis = dt.timestamp_millis() as u64;
    Ok(millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_genesis_config() {
        // Create a temporary genesis config file
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let config_path = temp_dir.path().join("genesis.json");

        let config_content = r#"{
            "chain_id": "test-chain",
            "genesis_time": 1733011200,
            "protocol_version": "1.0",
            "validators": [
                {
                    "address": "validator_1_address",
                    "protocol_pubkey": "0x1111111111111111111111111111111111111111111111111111111111111111",
                    "network_pubkey": "0x2222222222222222222222222222222222222222222222222222222222222222",
                    "worker_pubkey": "0x3333333333333333333333333333333333333333333333333333333333333333",
                    "stake_amount": 1000000000000000,
                    "network_address": "/ip4/127.0.0.1/tcp/9000",
                    "p2p_address": "/ip4/127.0.0.1/tcp/9000",
                    "name": "TestValidator",
                    "commission_rate": 0.05
                }
            ],
            "initial_supply": 1000000000000000000,
            "initial_accounts": [],
            "consensus_config": {
                "timeout_ms": 1000,
                "min_validators": 1
            },
            "fuel_config": {
                "base_cost": 1000,
                "max_per_block": 1000000
            },
            "network_config": {
                "name": "test",
                "network_id": 1
            }
        }"#;

        fs::write(&config_path, config_content).expect("Failed to write config file");

        let config = GenesisBlockManagerImpl::load_genesis_config(config_path.to_str().unwrap())
            .expect("Failed to load genesis config");

        assert_eq!(config.chain_id, "test-chain");
        assert_eq!(config.validators.len(), 1);
        assert_eq!(config.initial_supply, 1000000000000000000);
    }

    #[test]
    fn test_validate_genesis_config_valid() {
        let config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200, // Unix seconds for 2024-12-01T00:00:00Z
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        GenesisBlockManagerImpl::validate_genesis_config(&config)
            .expect("Validation should pass for valid config");
    }

    #[test]
    fn test_validate_genesis_config_empty_chain_id() {
        let mut config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        config.chain_id = String::new();

        let result = GenesisBlockManagerImpl::validate_genesis_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_genesis_config_empty_validators() {
        let mut config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        config.validators = vec![];

        let result = GenesisBlockManagerImpl::validate_genesis_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_genesis_config_zero_supply() {
        let mut config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        config.initial_supply = 0;

        let result = GenesisBlockManagerImpl::validate_genesis_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_genesis_config_insufficient_stake() {
        let mut config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        config.validators[0].stake_amount = 100; // Less than minimum

        let result = GenesisBlockManagerImpl::validate_genesis_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_genesis_block() {
        let config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        let block = GenesisBlockManagerImpl::create_genesis_block(&config)
            .expect("Failed to create genesis block");

        assert_eq!(block.number, 0);
        assert_eq!(block.parent_hash, [0u8; 32]);
        assert_eq!(block.validator, [0u8; 32]);
        assert_eq!(block.transactions.len(), 0);
        assert_eq!(block.gas_used, 0);
        assert_eq!(block.gas_limit, 0);
        assert_ne!(block.hash, [0u8; 32]); // Hash should not be all zeros
    }

    #[test]
    fn test_genesis_block_idempotence() {
        let config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        let block1 = GenesisBlockManagerImpl::create_genesis_block(&config)
            .expect("Failed to create genesis block");
        let block2 = GenesisBlockManagerImpl::create_genesis_block(&config)
            .expect("Failed to create genesis block");

        assert_eq!(block1.hash, block2.hash);
        assert_eq!(block1.timestamp, block2.timestamp);
    }

    #[test]
    fn test_persist_and_load_genesis_block() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = crate::RocksDatabase::open(temp_dir.path())
            .expect("Failed to open database");

        let config = GenesisConfig {
            chain_id: "test-chain".to_string(),
            genesis_time: 1733011200,
            protocol_version: "1.0".to_string(),
            validators: vec![ValidatorConfig {
                address: "validator_1_address".to_string(),
                protocol_pubkey: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                network_pubkey: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                worker_pubkey: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
                stake_amount: 1000000000000000,
                network_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                p2p_address: "/ip4/127.0.0.1/tcp/9000".to_string(),
                name: "TestValidator".to_string(),
                commission_rate: 0.05,
            }],
            initial_supply: 1000000000000000000,
            initial_accounts: vec![],
            consensus_config: ConsensusConfig {
                timeout_ms: 1000,
                min_validators: 1,
            },
            fuel_config: FuelConfig {
                base_cost: 1000,
                max_per_block: 1000000,
            },
            network_config: NetworkConfig {
                name: "test".to_string(),
                network_id: 1,
            },
        };

        let block = GenesisBlockManagerImpl::create_genesis_block(&config)
            .expect("Failed to create genesis block");

        GenesisBlockManagerImpl::persist_genesis_block(&block, &db)
            .expect("Failed to persist genesis block");

        let loaded = GenesisBlockManagerImpl::load_genesis_block(&db)
            .expect("Failed to load genesis block");

        assert!(loaded.is_some());
        let loaded_block = loaded.unwrap();
        assert_eq!(loaded_block.hash, block.hash);
        assert_eq!(loaded_block.number, block.number);
        assert_eq!(loaded_block.timestamp, block.timestamp);
    }

    #[test]
    fn test_load_nonexistent_genesis_block() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let db = crate::RocksDatabase::open(temp_dir.path())
            .expect("Failed to open database");

        let loaded = GenesisBlockManagerImpl::load_genesis_block(&db)
            .expect("Failed to load genesis block");

        assert!(loaded.is_none());
    }
}
