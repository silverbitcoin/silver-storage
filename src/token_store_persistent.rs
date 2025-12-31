//! Persistent Token storage with ParityDB backend
//!
//! Production-ready implementation for token contract storage with persistence:
//! - Token metadata storage with full persistence
//! - Token balance tracking per account
//! - Token allowance tracking (for approve/transferFrom)
//! - Token event logging (transfer, approval, mint, burn)
//! - Token search and filtering

use crate::db::{ParityDatabase, CF_ACCOUNT_STATE};
use crate::error::{Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Token metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenMetadata {
    /// Token name
    pub name: String,
    /// Token symbol
    pub symbol: String,
    /// Number of decimal places
    pub decimals: u8,
    /// Total supply in smallest units
    pub total_supply: u128,
    /// Creator/owner address
    pub creator: Vec<u8>,
    /// Contract address
    pub contract_address: Vec<u8>,
    /// Timestamp when created
    pub created_at: u64,
}

impl TokenMetadata {
    /// Create new token metadata
    pub fn new(
        name: String,
        symbol: String,
        decimals: u8,
        total_supply: u128,
        creator: Vec<u8>,
        contract_address: Vec<u8>,
        created_at: u64,
    ) -> Self {
        Self {
            name,
            symbol,
            decimals,
            total_supply,
            creator,
            contract_address,
            created_at,
        }
    }

    /// Get storage key for this token
    pub fn storage_key(&self) -> String {
        format!("token:metadata:{}", hex::encode(&self.contract_address))
    }
}

/// Token balance entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalance {
    /// Contract address
    pub contract_address: Vec<u8>,
    /// Account address
    pub account: Vec<u8>,
    /// Balance amount
    pub balance: u128,
    /// Last updated timestamp
    pub updated_at: u64,
}

impl TokenBalance {
    /// Get storage key for this balance
    pub fn storage_key(&self) -> String {
        format!(
            "token:balance:{}:{}",
            hex::encode(&self.contract_address),
            hex::encode(&self.account)
        )
    }
}

/// Token allowance entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenAllowance {
    /// Contract address
    pub contract_address: Vec<u8>,
    /// Owner address
    pub owner: Vec<u8>,
    /// Spender address
    pub spender: Vec<u8>,
    /// Allowance amount
    pub amount: u128,
    /// Last updated timestamp
    pub updated_at: u64,
}

impl TokenAllowance {
    /// Get storage key for this allowance
    pub fn storage_key(&self) -> String {
        format!(
            "token:allowance:{}:{}:{}",
            hex::encode(&self.contract_address),
            hex::encode(&self.owner),
            hex::encode(&self.spender)
        )
    }
}

/// Token event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenEvent {
    /// Event ID
    pub event_id: u64,
    /// Contract address
    pub contract_address: Vec<u8>,
    /// Event type (Transfer, Approval, Mint, Burn)
    pub event_type: String,
    /// From address (if applicable)
    pub from: Option<Vec<u8>>,
    /// To address (if applicable)
    pub to: Option<Vec<u8>>,
    /// Amount
    pub amount: u128,
    /// Timestamp
    pub timestamp: u64,
}

impl TokenEvent {
    /// Get storage key for this event
    pub fn storage_key(&self) -> String {
        format!(
            "token:event:{}:{}",
            hex::encode(&self.contract_address),
            self.event_id
        )
    }
}

/// Token Store with ParityDB backend
///
/// Provides persistent storage for token contracts with:
/// - Token metadata storage
/// - Token balance tracking per account
/// - Token allowance tracking
/// - Token event logging
/// - Token search and filtering
pub struct TokenStorePersistent {
    db: Arc<ParityDatabase>,
    /// Cache for token metadata
    metadata_cache: Arc<RwLock<HashMap<String, TokenMetadata>>>,
    /// Cache for token balances
    balance_cache: Arc<RwLock<HashMap<String, u128>>>,
    /// Next event ID counter
    #[allow(dead_code)]
    next_event_id: Arc<RwLock<u64>>,
}

impl TokenStorePersistent {
    /// Create new persistent token store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing TokenStorePersistent with ParityDB backend");
        Self {
            db,
            metadata_cache: Arc::new(RwLock::new(HashMap::new())),
            balance_cache: Arc::new(RwLock::new(HashMap::new())),
            next_event_id: Arc::new(RwLock::new(0)),
        }
    }

    /// Create a new token contract
    ///
    /// # Arguments
    /// * `metadata` - Token metadata
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if creation fails
    pub fn create_token(&self, metadata: &TokenMetadata) -> Result<()> {
        debug!("Creating token: {} ({})", metadata.name, metadata.symbol);

        // Serialize metadata
        let metadata_data =
            serde_json::to_vec(metadata).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(
            CF_ACCOUNT_STATE,
            metadata.storage_key().as_bytes(),
            &metadata_data,
        )?;

        // Update cache
        let contract_hex = hex::encode(&metadata.contract_address);
        self.metadata_cache
            .write()
            .insert(contract_hex.clone(), metadata.clone());

        // Add to token index
        let token_index_key = "token:index:all";
        let mut token_list = self.get_token_list()?;
        if !token_list.contains(&contract_hex) {
            token_list.push(contract_hex);
            let list_data = serde_json::to_vec(&token_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db
                .put(CF_ACCOUNT_STATE, token_index_key.as_bytes(), &list_data)?;
        }

        // Initialize creator balance with total supply
        let creator_balance = TokenBalance {
            contract_address: metadata.contract_address.clone(),
            account: metadata.creator.clone(),
            balance: metadata.total_supply,
            updated_at: metadata.created_at,
        };
        self.set_balance(&creator_balance)?;

        info!("Token created: {} ({})", metadata.name, metadata.symbol);
        Ok(())
    }

    /// Get token metadata
    ///
    /// # Arguments
    /// * `contract_address` - Contract address
    ///
    /// # Returns
    /// * `Ok(Some(TokenMetadata))` if found
    /// * `Ok(None)` if not found
    /// * `Err(Error)` if retrieval fails
    pub fn get_token_metadata(&self, contract_address: &[u8]) -> Result<Option<TokenMetadata>> {
        debug!(
            "Retrieving token metadata: {}",
            hex::encode(contract_address)
        );

        let contract_hex = hex::encode(contract_address);

        // Check cache first
        if let Some(metadata) = self.metadata_cache.read().get(&contract_hex) {
            return Ok(Some(metadata.clone()));
        }

        // Query database
        let key = format!("token:metadata:{}", contract_hex);
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let metadata: TokenMetadata = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;

                // Update cache
                self.metadata_cache
                    .write()
                    .insert(contract_hex, metadata.clone());
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Get token balance for an account
    ///
    /// # Arguments
    /// * `contract_address` - Contract address
    /// * `account` - Account address
    ///
    /// # Returns
    /// * `Ok(u128)` - Balance amount
    /// * `Err(Error)` if operation fails
    pub fn get_balance(&self, contract_address: &[u8], account: &[u8]) -> Result<u128> {
        debug!(
            "Getting balance for account: {} in token: {}",
            hex::encode(account),
            hex::encode(contract_address)
        );

        let cache_key = format!("{}:{}", hex::encode(contract_address), hex::encode(account));

        // Check cache first
        if let Some(balance) = self.balance_cache.read().get(&cache_key) {
            return Ok(*balance);
        }

        // Query database
        let key = format!(
            "token:balance:{}:{}",
            hex::encode(contract_address),
            hex::encode(account)
        );
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let balance_entry: TokenBalance = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;

                // Update cache
                self.balance_cache
                    .write()
                    .insert(cache_key, balance_entry.balance);
                Ok(balance_entry.balance)
            }
            None => Ok(0),
        }
    }

    /// Set token balance for an account
    ///
    /// # Arguments
    /// * `balance` - Balance entry to set
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn set_balance(&self, balance: &TokenBalance) -> Result<()> {
        debug!(
            "Setting balance for account: {} in token: {}",
            hex::encode(&balance.account),
            hex::encode(&balance.contract_address)
        );

        // Serialize balance
        let balance_data =
            serde_json::to_vec(balance).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(
            CF_ACCOUNT_STATE,
            balance.storage_key().as_bytes(),
            &balance_data,
        )?;

        // Update cache
        let cache_key = format!(
            "{}:{}",
            hex::encode(&balance.contract_address),
            hex::encode(&balance.account)
        );
        self.balance_cache
            .write()
            .insert(cache_key, balance.balance);

        info!(
            "Balance set for account: {} in token: {}",
            hex::encode(&balance.account),
            hex::encode(&balance.contract_address)
        );
        Ok(())
    }

    /// Get token allowance
    ///
    /// # Arguments
    /// * `contract_address` - Contract address
    /// * `owner` - Owner address
    /// * `spender` - Spender address
    ///
    /// # Returns
    /// * `Ok(u128)` - Allowance amount
    /// * `Err(Error)` if operation fails
    pub fn get_allowance(
        &self,
        contract_address: &[u8],
        owner: &[u8],
        spender: &[u8],
    ) -> Result<u128> {
        debug!(
            "Getting allowance for spender: {} from owner: {} in token: {}",
            hex::encode(spender),
            hex::encode(owner),
            hex::encode(contract_address)
        );

        let key = format!(
            "token:allowance:{}:{}:{}",
            hex::encode(contract_address),
            hex::encode(owner),
            hex::encode(spender)
        );

        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let allowance: TokenAllowance = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(allowance.amount)
            }
            None => Ok(0),
        }
    }

    /// Set token allowance
    ///
    /// # Arguments
    /// * `allowance` - Allowance entry to set
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn set_allowance(&self, allowance: &TokenAllowance) -> Result<()> {
        debug!(
            "Setting allowance for spender: {} from owner: {} in token: {}",
            hex::encode(&allowance.spender),
            hex::encode(&allowance.owner),
            hex::encode(&allowance.contract_address)
        );

        // Serialize allowance
        let allowance_data =
            serde_json::to_vec(allowance).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(
            CF_ACCOUNT_STATE,
            allowance.storage_key().as_bytes(),
            &allowance_data,
        )?;

        info!(
            "Allowance set for spender: {} from owner: {} in token: {}",
            hex::encode(&allowance.spender),
            hex::encode(&allowance.owner),
            hex::encode(&allowance.contract_address)
        );
        Ok(())
    }

    /// Log a token event
    ///
    /// # Arguments
    /// * `event` - Token event to log
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn log_event(&self, event: &TokenEvent) -> Result<()> {
        debug!(
            "Logging token event: {} in token: {}",
            event.event_type,
            hex::encode(&event.contract_address)
        );

        // Serialize event
        let event_data =
            serde_json::to_vec(event).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(
            CF_ACCOUNT_STATE,
            event.storage_key().as_bytes(),
            &event_data,
        )?;

        // Add to event index
        let event_index_key = format!("token:event_index:{}", hex::encode(&event.contract_address));
        let mut event_list = self.get_event_list(&event_index_key)?;
        if !event_list.contains(&event.event_id) {
            event_list.push(event.event_id);
            let list_data = serde_json::to_vec(&event_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db
                .put(CF_ACCOUNT_STATE, event_index_key.as_bytes(), &list_data)?;
        }

        info!(
            "Token event logged: {} in token: {}",
            event.event_type,
            hex::encode(&event.contract_address)
        );
        Ok(())
    }

    /// Get token events
    ///
    /// # Arguments
    /// * `contract_address` - Contract address
    /// * `limit` - Maximum number of events to return
    ///
    /// # Returns
    /// * `Ok(Vec<TokenEvent>)` - Token events
    /// * `Err(Error)` if operation fails
    pub fn get_events(&self, contract_address: &[u8], limit: u32) -> Result<Vec<TokenEvent>> {
        debug!(
            "Getting events for token: {} (limit: {})",
            hex::encode(contract_address),
            limit
        );

        let event_index_key = format!("token:event_index:{}", hex::encode(contract_address));
        let event_ids = self.get_event_list(&event_index_key)?;
        let mut result = Vec::new();

        for event_id in event_ids.iter().rev().take(limit as usize) {
            let key = format!("token:event:{}:{}", hex::encode(contract_address), event_id);
            if let Ok(Some(data)) = self.db.get(CF_ACCOUNT_STATE, key.as_bytes()) {
                if let Ok(event) = serde_json::from_slice::<TokenEvent>(&data) {
                    result.push(event);
                }
            }
        }

        info!("Retrieved {} events for token", result.len());
        Ok(result)
    }

    /// List all tokens
    ///
    /// # Returns
    /// * `Ok(Vec<TokenMetadata>)` - All tokens
    /// * `Err(Error)` if operation fails
    pub fn list_tokens(&self) -> Result<Vec<TokenMetadata>> {
        debug!("Listing all tokens");

        let token_addresses = self.get_token_list()?;
        let mut result = Vec::new();

        for address_hex in token_addresses {
            if let Ok(Some(metadata)) =
                self.get_token_metadata(&hex::decode(&address_hex).unwrap_or_default())
            {
                result.push(metadata);
            }
        }

        info!("Found {} tokens", result.len());
        Ok(result)
    }

    // Helper method to get token list
    fn get_token_list(&self) -> Result<Vec<String>> {
        let token_index_key = "token:index:all";
        match self.db.get(CF_ACCOUNT_STATE, token_index_key.as_bytes())? {
            Some(data) => {
                let list: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }

    // Helper method to get event list
    fn get_event_list(&self, key: &str) -> Result<Vec<u64>> {
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let list: Vec<u64> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_metadata_creation() {
        let metadata = TokenMetadata::new(
            "Test Token".to_string(),
            "TEST".to_string(),
            18,
            1_000_000_000_000_000_000,
            vec![1, 2, 3],
            vec![4, 5, 6],
            1234567890,
        );

        assert_eq!(metadata.name, "Test Token");
        assert_eq!(metadata.symbol, "TEST");
        assert_eq!(metadata.decimals, 18);
    }

    #[test]
    fn test_token_balance_creation() {
        let balance = TokenBalance {
            contract_address: vec![1, 2, 3],
            account: vec![4, 5, 6],
            balance: 1000,
            updated_at: 1234567890,
        };

        assert_eq!(balance.balance, 1000);
    }

    #[test]
    fn test_token_event_creation() {
        let event = TokenEvent {
            event_id: 1,
            contract_address: vec![1, 2, 3],
            event_type: "Transfer".to_string(),
            from: Some(vec![4, 5, 6]),
            to: Some(vec![7, 8, 9]),
            amount: 100,
            timestamp: 1234567890,
        };

        assert_eq!(event.event_type, "Transfer");
        assert_eq!(event.amount, 100);
    }
}
