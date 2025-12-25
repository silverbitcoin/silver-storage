//! Wallet storage with ParityDB backend
//!
//! Production-ready implementation for wallet and account management:
//! - Wallet metadata storage
//! - Account information and balances
//! - Key management (encrypted)
//! - Wallet statistics and caching
//! - Multi-wallet support

use crate::db::{ParityDatabase, CF_ACCOUNT_STATE};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;
use tracing::{debug, info};

/// Wallet information and metadata
///
/// Stores comprehensive information about a blockchain wallet including
/// balance, transaction count, key pool size, and encryption status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletInfo {
    /// Wallet name/identifier
    pub wallet_name: String,
    /// Wallet version
    pub wallet_version: u32,
    /// Current balance in satoshis
    pub balance: u128,
    /// Unconfirmed balance in satoshis
    pub unconfirmed_balance: u128,
    /// Immature balance (coinbase) in satoshis
    pub immature_balance: u128,
    /// Total transaction count
    pub txcount: u64,
    /// Size of key pool
    pub keypoolsize: u32,
    /// Size of HD internal key pool
    pub keypoolsize_hd_internal: u32,
    /// Wallet is encrypted
    pub encrypted: bool,
    /// Wallet is locked (if encrypted)
    pub locked: bool,
    /// Timestamp when wallet was created
    pub created_at: u64,
    /// Timestamp of last transaction
    pub last_transaction: u64,
}

impl WalletInfo {
    /// Create a new wallet info
    pub fn new(wallet_name: String) -> Self {
        Self {
            wallet_name,
            wallet_version: 1,
            balance: 0,
            unconfirmed_balance: 0,
            immature_balance: 0,
            txcount: 0,
            keypoolsize: 1000,
            keypoolsize_hd_internal: 1000,
            encrypted: false,
            locked: false,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            last_transaction: 0,
        }
    }

    /// Get the storage key for this wallet
    pub fn storage_key(&self) -> String {
        format!("wallet:info:{}", self.wallet_name)
    }
}

/// Account information
///
/// Represents a named account within a wallet with its own balance
/// and address list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    /// Account name
    pub account_name: String,
    /// Wallet this account belongs to
    pub wallet_name: String,
    /// Current balance in satoshis
    pub balance: u128,
    /// Number of addresses in account
    pub address_count: u64,
    /// Total transactions for account
    pub tx_count: u64,
    /// Timestamp when account was created
    pub created_at: u64,
}

impl AccountInfo {
    /// Create a new account info
    pub fn new(account_name: String, wallet_name: String) -> Self {
        Self {
            account_name,
            wallet_name,
            balance: 0,
            address_count: 0,
            tx_count: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Get the storage key for this account
    pub fn storage_key(&self) -> String {
        format!("wallet:account:{}:{}", self.wallet_name, self.account_name)
    }
}

/// Key information (encrypted)
///
/// Stores encrypted private key information for wallet addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    /// Address this key corresponds to
    pub address: Vec<u8>,
    /// Encrypted private key (never stored unencrypted)
    pub encrypted_key: Vec<u8>,
    /// Key derivation path (for HD wallets)
    pub derivation_path: Option<String>,
    /// Timestamp when key was created
    pub created_at: u64,
    /// Whether this is a change address
    pub is_change: bool,
}

impl KeyInfo {
    /// Create a new key info
    pub fn new(address: Vec<u8>, encrypted_key: Vec<u8>) -> Self {
        Self {
            address,
            encrypted_key,
            derivation_path: None,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            is_change: false,
        }
    }

    /// Get the storage key for this key
    pub fn storage_key(&self) -> String {
        format!("wallet:key:{}", hex::encode(&self.address))
    }
}

/// Wallet Store with ParityDB backend
///
/// Provides persistent storage for wallet information with:
/// - Wallet metadata storage
/// - Account information and balances
/// - Key management (encrypted)
/// - Wallet statistics with caching
/// - Multi-wallet support
pub struct WalletStore {
    db: Arc<ParityDatabase>,
    /// Cache for wallet information
    wallet_cache: Arc<RwLock<HashMap<String, WalletInfo>>>,
    /// Cache for account information
    account_cache: Arc<RwLock<HashMap<String, AccountInfo>>>,
}

impl WalletStore {
    /// Create new wallet store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing WalletStore with ParityDB backend");
        Self {
            db,
            wallet_cache: Arc::new(RwLock::new(HashMap::new())),
            account_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store wallet information
    ///
    /// # Arguments
    /// * `info` - Wallet information to store
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if storage fails
    pub fn store_wallet_info(&self, info: &WalletInfo) -> Result<()> {
        debug!("Storing wallet info: {}", info.wallet_name);

        // Serialize wallet info
        let info_data = serde_json::to_vec(info)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(CF_ACCOUNT_STATE, info.storage_key().as_bytes(), &info_data)?;

        // Update cache
        self.wallet_cache.write().insert(info.wallet_name.clone(), info.clone());

        // Add to wallet index
        let wallet_index_key = "wallet:index:all";
        let mut wallet_list = self.get_wallet_list()?;
        if !wallet_list.contains(&info.wallet_name) {
            wallet_list.push(info.wallet_name.clone());
            let list_data = serde_json::to_vec(&wallet_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, wallet_index_key.as_bytes(), &list_data)?;
        }

        info!("Wallet info stored: {}", info.wallet_name);
        Ok(())
    }

    /// Get wallet information
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name to query
    ///
    /// # Returns
    /// * `Ok(Some(WalletInfo))` if found
    /// * `Ok(None)` if not found
    /// * `Err(Error)` if retrieval fails
    pub fn get_wallet_info(&self, wallet_name: &str) -> Result<Option<WalletInfo>> {
        debug!("Retrieving wallet info: {}", wallet_name);

        // Check cache first
        if let Some(info) = self.wallet_cache.read().get(wallet_name) {
            return Ok(Some(info.clone()));
        }

        // Query database
        let key = format!("wallet:info:{}", wallet_name);
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let info: WalletInfo = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                
                // Update cache
                self.wallet_cache.write().insert(wallet_name.to_string(), info.clone());
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    /// List all wallets
    ///
    /// # Returns
    /// * `Ok(Vec<WalletInfo>)` - List of all wallets
    /// * `Err(Error)` if operation fails
    pub fn list_wallets(&self) -> Result<Vec<WalletInfo>> {
        debug!("Listing all wallets");

        let wallet_names = self.get_wallet_list()?;
        let mut result = Vec::new();

        for name in wallet_names {
            if let Some(info) = self.get_wallet_info(&name)? {
                result.push(info);
            }
        }

        info!("Found {} wallets", result.len());
        Ok(result)
    }

    /// Create a new account in a wallet
    ///
    /// # Arguments
    /// * `account` - Account information to create
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if creation fails
    pub fn create_account(&self, account: &AccountInfo) -> Result<()> {
        debug!("Creating account: {} in wallet: {}", account.account_name, account.wallet_name);

        // Check if wallet exists
        if self.get_wallet_info(&account.wallet_name)?.is_none() {
            return Err(Error::NotFound(format!("Wallet not found: {}", account.wallet_name)));
        }

        // Serialize account info
        let account_data = serde_json::to_vec(account)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(CF_ACCOUNT_STATE, account.storage_key().as_bytes(), &account_data)?;

        // Update cache
        let cache_key = format!("{}:{}", account.wallet_name, account.account_name);
        self.account_cache.write().insert(cache_key, account.clone());

        // Add to account index
        let account_index_key = format!("wallet:account_index:{}", account.wallet_name);
        let mut account_list = self.get_account_list(&account.wallet_name)?;
        if !account_list.contains(&account.account_name) {
            account_list.push(account.account_name.clone());
            let list_data = serde_json::to_vec(&account_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, account_index_key.as_bytes(), &list_data)?;
        }

        info!("Account created: {} in wallet: {}", account.account_name, account.wallet_name);
        Ok(())
    }

    /// Get account information
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name
    /// * `account_name` - Account name
    ///
    /// # Returns
    /// * `Ok(Some(AccountInfo))` if found
    /// * `Ok(None)` if not found
    /// * `Err(Error)` if retrieval fails
    pub fn get_account(&self, wallet_name: &str, account_name: &str) -> Result<Option<AccountInfo>> {
        debug!("Retrieving account: {} from wallet: {}", account_name, wallet_name);

        let cache_key = format!("{}:{}", wallet_name, account_name);

        // Check cache first
        if let Some(info) = self.account_cache.read().get(&cache_key) {
            return Ok(Some(info.clone()));
        }

        // Query database
        let key = format!("wallet:account:{}:{}", wallet_name, account_name);
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let info: AccountInfo = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                
                // Update cache
                self.account_cache.write().insert(cache_key, info.clone());
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    /// List all accounts in a wallet
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name
    ///
    /// # Returns
    /// * `Ok(Vec<AccountInfo>)` - List of accounts
    /// * `Err(Error)` if operation fails
    pub fn list_accounts(&self, wallet_name: &str) -> Result<Vec<AccountInfo>> {
        debug!("Listing accounts for wallet: {}", wallet_name);

        let account_names = self.get_account_list(wallet_name)?;
        let mut result = Vec::new();

        for name in account_names {
            if let Some(info) = self.get_account(wallet_name, &name)? {
                result.push(info);
            }
        }

        info!("Found {} accounts in wallet: {}", result.len(), wallet_name);
        Ok(result)
    }

    /// Get account balance
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name
    /// * `account_name` - Account name
    ///
    /// # Returns
    /// * `Ok(u128)` - Balance in satoshis
    /// * `Err(Error)` if operation fails
    pub fn get_account_balance(&self, wallet_name: &str, account_name: &str) -> Result<u128> {
        debug!("Getting balance for account: {} in wallet: {}", account_name, wallet_name);

        if let Some(account) = self.get_account(wallet_name, account_name)? {
            Ok(account.balance)
        } else {
            Ok(0)
        }
    }

    /// Update account balance
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name
    /// * `account_name` - Account name
    /// * `new_balance` - New balance in satoshis
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn update_account_balance(&self, wallet_name: &str, account_name: &str, new_balance: u128) -> Result<()> {
        debug!("Updating balance for account: {} in wallet: {}", account_name, wallet_name);

        if let Some(mut account) = self.get_account(wallet_name, account_name)? {
            account.balance = new_balance;
            
            // Serialize and store
            let account_data = serde_json::to_vec(&account)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, account.storage_key().as_bytes(), &account_data)?;

            // Update cache
            let cache_key = format!("{}:{}", wallet_name, account_name);
            self.account_cache.write().insert(cache_key, account);

            info!("Account balance updated: {} in wallet: {}", account_name, wallet_name);
            Ok(())
        } else {
            Err(Error::NotFound(format!("Account not found: {}", account_name)))
        }
    }

    /// Store encrypted key
    ///
    /// # Arguments
    /// * `key_info` - Key information to store
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if storage fails
    pub fn store_key(&self, key_info: &KeyInfo) -> Result<()> {
        debug!("Storing encrypted key for address: {}", hex::encode(&key_info.address));

        // Serialize key info
        let key_data = serde_json::to_vec(key_info)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(CF_ACCOUNT_STATE, key_info.storage_key().as_bytes(), &key_data)?;

        // Add to key index
        let key_index_key = "wallet:key_index:all";
        let mut key_list = self.get_key_list()?;
        let address_hex = hex::encode(&key_info.address);
        if !key_list.contains(&address_hex) {
            key_list.push(address_hex);
            let list_data = serde_json::to_vec(&key_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, key_index_key.as_bytes(), &list_data)?;
        }

        info!("Key stored for address: {}", hex::encode(&key_info.address));
        Ok(())
    }

    /// Get encrypted key
    ///
    /// # Arguments
    /// * `address` - Address to query
    ///
    /// # Returns
    /// * `Ok(Some(KeyInfo))` if found
    /// * `Ok(None)` if not found
    /// * `Err(Error)` if retrieval fails
    pub fn get_key(&self, address: &[u8]) -> Result<Option<KeyInfo>> {
        debug!("Retrieving key for address: {}", hex::encode(address));

        let key = format!("wallet:key:{}", hex::encode(address));
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let key_info: KeyInfo = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(Some(key_info))
            }
            None => Ok(None),
        }
    }

    /// List all keys
    ///
    /// # Returns
    /// * `Ok(Vec<KeyInfo>)` - List of all keys
    /// * `Err(Error)` if operation fails
    pub fn list_keys(&self) -> Result<Vec<KeyInfo>> {
        debug!("Listing all keys");

        let key_addresses = self.get_key_list()?;
        let mut result = Vec::new();

        for address_hex in key_addresses {
            if let Ok(address) = hex::decode(&address_hex) {
                if let Some(key_info) = self.get_key(&address)? {
                    result.push(key_info);
                }
            }
        }

        info!("Found {} keys", result.len());
        Ok(result)
    }

    /// Update wallet balance
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name
    /// * `new_balance` - New balance in satoshis
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn update_wallet_balance(&self, wallet_name: &str, new_balance: u128) -> Result<()> {
        debug!("Updating balance for wallet: {}", wallet_name);

        if let Some(mut info) = self.get_wallet_info(wallet_name)? {
            info.balance = new_balance;
            self.store_wallet_info(&info)?;
            Ok(())
        } else {
            Err(Error::NotFound(format!("Wallet not found: {}", wallet_name)))
        }
    }

    /// Increment wallet transaction count
    ///
    /// # Arguments
    /// * `wallet_name` - Wallet name
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn increment_tx_count(&self, wallet_name: &str) -> Result<()> {
        debug!("Incrementing transaction count for wallet: {}", wallet_name);

        if let Some(mut info) = self.get_wallet_info(wallet_name)? {
            info.txcount += 1;
            info.last_transaction = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            self.store_wallet_info(&info)?;
            Ok(())
        } else {
            Err(Error::NotFound(format!("Wallet not found: {}", wallet_name)))
        }
    }

    // Helper method to get wallet list
    fn get_wallet_list(&self) -> Result<Vec<String>> {
        let wallet_index_key = "wallet:index:all";
        match self.db.get(CF_ACCOUNT_STATE, wallet_index_key.as_bytes())? {
            Some(data) => {
                let list: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }

    // Helper method to get account list for a wallet
    fn get_account_list(&self, wallet_name: &str) -> Result<Vec<String>> {
        let account_index_key = format!("wallet:account_index:{}", wallet_name);
        match self.db.get(CF_ACCOUNT_STATE, account_index_key.as_bytes())? {
            Some(data) => {
                let list: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }

    // Helper method to get key list
    fn get_key_list(&self) -> Result<Vec<String>> {
        let key_index_key = "wallet:key_index:all";
        match self.db.get(CF_ACCOUNT_STATE, key_index_key.as_bytes())? {
            Some(data) => {
                let list: Vec<String> = serde_json::from_slice(&data)
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
    fn test_wallet_info_creation() {
        let info = WalletInfo::new("default".to_string());
        assert_eq!(info.wallet_name, "default");
        assert_eq!(info.balance, 0);
        assert_eq!(info.txcount, 0);
        assert_eq!(info.encrypted, false);
    }

    #[test]
    fn test_wallet_info_storage_key() {
        let info = WalletInfo::new("default".to_string());
        assert_eq!(info.storage_key(), "wallet:info:default");
    }

    #[test]
    fn test_account_info_creation() {
        let account = AccountInfo::new("mining".to_string(), "default".to_string());
        assert_eq!(account.account_name, "mining");
        assert_eq!(account.wallet_name, "default");
        assert_eq!(account.balance, 0);
    }

    #[test]
    fn test_account_info_storage_key() {
        let account = AccountInfo::new("mining".to_string(), "default".to_string());
        assert_eq!(account.storage_key(), "wallet:account:default:mining");
    }

    #[test]
    fn test_key_info_creation() {
        let key = KeyInfo::new(vec![1, 2, 3], vec![4, 5, 6]);
        assert_eq!(key.address, vec![1, 2, 3]);
        assert_eq!(key.encrypted_key, vec![4, 5, 6]);
        assert_eq!(key.is_change, false);
    }
}
