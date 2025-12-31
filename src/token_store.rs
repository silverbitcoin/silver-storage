//! Token storage and state management

use crate::error::Result;
use silver_core::{
    SilverAddress, TokenApprovalEvent, TokenBurnEvent, TokenMetadata, TokenMintEvent, TokenState,
    TokenTransferEvent, TransactionDigest,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, error};

/// Token contract storage
#[derive(Clone)]
pub struct TokenStore {
    /// Token states: contract_address -> TokenState
    tokens: Arc<RwLock<HashMap<SilverAddress, TokenState>>>,
    /// Transfer events log
    transfer_events: Arc<RwLock<Vec<TokenTransferEvent>>>,
    /// Approval events log
    approval_events: Arc<RwLock<Vec<TokenApprovalEvent>>>,
    /// Mint events log
    mint_events: Arc<RwLock<Vec<TokenMintEvent>>>,
    /// Burn events log
    burn_events: Arc<RwLock<Vec<TokenBurnEvent>>>,
}

impl TokenStore {
    /// Create a new token store
    pub fn new() -> Self {
        Self {
            tokens: Arc::new(RwLock::new(HashMap::new())),
            transfer_events: Arc::new(RwLock::new(Vec::new())),
            approval_events: Arc::new(RwLock::new(Vec::new())),
            mint_events: Arc::new(RwLock::new(Vec::new())),
            burn_events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a new token contract
    pub fn create_token(
        &self,
        contract_address: SilverAddress,
        name: String,
        symbol: String,
        decimals: u8,
        initial_supply: u128,
        creator: SilverAddress,
        created_at: u64,
    ) -> Result<TokenMetadata> {
        let mut tokens = self.tokens.write().map_err(|e| {
            error!("Failed to acquire write lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        if tokens.contains_key(&contract_address) {
            return Err(crate::error::Error::InvalidData(format!(
                "Token contract {} already exists",
                contract_address
            )));
        }

        let state = TokenState::new(
            name,
            symbol,
            decimals,
            initial_supply,
            creator,
            contract_address,
            created_at,
        );

        let metadata = state.metadata.clone();
        tokens.insert(contract_address, state);

        debug!(
            "Created token contract: {} ({})",
            contract_address, metadata.symbol
        );

        Ok(metadata)
    }

    /// Get token metadata
    pub fn get_token_metadata(&self, contract_address: &SilverAddress) -> Result<TokenMetadata> {
        let tokens = self.tokens.read().map_err(|e| {
            error!("Failed to acquire read lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        tokens
            .get(contract_address)
            .map(|state| state.metadata.clone())
            .ok_or_else(|| {
                crate::error::Error::NotFound(format!(
                    "Token contract {} not found",
                    contract_address
                ))
            })
    }

    /// Get token balance
    pub fn get_balance(
        &self,
        contract_address: &SilverAddress,
        account: &SilverAddress,
    ) -> Result<u128> {
        let tokens = self.tokens.read().map_err(|e| {
            error!("Failed to acquire read lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        let state = tokens.get(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        Ok(state.balance_of(account))
    }

    /// Get token allowance
    pub fn get_allowance(
        &self,
        contract_address: &SilverAddress,
        owner: &SilverAddress,
        spender: &SilverAddress,
    ) -> Result<u128> {
        let tokens = self.tokens.read().map_err(|e| {
            error!("Failed to acquire read lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        let state = tokens.get(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        Ok(state.allowance(owner, spender))
    }

    /// Transfer tokens
    pub fn transfer(
        &self,
        contract_address: &SilverAddress,
        from: &SilverAddress,
        to: &SilverAddress,
        amount: u128,
        tx_digest: TransactionDigest,
        block_number: u64,
    ) -> Result<()> {
        let mut tokens = self.tokens.write().map_err(|e| {
            error!("Failed to acquire write lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        let state = tokens.get_mut(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        state
            .transfer(from, to, amount)
            .map_err(|e| crate::error::Error::InvalidData(format!("Transfer failed: {}", e)))?;

        let event = TokenTransferEvent {
            token: *contract_address,
            from: *from,
            to: *to,
            amount,
            tx_digest,
            block_number,
            event_index: 0,
        };

        let mut events = self.transfer_events.write().map_err(|e| {
            error!("Failed to acquire write lock on transfer events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        events.push(event);

        debug!("Token transfer: {} -> {} amount: {}", from, to, amount);

        Ok(())
    }

    /// Transfer tokens from (using allowance)
    pub fn transfer_from(
        &self,
        contract_address: &SilverAddress,
        owner: &SilverAddress,
        spender: &SilverAddress,
        to: &SilverAddress,
        amount: u128,
        tx_digest: TransactionDigest,
        block_number: u64,
    ) -> Result<()> {
        let mut tokens = self.tokens.write().map_err(|e| {
            error!("Failed to acquire write lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        let state = tokens.get_mut(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        state
            .transfer_from(owner, spender, to, amount)
            .map_err(|e| {
                crate::error::Error::InvalidData(format!("Transfer from failed: {}", e))
            })?;

        let event = TokenTransferEvent {
            token: *contract_address,
            from: *owner,
            to: *to,
            amount,
            tx_digest,
            block_number,
            event_index: 0,
        };

        let mut events = self.transfer_events.write().map_err(|e| {
            error!("Failed to acquire write lock on transfer events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        events.push(event);

        debug!(
            "Token transfer from: {} -> {} amount: {}",
            owner, to, amount
        );

        Ok(())
    }

    /// Approve spender
    pub fn approve(
        &self,
        contract_address: &SilverAddress,
        owner: &SilverAddress,
        spender: &SilverAddress,
        amount: u128,
        tx_digest: TransactionDigest,
        block_number: u64,
    ) -> Result<()> {
        let mut tokens = self.tokens.write().map_err(|e| {
            error!("Failed to acquire write lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        let state = tokens.get_mut(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        state
            .approve(owner, spender, amount)
            .map_err(|e| crate::error::Error::InvalidData(format!("Approval failed: {}", e)))?;

        let event = TokenApprovalEvent {
            token: *contract_address,
            owner: *owner,
            spender: *spender,
            amount,
            tx_digest,
            block_number,
            event_index: 0,
        };

        let mut events = self.approval_events.write().map_err(|e| {
            error!("Failed to acquire write lock on approval events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        events.push(event);

        debug!(
            "Token approval: {} -> {} amount: {}",
            owner, spender, amount
        );

        Ok(())
    }

    /// Mint tokens
    pub fn mint(
        &self,
        contract_address: &SilverAddress,
        to: &SilverAddress,
        amount: u128,
        tx_digest: TransactionDigest,
        block_number: u64,
    ) -> Result<()> {
        let mut tokens = self.tokens.write().map_err(|e| {
            error!("Failed to acquire write lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        let state = tokens.get_mut(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        state
            .mint(to, amount)
            .map_err(|e| crate::error::Error::InvalidData(format!("Mint failed: {}", e)))?;

        let event = TokenMintEvent {
            token: *contract_address,
            to: *to,
            amount,
            tx_digest,
            block_number,
            event_index: 0,
        };

        let mut events = self.mint_events.write().map_err(|e| {
            error!("Failed to acquire write lock on mint events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        events.push(event);

        debug!("Token minted: {} amount: {}", to, amount);

        Ok(())
    }

    /// Burn tokens
    pub fn burn(
        &self,
        contract_address: &SilverAddress,
        from: &SilverAddress,
        amount: u128,
        tx_digest: TransactionDigest,
        block_number: u64,
    ) -> Result<()> {
        let mut tokens = self.tokens.write().map_err(|e| {
            error!("Failed to acquire write lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        let state = tokens.get_mut(contract_address).ok_or_else(|| {
            crate::error::Error::NotFound(format!("Token contract {} not found", contract_address))
        })?;

        state
            .burn(from, amount)
            .map_err(|e| crate::error::Error::InvalidData(format!("Burn failed: {}", e)))?;

        let event = TokenBurnEvent {
            token: *contract_address,
            from: *from,
            amount,
            tx_digest,
            block_number,
            event_index: 0,
        };

        let mut events = self.burn_events.write().map_err(|e| {
            error!("Failed to acquire write lock on burn events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire write lock".to_string())
        })?;

        events.push(event);

        debug!("Token burned: {} amount: {}", from, amount);

        Ok(())
    }

    /// Get transfer events for a token
    pub fn get_transfer_events(
        &self,
        contract_address: &SilverAddress,
        limit: usize,
    ) -> Result<Vec<TokenTransferEvent>> {
        let events = self.transfer_events.read().map_err(|e| {
            error!("Failed to acquire read lock on transfer events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        Ok(events
            .iter()
            .filter(|e| e.token == *contract_address)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Get approval events for a token
    pub fn get_approval_events(
        &self,
        contract_address: &SilverAddress,
        limit: usize,
    ) -> Result<Vec<TokenApprovalEvent>> {
        let events = self.approval_events.read().map_err(|e| {
            error!("Failed to acquire read lock on approval events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        Ok(events
            .iter()
            .filter(|e| e.token == *contract_address)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Get mint events for a token
    pub fn get_mint_events(
        &self,
        contract_address: &SilverAddress,
        limit: usize,
    ) -> Result<Vec<TokenMintEvent>> {
        let events = self.mint_events.read().map_err(|e| {
            error!("Failed to acquire read lock on mint events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        Ok(events
            .iter()
            .filter(|e| e.token == *contract_address)
            .take(limit)
            .cloned()
            .collect())
    }

    /// Get burn events for a token
    pub fn get_burn_events(
        &self,
        contract_address: &SilverAddress,
        limit: usize,
    ) -> Result<Vec<TokenBurnEvent>> {
        let events = self.burn_events.read().map_err(|e| {
            error!("Failed to acquire read lock on burn events: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        Ok(events
            .iter()
            .filter(|e| e.token == *contract_address)
            .take(limit)
            .cloned()
            .collect())
    }

    /// List all token contracts
    pub fn list_tokens(&self) -> Result<Vec<TokenMetadata>> {
        let tokens = self.tokens.read().map_err(|e| {
            error!("Failed to acquire read lock on tokens: {}", e);
            crate::error::Error::InvalidData("Failed to acquire read lock".to_string())
        })?;

        Ok(tokens
            .values()
            .map(|state| state.metadata.clone())
            .collect())
    }
}

impl Default for TokenStore {
    fn default() -> Self {
        Self::new()
    }
}
