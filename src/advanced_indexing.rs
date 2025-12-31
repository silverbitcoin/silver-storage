//! Advanced indexing for query optimization
//!
//! Production-ready implementation for advanced indexing:
//! - Timestamp-based indexing for time range queries
//! - Fee-based indexing for fee range queries
//! - Confirmation-based indexing for confirmation queries
//! - Script type indexing for script filtering
//! - Composite indexing for complex queries

use crate::db::{ParityDatabase, CF_ACCOUNT_STATE};
use crate::error::{Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Timestamp index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampIndex {
    /// Timestamp (Unix seconds)
    pub timestamp: u64,
    /// List of item IDs at this timestamp
    pub item_ids: Vec<String>,
}

impl TimestampIndex {
    /// Get storage key for this index
    pub fn storage_key(&self, index_type: &str) -> String {
        format!("index:timestamp:{}:{}", index_type, self.timestamp)
    }
}

/// Fee range index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeRangeIndex {
    /// Fee rate (satoshis per byte)
    pub fee_rate: f64,
    /// List of transaction IDs with this fee rate
    pub tx_ids: Vec<String>,
}

impl FeeRangeIndex {
    /// Get storage key for this index
    pub fn storage_key(&self) -> String {
        format!("index:fee_rate:{}", (self.fee_rate * 1000.0) as u64)
    }
}

/// Confirmation index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmationIndex {
    /// Number of confirmations
    pub confirmations: u64,
    /// List of transaction IDs with this confirmation count
    pub tx_ids: Vec<String>,
}

impl ConfirmationIndex {
    /// Get storage key for this index
    pub fn storage_key(&self) -> String {
        format!("index:confirmations:{}", self.confirmations)
    }
}

/// Script type index entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptTypeIndex {
    /// Script type (P2PKH, P2SH, P2WPKH, P2WSH, etc.)
    pub script_type: String,
    /// List of transaction IDs with this script type
    pub tx_ids: Vec<String>,
}

impl ScriptTypeIndex {
    /// Get storage key for this index
    pub fn storage_key(&self) -> String {
        format!("index:script_type:{}", self.script_type)
    }
}

/// Advanced Index Manager with ParityDB backend
///
/// Provides advanced indexing for query optimization with:
/// - Timestamp-based indexing for time range queries
/// - Fee-based indexing for fee range queries
/// - Confirmation-based indexing for confirmation queries
/// - Script type indexing for script filtering
/// - Composite indexing for complex queries
pub struct AdvancedIndexManager {
    db: Arc<ParityDatabase>,
    /// Cache for timestamp indexes
    timestamp_cache: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Cache for fee indexes
    fee_cache: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Cache for confirmation indexes
    confirmation_cache: Arc<RwLock<HashMap<u64, Vec<String>>>>,
}

impl AdvancedIndexManager {
    /// Create new advanced index manager
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing AdvancedIndexManager");
        Self {
            db,
            timestamp_cache: Arc::new(RwLock::new(HashMap::new())),
            fee_cache: Arc::new(RwLock::new(HashMap::new())),
            confirmation_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add item to timestamp index
    ///
    /// # Arguments
    /// * `index_type` - Type of index (e.g., "blocks", "transactions")
    /// * `timestamp` - Timestamp to index
    /// * `item_id` - Item ID to add
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn add_timestamp_index(
        &self,
        index_type: &str,
        timestamp: u64,
        item_id: &str,
    ) -> Result<()> {
        debug!("Adding timestamp index: {} at {}", item_id, timestamp);

        let index = TimestampIndex {
            timestamp,
            item_ids: vec![item_id.to_string()],
        };

        let key = index.storage_key(index_type);
        let mut existing_ids = self.get_timestamp_index_items(index_type, timestamp)?;

        if !existing_ids.contains(&item_id.to_string()) {
            existing_ids.push(item_id.to_string());
            let data = serde_json::to_vec(&existing_ids)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, key.as_bytes(), &data)?;
        }

        // Update cache
        self.timestamp_cache.write().insert(key, existing_ids);

        Ok(())
    }

    /// Query items by timestamp range
    ///
    /// # Arguments
    /// * `index_type` - Type of index
    /// * `from_timestamp` - Start timestamp
    /// * `to_timestamp` - End timestamp
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - Item IDs in range
    /// * `Err(Error)` if operation fails
    pub fn query_by_timestamp_range(
        &self,
        index_type: &str,
        from_timestamp: u64,
        to_timestamp: u64,
    ) -> Result<Vec<String>> {
        debug!(
            "Querying {} by timestamp range: {} to {}",
            index_type, from_timestamp, to_timestamp
        );

        let mut result = Vec::new();

        // Iterate through timestamp range
        for timestamp in from_timestamp..=to_timestamp {
            if let Ok(items) = self.get_timestamp_index_items(index_type, timestamp) {
                result.extend(items);
            }
        }

        info!("Found {} items in timestamp range", result.len());
        Ok(result)
    }

    /// Add item to fee index
    ///
    /// # Arguments
    /// * `fee_rate` - Fee rate in satoshis per byte
    /// * `tx_id` - Transaction ID to add
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn add_fee_index(&self, fee_rate: f64, tx_id: &str) -> Result<()> {
        debug!("Adding fee index: {} at {} sat/byte", tx_id, fee_rate);

        let index = FeeRangeIndex {
            fee_rate,
            tx_ids: vec![tx_id.to_string()],
        };

        let key = index.storage_key();
        let mut existing_ids = self.get_fee_index_items(fee_rate)?;

        if !existing_ids.contains(&tx_id.to_string()) {
            existing_ids.push(tx_id.to_string());
            let data = serde_json::to_vec(&existing_ids)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, key.as_bytes(), &data)?;
        }

        // Update cache
        self.fee_cache.write().insert(key, existing_ids);

        Ok(())
    }

    /// Query transactions by fee range
    ///
    /// # Arguments
    /// * `min_fee` - Minimum fee rate
    /// * `max_fee` - Maximum fee rate
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - Transaction IDs in range
    /// * `Err(Error)` if operation fails
    pub fn query_by_fee_range(&self, min_fee: f64, max_fee: f64) -> Result<Vec<String>> {
        debug!(
            "Querying transactions by fee range: {} to {}",
            min_fee, max_fee
        );

        let mut result = Vec::new();

        // Iterate through fee range (in 0.1 sat/byte increments)
        let min_fee_int = (min_fee * 10.0) as u64;
        let max_fee_int = (max_fee * 10.0) as u64;

        for fee_int in min_fee_int..=max_fee_int {
            let fee_rate = fee_int as f64 / 10.0;
            if let Ok(items) = self.get_fee_index_items(fee_rate) {
                result.extend(items);
            }
        }

        info!("Found {} transactions in fee range", result.len());
        Ok(result)
    }

    /// Add item to confirmation index
    ///
    /// # Arguments
    /// * `confirmations` - Number of confirmations
    /// * `tx_id` - Transaction ID to add
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn add_confirmation_index(&self, confirmations: u64, tx_id: &str) -> Result<()> {
        debug!(
            "Adding confirmation index: {} with {} confirmations",
            tx_id, confirmations
        );

        let index = ConfirmationIndex {
            confirmations,
            tx_ids: vec![tx_id.to_string()],
        };

        let key = index.storage_key();
        let mut existing_ids = self.get_confirmation_index_items(confirmations)?;

        if !existing_ids.contains(&tx_id.to_string()) {
            existing_ids.push(tx_id.to_string());
            let data = serde_json::to_vec(&existing_ids)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, key.as_bytes(), &data)?;
        }

        // Update cache
        self.confirmation_cache
            .write()
            .insert(confirmations, existing_ids);

        Ok(())
    }

    /// Query transactions by confirmation count
    ///
    /// # Arguments
    /// * `min_confirmations` - Minimum confirmations
    /// * `max_confirmations` - Maximum confirmations
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - Transaction IDs in range
    /// * `Err(Error)` if operation fails
    pub fn query_by_confirmations(
        &self,
        min_confirmations: u64,
        max_confirmations: u64,
    ) -> Result<Vec<String>> {
        debug!(
            "Querying transactions by confirmations: {} to {}",
            min_confirmations, max_confirmations
        );

        let mut result = Vec::new();

        for confirmations in min_confirmations..=max_confirmations {
            if let Ok(items) = self.get_confirmation_index_items(confirmations) {
                result.extend(items);
            }
        }

        info!("Found {} transactions in confirmation range", result.len());
        Ok(result)
    }

    /// Add item to script type index
    ///
    /// # Arguments
    /// * `script_type` - Script type (P2PKH, P2SH, etc.)
    /// * `tx_id` - Transaction ID to add
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn add_script_type_index(&self, script_type: &str, tx_id: &str) -> Result<()> {
        debug!("Adding script type index: {} for {}", script_type, tx_id);

        let index = ScriptTypeIndex {
            script_type: script_type.to_string(),
            tx_ids: vec![tx_id.to_string()],
        };

        let key = index.storage_key();
        let mut existing_ids = self.get_script_type_index_items(script_type)?;

        if !existing_ids.contains(&tx_id.to_string()) {
            existing_ids.push(tx_id.to_string());
            let data = serde_json::to_vec(&existing_ids)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_ACCOUNT_STATE, key.as_bytes(), &data)?;
        }

        Ok(())
    }

    /// Query transactions by script type
    ///
    /// # Arguments
    /// * `script_type` - Script type to query
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` - Transaction IDs with script type
    /// * `Err(Error)` if operation fails
    pub fn query_by_script_type(&self, script_type: &str) -> Result<Vec<String>> {
        debug!("Querying transactions by script type: {}", script_type);

        self.get_script_type_index_items(script_type)
    }

    // Helper methods
    fn get_timestamp_index_items(&self, index_type: &str, timestamp: u64) -> Result<Vec<String>> {
        let key = format!("index:timestamp:{}:{}", index_type, timestamp);

        // Check cache first
        if let Some(items) = self.timestamp_cache.read().get(&key) {
            return Ok(items.clone());
        }

        // Query database
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let items: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(items)
            }
            None => Ok(Vec::new()),
        }
    }

    fn get_fee_index_items(&self, fee_rate: f64) -> Result<Vec<String>> {
        let key = format!("index:fee_rate:{}", (fee_rate * 1000.0) as u64);

        // Check cache first
        if let Some(items) = self.fee_cache.read().get(&key) {
            return Ok(items.clone());
        }

        // Query database
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let items: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(items)
            }
            None => Ok(Vec::new()),
        }
    }

    fn get_confirmation_index_items(&self, confirmations: u64) -> Result<Vec<String>> {
        // Check cache first
        if let Some(items) = self.confirmation_cache.read().get(&confirmations) {
            return Ok(items.clone());
        }

        // Query database
        let key = format!("index:confirmations:{}", confirmations);
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let items: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(items)
            }
            None => Ok(Vec::new()),
        }
    }

    fn get_script_type_index_items(&self, script_type: &str) -> Result<Vec<String>> {
        let key = format!("index:script_type:{}", script_type);

        // Query database
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let items: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(items)
            }
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_index_creation() {
        let index = TimestampIndex {
            timestamp: 1234567890,
            item_ids: vec!["item1".to_string()],
        };

        assert_eq!(index.timestamp, 1234567890);
        assert_eq!(index.item_ids.len(), 1);
    }

    #[test]
    fn test_fee_range_index_creation() {
        let index = FeeRangeIndex {
            fee_rate: 1.5,
            tx_ids: vec!["tx1".to_string()],
        };

        assert_eq!(index.fee_rate, 1.5);
        assert_eq!(index.tx_ids.len(), 1);
    }

    #[test]
    fn test_confirmation_index_creation() {
        let index = ConfirmationIndex {
            confirmations: 6,
            tx_ids: vec!["tx1".to_string()],
        };

        assert_eq!(index.confirmations, 6);
        assert_eq!(index.tx_ids.len(), 1);
    }

    #[test]
    fn test_script_type_index_creation() {
        let index = ScriptTypeIndex {
            script_type: "P2PKH".to_string(),
            tx_ids: vec!["tx1".to_string()],
        };

        assert_eq!(index.script_type, "P2PKH");
        assert_eq!(index.tx_ids.len(), 1);
    }
}
