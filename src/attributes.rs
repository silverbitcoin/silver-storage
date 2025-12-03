//! Flexible attributes storage for dynamic object properties
//!
//! This module provides key-value storage for dynamic attributes that can be
//! attached to objects. Attributes are stored separately from objects to allow
//! flexible schema evolution without modifying core object structures.
//!
//! # Features
//! - Dynamic key-value attributes per object
//! - Parent object linking
//! - Efficient attribute queries by object ID
//! - Batch operations for multiple attributes
//! - Attribute removal and updates

use crate::{
    db::{RocksDatabase, CF_FLEXIBLE_ATTRIBUTES},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use silver_core::ObjectID;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Attribute value types
///
/// Supports common data types for flexible attributes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttributeValue {
    /// String value
    String(String),
    /// Integer value (64-bit signed)
    Integer(i64),
    /// Unsigned integer value (64-bit)
    UnsignedInteger(u64),
    /// Boolean value
    Boolean(bool),
    /// Binary data
    Bytes(Vec<u8>),
    /// Floating point value
    Float(f64),
    /// Null/None value
    Null,
}

impl AttributeValue {
    /// Get value as string if it's a string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            AttributeValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get value as integer if it's an integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            AttributeValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Get value as unsigned integer if it's an unsigned integer
    pub fn as_unsigned_integer(&self) -> Option<u64> {
        match self {
            AttributeValue::UnsignedInteger(u) => Some(*u),
            _ => None,
        }
    }

    /// Get value as boolean if it's a boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            AttributeValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Get value as bytes if it's bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            AttributeValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Get value as float if it's a float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            AttributeValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, AttributeValue::Null)
    }
}

/// Attribute store for flexible object attributes
///
/// Provides storage for dynamic key-value attributes that can be attached
/// to any object. Attributes are indexed by object ID for efficient queries.
pub struct AttributeStore {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
}

impl AttributeStore {
    /// Create a new attribute store
    ///
    /// # Arguments
    /// * `db` - Shared reference to the RocksDB database
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing AttributeStore");
        Self { db }
    }

    /// Set an attribute for an object
    ///
    /// If the attribute already exists, it will be overwritten.
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    /// * `key` - Attribute key
    /// * `value` - Attribute value
    ///
    /// # Errors
    /// Returns error if database write fails
    pub fn set_attribute(
        &self,
        object_id: &ObjectID,
        key: &str,
        value: AttributeValue,
    ) -> Result<()> {
        debug!("Setting attribute {} for object {}", key, object_id);

        // Validate key
        if key.is_empty() {
            return Err(Error::InvalidData(
                "Attribute key cannot be empty".to_string(),
            ));
        }

        if key.len() > 256 {
            return Err(Error::InvalidData(format!(
                "Attribute key too long: {} bytes (max 256)",
                key.len()
            )));
        }

        // Create storage key
        let storage_key = self.make_attribute_key(object_id, key);

        // Serialize value
        let value_bytes = bincode::serialize(&value)?;

        // Store attribute
        self.db
            .put(CF_FLEXIBLE_ATTRIBUTES, &storage_key, &value_bytes)?;

        debug!(
            "Attribute {} set for object {} ({} bytes)",
            key,
            object_id,
            value_bytes.len()
        );

        Ok(())
    }

    /// Get an attribute for an object
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    /// * `key` - Attribute key
    ///
    /// # Returns
    /// - `Ok(Some(value))` if attribute exists
    /// - `Ok(None)` if attribute doesn't exist
    /// - `Err` on database or deserialization error
    pub fn get_attribute(&self, object_id: &ObjectID, key: &str) -> Result<Option<AttributeValue>> {
        debug!("Getting attribute {} for object {}", key, object_id);

        let storage_key = self.make_attribute_key(object_id, key);
        let value_bytes = self.db.get(CF_FLEXIBLE_ATTRIBUTES, &storage_key)?;

        match value_bytes {
            Some(bytes) => {
                let value: AttributeValue = bincode::deserialize(&bytes)?;
                debug!("Attribute {} retrieved for object {}", key, object_id);
                Ok(Some(value))
            }
            None => {
                debug!("Attribute {} not found for object {}", key, object_id);
                Ok(None)
            }
        }
    }

    /// Check if an attribute exists
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    /// * `key` - Attribute key
    pub fn has_attribute(&self, object_id: &ObjectID, key: &str) -> Result<bool> {
        let storage_key = self.make_attribute_key(object_id, key);
        self.db.exists(CF_FLEXIBLE_ATTRIBUTES, &storage_key)
    }

    /// Remove an attribute from an object
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    /// * `key` - Attribute key
    ///
    /// # Errors
    /// Returns error if database write fails
    pub fn remove_attribute(&self, object_id: &ObjectID, key: &str) -> Result<()> {
        debug!("Removing attribute {} from object {}", key, object_id);

        let storage_key = self.make_attribute_key(object_id, key);
        self.db.delete(CF_FLEXIBLE_ATTRIBUTES, &storage_key)?;

        debug!("Attribute {} removed from object {}", key, object_id);
        Ok(())
    }

    /// Get all attributes for an object
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    ///
    /// # Returns
    /// HashMap of attribute key-value pairs
    pub fn get_all_attributes(
        &self,
        object_id: &ObjectID,
    ) -> Result<HashMap<String, AttributeValue>> {
        debug!("Getting all attributes for object {}", object_id);

        let prefix = self.make_attribute_prefix(object_id);
        let mut attributes = HashMap::new();

        // Iterate over all attributes with this object ID prefix
        for result in self.db.iter_prefix(CF_FLEXIBLE_ATTRIBUTES, &prefix) {
            let (key_bytes, value_bytes) = result?;

            // Extract attribute key from storage key
            // Storage key format: object_id (64) + key_length (2) + key
            if key_bytes.len() > 66 {
                let key_str = String::from_utf8(key_bytes[66..].to_vec()).map_err(|e| {
                    Error::InvalidData(format!("Invalid UTF-8 in attribute key: {}", e))
                })?;

                let value: AttributeValue = bincode::deserialize(&value_bytes)?;
                attributes.insert(key_str, value);
            }
        }

        debug!(
            "Retrieved {} attributes for object {}",
            attributes.len(),
            object_id
        );

        Ok(attributes)
    }

    /// Set multiple attributes for an object atomically
    ///
    /// All attributes are set in a single atomic operation.
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    /// * `attributes` - HashMap of attribute key-value pairs
    ///
    /// # Errors
    /// Returns error if any attribute is invalid or database write fails.
    /// On error, no attributes are set (atomic operation).
    pub fn set_attributes(
        &self,
        object_id: &ObjectID,
        attributes: &HashMap<String, AttributeValue>,
    ) -> Result<()> {
        if attributes.is_empty() {
            return Ok(());
        }

        info!(
            "Setting {} attributes for object {}",
            attributes.len(),
            object_id
        );

        // Validate all keys first
        for key in attributes.keys() {
            if key.is_empty() {
                return Err(Error::InvalidData(
                    "Attribute key cannot be empty".to_string(),
                ));
            }
            if key.len() > 256 {
                return Err(Error::InvalidData(format!(
                    "Attribute key too long: {} bytes (max 256)",
                    key.len()
                )));
            }
        }

        // Create atomic batch
        let mut batch = self.db.batch();

        for (key, value) in attributes {
            let storage_key = self.make_attribute_key(object_id, key);
            let value_bytes = bincode::serialize(value)?;
            self.db.batch_put(
                &mut batch,
                CF_FLEXIBLE_ATTRIBUTES,
                &storage_key,
                &value_bytes,
            )?;
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!(
            "Set {} attributes for object {} successfully",
            attributes.len(),
            object_id
        );

        Ok(())
    }

    /// Remove all attributes for an object
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    ///
    /// # Errors
    /// Returns error if database write fails
    pub fn remove_all_attributes(&self, object_id: &ObjectID) -> Result<()> {
        debug!("Removing all attributes for object {}", object_id);

        let prefix = self.make_attribute_prefix(object_id);
        let mut batch = self.db.batch();
        let mut count = 0;

        // Collect all keys to delete
        for result in self.db.iter_prefix(CF_FLEXIBLE_ATTRIBUTES, &prefix) {
            let (key_bytes, _) = result?;
            self.db
                .batch_delete(&mut batch, CF_FLEXIBLE_ATTRIBUTES, &key_bytes)?;
            count += 1;
        }

        // Write batch atomically
        if count > 0 {
            self.db.write_batch(batch)?;
        }

        debug!("Removed {} attributes from object {}", count, object_id);
        Ok(())
    }

    /// Get the number of attributes for an object
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    pub fn get_attribute_count(&self, object_id: &ObjectID) -> Result<usize> {
        let prefix = self.make_attribute_prefix(object_id);
        let count = self
            .db
            .iter_prefix(CF_FLEXIBLE_ATTRIBUTES, &prefix)
            .filter_map(|r| r.ok())
            .count();
        Ok(count)
    }

    /// Get all attribute keys for an object
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    ///
    /// # Returns
    /// Vector of attribute keys
    pub fn get_attribute_keys(&self, object_id: &ObjectID) -> Result<Vec<String>> {
        debug!("Getting attribute keys for object {}", object_id);

        let prefix = self.make_attribute_prefix(object_id);
        let mut keys = Vec::new();

        for result in self.db.iter_prefix(CF_FLEXIBLE_ATTRIBUTES, &prefix) {
            let (key_bytes, _) = result?;

            // Extract attribute key from storage key
            if key_bytes.len() > 66 {
                let key_str = String::from_utf8(key_bytes[66..].to_vec()).map_err(|e| {
                    Error::InvalidData(format!("Invalid UTF-8 in attribute key: {}", e))
                })?;
                keys.push(key_str);
            }
        }

        debug!(
            "Found {} attribute keys for object {}",
            keys.len(),
            object_id
        );
        Ok(keys)
    }

    // ========== Private Helper Methods ==========

    /// Create attribute storage key
    ///
    /// Key format: object_id (64 bytes) + key_length (2 bytes) + key
    fn make_attribute_key(&self, object_id: &ObjectID, key: &str) -> Vec<u8> {
        let key_bytes = key.as_bytes();
        let key_len = key_bytes.len() as u16;

        let mut storage_key = Vec::with_capacity(64 + 2 + key_bytes.len());
        storage_key.extend_from_slice(object_id.as_bytes());
        storage_key.extend_from_slice(&key_len.to_be_bytes());
        storage_key.extend_from_slice(key_bytes);

        storage_key
    }

    /// Create attribute prefix for iteration
    ///
    /// Prefix format: object_id (64 bytes)
    fn make_attribute_prefix(&self, object_id: &ObjectID) -> Vec<u8> {
        object_id.as_bytes().to_vec()
    }
}
