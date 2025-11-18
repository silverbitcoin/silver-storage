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
            return Err(Error::InvalidData("Attribute key cannot be empty".to_string()));
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
    pub fn get_all_attributes(&self, object_id: &ObjectID) -> Result<HashMap<String, AttributeValue>> {
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
                return Err(Error::InvalidData("Attribute key cannot be empty".to_string()));
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
            self.db
                .batch_put(&mut batch, CF_FLEXIBLE_ATTRIBUTES, &storage_key, &value_bytes);
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
                .batch_delete(&mut batch, CF_FLEXIBLE_ATTRIBUTES, &key_bytes);
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

        debug!("Found {} attribute keys for object {}", keys.len(), object_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (AttributeStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
        let store = AttributeStore::new(db);
        (store, temp_dir)
    }

    fn create_test_object_id(id: u8) -> ObjectID {
        ObjectID::new([id; 64])
    }

    #[test]
    fn test_set_and_get_attribute() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Set string attribute
        store
            .set_attribute(&object_id, "name", AttributeValue::String("Alice".to_string()))
            .unwrap();

        // Get attribute
        let value = store.get_attribute(&object_id, "name").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_string(), Some("Alice"));
    }

    #[test]
    fn test_attribute_types() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // String
        store
            .set_attribute(&object_id, "str", AttributeValue::String("test".to_string()))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "str")
                .unwrap()
                .unwrap()
                .as_string(),
            Some("test")
        );

        // Integer
        store
            .set_attribute(&object_id, "int", AttributeValue::Integer(-42))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "int")
                .unwrap()
                .unwrap()
                .as_integer(),
            Some(-42)
        );

        // Unsigned Integer
        store
            .set_attribute(&object_id, "uint", AttributeValue::UnsignedInteger(100))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "uint")
                .unwrap()
                .unwrap()
                .as_unsigned_integer(),
            Some(100)
        );

        // Boolean
        store
            .set_attribute(&object_id, "bool", AttributeValue::Boolean(true))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "bool")
                .unwrap()
                .unwrap()
                .as_boolean(),
            Some(true)
        );

        // Bytes
        store
            .set_attribute(&object_id, "bytes", AttributeValue::Bytes(vec![1, 2, 3]))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "bytes")
                .unwrap()
                .unwrap()
                .as_bytes(),
            Some(&[1, 2, 3][..])
        );

        // Float
        store
            .set_attribute(&object_id, "float", AttributeValue::Float(3.14))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "float")
                .unwrap()
                .unwrap()
                .as_float(),
            Some(3.14)
        );

        // Null
        store
            .set_attribute(&object_id, "null", AttributeValue::Null)
            .unwrap();
        assert!(store
            .get_attribute(&object_id, "null")
            .unwrap()
            .unwrap()
            .is_null());
    }

    #[test]
    fn test_has_attribute() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Should not exist initially
        assert!(!store.has_attribute(&object_id, "key").unwrap());

        // Set attribute
        store
            .set_attribute(&object_id, "key", AttributeValue::String("value".to_string()))
            .unwrap();

        // Should exist now
        assert!(store.has_attribute(&object_id, "key").unwrap());
    }

    #[test]
    fn test_remove_attribute() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Set attribute
        store
            .set_attribute(&object_id, "key", AttributeValue::String("value".to_string()))
            .unwrap();
        assert!(store.has_attribute(&object_id, "key").unwrap());

        // Remove attribute
        store.remove_attribute(&object_id, "key").unwrap();

        // Should not exist anymore
        assert!(!store.has_attribute(&object_id, "key").unwrap());
    }

    #[test]
    fn test_get_all_attributes() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Set multiple attributes
        store
            .set_attribute(&object_id, "name", AttributeValue::String("Alice".to_string()))
            .unwrap();
        store
            .set_attribute(&object_id, "age", AttributeValue::Integer(30))
            .unwrap();
        store
            .set_attribute(&object_id, "active", AttributeValue::Boolean(true))
            .unwrap();

        // Get all attributes
        let attributes = store.get_all_attributes(&object_id).unwrap();
        assert_eq!(attributes.len(), 3);
        assert!(attributes.contains_key("name"));
        assert!(attributes.contains_key("age"));
        assert!(attributes.contains_key("active"));
    }

    #[test]
    fn test_set_attributes_batch() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        let mut attributes = HashMap::new();
        attributes.insert("key1".to_string(), AttributeValue::String("value1".to_string()));
        attributes.insert("key2".to_string(), AttributeValue::Integer(42));
        attributes.insert("key3".to_string(), AttributeValue::Boolean(false));

        // Set all attributes atomically
        store.set_attributes(&object_id, &attributes).unwrap();

        // Verify all attributes exist
        assert_eq!(
            store
                .get_attribute(&object_id, "key1")
                .unwrap()
                .unwrap()
                .as_string(),
            Some("value1")
        );
        assert_eq!(
            store
                .get_attribute(&object_id, "key2")
                .unwrap()
                .unwrap()
                .as_integer(),
            Some(42)
        );
        assert_eq!(
            store
                .get_attribute(&object_id, "key3")
                .unwrap()
                .unwrap()
                .as_boolean(),
            Some(false)
        );
    }

    #[test]
    fn test_remove_all_attributes() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Set multiple attributes
        store
            .set_attribute(&object_id, "key1", AttributeValue::String("value1".to_string()))
            .unwrap();
        store
            .set_attribute(&object_id, "key2", AttributeValue::Integer(42))
            .unwrap();
        store
            .set_attribute(&object_id, "key3", AttributeValue::Boolean(true))
            .unwrap();

        // Verify attributes exist
        assert_eq!(store.get_attribute_count(&object_id).unwrap(), 3);

        // Remove all attributes
        store.remove_all_attributes(&object_id).unwrap();

        // Verify all attributes are removed
        assert_eq!(store.get_attribute_count(&object_id).unwrap(), 0);
    }

    #[test]
    fn test_get_attribute_keys() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Set multiple attributes
        store
            .set_attribute(&object_id, "name", AttributeValue::String("Alice".to_string()))
            .unwrap();
        store
            .set_attribute(&object_id, "age", AttributeValue::Integer(30))
            .unwrap();

        // Get attribute keys
        let keys = store.get_attribute_keys(&object_id).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
    }

    #[test]
    fn test_update_attribute() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Set initial value
        store
            .set_attribute(&object_id, "counter", AttributeValue::Integer(1))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "counter")
                .unwrap()
                .unwrap()
                .as_integer(),
            Some(1)
        );

        // Update value
        store
            .set_attribute(&object_id, "counter", AttributeValue::Integer(2))
            .unwrap();
        assert_eq!(
            store
                .get_attribute(&object_id, "counter")
                .unwrap()
                .unwrap()
                .as_integer(),
            Some(2)
        );
    }

    #[test]
    fn test_attributes_isolated_by_object() {
        let (store, _temp) = create_test_store();
        let object1 = create_test_object_id(1);
        let object2 = create_test_object_id(2);

        // Set attributes for object1
        store
            .set_attribute(&object1, "key", AttributeValue::String("value1".to_string()))
            .unwrap();

        // Set attributes for object2
        store
            .set_attribute(&object2, "key", AttributeValue::String("value2".to_string()))
            .unwrap();

        // Verify attributes are isolated
        assert_eq!(
            store
                .get_attribute(&object1, "key")
                .unwrap()
                .unwrap()
                .as_string(),
            Some("value1")
        );
        assert_eq!(
            store
                .get_attribute(&object2, "key")
                .unwrap()
                .unwrap()
                .as_string(),
            Some("value2")
        );
    }

    #[test]
    fn test_empty_key_rejected() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        let result = store.set_attribute(&object_id, "", AttributeValue::String("value".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_long_key_rejected() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        let long_key = "a".repeat(257);
        let result = store.set_attribute(&object_id, &long_key, AttributeValue::String("value".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_get_attribute_count() {
        let (store, _temp) = create_test_store();
        let object_id = create_test_object_id(1);

        // Initially 0
        assert_eq!(store.get_attribute_count(&object_id).unwrap(), 0);

        // Add attributes
        store
            .set_attribute(&object_id, "key1", AttributeValue::Integer(1))
            .unwrap();
        store
            .set_attribute(&object_id, "key2", AttributeValue::Integer(2))
            .unwrap();

        // Should be 2
        assert_eq!(store.get_attribute_count(&object_id).unwrap(), 2);
    }
}
