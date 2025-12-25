//! Persistent Event storage with ParityDB backend
//!
//! Production-ready implementation for event storage with persistence:
//! - Event storage with full persistence to ParityDB
//! - Event indexing by transaction, object, and type
//! - Event archival and pruning
//! - Event pagination support
//! - Event search and filtering

use crate::db::{ParityDatabase, CF_EVENTS};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;
use tracing::{debug, info};

/// Event type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// Object was created
    ObjectCreated,
    /// Object was modified
    ObjectModified,
    /// Object was deleted
    ObjectDeleted,
    /// Object was transferred
    ObjectTransferred,
    /// Object was shared
    ObjectShared,
    /// Object was frozen (made immutable)
    ObjectFrozen,
    /// Coin was split
    CoinSplit,
    /// Coins were merged
    CoinMerged,
    /// Module was published
    ModulePublished,
    /// Function was called
    FunctionCalled,
    /// Custom event from smart contract
    Custom(String),
}

/// Blockchain event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event ID (unique identifier)
    pub event_id: u64,
    /// Transaction that generated this event
    pub transaction_digest: String,
    /// Event type
    pub event_type: EventType,
    /// Object ID related to this event (if applicable)
    pub object_id: Option<String>,
    /// Event data (serialized)
    pub data: Vec<u8>,
    /// Timestamp when event was created (Unix milliseconds)
    pub timestamp: u64,
    /// Event index within transaction
    pub event_index: u32,
}

impl Event {
    /// Create a new event
    pub fn new(
        event_id: u64,
        transaction_digest: String,
        event_type: EventType,
        object_id: Option<String>,
        data: Vec<u8>,
        timestamp: u64,
        event_index: u32,
    ) -> Self {
        Self {
            event_id,
            transaction_digest,
            event_type,
            object_id,
            data,
            timestamp,
            event_index,
        }
    }

    /// Get the storage key for this event
    pub fn storage_key(&self) -> String {
        format!("event:{}", self.event_id)
    }
}

/// Event Store with ParityDB backend
///
/// Provides persistent storage for blockchain events with:
/// - Full persistence to ParityDB
/// - Event indexing by transaction, object, and type
/// - Event archival and pruning
/// - Event pagination support
/// - Event search and filtering
pub struct EventStorePersistent {
    db: Arc<ParityDatabase>,
    /// Next event ID counter
    #[allow(dead_code)]
    next_event_id: Arc<RwLock<u64>>,
    /// Cache for recent events
    event_cache: Arc<RwLock<HashMap<u64, Event>>>,
}

impl EventStorePersistent {
    /// Create new persistent event store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing EventStorePersistent with ParityDB backend");
        Self {
            db,
            next_event_id: Arc::new(RwLock::new(0)),
            event_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store an event persistently
    ///
    /// # Arguments
    /// * `event` - Event to store
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if storage fails
    pub fn store_event(&self, event: &Event) -> Result<()> {
        debug!("Storing event: {} (type: {:?})", event.event_id, event.event_type);

        // Serialize event
        let event_data = serde_json::to_vec(event)
            .map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db.put(CF_EVENTS, event.storage_key().as_bytes(), &event_data)?;

        // Update cache
        self.event_cache.write().insert(event.event_id, event.clone());

        // Add to transaction index
        let tx_index_key = format!("event_tx_index:{}", event.transaction_digest);
        let mut tx_events = self.get_event_list(&tx_index_key)?;
        if !tx_events.contains(&event.event_id) {
            tx_events.push(event.event_id);
            let list_data = serde_json::to_vec(&tx_events)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_EVENTS, tx_index_key.as_bytes(), &list_data)?;
        }

        // Add to object index if applicable
        if let Some(object_id) = &event.object_id {
            let obj_index_key = format!("event_obj_index:{}", object_id);
            let mut obj_events = self.get_event_list(&obj_index_key)?;
            if !obj_events.contains(&event.event_id) {
                obj_events.push(event.event_id);
                let list_data = serde_json::to_vec(&obj_events)
                    .map_err(|e| Error::SerializationError(e.to_string()))?;
                self.db.put(CF_EVENTS, obj_index_key.as_bytes(), &list_data)?;
            }
        }

        // Add to type index
        let type_key = format!("event_type_index:{:?}", event.event_type);
        let mut type_events = self.get_event_list(&type_key)?;
        if !type_events.contains(&event.event_id) {
            type_events.push(event.event_id);
            let list_data = serde_json::to_vec(&type_events)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_EVENTS, type_key.as_bytes(), &list_data)?;
        }

        // Add to global event index
        let global_index_key = "event_index:all";
        let mut all_events = self.get_event_list(global_index_key)?;
        if !all_events.contains(&event.event_id) {
            all_events.push(event.event_id);
            let list_data = serde_json::to_vec(&all_events)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_EVENTS, global_index_key.as_bytes(), &list_data)?;
        }

        info!("Event stored: {} (type: {:?})", event.event_id, event.event_type);
        Ok(())
    }

    /// Get an event by ID
    ///
    /// # Arguments
    /// * `event_id` - Event ID to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(Event))` if found
    /// * `Ok(None)` if not found
    /// * `Err(Error)` if retrieval fails
    pub fn get_event(&self, event_id: u64) -> Result<Option<Event>> {
        debug!("Retrieving event: {}", event_id);

        // Check cache first
        if let Some(event) = self.event_cache.read().get(&event_id) {
            return Ok(Some(event.clone()));
        }

        // Query database
        let key = format!("event:{}", event_id);
        match self.db.get(CF_EVENTS, key.as_bytes())? {
            Some(data) => {
                let event: Event = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                
                // Update cache
                self.event_cache.write().insert(event_id, event.clone());
                Ok(Some(event))
            }
            None => Ok(None),
        }
    }

    /// Get events by transaction
    ///
    /// # Arguments
    /// * `transaction_digest` - Transaction digest to query
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - Events for transaction
    /// * `Err(Error)` if operation fails
    pub fn get_events_by_transaction(&self, transaction_digest: &str) -> Result<Vec<Event>> {
        debug!("Getting events for transaction: {}", transaction_digest);

        let tx_index_key = format!("event_tx_index:{}", transaction_digest);
        let event_ids = self.get_event_list(&tx_index_key)?;
        let mut result = Vec::new();

        for event_id in event_ids {
            if let Some(event) = self.get_event(event_id)? {
                result.push(event);
            }
        }

        info!("Found {} events for transaction", result.len());
        Ok(result)
    }

    /// Get events by object
    ///
    /// # Arguments
    /// * `object_id` - Object ID to query
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - Events for object
    /// * `Err(Error)` if operation fails
    pub fn get_events_by_object(&self, object_id: &str) -> Result<Vec<Event>> {
        debug!("Getting events for object: {}", object_id);

        let obj_index_key = format!("event_obj_index:{}", object_id);
        let event_ids = self.get_event_list(&obj_index_key)?;
        let mut result = Vec::new();

        for event_id in event_ids {
            if let Some(event) = self.get_event(event_id)? {
                result.push(event);
            }
        }

        info!("Found {} events for object", result.len());
        Ok(result)
    }

    /// Get events by type
    ///
    /// # Arguments
    /// * `event_type` - Event type to query
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - Events of type
    /// * `Err(Error)` if operation fails
    pub fn get_events_by_type(&self, event_type: &EventType) -> Result<Vec<Event>> {
        debug!("Getting events of type: {:?}", event_type);

        let type_key = format!("event_type_index:{:?}", event_type);
        let event_ids = self.get_event_list(&type_key)?;
        let mut result = Vec::new();

        for event_id in event_ids {
            if let Some(event) = self.get_event(event_id)? {
                result.push(event);
            }
        }

        info!("Found {} events of type", result.len());
        Ok(result)
    }

    /// Get all events with pagination
    ///
    /// # Arguments
    /// * `page` - Page number (0-based)
    /// * `page_size` - Number of events per page
    ///
    /// # Returns
    /// * `Ok(Vec<Event>)` - Events for page
    /// * `Err(Error)` if operation fails
    pub fn get_events_paginated(&self, page: u32, page_size: u32) -> Result<Vec<Event>> {
        debug!("Getting events page: {}, size: {}", page, page_size);

        let all_event_ids = self.get_event_list("event_index:all")?;
        let start = (page as usize) * (page_size as usize);
        let _end = start + (page_size as usize);

        let mut result = Vec::new();
        for event_id in all_event_ids.iter().skip(start).take(page_size as usize) {
            if let Some(event) = self.get_event(*event_id)? {
                result.push(event);
            }
        }

        info!("Retrieved {} events for page {}", result.len(), page);
        Ok(result)
    }

    /// Get total event count
    ///
    /// # Returns
    /// * `Ok(u64)` - Total number of events
    /// * `Err(Error)` if operation fails
    pub fn get_event_count(&self) -> Result<u64> {
        let all_event_ids = self.get_event_list("event_index:all")?;
        Ok(all_event_ids.len() as u64)
    }

    /// Delete an event
    ///
    /// # Arguments
    /// * `event_id` - Event ID to delete
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if deletion fails
    pub fn delete_event(&self, event_id: u64) -> Result<()> {
        debug!("Deleting event: {}", event_id);

        // Get event first to remove from indexes
        if let Some(event) = self.get_event(event_id)? {
            // Remove from database
            let key = format!("event:{}", event_id);
            self.db.delete(CF_EVENTS, key.as_bytes())?;

            // Remove from cache
            self.event_cache.write().remove(&event_id);

            // Remove from transaction index
            let tx_index_key = format!("event_tx_index:{}", event.transaction_digest);
            let mut tx_events = self.get_event_list(&tx_index_key)?;
            tx_events.retain(|&id| id != event_id);
            if !tx_events.is_empty() {
                let list_data = serde_json::to_vec(&tx_events)
                    .map_err(|e| Error::SerializationError(e.to_string()))?;
                self.db.put(CF_EVENTS, tx_index_key.as_bytes(), &list_data)?;
            } else {
                self.db.delete(CF_EVENTS, tx_index_key.as_bytes())?;
            }

            // Remove from object index if applicable
            if let Some(object_id) = &event.object_id {
                let obj_index_key = format!("event_obj_index:{}", object_id);
                let mut obj_events = self.get_event_list(&obj_index_key)?;
                obj_events.retain(|&id| id != event_id);
                if !obj_events.is_empty() {
                    let list_data = serde_json::to_vec(&obj_events)
                        .map_err(|e| Error::SerializationError(e.to_string()))?;
                    self.db.put(CF_EVENTS, obj_index_key.as_bytes(), &list_data)?;
                } else {
                    self.db.delete(CF_EVENTS, obj_index_key.as_bytes())?;
                }
            }

            // Remove from global index
            let global_index_key = "event_index:all";
            let mut all_events = self.get_event_list(global_index_key)?;
            all_events.retain(|&id| id != event_id);
            let list_data = serde_json::to_vec(&all_events)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db.put(CF_EVENTS, global_index_key.as_bytes(), &list_data)?;

            info!("Event deleted: {}", event_id);
            Ok(())
        } else {
            Err(Error::NotFound(format!("Event not found: {}", event_id)))
        }
    }

    /// Prune old events (keep only recent events)
    ///
    /// # Arguments
    /// * `keep_count` - Number of recent events to keep
    ///
    /// # Returns
    /// * `Ok(u64)` - Number of events deleted
    /// * `Err(Error)` if operation fails
    pub fn prune_old_events(&self, keep_count: u64) -> Result<u64> {
        debug!("Pruning old events, keeping: {}", keep_count);

        let all_event_ids = self.get_event_list("event_index:all")?;
        let total_count = all_event_ids.len() as u64;

        if total_count <= keep_count {
            return Ok(0);
        }

        let delete_count = total_count - keep_count;
        let mut deleted = 0u64;

        // Delete oldest events
        for event_id in all_event_ids.iter().take(delete_count as usize) {
            if self.delete_event(*event_id).is_ok() {
                deleted += 1;
            }
        }

        info!("Pruned {} old events", deleted);
        Ok(deleted)
    }

    // Helper method to get event list
    fn get_event_list(&self, key: &str) -> Result<Vec<u64>> {
        match self.db.get(CF_EVENTS, key.as_bytes())? {
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
    fn test_event_creation() {
        let event = Event::new(
            1,
            "tx123".to_string(),
            EventType::ObjectCreated,
            Some("obj456".to_string()),
            vec![],
            1234567890,
            0,
        );

        assert_eq!(event.event_id, 1);
        assert_eq!(event.transaction_digest, "tx123");
        assert_eq!(event.event_type, EventType::ObjectCreated);
    }

    #[test]
    fn test_event_storage_key() {
        let event = Event::new(
            1,
            "tx123".to_string(),
            EventType::ObjectCreated,
            None,
            vec![],
            1234567890,
            0,
        );

        assert_eq!(event.storage_key(), "event:1");
    }

    #[test]
    fn test_event_type_equality() {
        let type1 = EventType::ObjectCreated;
        let type2 = EventType::ObjectCreated;
        assert_eq!(type1, type2);
    }
}
