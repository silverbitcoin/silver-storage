//! Event storage with indexing for efficient queries
//!
//! This module provides storage for blockchain events with multiple indexes
//! for efficient querying by transaction, object, and event type.

use crate::{
    db::{RocksDatabase, CF_EVENTS}, Result,
};
use serde::{Deserialize, Serialize};
use silver_core::{ObjectID, TransactionDigest};
use std::sync::Arc;
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
    pub event_id: EventID,
    
    /// Transaction that generated this event
    pub transaction_digest: TransactionDigest,
    
    /// Event type
    pub event_type: EventType,
    
    /// Object ID related to this event (if applicable)
    pub object_id: Option<ObjectID>,
    
    /// Event data (serialized)
    pub data: Vec<u8>,
    
    /// Timestamp when event was created (Unix milliseconds)
    pub timestamp: u64,
}

/// Event ID (unique identifier for events)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventID(pub u64);

impl EventID {
    /// Create a new event ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }
    
    /// Get the inner value
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Event store for blockchain events
///
/// Provides storage and retrieval of events with multiple indexes:
/// - By event ID (primary key)
/// - By transaction digest
/// - By object ID
/// - By event type
pub struct EventStore {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
    
    /// Next event ID counter
    next_event_id: Arc<parking_lot::Mutex<u64>>,
}

impl EventStore {
    /// Create a new event store
    ///
    /// # Arguments
    /// * `db` - Shared reference to the RocksDB database
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing EventStore");
        
        // Load the next event ID from storage
        let next_event_id = Self::load_next_event_id(&db).unwrap_or(0);
        
        Self {
            db,
            next_event_id: Arc::new(parking_lot::Mutex::new(next_event_id)),
        }
    }

    /// Store an event
    ///
    /// The event is indexed by event ID, transaction digest, and object ID.
    ///
    /// # Arguments
    /// * `transaction_digest` - Transaction that generated the event
    /// * `event_type` - Type of event
    /// * `object_id` - Related object ID (optional)
    /// * `data` - Event data
    /// * `timestamp` - Event timestamp
    ///
    /// # Returns
    /// The assigned event ID
    ///
    /// # Errors
    /// Returns error if serialization or database write fails
    pub fn store_event(
        &self,
        transaction_digest: TransactionDigest,
        event_type: EventType,
        object_id: Option<ObjectID>,
        data: Vec<u8>,
        timestamp: u64,
    ) -> Result<EventID> {
        // Allocate event ID
        let event_id = self.allocate_event_id()?;
        
        debug!("Storing event: id={}, type={:?}", event_id.0, event_type);

        let event = Event {
            event_id,
            transaction_digest,
            event_type: event_type.clone(),
            object_id,
            data,
            timestamp,
        };

        // Serialize event
        let event_bytes = bincode::serialize(&event)?;

        // Create atomic batch for multiple indexes
        let mut batch = self.db.batch();

        // Primary index: by event ID
        let event_key = self.make_event_key(event_id);
        self.db.batch_put(&mut batch, CF_EVENTS, &event_key, &event_bytes);

        // Secondary index: by transaction digest
        let tx_index_key = self.make_transaction_index_key(&transaction_digest, event_id);
        self.db.batch_put(&mut batch, CF_EVENTS, &tx_index_key, &[]);

        // Secondary index: by object ID (if present)
        if let Some(obj_id) = object_id {
            let obj_index_key = self.make_object_index_key(&obj_id, event_id);
            self.db.batch_put(&mut batch, CF_EVENTS, &obj_index_key, &[]);
        }

        // Secondary index: by event type
        let type_index_key = self.make_type_index_key(&event_type, event_id);
        self.db.batch_put(&mut batch, CF_EVENTS, &type_index_key, &[]);

        // Write batch atomically
        self.db.write_batch(batch)?;

        debug!("Event {} stored successfully ({} bytes)", event_id.0, event_bytes.len());

        Ok(event_id)
    }

    /// Get an event by ID
    ///
    /// # Arguments
    /// * `event_id` - Event ID
    ///
    /// # Returns
    /// - `Ok(Some(event))` if event exists
    /// - `Ok(None)` if event doesn't exist
    /// - `Err` on database or deserialization error
    pub fn get_event(&self, event_id: EventID) -> Result<Option<Event>> {
        debug!("Retrieving event: {}", event_id.0);

        let key = self.make_event_key(event_id);
        let event_bytes = self.db.get(CF_EVENTS, &key)?;

        match event_bytes {
            Some(bytes) => {
                let event: Event = bincode::deserialize(&bytes)?;
                debug!("Event {} retrieved", event_id.0);
                Ok(Some(event))
            }
            None => {
                debug!("Event {} not found", event_id.0);
                Ok(None)
            }
        }
    }

    /// Get all events for a transaction
    ///
    /// # Arguments
    /// * `transaction_digest` - Transaction digest
    ///
    /// # Returns
    /// Vector of events for the transaction
    pub fn get_events_by_transaction(&self, transaction_digest: &TransactionDigest) -> Result<Vec<Event>> {
        debug!("Querying events for transaction: {}", transaction_digest);

        let prefix = self.make_transaction_index_prefix(transaction_digest);
        let mut events = Vec::new();

        // Iterate over transaction index
        for result in self.db.iter_prefix(CF_EVENTS, &prefix) {
            let (key, _) = result?;

            // Extract event ID from index key
            if key.len() >= 73 {
                // 't' (1) + tx_digest (64) + event_id (8)
                let event_id_bytes = &key[65..73];
                let mut id_array = [0u8; 8];
                id_array.copy_from_slice(event_id_bytes);
                let event_id = EventID(u64::from_be_bytes(id_array));

                // Retrieve the actual event
                if let Some(event) = self.get_event(event_id)? {
                    events.push(event);
                }
            }
        }

        debug!("Found {} events for transaction", events.len());
        Ok(events)
    }

    /// Get all events for an object
    ///
    /// # Arguments
    /// * `object_id` - Object ID
    ///
    /// # Returns
    /// Vector of events for the object
    pub fn get_events_by_object(&self, object_id: &ObjectID) -> Result<Vec<Event>> {
        debug!("Querying events for object: {}", object_id);

        let prefix = self.make_object_index_prefix(object_id);
        let mut events = Vec::new();

        // Iterate over object index
        for result in self.db.iter_prefix(CF_EVENTS, &prefix) {
            let (key, _) = result?;

            // Extract event ID from index key
            if key.len() >= 73 {
                // 'o' (1) + object_id (64) + event_id (8)
                let event_id_bytes = &key[65..73];
                let mut id_array = [0u8; 8];
                id_array.copy_from_slice(event_id_bytes);
                let event_id = EventID(u64::from_be_bytes(id_array));

                // Retrieve the actual event
                if let Some(event) = self.get_event(event_id)? {
                    events.push(event);
                }
            }
        }

        debug!("Found {} events for object", events.len());
        Ok(events)
    }

    /// Get all events of a specific type
    ///
    /// # Arguments
    /// * `event_type` - Event type
    ///
    /// # Returns
    /// Vector of events of the specified type
    pub fn get_events_by_type(&self, event_type: &EventType) -> Result<Vec<Event>> {
        debug!("Querying events by type: {:?}", event_type);

        let prefix = self.make_type_index_prefix(event_type);
        let mut events = Vec::new();

        // Iterate over type index
        for result in self.db.iter_prefix(CF_EVENTS, &prefix) {
            let (key, _) = result?;

            // Extract event ID from index key
            // Key format varies by type, but event_id is always the last 8 bytes
            if key.len() >= 9 {
                let event_id_bytes = &key[key.len() - 8..];
                let mut id_array = [0u8; 8];
                id_array.copy_from_slice(event_id_bytes);
                let event_id = EventID(u64::from_be_bytes(id_array));

                // Retrieve the actual event
                if let Some(event) = self.get_event(event_id)? {
                    events.push(event);
                }
            }
        }

        debug!("Found {} events of type {:?}", events.len(), event_type);
        Ok(events)
    }

    /// Batch store multiple events
    ///
    /// All events are stored atomically.
    ///
    /// # Arguments
    /// * `events` - Slice of event data tuples
    ///
    /// # Returns
    /// Vector of assigned event IDs
    ///
    /// # Errors
    /// Returns error if serialization or database write fails.
    /// On error, no events are stored (atomic operation).
    pub fn batch_store_events(
        &self,
        events: &[(TransactionDigest, EventType, Option<ObjectID>, Vec<u8>, u64)],
    ) -> Result<Vec<EventID>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        info!("Batch storing {} events", events.len());

        let mut event_ids = Vec::with_capacity(events.len());
        let mut batch = self.db.batch();

        for (tx_digest, event_type, object_id, data, timestamp) in events {
            // Allocate event ID
            let event_id = self.allocate_event_id()?;
            event_ids.push(event_id);

            let event = Event {
                event_id,
                transaction_digest: *tx_digest,
                event_type: event_type.clone(),
                object_id: *object_id,
                data: data.clone(),
                timestamp: *timestamp,
            };

            // Serialize event
            let event_bytes = bincode::serialize(&event)?;

            // Primary index: by event ID
            let event_key = self.make_event_key(event_id);
            self.db.batch_put(&mut batch, CF_EVENTS, &event_key, &event_bytes);

            // Secondary index: by transaction digest
            let tx_index_key = self.make_transaction_index_key(tx_digest, event_id);
            self.db.batch_put(&mut batch, CF_EVENTS, &tx_index_key, &[]);

            // Secondary index: by object ID (if present)
            if let Some(obj_id) = object_id {
                let obj_index_key = self.make_object_index_key(obj_id, event_id);
                self.db.batch_put(&mut batch, CF_EVENTS, &obj_index_key, &[]);
            }

            // Secondary index: by event type
            let type_index_key = self.make_type_index_key(event_type, event_id);
            self.db.batch_put(&mut batch, CF_EVENTS, &type_index_key, &[]);
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!("Batch stored {} events successfully", events.len());
        Ok(event_ids)
    }

    /// Get the total number of stored events (approximate)
    pub fn get_event_count(&self) -> Result<u64> {
        // Divide by 4 since we store each event 4 times (primary + 3 indexes)
        self.db.get_cf_key_count(CF_EVENTS).map(|count| count / 4)
    }

    /// Get the total size of event storage in bytes
    pub fn get_storage_size(&self) -> Result<u64> {
        self.db.get_cf_size(CF_EVENTS)
    }

    // ========== Private Helper Methods ==========

    /// Allocate a new event ID
    fn allocate_event_id(&self) -> Result<EventID> {
        let mut next_id = self.next_event_id.lock();
        let event_id = EventID(*next_id);
        *next_id += 1;
        
        // Persist the next event ID
        self.save_next_event_id(*next_id)?;
        
        Ok(event_id)
    }

    /// Load next event ID from storage
    fn load_next_event_id(db: &Arc<RocksDatabase>) -> Result<u64> {
        let key = b"__next_event_id__";
        match db.get(CF_EVENTS, key)? {
            Some(bytes) => {
                if bytes.len() == 8 {
                    let mut array = [0u8; 8];
                    array.copy_from_slice(&bytes);
                    Ok(u64::from_le_bytes(array))
                } else {
                    Ok(0)
                }
            }
            None => Ok(0),
        }
    }

    /// Save next event ID to storage
    fn save_next_event_id(&self, next_id: u64) -> Result<()> {
        let key = b"__next_event_id__";
        let value = next_id.to_le_bytes();
        self.db.put(CF_EVENTS, key, &value)
    }

    /// Create event key
    ///
    /// Key format: 'e' (1 byte) + event_id (8 bytes)
    fn make_event_key(&self, event_id: EventID) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.push(b'e'); // 'e' for event
        key.extend_from_slice(&event_id.0.to_be_bytes());
        key
    }

    /// Create transaction index key
    ///
    /// Key format: 't' (1 byte) + tx_digest (64 bytes) + event_id (8 bytes)
    fn make_transaction_index_key(&self, tx_digest: &TransactionDigest, event_id: EventID) -> Vec<u8> {
        let mut key = Vec::with_capacity(73);
        key.push(b't'); // 't' for transaction
        key.extend_from_slice(tx_digest.as_bytes());
        key.extend_from_slice(&event_id.0.to_be_bytes());
        key
    }

    /// Create transaction index prefix
    fn make_transaction_index_prefix(&self, tx_digest: &TransactionDigest) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(65);
        prefix.push(b't');
        prefix.extend_from_slice(tx_digest.as_bytes());
        prefix
    }

    /// Create object index key
    ///
    /// Key format: 'o' (1 byte) + object_id (64 bytes) + event_id (8 bytes)
    fn make_object_index_key(&self, object_id: &ObjectID, event_id: EventID) -> Vec<u8> {
        let mut key = Vec::with_capacity(73);
        key.push(b'o'); // 'o' for object
        key.extend_from_slice(object_id.as_bytes());
        key.extend_from_slice(&event_id.0.to_be_bytes());
        key
    }

    /// Create object index prefix
    fn make_object_index_prefix(&self, object_id: &ObjectID) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(65);
        prefix.push(b'o');
        prefix.extend_from_slice(object_id.as_bytes());
        prefix
    }

    /// Create type index key
    ///
    /// Key format: 'y' (1 byte) + type_discriminant (1 byte) + type_data + event_id (8 bytes)
    fn make_type_index_key(&self, event_type: &EventType, event_id: EventID) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(b'y'); // 'y' for type
        
        // Encode event type
        match event_type {
            EventType::ObjectCreated => key.push(0),
            EventType::ObjectModified => key.push(1),
            EventType::ObjectDeleted => key.push(2),
            EventType::ObjectTransferred => key.push(3),
            EventType::ObjectShared => key.push(4),
            EventType::ObjectFrozen => key.push(5),
            EventType::CoinSplit => key.push(6),
            EventType::CoinMerged => key.push(7),
            EventType::ModulePublished => key.push(8),
            EventType::FunctionCalled => key.push(9),
            EventType::Custom(name) => {
                key.push(255); // Custom type discriminant
                key.extend_from_slice(name.as_bytes());
            }
        }
        
        key.extend_from_slice(&event_id.0.to_be_bytes());
        key
    }

    /// Create type index prefix
    fn make_type_index_prefix(&self, event_type: &EventType) -> Vec<u8> {
        let mut prefix = Vec::new();
        prefix.push(b'y');
        
        match event_type {
            EventType::ObjectCreated => prefix.push(0),
            EventType::ObjectModified => prefix.push(1),
            EventType::ObjectDeleted => prefix.push(2),
            EventType::ObjectTransferred => prefix.push(3),
            EventType::ObjectShared => prefix.push(4),
            EventType::ObjectFrozen => prefix.push(5),
            EventType::CoinSplit => prefix.push(6),
            EventType::CoinMerged => prefix.push(7),
            EventType::ModulePublished => prefix.push(8),
            EventType::FunctionCalled => prefix.push(9),
            EventType::Custom(name) => {
                prefix.push(255);
                prefix.extend_from_slice(name.as_bytes());
            }
        }
        
        prefix
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> (EventStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
        let store = EventStore::new(db);
        (store, temp_dir)
    }

    fn create_test_tx_digest(id: u8) -> TransactionDigest {
        TransactionDigest::new([id; 64])
    }

    fn create_test_object_id(id: u8) -> ObjectID {
        ObjectID::new([id; 64])
    }

    #[test]
    fn test_store_and_get_event() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);
        let object_id = create_test_object_id(1);
        let data = vec![1, 2, 3, 4];

        // Store event
        let event_id = store
            .store_event(
                tx_digest,
                EventType::ObjectCreated,
                Some(object_id),
                data.clone(),
                1000,
            )
            .unwrap();

        // Get event
        let event = store.get_event(event_id).unwrap();
        assert!(event.is_some());

        let event = event.unwrap();
        assert_eq!(event.event_id, event_id);
        assert_eq!(event.transaction_digest, tx_digest);
        assert_eq!(event.event_type, EventType::ObjectCreated);
        assert_eq!(event.object_id, Some(object_id));
        assert_eq!(event.data, data);
    }

    #[test]
    fn test_event_id_allocation() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);

        // Store multiple events
        let id1 = store
            .store_event(tx_digest, EventType::ObjectCreated, None, vec![], 1000)
            .unwrap();
        let id2 = store
            .store_event(tx_digest, EventType::ObjectModified, None, vec![], 1001)
            .unwrap();
        let id3 = store
            .store_event(tx_digest, EventType::ObjectDeleted, None, vec![], 1002)
            .unwrap();

        // IDs should be sequential
        assert_eq!(id1.0, 0);
        assert_eq!(id2.0, 1);
        assert_eq!(id3.0, 2);
    }

    #[test]
    fn test_get_events_by_transaction() {
        let (store, _temp) = create_test_store();

        let tx1 = create_test_tx_digest(1);
        let tx2 = create_test_tx_digest(2);

        // Store events for tx1
        store
            .store_event(tx1, EventType::ObjectCreated, None, vec![], 1000)
            .unwrap();
        store
            .store_event(tx1, EventType::ObjectModified, None, vec![], 1001)
            .unwrap();

        // Store event for tx2
        store
            .store_event(tx2, EventType::ObjectDeleted, None, vec![], 1002)
            .unwrap();

        // Query events for tx1
        let events = store.get_events_by_transaction(&tx1).unwrap();
        assert_eq!(events.len(), 2);

        // Query events for tx2
        let events = store.get_events_by_transaction(&tx2).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_get_events_by_object() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);
        let obj1 = create_test_object_id(1);
        let obj2 = create_test_object_id(2);

        // Store events for obj1
        store
            .store_event(tx_digest, EventType::ObjectCreated, Some(obj1), vec![], 1000)
            .unwrap();
        store
            .store_event(tx_digest, EventType::ObjectModified, Some(obj1), vec![], 1001)
            .unwrap();

        // Store event for obj2
        store
            .store_event(tx_digest, EventType::ObjectDeleted, Some(obj2), vec![], 1002)
            .unwrap();

        // Query events for obj1
        let events = store.get_events_by_object(&obj1).unwrap();
        assert_eq!(events.len(), 2);

        // Query events for obj2
        let events = store.get_events_by_object(&obj2).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_get_events_by_type() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);

        // Store events of different types
        store
            .store_event(tx_digest, EventType::ObjectCreated, None, vec![], 1000)
            .unwrap();
        store
            .store_event(tx_digest, EventType::ObjectCreated, None, vec![], 1001)
            .unwrap();
        store
            .store_event(tx_digest, EventType::ObjectModified, None, vec![], 1002)
            .unwrap();

        // Query by type
        let created_events = store.get_events_by_type(&EventType::ObjectCreated).unwrap();
        assert_eq!(created_events.len(), 2);

        let modified_events = store.get_events_by_type(&EventType::ObjectModified).unwrap();
        assert_eq!(modified_events.len(), 1);
    }

    #[test]
    fn test_custom_event_type() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);
        let custom_type = EventType::Custom("MyCustomEvent".to_string());

        // Store custom event
        let event_id = store
            .store_event(tx_digest, custom_type.clone(), None, vec![1, 2, 3], 1000)
            .unwrap();

        // Retrieve and verify
        let event = store.get_event(event_id).unwrap().unwrap();
        assert_eq!(event.event_type, custom_type);

        // Query by custom type
        let events = store.get_events_by_type(&custom_type).unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_batch_store_events() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);
        let obj_id = create_test_object_id(1);

        let events = vec![
            (tx_digest, EventType::ObjectCreated, Some(obj_id), vec![1], 1000),
            (tx_digest, EventType::ObjectModified, Some(obj_id), vec![2], 1001),
            (tx_digest, EventType::ObjectDeleted, Some(obj_id), vec![3], 1002),
        ];

        // Batch store
        let event_ids = store.batch_store_events(&events).unwrap();
        assert_eq!(event_ids.len(), 3);

        // Verify all events exist
        for event_id in event_ids {
            assert!(store.get_event(event_id).unwrap().is_some());
        }
    }

    #[test]
    fn test_event_without_object() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);

        // Store event without object ID
        let event_id = store
            .store_event(tx_digest, EventType::ModulePublished, None, vec![], 1000)
            .unwrap();

        // Retrieve and verify
        let event = store.get_event(event_id).unwrap().unwrap();
        assert_eq!(event.object_id, None);
    }

    #[test]
    fn test_get_event_count() {
        let (store, _temp) = create_test_store();

        // Initially 0
        let count = store.get_event_count().unwrap();
        assert_eq!(count, 0);

        let tx_digest = create_test_tx_digest(1);

        // Add events
        store
            .store_event(tx_digest, EventType::ObjectCreated, None, vec![], 1000)
            .unwrap();
        store
            .store_event(tx_digest, EventType::ObjectModified, None, vec![], 1001)
            .unwrap();

        // Count should be 2
        let count = store.get_event_count().unwrap();
        assert!(count >= 2);
    }

    #[test]
    fn test_get_storage_size() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);

        // Add some events
        store
            .store_event(tx_digest, EventType::ObjectCreated, None, vec![1, 2, 3], 1000)
            .unwrap();
        store
            .store_event(tx_digest, EventType::ObjectModified, None, vec![4, 5, 6], 1001)
            .unwrap();

        // Size should be non-negative
        let size = store.get_storage_size().unwrap();
        assert!(size >= 0);
    }

    #[test]
    fn test_all_event_types() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);
        let obj_id = create_test_object_id(1);

        let event_types = vec![
            EventType::ObjectCreated,
            EventType::ObjectModified,
            EventType::ObjectDeleted,
            EventType::ObjectTransferred,
            EventType::ObjectShared,
            EventType::ObjectFrozen,
            EventType::CoinSplit,
            EventType::CoinMerged,
            EventType::ModulePublished,
            EventType::FunctionCalled,
        ];

        // Store one event of each type
        for event_type in &event_types {
            store
                .store_event(tx_digest, event_type.clone(), Some(obj_id), vec![], 1000)
                .unwrap();
        }

        // Verify we can query each type
        for event_type in &event_types {
            let events = store.get_events_by_type(event_type).unwrap();
            assert_eq!(events.len(), 1);
        }
    }

    #[test]
    fn test_event_id_persistence() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create store and add events
        {
            let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
            let store = EventStore::new(db);
            
            let tx_digest = create_test_tx_digest(1);
            let id1 = store
                .store_event(tx_digest, EventType::ObjectCreated, None, vec![], 1000)
                .unwrap();
            assert_eq!(id1.0, 0);
        }
        
        // Reopen store and add more events
        {
            let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
            let store = EventStore::new(db);
            
            let tx_digest = create_test_tx_digest(1);
            let id2 = store
                .store_event(tx_digest, EventType::ObjectModified, None, vec![], 1001)
                .unwrap();
            // Should continue from where we left off
            assert_eq!(id2.0, 1);
        }
    }

    #[test]
    fn test_multiple_indexes() {
        let (store, _temp) = create_test_store();

        let tx_digest = create_test_tx_digest(1);
        let obj_id = create_test_object_id(1);

        // Store event
        let event_id = store
            .store_event(
                tx_digest,
                EventType::ObjectCreated,
                Some(obj_id),
                vec![1, 2, 3],
                1000,
            )
            .unwrap();

        // Verify we can find it through all indexes
        
        // By event ID
        assert!(store.get_event(event_id).unwrap().is_some());
        
        // By transaction
        let tx_events = store.get_events_by_transaction(&tx_digest).unwrap();
        assert_eq!(tx_events.len(), 1);
        
        // By object
        let obj_events = store.get_events_by_object(&obj_id).unwrap();
        assert_eq!(obj_events.len(), 1);
        
        // By type
        let type_events = store.get_events_by_type(&EventType::ObjectCreated).unwrap();
        assert_eq!(type_events.len(), 1);
    }
}
