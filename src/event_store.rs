//! Event storage with indexing for efficient queries
//!
//! This module provides storage for blockchain events with multiple indexes
//! for efficient querying by transaction, object, and event type.

use crate::{
    db::{RocksDatabase, CF_EVENTS},
    Result,
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
        self.db
            .batch_put(&mut batch, CF_EVENTS, &event_key, &event_bytes)?;

        // Secondary index: by transaction digest
        let tx_index_key = self.make_transaction_index_key(&transaction_digest, event_id);
        self.db.batch_put(&mut batch, CF_EVENTS, &tx_index_key, &[])?;

        // Secondary index: by object ID (if present)
        if let Some(obj_id) = object_id {
            let obj_index_key = self.make_object_index_key(&obj_id, event_id);
            self.db
                .batch_put(&mut batch, CF_EVENTS, &obj_index_key, &[])?;
        }

        // Secondary index: by event type
        let type_index_key = self.make_type_index_key(&event_type, event_id);
        self.db
            .batch_put(&mut batch, CF_EVENTS, &type_index_key, &[])?;

        // Write batch atomically
        self.db.write_batch(batch)?;

        debug!(
            "Event {} stored successfully ({} bytes)",
            event_id.0,
            event_bytes.len()
        );

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
    pub fn get_events_by_transaction(
        &self,
        transaction_digest: &TransactionDigest,
    ) -> Result<Vec<Event>> {
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
            self.db
                .batch_put(&mut batch, CF_EVENTS, &event_key, &event_bytes)?;

            // Secondary index: by transaction digest
            let tx_index_key = self.make_transaction_index_key(tx_digest, event_id);
            self.db.batch_put(&mut batch, CF_EVENTS, &tx_index_key, &[])?;

            // Secondary index: by object ID (if present)
            if let Some(obj_id) = object_id {
                let obj_index_key = self.make_object_index_key(obj_id, event_id);
                self.db
                    .batch_put(&mut batch, CF_EVENTS, &obj_index_key, &[])?;
            }

            // Secondary index: by event type
            let type_index_key = self.make_type_index_key(event_type, event_id);
            self.db
                .batch_put(&mut batch, CF_EVENTS, &type_index_key, &[])?;
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
    fn make_transaction_index_key(
        &self,
        tx_digest: &TransactionDigest,
        event_id: EventID,
    ) -> Vec<u8> {
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
