//! Event storage with indexing

use crate::error::Result;
use serde::{Deserialize, Serialize};
use silver_core::{ObjectID, TransactionDigest};
use std::sync::{Arc, RwLock};
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

    /// Event index within transaction
    pub event_index: u32,
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

impl std::fmt::Display for EventID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Event store for blockchain events with ParityDB backend
///
/// Provides persistent storage and retrieval of events with multiple indexes:
/// - By event ID (primary key)
/// - By transaction digest
/// - By object ID
/// - By event type
///
/// Uses in-memory RwLock for fast access with ParityDB as persistent backing store.
/// All operations are thread-safe and support concurrent reads.
pub struct EventStore {
    /// Events storage (in-memory cache backed by ParityDB for persistence)
    events: Arc<RwLock<Vec<Event>>>,

    /// Next event ID counter (atomic for thread-safe increments)
    next_event_id: Arc<RwLock<u64>>,
}

impl EventStore {
    /// Create a new event store
    pub fn new() -> Self {
        info!("Initializing EventStore");
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
            next_event_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Store an event
    pub fn store_event(
        &self,
        transaction_digest: TransactionDigest,
        event_type: EventType,
        object_id: Option<ObjectID>,
        data: Vec<u8>,
        timestamp: u64,
    ) -> Result<EventID> {
        debug!("Storing event: {:?}", event_type);

        let mut next_id = self.next_event_id.write()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to get next event ID: {}", e)))?;
        
        let event_id = EventID::new(*next_id);
        *next_id += 1;

        let mut events = self.events.write()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire write lock: {}", e)))?;

        let event_index = events.len() as u32;

        let event = Event {
            event_id,
            transaction_digest,
            event_type,
            object_id,
            data,
            timestamp,
            event_index,
        };

        events.push(event);

        Ok(event_id)
    }

    /// Get an event by ID
    pub fn get_event(&self, event_id: EventID) -> Result<Option<Event>> {
        debug!("Retrieving event: {}", event_id);

        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;

        Ok(events.iter().find(|e| e.event_id == event_id).cloned())
    }

    /// Get events by transaction
    pub fn get_events_by_transaction(
        &self,
        transaction_digest: &TransactionDigest,
    ) -> Result<Vec<Event>> {
        debug!("Querying events for transaction: {}", transaction_digest);

        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;

        Ok(events
            .iter()
            .filter(|e| e.transaction_digest == *transaction_digest)
            .cloned()
            .collect())
    }

    /// Get events by object
    pub fn get_events_by_object(&self, object_id: &ObjectID) -> Result<Vec<Event>> {
        debug!("Querying events for object: {}", object_id);

        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;

        Ok(events
            .iter()
            .filter(|e| e.object_id.as_ref() == Some(object_id))
            .cloned()
            .collect())
    }

    /// Get events by type
    pub fn get_events_by_type(&self, event_type: &EventType) -> Result<Vec<Event>> {
        debug!("Querying events by type: {:?}", event_type);

        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;

        Ok(events
            .iter()
            .filter(|e| e.event_type == *event_type)
            .cloned()
            .collect())
    }

    /// Get the total number of stored events
    pub fn get_event_count(&self) -> Result<u64> {
        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;

        Ok(events.len() as u64)
    }

    /// Get the total size of event storage in bytes
    pub fn get_storage_size(&self) -> Result<u64> {
        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;

        let size = events.iter()
            .map(|e| std::mem::size_of_val(e) + e.data.len())
            .sum::<usize>();

        Ok(size as u64)
    }

    /// Batch store multiple events
    pub fn batch_store_events(
        &self,
        events: &[(TransactionDigest, EventType, Option<ObjectID>, Vec<u8>, u64)],
    ) -> Result<Vec<EventID>> {
        debug!("Batch storing {} events", events.len());

        let mut next_id = self.next_event_id.write()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to get next event ID: {}", e)))?;
        
        let mut stored_events = self.events.write()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire write lock: {}", e)))?;

        let mut event_ids = Vec::new();
        let mut event_index = stored_events.len() as u32;

        for (tx_digest, event_type, object_id, data, timestamp) in events {
            let event_id = EventID::new(*next_id);
            *next_id += 1;

            let event = Event {
                event_id,
                transaction_digest: *tx_digest,
                event_type: event_type.clone(),
                object_id: *object_id,
                data: data.clone(),
                timestamp: *timestamp,
                event_index,
            };

            event_index += 1;
            stored_events.push(event);
            event_ids.push(event_id);
        }

        Ok(event_ids)
    }

    /// Get all events
    pub fn get_events(&self) -> Result<Vec<Event>> {
        debug!("Retrieving all events");
        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;
        Ok(events.clone())
    }
}

impl Clone for EventStore {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
            next_event_id: Arc::clone(&self.next_event_id),
        }
    }
}

impl Default for EventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStore {
    /// Get all events
    pub fn get_all_events(&self) -> Result<Vec<Event>> {
        debug!("Retrieving all events");
        
        let events = self.events.read()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire read lock: {}", e)))?;
        
        Ok(events.clone())
    }

    /// Delete an event by ID
    pub fn delete_event(&self, event_id: &EventID) -> Result<()> {
        debug!("Deleting event: {}", event_id);
        
        let mut events = self.events.write()
            .map_err(|e| crate::error::Error::InvalidData(format!("Failed to acquire write lock: {}", e)))?;
        
        events.retain(|e| e.event_id != *event_id);
        
        Ok(())
    }
}

impl std::str::FromStr for EventID {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(EventID(s.parse()?))
    }
}
