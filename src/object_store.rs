//! Object storage with versioning using ParityDB

use crate::db::ParityDatabase;
use crate::error::Result;
use silver_core::{Object, ObjectID};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Column family indices for object storage
#[allow(dead_code)]
const CF_OBJECTS: u8 = 0;
#[allow(dead_code)]
const CF_OWNER_INDEX: u8 = 1;
#[allow(dead_code)]
const CF_VERSION_INDEX: u8 = 2;

/// Object store for blockchain objects with ParityDB backend
///
/// Provides persistent storage for blockchain objects with:
/// - Efficient key-value lookups
/// - Atomic operations
/// - Compression support
pub struct ObjectStore {
    db: Arc<ParityDatabase>,
}

impl ObjectStore {
    /// Create a new object store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing ObjectStore with ParityDB backend");
        Self { db }
    }

    /// Store an object using bincode 2.0 API
    pub fn store_object(&self, object: &Object) -> Result<()> {
        debug!("Storing object: {}", object.id);

        let key = format!("obj:{}", object.id).into_bytes();
        let value = serde_json::to_vec(object).map_err(crate::error::Error::Serialization)?;

        self.db.put("objects", &key, &value)?;

        // Update the all_objects_index to include this object ID
        let all_objects_index_key = b"all_objects_index".to_vec();

        let mut object_ids = Vec::new();

        // Load existing index
        if let Ok(Some(index_data)) = self.db.get("objects", &all_objects_index_key) {
            if let Ok(existing_ids) = serde_json::from_slice::<Vec<ObjectID>>(&index_data) {
                object_ids = existing_ids;
            }
        }

        // Add this object ID if not already present
        if !object_ids.contains(&object.id) {
            object_ids.push(object.id);
        }

        // Store updated index
        let index_value =
            serde_json::to_vec(&object_ids).map_err(crate::error::Error::Serialization)?;
        self.db
            .put("objects", &all_objects_index_key, &index_value)?;

        Ok(())
    }

    /// Get an object by ID using bincode 2.0 API
    pub fn get_object(&self, object_id: &ObjectID) -> Result<Option<Object>> {
        debug!("Retrieving object: {}", object_id);

        let key = format!("obj:{}", object_id).into_bytes();

        match self.db.get("objects", &key)? {
            Some(data) => {
                let object = serde_json::from_slice::<Object>(&data)
                    .map_err(crate::error::Error::Serialization)?;
                Ok(Some(object))
            }
            None => Ok(None),
        }
    }

    /// Delete an object using bincode 2.0 API
    pub fn delete_object(&self, object_id: &ObjectID) -> Result<()> {
        debug!("Deleting object: {}", object_id);

        let key = format!("obj:{}", object_id).into_bytes();
        self.db.delete("objects", &key)?;

        // Update the all_objects_index to remove this object ID
        let all_objects_index_key = b"all_objects_index".to_vec();

        if let Ok(Some(index_data)) = self.db.get("objects", &all_objects_index_key) {
            if let Ok(mut object_ids) = serde_json::from_slice::<Vec<ObjectID>>(&index_data) {
                // Remove this object ID from the index
                object_ids.retain(|id| id != object_id);

                // Store updated index
                let index_value =
                    serde_json::to_vec(&object_ids).map_err(crate::error::Error::Serialization)?;
                self.db
                    .put("objects", &all_objects_index_key, &index_value)?;
            }
        }

        Ok(())
    }

    /// Check if an object exists
    pub fn object_exists(&self, object_id: &ObjectID) -> Result<bool> {
        let key = format!("obj:{}", object_id).into_bytes();
        Ok(self.db.get("objects", &key)?.is_some())
    }

    /// Store a snapshot (for consensus integration)
    pub fn store_snapshot(&self, snapshot_data: &[u8]) -> Result<()> {
        debug!("Storing snapshot via ObjectStore");
        let key = b"snapshot:latest".to_vec();
        self.db.put("objects", &key, snapshot_data)?;
        Ok(())
    }

    /// Get objects by owner using bincode 2.0 API
    pub fn get_objects_by_owner(
        &self,
        owner: &silver_core::SilverAddress,
    ) -> Result<Vec<silver_core::Object>> {
        debug!("Retrieving objects for owner: {}", owner);

        let mut objects = Vec::new();

        // Create owner index key for efficient lookup
        // Format: "owner_index:<owner_address>"
        let owner_index_key = format!("owner_index:{}", owner);

        // Query the owner_index column family for all objects owned by this address
        // The owner_index stores mappings from owner to object IDs
        match self.db.get("owner_index", owner_index_key.as_bytes()) {
            Ok(Some(value)) => {
                // Deserialize the list of object IDs owned by this owner
                let object_ids: Vec<silver_core::ObjectID> =
                    serde_json::from_slice(&value).map_err(crate::error::Error::Serialization)?;

                // Retrieve each object from the main object store
                for object_id in object_ids {
                    let obj_key = format!("obj:{}", object_id);
                    if let Ok(Some(obj_bytes)) = self.db.get("objects", obj_key.as_bytes()) {
                        let object: silver_core::Object = serde_json::from_slice(&obj_bytes)
                            .map_err(crate::error::Error::Serialization)?;
                        objects.push(object);
                    }
                }
            }
            Ok(None) => {
                // No objects found for this owner
                debug!("No objects found for owner: {}", owner);
            }
            Err(e) => {
                warn!("Error querying owner index: {}", e);
                // Fallback: iterate all objects and filter by owner
                let all_objects = self.iterate_all_objects()?;
                for object in all_objects {
                    if let silver_core::Owner::AddressOwner(obj_owner) = &object.owner {
                        if obj_owner == owner {
                            objects.push(object);
                        }
                    }
                }
            }
        }

        debug!("Retrieved {} objects for owner {}", objects.len(), owner);
        Ok(objects)
    }

    /// Iterate all objects using bincode 2.0 API
    pub fn iterate_all_objects(&self) -> Result<Vec<silver_core::Object>> {
        debug!("Iterating all objects");

        let mut objects = Vec::new();

        // Query the all_objects_index that tracks all object IDs
        // This index is maintained whenever objects are stored or deleted
        let all_objects_index_key = b"all_objects_index".to_vec();

        if let Ok(Some(index_data)) = self.db.get("objects", &all_objects_index_key) {
            // Deserialize the list of all object IDs
            if let Ok(object_ids) = serde_json::from_slice::<Vec<ObjectID>>(&index_data) {
                for object_id in object_ids {
                    // Retrieve each object from storage
                    let key = format!("obj:{}", object_id).into_bytes();

                    match self.db.get("objects", &key) {
                        Ok(Some(data)) => {
                            match serde_json::from_slice::<silver_core::Object>(&data) {
                                Ok(object) => {
                                    objects.push(object);
                                }
                                Err(e) => {
                                    debug!("Failed to deserialize object {}: {}", object_id, e);
                                }
                            }
                        }
                        Ok(None) => {
                            debug!("Object {} in index but not in storage", object_id);
                        }
                        Err(e) => {
                            debug!("Failed to retrieve object {}: {}", object_id, e);
                        }
                    }
                }
            }
        }

        debug!("Iterated {} total objects", objects.len());
        Ok(objects)
    }

    /// Get object history using bincode 2.0 API
    pub fn get_object_history(&self, object_id: &ObjectID) -> Result<Vec<silver_core::Object>> {
        debug!("Retrieving history for object: {}", object_id);

        let mut history = Vec::new();
        let object_id_str = object_id.to_string();
        let history_index_key = format!("history_index:{}", object_id_str).into_bytes();

        // Query the history index for all versions of this object
        if let Ok(Some(index_data)) = self.db.get("object_history", &history_index_key) {
            if let Ok(versions) = serde_json::from_slice::<Vec<u64>>(&index_data) {
                for version in versions {
                    let key = format!("history:{}:{}", object_id_str, version).into_bytes();
                    if let Ok(Some(data)) = self.db.get("object_history", &key) {
                        if let Ok(object) = serde_json::from_slice::<silver_core::Object>(&data) {
                            history.push(object);
                        }
                    }
                }
            }
        }

        // Sort by version number (descending - newest first)
        history.sort_by(|a, b| b.version.cmp(&a.version));

        debug!(
            "Retrieved {} versions for object {}",
            history.len(),
            object_id
        );
        Ok(history)
    }

    /// Get sender's public key using bincode 2.0 API
    pub fn get_sender_public_key(
        &self,
        sender: &silver_core::SilverAddress,
    ) -> Result<silver_core::PublicKey> {
        debug!("Retrieving public key for sender: {}", sender);

        let key = format!("pubkey:{}", sender).into_bytes();

        match self.db.get("objects", &key)? {
            Some(data) => {
                let pubkey = serde_json::from_slice::<silver_core::PublicKey>(&data)
                    .map_err(crate::error::Error::Serialization)?;
                debug!("Retrieved public key for sender {}", sender);
                Ok(pubkey)
            }
            None => {
                // If public key not found, return error instead of default
                Err(crate::error::Error::NotFound(format!(
                    "Public key not found for sender {}",
                    sender
                )))
            }
        }
    }
}

impl Clone for ObjectStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}
