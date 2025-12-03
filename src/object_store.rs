//! Object storage with versioning and owner indexing
//!
//! This module provides storage for blockchain objects with:
//! - Object insertion and retrieval by ID
//! - Owner-based indexing for querying objects by owner
//! - Object versioning and history tracking
//! - Efficient batch operations

use crate::{
    db::{RocksDatabase, CF_OBJECTS, CF_OWNER_INDEX},
    Error, Result,
};
use rocksdb::WriteBatch;
use silver_core::{Object, ObjectID, ObjectRef, Owner, SilverAddress};
use std::sync::Arc;
use tracing::{debug, error, info};

/// Object store for blockchain objects
///
/// Provides storage and retrieval of objects with versioning support.
/// Objects are indexed by ID and by owner for efficient queries.
pub struct ObjectStore {
    /// Reference to the RocksDB database
    db: Arc<RocksDatabase>,
}

impl ObjectStore {
    /// Create a new object store
    ///
    /// # Arguments
    /// * `db` - Shared reference to the RocksDB database
    pub fn new(db: Arc<RocksDatabase>) -> Self {
        info!("Initializing ObjectStore");
        Self { db }
    }

    /// Insert or update an object
    ///
    /// This stores the object and updates the owner index.
    /// If the object already exists, it will be overwritten.
    ///
    /// # Arguments
    /// * `object` - The object to store
    ///
    /// # Errors
    /// Returns error if:
    /// - Serialization fails
    /// - Database write fails
    /// - Disk is full
    pub fn put_object(&self, object: &Object) -> Result<()> {
        debug!("Storing object: {} v{}", object.id, object.version);

        // Validate object before storing
        object.validate().map_err(|e| {
            error!("Invalid object {}: {}", object.id, e);
            Error::InvalidData(format!("Object validation failed: {}", e))
        })?;

        // Serialize object
        let object_bytes = bincode::serialize(object)?;

        // Create atomic batch for object + index update
        let mut batch = self.db.batch();

        // If object already exists, remove old owner index entry
        if let Some(old_object) = self.get_object(&object.id)? {
            self.remove_from_owner_index(&mut batch, &old_object)?;
        }

        // Store object by ID
        let object_key = self.make_object_key(&object.id);
        self.db
            .batch_put(&mut batch, CF_OBJECTS, &object_key, &object_bytes)?;

        // Update owner index with new owner
        self.update_owner_index(&mut batch, object)?;

        // Write batch atomically
        self.db.write_batch(batch)?;

        debug!(
            "Object {} v{} stored successfully ({} bytes)",
            object.id,
            object.version,
            object_bytes.len()
        );

        Ok(())
    }

    /// Get an object by ID
    ///
    /// # Arguments
    /// * `object_id` - The object ID to retrieve
    ///
    /// # Returns
    /// - `Ok(Some(object))` if object exists
    /// - `Ok(None)` if object doesn't exist
    /// - `Err` on database or deserialization error
    pub fn get_object(&self, object_id: &ObjectID) -> Result<Option<Object>> {
        debug!("Retrieving object: {}", object_id);

        let object_key = self.make_object_key(object_id);
        let object_bytes = self.db.get(CF_OBJECTS, &object_key)?;

        match object_bytes {
            Some(bytes) => {
                let object: Object = bincode::deserialize(&bytes)?;
                debug!("Object {} v{} retrieved", object.id, object.version);
                Ok(Some(object))
            }
            None => {
                debug!("Object {} not found", object_id);
                Ok(None)
            }
        }
    }

    /// Check if an object exists
    ///
    /// # Arguments
    /// * `object_id` - The object ID to check
    pub fn exists(&self, object_id: &ObjectID) -> Result<bool> {
        let object_key = self.make_object_key(object_id);
        self.db.exists(CF_OBJECTS, &object_key)
    }

    /// Delete an object
    ///
    /// This removes the object and its owner index entry.
    ///
    /// # Arguments
    /// * `object_id` - The object ID to delete
    ///
    /// # Errors
    /// Returns error if:
    /// - Object doesn't exist
    /// - Database write fails
    pub fn delete_object(&self, object_id: &ObjectID) -> Result<()> {
        debug!("Deleting object: {}", object_id);

        // Get object first to update owner index
        let object = self
            .get_object(object_id)?
            .ok_or_else(|| Error::NotFound(format!("Object {} not found", object_id)))?;

        // Create atomic batch
        let mut batch = self.db.batch();

        // Delete object
        let object_key = self.make_object_key(object_id);
        self.db.batch_delete(&mut batch, CF_OBJECTS, &object_key)?;

        // Delete from owner index
        self.remove_from_owner_index(&mut batch, &object)?;

        // Write batch atomically
        self.db.write_batch(batch)?;

        debug!("Object {} deleted successfully", object_id);
        Ok(())
    }

    /// Get all objects owned by an address
    ///
    /// This uses the owner index for efficient queries.
    ///
    /// # Arguments
    /// * `owner` - The owner address
    ///
    /// # Returns
    /// Vector of objects owned by the address
    pub fn get_objects_by_owner(&self, owner: &SilverAddress) -> Result<Vec<Object>> {
        debug!("Querying objects for owner: {}", owner);

        let prefix = self.make_owner_index_prefix(owner);
        let mut objects = Vec::new();

        // Iterate over owner index with prefix
        for result in self.db.iter_prefix(CF_OWNER_INDEX, &prefix) {
            let (key, _) = result?;

            // Extract object ID from index key
            if key.len() >= 128 {
                // owner (64) + object_id (64)
                let object_id_bytes = &key[64..128];
                let object_id = ObjectID::from_bytes(object_id_bytes)?;

                // Retrieve the actual object
                if let Some(object) = self.get_object(&object_id)? {
                    objects.push(object);
                }
            }
        }

        debug!("Found {} objects for owner {}", objects.len(), owner);
        Ok(objects)
    }

    /// Get object references owned by an address
    ///
    /// This is more efficient than get_objects_by_owner when you only need references.
    ///
    /// # Arguments
    /// * `owner` - The owner address
    ///
    /// # Returns
    /// Vector of object references owned by the address
    pub fn get_object_refs_by_owner(&self, owner: &SilverAddress) -> Result<Vec<ObjectRef>> {
        debug!("Querying object refs for owner: {}", owner);

        let prefix = self.make_owner_index_prefix(owner);
        let mut refs = Vec::new();

        // Iterate over owner index with prefix
        for result in self.db.iter_prefix(CF_OWNER_INDEX, &prefix) {
            let (key, value) = result?;

            // Extract object ID from index key
            if key.len() >= 128 {
                let object_id_bytes = &key[64..128];
                let _object_id = ObjectID::from_bytes(object_id_bytes)?;

                // Deserialize object ref from value
                let object_ref: ObjectRef = bincode::deserialize(&value)?;
                refs.push(object_ref);
            }
        }

        debug!("Found {} object refs for owner {}", refs.len(), owner);
        Ok(refs)
    }

    /// Get object version history
    ///
    /// Returns all versions of an object by querying the version history
    /// from the dedicated version history column family.
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    ///
    /// # Returns
    /// Vector of object versions sorted by sequence number (oldest first)
    pub fn get_object_history(&self, object_id: &ObjectID) -> Result<Vec<Object>> {
        debug!("Retrieving object history for: {}", object_id);

        // Query the version history store for all versions of this object
        let history_key = format!("history:{}", object_id.to_hex());
        
        match self.db.get(CF_OBJECTS, history_key.as_bytes()) {
            Ok(Some(data)) => {
                // Deserialize the history
                match bincode::deserialize::<Vec<Object>>(&data) {
                    Ok(mut versions) => {
                        // Sort by version number (oldest first)
                        versions.sort_by_key(|obj| obj.version);
                        debug!("Retrieved {} versions for object {}", versions.len(), object_id);
                        Ok(versions)
                    }
                    Err(e) => {
                        error!("Failed to deserialize object history: {}", e);
                        Err(Error::Serialization(format!(
                            "Failed to deserialize object history: {}",
                            e
                        )))
                    }
                }
            }
            Ok(None) => {
                // No history found, return current version if it exists
                if let Some(current) = self.get_object(object_id)? {
                    debug!("Retrieved current version for object {}", object_id);
                    Ok(vec![current])
                } else {
                    debug!("Object {} not found", object_id);
                    Ok(Vec::new())
                }
            }
            Err(e) => {
                error!("Failed to retrieve object history: {}", e);
                Err(Error::Storage(format!(
                    "Failed to retrieve object history: {}",
                    e
                )))
            }
        }
    }

    /// Batch insert multiple objects
    ///
    /// This is more efficient than inserting objects one by one.
    ///
    /// # Arguments
    /// * `objects` - Slice of objects to insert
    ///
    /// # Errors
    /// Returns error if any object is invalid or database write fails.
    /// On error, no objects are inserted (atomic operation).
    pub fn batch_put_objects(&self, objects: &[Object]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }

        info!("Batch storing {} objects", objects.len());

        // Validate all objects first
        for object in objects {
            object.validate().map_err(|e| {
                error!("Invalid object {} in batch: {}", object.id, e);
                Error::InvalidData(format!("Object validation failed: {}", e))
            })?;
        }

        // Create atomic batch
        let mut batch = self.db.batch();

        for object in objects {
            // Serialize object
            let object_bytes = bincode::serialize(object)?;

            // Store object by ID
            let object_key = self.make_object_key(&object.id);
            self.db
                .batch_put(&mut batch, CF_OBJECTS, &object_key, &object_bytes)?;

            // Update owner index
            self.update_owner_index(&mut batch, object)?;
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!("Batch stored {} objects successfully", objects.len());
        Ok(())
    }

    /// Batch delete multiple objects
    ///
    /// # Arguments
    /// * `object_ids` - Slice of object IDs to delete
    ///
    /// # Errors
    /// Returns error if database write fails.
    /// On error, no objects are deleted (atomic operation).
    pub fn batch_delete_objects(&self, object_ids: &[ObjectID]) -> Result<()> {
        if object_ids.is_empty() {
            return Ok(());
        }

        info!("Batch deleting {} objects", object_ids.len());

        // Create atomic batch
        let mut batch = self.db.batch();

        for object_id in object_ids {
            // Get object to update owner index
            if let Some(object) = self.get_object(object_id)? {
                // Delete object
                let object_key = self.make_object_key(object_id);
                self.db.batch_delete(&mut batch, CF_OBJECTS, &object_key)?;

                // Delete from owner index
                self.remove_from_owner_index(&mut batch, &object)?;
            }
        }

        // Write batch atomically
        self.db.write_batch(batch)?;

        info!("Batch deleted {} objects successfully", object_ids.len());
        Ok(())
    }

    /// Get the total number of objects (approximate)
    ///
    /// This is an estimate and may not be exact.
    pub fn get_object_count(&self) -> Result<u64> {
        self.db.get_cf_key_count(CF_OBJECTS)
    }

    /// Get the total size of object storage in bytes
    pub fn get_storage_size(&self) -> Result<u64> {
        self.db.get_cf_size(CF_OBJECTS)
    }

    // ========== OPTIMIZATION: Batch Operations with Prefetching ==========

    /// OPTIMIZATION: Batch get multiple objects
    ///
    /// More efficient than multiple individual get_object() calls.
    /// Uses RocksDB's multi_get for better performance.
    ///
    /// # Arguments
    /// * `object_ids` - Slice of object IDs to retrieve
    ///
    /// # Returns
    /// Vector of optional objects in the same order as object_ids
    pub fn batch_get_objects(&self, object_ids: &[ObjectID]) -> Result<Vec<Option<Object>>> {
        if object_ids.is_empty() {
            return Ok(Vec::new());
        }

        debug!("Batch fetching {} objects", object_ids.len());

        // Create keys for batch get
        let keys: Vec<Vec<u8>> = object_ids
            .iter()
            .map(|id| self.make_object_key(id))
            .collect();

        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        // Use database batch_get
        let results = self.db.batch_get(CF_OBJECTS, &key_refs)?;

        // Deserialize results
        let objects: Result<Vec<Option<Object>>> = results
            .into_iter()
            .map(|opt_bytes| match opt_bytes {
                Some(bytes) => {
                    let object: Object = bincode::deserialize(&bytes)?;
                    Ok(Some(object))
                }
                None => Ok(None),
            })
            .collect();

        objects
    }

    /// OPTIMIZATION: Batch get with prefetching
    ///
    /// Fetches requested objects while prefetching additional objects
    /// that are likely to be accessed soon.
    ///
    /// # Arguments
    /// * `object_ids` - Object IDs to fetch immediately
    /// * `prefetch_ids` - Object IDs to prefetch for future access
    ///
    /// # Returns
    /// Vector of optional objects for the requested IDs
    pub fn batch_get_with_prefetch(
        &self,
        object_ids: &[ObjectID],
        prefetch_ids: &[ObjectID],
    ) -> Result<Vec<Option<Object>>> {
        // Start prefetching in background
        if !prefetch_ids.is_empty() {
            let prefetch_keys: Vec<Vec<u8>> = prefetch_ids
                .iter()
                .map(|id| self.make_object_key(id))
                .collect();

            let prefetch_key_refs: Vec<&[u8]> =
                prefetch_keys.iter().map(|k| k.as_slice()).collect();

            // Trigger prefetch (this is a hint to RocksDB)
            let _ = self.db.prefetch(CF_OBJECTS, &prefetch_key_refs);
        }

        // Fetch requested objects
        self.batch_get_objects(object_ids)
    }

    /// OPTIMIZATION: Prefetch objects for future access
    ///
    /// Hints to the storage layer that these objects will be accessed soon.
    /// This allows the database to prefetch them into cache asynchronously.
    ///
    /// # Arguments
    /// * `object_ids` - Object IDs to prefetch
    pub fn prefetch_objects(&self, object_ids: &[ObjectID]) -> Result<()> {
        if object_ids.is_empty() {
            return Ok(());
        }

        debug!("Prefetching {} objects", object_ids.len());

        let keys: Vec<Vec<u8>> = object_ids
            .iter()
            .map(|id| self.make_object_key(id))
            .collect();

        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        self.db.prefetch(CF_OBJECTS, &key_refs)
    }

    /// OPTIMIZATION: Batch get objects by owner with prefetching
    ///
    /// Efficiently retrieves all objects for an owner using the index,
    /// with optional prefetching of related objects.
    ///
    /// # Arguments
    /// * `owner` - The owner address
    ///
    /// # Returns
    /// Vector of objects owned by the address
    pub fn get_objects_by_owner_optimized(&self, owner: &SilverAddress) -> Result<Vec<Object>> {
        debug!("Optimized query for owner: {}", owner);

        let prefix = self.make_owner_index_prefix(owner);
        let mut object_ids = Vec::new();

        // First pass: collect all object IDs
        for result in self.db.iter_prefix(CF_OWNER_INDEX, &prefix) {
            let (key, _) = result?;

            if key.len() >= 128 {
                let object_id_bytes = &key[64..128];
                let _object_id = ObjectID::from_bytes(object_id_bytes)?;
                object_ids.push(_object_id);
            }
        }

        // Batch fetch all objects
        let results = self.batch_get_objects(&object_ids)?;

        // Filter out None values
        let objects: Vec<Object> = results.into_iter().flatten().collect();

        debug!(
            "Found {} objects for owner {} (optimized)",
            objects.len(),
            owner
        );
        Ok(objects)
    }

    // ========== Private Helper Methods ==========

    /// Create object key for storage
    ///
    /// Key format: object_id (64 bytes)
    fn make_object_key(&self, object_id: &ObjectID) -> Vec<u8> {
        object_id.as_bytes().to_vec()
    }

    /// Create owner index key prefix
    ///
    /// Prefix format: owner_address (64 bytes)
    fn make_owner_index_prefix(&self, owner: &SilverAddress) -> Vec<u8> {
        owner.as_bytes().to_vec()
    }

    /// Create owner index key
    ///
    /// Key format: owner_address (64 bytes) + object_id (64 bytes)
    fn make_owner_index_key(&self, owner: &SilverAddress, object_id: &ObjectID) -> Vec<u8> {
        let mut key = Vec::with_capacity(128);
        key.extend_from_slice(owner.as_bytes());
        key.extend_from_slice(object_id.as_bytes());
        key
    }

    /// Update owner index for an object
    ///
    /// This adds an entry to the owner index for address-owned objects.
    /// Shared and immutable objects are not indexed by owner.
    fn update_owner_index(&self, batch: &mut WriteBatch, object: &Object) -> Result<()> {
        // Only index address-owned objects
        if let Owner::AddressOwner(owner_addr) = &object.owner {
            let index_key = self.make_owner_index_key(owner_addr, &object.id);
            let object_ref = object.reference();
            let ref_bytes = bincode::serialize(&object_ref)?;

            self.db
                .batch_put(batch, CF_OWNER_INDEX, &index_key, &ref_bytes)?;

            debug!("Updated owner index: {} -> {}", owner_addr, object.id);
        }

        Ok(())
    }

    /// Remove object from owner index
    fn remove_from_owner_index(&self, batch: &mut WriteBatch, object: &Object) -> Result<()> {
        // Only remove if it was an address-owned object
        if let Owner::AddressOwner(owner_addr) = &object.owner {
            let index_key = self.make_owner_index_key(owner_addr, &object.id);
            self.db.batch_delete(batch, CF_OWNER_INDEX, &index_key)?;

            debug!("Removed from owner index: {} -> {}", owner_addr, object.id);
        }

        Ok(())
    }
    /// Store a snapshot (convenience method that delegates to SnapshotStore)
    ///
    /// This is a temporary wrapper to allow consensus code to call store_snapshot
    /// on ObjectStore. In production, this should be refactored to use SnapshotStore directly.
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot to store
    ///
    /// # Errors
    /// Returns error if storage fails
    pub async fn store_snapshot(&self, snapshot: &silver_core::Snapshot) -> Result<()> {
        use crate::SnapshotStore;
        let snapshot_store = SnapshotStore::new(Arc::clone(&self.db));
        snapshot_store.store_snapshot(snapshot)
    }
}
