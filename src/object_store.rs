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
            .batch_put(&mut batch, CF_OBJECTS, &object_key, &object_bytes);

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
        self.db.batch_delete(&mut batch, CF_OBJECTS, &object_key);

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
                let object_id = ObjectID::from_bytes(object_id_bytes)?;

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
    /// Returns all versions of an object by querying the version history.
    /// Note: This is a placeholder for future implementation when we add
    /// version history tracking in a separate column family.
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    ///
    /// # Returns
    /// Vector of object versions (currently only returns current version)
    pub fn get_object_history(&self, object_id: &ObjectID) -> Result<Vec<Object>> {
        debug!("Retrieving object history for: {}", object_id);

        // For now, just return the current version
        // TODO: Implement full version history tracking
        let mut history = Vec::new();
        if let Some(object) = self.get_object(object_id)? {
            history.push(object);
        }

        debug!("Retrieved {} versions for object {}", history.len(), object_id);
        Ok(history)
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
                .batch_put(&mut batch, CF_OBJECTS, &object_key, &object_bytes);

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
                self.db.batch_delete(&mut batch, CF_OBJECTS, &object_key);

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
            .map(|opt_bytes| {
                match opt_bytes {
                    Some(bytes) => {
                        let object: Object = bincode::deserialize(&bytes)?;
                        Ok(Some(object))
                    }
                    None => Ok(None),
                }
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

            let prefetch_key_refs: Vec<&[u8]> = prefetch_keys.iter().map(|k| k.as_slice()).collect();

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
    pub fn get_objects_by_owner_optimized(
        &self,
        owner: &SilverAddress,
    ) -> Result<Vec<Object>> {
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

        debug!("Found {} objects for owner {} (optimized)", objects.len(), owner);
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
    fn update_owner_index(
        &self,
        batch: &mut WriteBatch,
        object: &Object,
    ) -> Result<()> {
        // Only index address-owned objects
        if let Owner::AddressOwner(owner_addr) = &object.owner {
            let index_key = self.make_owner_index_key(owner_addr, &object.id);
            let object_ref = object.reference();
            let ref_bytes = bincode::serialize(&object_ref)?;

            self.db
                .batch_put(batch, CF_OWNER_INDEX, &index_key, &ref_bytes);

            debug!(
                "Updated owner index: {} -> {}",
                owner_addr, object.id
            );
        }

        Ok(())
    }

    /// Remove object from owner index
    fn remove_from_owner_index(
        &self,
        batch: &mut WriteBatch,
        object: &Object,
    ) -> Result<()> {
        // Only remove if it was an address-owned object
        if let Owner::AddressOwner(owner_addr) = &object.owner {
            let index_key = self.make_owner_index_key(owner_addr, &object.id);
            self.db.batch_delete(batch, CF_OWNER_INDEX, &index_key);

            debug!(
                "Removed from owner index: {} -> {}",
                owner_addr, object.id
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use silver_core::object::ObjectType;
    use silver_core::TransactionDigest;
    use tempfile::TempDir;

    fn create_test_store() -> (ObjectStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(RocksDatabase::open(temp_dir.path()).unwrap());
        let store = ObjectStore::new(db);
        (store, temp_dir)
    }

    fn create_test_object(id: u8, owner: u8, version: u64) -> Object {
        Object::new(
            ObjectID::new([id; 64]),
            SequenceNumber::new(version),
            Owner::AddressOwner(SilverAddress::new([owner; 64])),
            ObjectType::Coin,
            vec![1, 2, 3, 4],
            TransactionDigest::new([0; 64]),
            1000,
        )
    }

    #[test]
    fn test_put_and_get_object() {
        let (store, _temp) = create_test_store();

        let object = create_test_object(1, 10, 0);
        let object_id = object.id;

        // Put object
        store.put_object(&object).unwrap();

        // Get object
        let retrieved = store.get_object(&object_id).unwrap();
        assert!(retrieved.is_some());

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, object.id);
        assert_eq!(retrieved.version, object.version);
        assert_eq!(retrieved.data, object.data);
    }

    #[test]
    fn test_object_exists() {
        let (store, _temp) = create_test_store();

        let object = create_test_object(1, 10, 0);
        let object_id = object.id;

        // Should not exist initially
        assert!(!store.exists(&object_id).unwrap());

        // Put object
        store.put_object(&object).unwrap();

        // Should exist now
        assert!(store.exists(&object_id).unwrap());
    }

    #[test]
    fn test_delete_object() {
        let (store, _temp) = create_test_store();

        let object = create_test_object(1, 10, 0);
        let object_id = object.id;

        // Put object
        store.put_object(&object).unwrap();
        assert!(store.exists(&object_id).unwrap());

        // Delete object
        store.delete_object(&object_id).unwrap();

        // Should not exist anymore
        assert!(!store.exists(&object_id).unwrap());
    }

    #[test]
    fn test_get_objects_by_owner() {
        let (store, _temp) = create_test_store();

        let owner1 = SilverAddress::new([10; 64]);
        let owner2 = SilverAddress::new([20; 64]);

        // Create objects for owner1
        let obj1 = create_test_object(1, 10, 0);
        let obj2 = create_test_object(2, 10, 0);

        // Create object for owner2
        let obj3 = create_test_object(3, 20, 0);

        // Store all objects
        store.put_object(&obj1).unwrap();
        store.put_object(&obj2).unwrap();
        store.put_object(&obj3).unwrap();

        // Query owner1's objects
        let owner1_objects = store.get_objects_by_owner(&owner1).unwrap();
        assert_eq!(owner1_objects.len(), 2);

        // Query owner2's objects
        let owner2_objects = store.get_objects_by_owner(&owner2).unwrap();
        assert_eq!(owner2_objects.len(), 1);
    }

    #[test]
    fn test_get_object_refs_by_owner() {
        let (store, _temp) = create_test_store();

        let owner = SilverAddress::new([10; 64]);

        // Create objects
        let obj1 = create_test_object(1, 10, 0);
        let obj2 = create_test_object(2, 10, 1);

        // Store objects
        store.put_object(&obj1).unwrap();
        store.put_object(&obj2).unwrap();

        // Query object refs
        let refs = store.get_object_refs_by_owner(&owner).unwrap();
        assert_eq!(refs.len(), 2);

        // Verify refs contain correct versions
        assert!(refs.iter().any(|r| r.version.value() == 0));
        assert!(refs.iter().any(|r| r.version.value() == 1));
    }

    #[test]
    fn test_batch_put_objects() {
        let (store, _temp) = create_test_store();

        let objects = vec![
            create_test_object(1, 10, 0),
            create_test_object(2, 10, 0),
            create_test_object(3, 20, 0),
        ];

        // Batch put
        store.batch_put_objects(&objects).unwrap();

        // Verify all objects exist
        for obj in &objects {
            assert!(store.exists(&obj.id).unwrap());
        }
    }

    #[test]
    fn test_batch_delete_objects() {
        let (store, _temp) = create_test_store();

        let objects = vec![
            create_test_object(1, 10, 0),
            create_test_object(2, 10, 0),
            create_test_object(3, 20, 0),
        ];

        // Put objects
        store.batch_put_objects(&objects).unwrap();

        // Collect IDs
        let ids: Vec<ObjectID> = objects.iter().map(|o| o.id).collect();

        // Batch delete
        store.batch_delete_objects(&ids).unwrap();

        // Verify all objects are deleted
        for id in &ids {
            assert!(!store.exists(id).unwrap());
        }
    }

    #[test]
    fn test_update_object_version() {
        let (store, _temp) = create_test_store();

        let object_v0 = create_test_object(1, 10, 0);
        let object_id = object_v0.id;

        // Store version 0
        store.put_object(&object_v0).unwrap();

        // Update to version 1
        let object_v1 = create_test_object(1, 10, 1);
        store.put_object(&object_v1).unwrap();

        // Retrieve and verify it's version 1
        let retrieved = store.get_object(&object_id).unwrap().unwrap();
        assert_eq!(retrieved.version.value(), 1);
    }

    #[test]
    fn test_shared_object_not_in_owner_index() {
        let (store, _temp) = create_test_store();

        // Create shared object
        let mut object = create_test_object(1, 10, 0);
        object.owner = Owner::Shared {
            initial_shared_version: SequenceNumber::new(0),
        };

        // Store object
        store.put_object(&object).unwrap();

        // Query by original owner - should find nothing
        let owner = SilverAddress::new([10; 64]);
        let objects = store.get_objects_by_owner(&owner).unwrap();
        assert_eq!(objects.len(), 0);
    }

    #[test]
    fn test_immutable_object_not_in_owner_index() {
        let (store, _temp) = create_test_store();

        // Create immutable object
        let mut object = create_test_object(1, 10, 0);
        object.owner = Owner::Immutable;

        // Store object
        store.put_object(&object).unwrap();

        // Query by original owner - should find nothing
        let owner = SilverAddress::new([10; 64]);
        let objects = store.get_objects_by_owner(&owner).unwrap();
        assert_eq!(objects.len(), 0);
    }

    #[test]
    fn test_get_object_count() {
        let (store, _temp) = create_test_store();

        // Initially should be 0
        let count = store.get_object_count().unwrap();
        assert_eq!(count, 0);

        // Add objects
        let objects = vec![
            create_test_object(1, 10, 0),
            create_test_object(2, 10, 0),
            create_test_object(3, 20, 0),
        ];
        store.batch_put_objects(&objects).unwrap();

        // Count should be 3 (approximate)
        let count = store.get_object_count().unwrap();
        assert!(count >= 3);
    }

    #[test]
    fn test_get_storage_size() {
        let (store, _temp) = create_test_store();

        // Add some objects
        let objects = vec![
            create_test_object(1, 10, 0),
            create_test_object(2, 10, 0),
        ];
        store.batch_put_objects(&objects).unwrap();

        // Size should be non-negative
        let size = store.get_storage_size().unwrap();
        assert!(size >= 0);
    }

    #[test]
    fn test_owner_index_updated_on_transfer() {
        let (store, _temp) = create_test_store();

        let owner1 = SilverAddress::new([10; 64]);
        let owner2 = SilverAddress::new([20; 64]);

        // Create object owned by owner1
        let object = create_test_object(1, 10, 0);
        store.put_object(&object).unwrap();

        // Verify owner1 has the object
        let owner1_objects = store.get_objects_by_owner(&owner1).unwrap();
        assert_eq!(owner1_objects.len(), 1);

        // Transfer to owner2
        let mut transferred = object.clone();
        transferred.owner = Owner::AddressOwner(owner2);
        transferred.version = SequenceNumber::new(1);
        store.put_object(&transferred).unwrap();

        // Verify owner1 no longer has it (old index entry should be gone)
        let owner1_objects = store.get_objects_by_owner(&owner1).unwrap();
        assert_eq!(owner1_objects.len(), 0);

        // Verify owner2 has it
        let owner2_objects = store.get_objects_by_owner(&owner2).unwrap();
        assert_eq!(owner2_objects.len(), 1);
    }
}
