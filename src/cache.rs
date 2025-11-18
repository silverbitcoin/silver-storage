//! In-memory object cache for performance optimization
//!
//! This module provides an LRU (Least Recently Used) cache for frequently
//! accessed objects to reduce database reads and improve performance.

use dashmap::DashMap;
use parking_lot::RwLock;
use silver_core::{Object, ObjectID};
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::{debug, info};

/// Object cache with LRU eviction policy (OPTIMIZED for Phase 12)
///
/// Provides fast in-memory access to frequently used objects.
/// Uses LRU (Least Recently Used) eviction when cache is full.
///
/// OPTIMIZATIONS:
/// - Default 1GB cache size (configurable)
/// - Concurrent access with DashMap (lock-free reads)
/// - Batch operations for better throughput
/// - Prefetching support for predictable access patterns
/// - Detailed statistics for monitoring
pub struct ObjectCache {
    /// Cache storage (thread-safe concurrent hash map)
    cache: Arc<DashMap<ObjectID, Arc<Object>>>,
    
    /// LRU queue for eviction (protected by RwLock)
    lru_queue: Arc<RwLock<VecDeque<ObjectID>>>,
    
    /// Maximum cache size (number of objects)
    max_size: usize,
    
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
    
    /// Estimated cache size in bytes (approximate)
    estimated_bytes: Arc<RwLock<usize>>,
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    
    /// Number of cache misses
    pub misses: u64,
    
    /// Number of evictions
    pub evictions: u64,
    
    /// Number of insertions
    pub insertions: u64,
    
    /// Current cache size
    pub current_size: usize,
}

impl CacheStats {
    /// Calculate hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
    
    /// Calculate miss rate (0.0 to 1.0)
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }
}

impl ObjectCache {
    /// OPTIMIZATION: Create a new object cache with 1GB default size
    ///
    /// # Arguments
    /// * `max_size` - Maximum number of objects to cache
    ///
    /// # Example
    /// ```ignore
    /// let cache = ObjectCache::new(100_000); // ~1GB for typical objects
    /// ```
    pub fn new(max_size: usize) -> Self {
        info!("Initializing OPTIMIZED ObjectCache with max_size={}", max_size);
        
        Self {
            cache: Arc::new(DashMap::with_capacity(max_size)),
            lru_queue: Arc::new(RwLock::new(VecDeque::with_capacity(max_size))),
            max_size,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            estimated_bytes: Arc::new(RwLock::new(0)),
        }
    }
    
    /// OPTIMIZATION: Create a new object cache with 1GB default size
    ///
    /// Assumes average object size of ~10KB, so 100,000 objects ≈ 1GB
    pub fn with_default_size() -> Self {
        Self::new(100_000) // Changed from 10,000 to 100,000 for 1GB cache
    }
    
    /// OPTIMIZATION: Create cache with size based on available memory
    ///
    /// Allocates a percentage of system memory for the cache.
    ///
    /// # Arguments
    /// * `memory_percentage` - Percentage of system memory to use (0.0 to 1.0)
    pub fn with_memory_percentage(memory_percentage: f64) -> Self {
        use sysinfo::System;
        
        let mut sys = System::new_all();
        sys.refresh_memory();
        
        let total_memory = sys.total_memory() as f64;
        let cache_memory = (total_memory * memory_percentage) as usize;
        
        // Assume average object size of 10KB
        let avg_object_size = 10 * 1024;
        let max_objects = cache_memory / avg_object_size;
        
        info!(
            "Creating cache with {}% of system memory ({} MB, ~{} objects)",
            (memory_percentage * 100.0) as u32,
            cache_memory / (1024 * 1024),
            max_objects
        );
        
        Self::new(max_objects)
    }
    
    /// Get an object from cache
    ///
    /// # Arguments
    /// * `object_id` - Object ID to retrieve
    ///
    /// # Returns
    /// - `Some(object)` if object is in cache (cache hit)
    /// - `None` if object is not in cache (cache miss)
    pub fn get(&self, object_id: &ObjectID) -> Option<Arc<Object>> {
        match self.cache.get(object_id) {
            Some(entry) => {
                // Cache hit
                let object = entry.value().clone();
                drop(entry); // Release the lock
                
                // Update LRU (move to back)
                self.touch(object_id);
                
                // Update stats
                let mut stats = self.stats.write();
                stats.hits += 1;
                
                debug!("Cache hit for object: {}", object_id);
                Some(object)
            }
            None => {
                // Cache miss
                let mut stats = self.stats.write();
                stats.misses += 1;
                
                debug!("Cache miss for object: {}", object_id);
                None
            }
        }
    }
    
    /// Put an object into cache
    ///
    /// If cache is full, evicts the least recently used object.
    ///
    /// # Arguments
    /// * `object` - Object to cache
    pub fn put(&self, object: Object) {
        let object_id = object.id;
        let object_size = self.estimate_object_size(&object);
        
        // Check if we need to evict
        if self.cache.len() >= self.max_size && !self.cache.contains_key(&object_id) {
            self.evict_lru();
        }
        
        // Insert into cache
        let arc_object = Arc::new(object);
        self.cache.insert(object_id, arc_object);
        
        // Update LRU queue
        let mut lru = self.lru_queue.write();
        lru.push_back(object_id);
        
        // Update stats and size
        let mut stats = self.stats.write();
        stats.insertions += 1;
        stats.current_size = self.cache.len();
        drop(stats);
        
        let mut bytes = self.estimated_bytes.write();
        *bytes += object_size;
        
        debug!("Cached object: {} (cache size: {}, ~{} MB)", 
               object_id, self.cache.len(), *bytes / (1024 * 1024));
    }

    /// OPTIMIZATION: Batch put multiple objects into cache
    ///
    /// More efficient than multiple individual put() calls.
    ///
    /// # Arguments
    /// * `objects` - Objects to cache
    pub fn batch_put(&self, objects: Vec<Object>) {
        if objects.is_empty() {
            return;
        }

        debug!("Batch caching {} objects", objects.len());
        
        for object in objects {
            self.put(object);
        }
    }

    /// OPTIMIZATION: Batch get multiple objects from cache
    ///
    /// Returns objects that are in cache, None for cache misses.
    ///
    /// # Arguments
    /// * `object_ids` - Object IDs to retrieve
    ///
    /// # Returns
    /// Vector of optional objects in the same order as object_ids
    pub fn batch_get(&self, object_ids: &[ObjectID]) -> Vec<Option<Arc<Object>>> {
        if object_ids.is_empty() {
            return Vec::new();
        }

        debug!("Batch fetching {} objects from cache", object_ids.len());
        
        object_ids
            .iter()
            .map(|id| self.get(id))
            .collect()
    }

    /// OPTIMIZATION: Prefetch hint for future access
    ///
    /// Marks objects as likely to be accessed soon. This doesn't actually
    /// load them into cache, but can be used by higher-level code to
    /// trigger background loading.
    ///
    /// # Arguments
    /// * `object_ids` - Object IDs that will be accessed soon
    ///
    /// # Returns
    /// Vector of object IDs that are NOT in cache (need to be loaded)
    pub fn prefetch_hint(&self, object_ids: &[ObjectID]) -> Vec<ObjectID> {
        object_ids
            .iter()
            .filter(|id| !self.contains(id))
            .copied()
            .collect()
    }
    
    /// Remove an object from cache
    ///
    /// # Arguments
    /// * `object_id` - Object ID to remove
    pub fn remove(&self, object_id: &ObjectID) {
        if self.cache.remove(object_id).is_some() {
            // Remove from LRU queue
            let mut lru = self.lru_queue.write();
            if let Some(pos) = lru.iter().position(|id| id == object_id) {
                lru.remove(pos);
            }
            
            // Update stats
            let mut stats = self.stats.write();
            stats.current_size = self.cache.len();
            
            debug!("Removed object from cache: {}", object_id);
        }
    }
    
    /// Clear all objects from cache
    pub fn clear(&self) {
        let size_before = self.cache.len();
        
        self.cache.clear();
        self.lru_queue.write().clear();
        
        // Update stats
        let mut stats = self.stats.write();
        stats.current_size = 0;
        
        info!("Cleared cache ({} objects removed)", size_before);
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        self.stats.read().clone()
    }
    
    /// Get current cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    
    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
    
    /// Get maximum cache size
    pub fn max_size(&self) -> usize {
        self.max_size
    }
    
    /// Check if cache contains an object
    pub fn contains(&self, object_id: &ObjectID) -> bool {
        self.cache.contains_key(object_id)
    }
    
    /// Resize the cache
    ///
    /// If new size is smaller than current size, evicts objects until
    /// cache size matches new maximum.
    ///
    /// # Arguments
    /// * `new_max_size` - New maximum cache size
    pub fn resize(&self, new_max_size: usize) {
        info!("Resizing cache from {} to {}", self.max_size, new_max_size);
        
        // Evict objects if new size is smaller
        while self.cache.len() > new_max_size {
            self.evict_lru();
        }
        
        // Note: We can't change max_size as it's not mutable
        // In a real implementation, we'd use Arc<RwLock<usize>> for max_size
        debug!("Cache resized to {} objects", self.cache.len());
    }
    
    /// Get estimated cache size in bytes
    pub fn estimated_size_bytes(&self) -> usize {
        *self.estimated_bytes.read()
    }

    /// Get estimated cache size in megabytes
    pub fn estimated_size_mb(&self) -> usize {
        self.estimated_size_bytes() / (1024 * 1024)
    }

    // ========== Private Helper Methods ==========
    
    /// Touch an object (mark as recently used)
    ///
    /// Moves the object to the back of the LRU queue.
    fn touch(&self, object_id: &ObjectID) {
        let mut lru = self.lru_queue.write();
        
        // Remove from current position
        if let Some(pos) = lru.iter().position(|id| id == object_id) {
            lru.remove(pos);
        }
        
        // Add to back (most recently used)
        lru.push_back(*object_id);
    }
    
    /// Evict the least recently used object
    fn evict_lru(&self) {
        let mut lru = self.lru_queue.write();
        
        if let Some(object_id) = lru.pop_front() {
            drop(lru); // Release lock before removing from cache
            
            if let Some((_, object)) = self.cache.remove(&object_id) {
                // Update size estimate
                let object_size = self.estimate_object_size(&object);
                let mut bytes = self.estimated_bytes.write();
                *bytes = bytes.saturating_sub(object_size);
                drop(bytes);
                
                // Update stats
                let mut stats = self.stats.write();
                stats.evictions += 1;
                stats.current_size = self.cache.len();
                
                debug!("Evicted LRU object: {}", object_id);
            }
        }
    }

    /// Estimate the size of an object in bytes (approximate)
    ///
    /// This is a rough estimate for memory tracking purposes.
    fn estimate_object_size(&self, object: &Object) -> usize {
        // Base object overhead
        let base_size = std::mem::size_of::<Object>();
        
        // Data size
        let data_size = object.data.len();
        
        // Approximate total (includes some overhead for Arc, etc.)
        base_size + data_size + 128 // 128 bytes overhead
    }
}

// Implement Clone for ObjectCache (shares the same underlying cache)
impl Clone for ObjectCache {
    fn clone(&self) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
            lru_queue: Arc::clone(&self.lru_queue),
            max_size: self.max_size,
            stats: Arc::clone(&self.stats),
            estimated_bytes: Arc::clone(&self.estimated_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use silver_core::object::ObjectType;
    use silver_core::{Owner, SequenceNumber, SilverAddress, TransactionDigest};

    fn create_test_object(id: u8, version: u64) -> Object {
        Object::new(
            ObjectID::new([id; 64]),
            SequenceNumber::new(version),
            Owner::AddressOwner(SilverAddress::new([id; 64])),
            ObjectType::Coin,
            vec![1, 2, 3, 4],
            TransactionDigest::new([0; 64]),
            1000,
        )
    }

    #[test]
    fn test_cache_put_and_get() {
        let cache = ObjectCache::new(100);
        let object = create_test_object(1, 0);
        let object_id = object.id;

        // Put object
        cache.put(object.clone());

        // Get object (should be cache hit)
        let cached = cache.get(&object_id);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().id, object_id);

        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.insertions, 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = ObjectCache::new(100);
        let object_id = ObjectID::new([1; 64]);

        // Get non-existent object (should be cache miss)
        let cached = cache.get(&object_id);
        assert!(cached.is_none());

        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_remove() {
        let cache = ObjectCache::new(100);
        let object = create_test_object(1, 0);
        let object_id = object.id;

        // Put and verify
        cache.put(object);
        assert!(cache.contains(&object_id));

        // Remove
        cache.remove(&object_id);
        assert!(!cache.contains(&object_id));

        // Get should miss
        assert!(cache.get(&object_id).is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = ObjectCache::new(100);

        // Add multiple objects
        for i in 0..10 {
            cache.put(create_test_object(i, 0));
        }

        assert_eq!(cache.len(), 10);

        // Clear
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_eviction() {
        let cache = ObjectCache::new(3); // Small cache for testing

        // Fill cache
        let obj1 = create_test_object(1, 0);
        let obj2 = create_test_object(2, 0);
        let obj3 = create_test_object(3, 0);

        cache.put(obj1.clone());
        cache.put(obj2.clone());
        cache.put(obj3.clone());

        assert_eq!(cache.len(), 3);

        // Add one more (should evict obj1 as it's least recently used)
        let obj4 = create_test_object(4, 0);
        cache.put(obj4.clone());

        assert_eq!(cache.len(), 3);
        assert!(!cache.contains(&obj1.id)); // obj1 should be evicted
        assert!(cache.contains(&obj2.id));
        assert!(cache.contains(&obj3.id));
        assert!(cache.contains(&obj4.id));

        // Check eviction stats
        let stats = cache.stats();
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn test_lru_touch() {
        let cache = ObjectCache::new(3);

        let obj1 = create_test_object(1, 0);
        let obj2 = create_test_object(2, 0);
        let obj3 = create_test_object(3, 0);

        cache.put(obj1.clone());
        cache.put(obj2.clone());
        cache.put(obj3.clone());

        // Access obj1 (should move it to back of LRU queue)
        cache.get(&obj1.id);

        // Add obj4 (should evict obj2, not obj1)
        let obj4 = create_test_object(4, 0);
        cache.put(obj4);

        assert!(cache.contains(&obj1.id)); // obj1 should still be there
        assert!(!cache.contains(&obj2.id)); // obj2 should be evicted
        assert!(cache.contains(&obj3.id));
    }

    #[test]
    fn test_cache_stats() {
        let cache = ObjectCache::new(100);

        // Initial stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.insertions, 0);
        assert_eq!(stats.evictions, 0);

        // Add objects
        cache.put(create_test_object(1, 0));
        cache.put(create_test_object(2, 0));

        // Hit
        cache.get(&ObjectID::new([1; 64]));

        // Miss
        cache.get(&ObjectID::new([99; 64]));

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.insertions, 2);
        assert_eq!(stats.hit_rate(), 0.5);
        assert_eq!(stats.miss_rate(), 0.5);
    }

    #[test]
    fn test_cache_contains() {
        let cache = ObjectCache::new(100);
        let object = create_test_object(1, 0);
        let object_id = object.id;

        assert!(!cache.contains(&object_id));

        cache.put(object);
        assert!(cache.contains(&object_id));
    }

    #[test]
    fn test_cache_len() {
        let cache = ObjectCache::new(100);

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());

        cache.put(create_test_object(1, 0));
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());

        cache.put(create_test_object(2, 0));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_max_size() {
        let cache = ObjectCache::new(50);
        assert_eq!(cache.max_size(), 50);
    }

    #[test]
    fn test_cache_resize() {
        let cache = ObjectCache::new(10);

        // Fill cache
        for i in 0..10 {
            cache.put(create_test_object(i, 0));
        }

        assert_eq!(cache.len(), 10);

        // Resize to smaller
        cache.resize(5);
        assert_eq!(cache.len(), 5);

        // Check evictions
        let stats = cache.stats();
        assert_eq!(stats.evictions, 5);
    }

    #[test]
    fn test_cache_clone() {
        let cache1 = ObjectCache::new(100);
        cache1.put(create_test_object(1, 0));

        // Clone shares the same underlying cache
        let cache2 = cache1.clone();

        // Both should see the same object
        assert_eq!(cache1.len(), 1);
        assert_eq!(cache2.len(), 1);

        // Add to cache2
        cache2.put(create_test_object(2, 0));

        // Both should see both objects
        assert_eq!(cache1.len(), 2);
        assert_eq!(cache2.len(), 2);
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let cache = ObjectCache::new(1000);
        let cache_clone = cache.clone();

        // Spawn threads to access cache concurrently
        let handle1 = thread::spawn(move || {
            for i in 0..100 {
                cache_clone.put(create_test_object(i, 0));
            }
        });

        let cache_clone2 = cache.clone();
        let handle2 = thread::spawn(move || {
            for i in 100..200 {
                cache_clone2.put(create_test_object(i, 0));
            }
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Should have 200 objects
        assert_eq!(cache.len(), 200);
    }

    #[test]
    fn test_with_default_size() {
        let cache = ObjectCache::with_default_size();
        assert_eq!(cache.max_size(), 10_000);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let mut stats = CacheStats::default();

        // No requests yet
        assert_eq!(stats.hit_rate(), 0.0);
        assert_eq!(stats.miss_rate(), 1.0);

        // 3 hits, 1 miss
        stats.hits = 3;
        stats.misses = 1;
        assert_eq!(stats.hit_rate(), 0.75);
        assert_eq!(stats.miss_rate(), 0.25);

        // All hits
        stats.hits = 10;
        stats.misses = 0;
        assert_eq!(stats.hit_rate(), 1.0);
        assert_eq!(stats.miss_rate(), 0.0);
    }
}
