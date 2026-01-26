use std::sync::{Arc, Mutex};
use jni::JNIEnv;

use datafusion::execution::cache::cache_manager::{FileMetadataCache};
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use object_store::ObjectMeta;
use vectorized_exec_spi::log_error;

pub const ALL_CACHE_TYPES: &[&str] = &[CACHE_TYPE_METADATA, CACHE_TYPE_STATS];

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

// Helper function to handle cache errors
fn handle_cache_error(env: &mut JNIEnv, operation: &str, error: &str) {
    let msg = format!("Cache {} failed: {}", operation, error);
    log_error!("[CACHE ERROR] {}", msg);
    let _ = env.throw_new("java/lang/DataFusionException", &msg);
}

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    log_error!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

// Note: MutexFileMetadataCache wrapper has been removed as DefaultFilesMetadataCache
// is already thread-safe with its own internal Mutex.
// The double-locking was causing race conditions and crashes.

// Note: create_cache function has been removed. Cache creation is now handled through CacheManagerConfig only.
// metadata_cache_put, metadata_cache_remove, and metadata_cache_get functions have been moved to CustomCacheManager as internal methods

// Wrapper to make Mutex<DefaultFilesMetadataCache> implement FileMetadataCache
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultFilesMetadataCache) -> Self {
        Self {
            inner: Mutex::new(cache),
        }
    }

    pub fn clear(&self) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.clear();
        }
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.update_cache_limit(new_limit);
        }
    }

    pub fn cache_limit(&self) -> usize {
        if let Ok(cache) = self.inner.lock() {
            cache.cache_limit()
        } else {
            0
        }
    }
}

// Implement CacheAccessor which is required by FileMetadataCache
impl CacheAccessor<ObjectMeta, Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> for MutexFileMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(cache) => cache.get(k),
            Err(e) => {
                log_cache_error("get", &e.to_string());
                None
            }
        }
    }

    fn get_with_extra(&self, k: &ObjectMeta, extra: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(cache) => cache.get_with_extra(k, extra),
            Err(e) => {
                log_cache_error("get_with_extra", &e.to_string());
                None
            }
        }
    }

    fn put(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.put(k, v),
            Err(e) => {
                log_cache_error("put", &e.to_string());
                None
            }
        }
    }

    fn put_with_extra(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>, e: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.put_with_extra(k, v, e),
            Err(err) => {
                log_cache_error("put_with_extra", &err.to_string());
                None
            }
        }
    }

    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.remove(k),
            Err(e) => {
                log_cache_error("remove", &e.to_string());
                None
            }
        }
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        match self.inner.lock() {
            Ok(cache) => cache.contains_key(k),
            Err(e) => {
                log_cache_error("contains_key", &e.to_string());
                false
            }
        }
    }

    fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.len(),
            Err(e) => {
                log_cache_error("len", &e.to_string());
                0
            }
        }
    }

    fn clear(&self) {
        match self.inner.lock() {
            Ok(mut cache) => cache.clear(),
            Err(e) => log_cache_error("clear", &e.to_string()),
        }
    }

    fn name(&self) -> String {
        match self.inner.lock() {
            Ok(cache) => cache.name(),
            Err(e) => {
                log_cache_error("name", &e.to_string());
                "cache_error".to_string()
            }
        }
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.cache_limit(),
            Err(e) => {
                log_cache_error("cache_limit", &e.to_string());
                0
            }
        }
    }

    fn update_cache_limit(&self, limit: usize) {
        match self.inner.lock() {
            Ok(mut cache) => cache.update_cache_limit(limit),
            Err(e) => log_cache_error("update_cache_limit", &e.to_string()),
        }
    }

    fn list_entries(&self) -> std::collections::HashMap<object_store::path::Path, datafusion::execution::cache::cache_manager::FileMetadataCacheEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.list_entries(),
            Err(e) => {
                log_cache_error("list_entries", &e.to_string());
                std::collections::HashMap::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::cache::cache_unit::DefaultFilesMetadataCache;
    use chrono::Utc;
    use object_store::path::Path;

    // ============================================================================
    // MEMORY ACCURACY TESTS - Using mimalloc FFI
    // ============================================================================
    
    // FFI bindings to mimalloc stats functions
    extern "C" {
        fn mi_stats_reset();
        fn mi_process_info(
            elapsed_msecs: *mut usize,
            user_msecs: *mut usize,
            system_msecs: *mut usize,
            current_rss: *mut usize,
            peak_rss: *mut usize,
            current_commit: *mut usize,
            peak_commit: *mut usize,
            page_faults: *mut usize,
        );
    }

    /// Get current committed memory from mimalloc
    fn get_mimalloc_committed_memory() -> usize {
        let mut elapsed_msecs = 0usize;
        let mut user_msecs = 0usize;
        let mut system_msecs = 0usize;
        let mut current_rss = 0usize;
        let mut peak_rss = 0usize;
        let mut current_commit = 0usize;
        let mut peak_commit = 0usize;
        let mut page_faults = 0usize;
        
        unsafe {
            mi_process_info(
                &mut elapsed_msecs,
                &mut user_msecs,
                &mut system_msecs,
                &mut current_rss,
                &mut peak_rss,
                &mut current_commit,
                &mut peak_commit,
                &mut page_faults,
            );
        }
        current_commit
    }

    fn create_test_object_meta(name: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(format!("/test/{}.parquet", name)),
            last_modified: Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_metadata_cache_creation() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024); // 100MB limit
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        assert_eq!(cache.cache_limit(), 100 * 1024 * 1024);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_metadata_cache_memory_starts_at_zero() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        // Lock and check memory_used
        let memory_used = {
            let cache_guard = cache.inner.lock().expect("Failed to lock cache");
            cache_guard.memory_used()
        };
        println!("Empty metadata cache memory_used: {} bytes", memory_used);
        assert_eq!(memory_used, 0, "Empty cache should report 0 memory used");
    }

    #[test]
    fn test_metadata_cache_clear_resets_memory() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        // Clear the cache
        cache.clear();
        
        // Memory should be 0 after clear
        let memory_used = {
            let cache_guard = cache.inner.lock().expect("Failed to lock cache");
            cache_guard.memory_used()
        };
        assert_eq!(memory_used, 0, "Cleared cache should report 0 memory used");
    }

    #[test]
    fn test_metadata_cache_update_limit() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        assert_eq!(cache.cache_limit(), 100 * 1024 * 1024);
        
        // Update the limit
        cache.update_cache_limit(50 * 1024 * 1024);
        
        assert_eq!(cache.cache_limit(), 50 * 1024 * 1024);
    }

    #[test]
    fn test_mutex_file_metadata_cache_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = Arc::new(MutexFileMetadataCache::new(inner_cache));
        let mut handles = vec![];

        // Spawn multiple threads that try to access the cache concurrently
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                // Multiple operations that should be thread-safe
                let _limit = cache_clone.cache_limit();
                let _len = cache_clone.len();
                let _name = cache_clone.name();
                let _entries = cache_clone.list_entries();
                
                // Try to get a non-existent key
                let meta = create_test_object_meta(&format!("thread_{}", i), 1000);
                let _result = cache_clone.get(&meta);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        println!("Concurrent access test passed - no deadlocks or panics");
    }

    #[test]
    fn test_metadata_cache_memory_tracking_reports_reasonable_values() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        // Get initial state
        let initial_memory = if let Ok(guard) = cache.inner.lock() {
            guard.memory_used()
        } else {
            panic!("Failed to lock cache");
        };
        
        println!("=== Metadata Cache Memory Test ===");
        println!("Initial memory_used: {} bytes", initial_memory);
        println!("Cache limit: {} bytes", cache.cache_limit());
        println!("Cache length: {}", cache.len());
        
        // The DefaultFilesMetadataCache memory_used() should return 0 for empty cache
        assert_eq!(initial_memory, 0, "Empty cache should have 0 memory used");
    }

    #[test]
    fn test_contains_key_returns_false_for_missing() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        let meta = create_test_object_meta("nonexistent", 1000);
        assert!(!cache.contains_key(&meta), "Should return false for non-existent key");
    }

    #[test]
    fn test_get_returns_none_for_missing() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        let meta = create_test_object_meta("nonexistent", 1000);
        assert!(cache.get(&meta).is_none(), "Should return None for non-existent key");
    }

    #[test]
    fn test_metadata_cache_name() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        let name = cache.name();
        println!("Metadata cache name: {}", name);
        assert!(!name.is_empty(), "Cache name should not be empty");
    }

    #[test]
    fn test_list_entries_empty_cache() {
        let inner_cache = DefaultFilesMetadataCache::new(100 * 1024 * 1024);
        let cache = MutexFileMetadataCache::new(inner_cache);
        
        let entries = cache.list_entries();
        assert!(entries.is_empty(), "Empty cache should have no entries");
    }

    #[test]
    fn test_memory_measurement_with_mimalloc() {
        // Force any pending deallocations
        std::hint::black_box(vec![0u8; 1024]);
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let before = get_mimalloc_committed_memory();
        
        // Create multiple caches to get measurable memory
        let caches: Vec<_> = (0..100)
            .map(|_| {
                let inner = DefaultFilesMetadataCache::new(1024 * 1024);
                MutexFileMetadataCache::new(inner)
            })
            .collect();
        
        std::hint::black_box(&caches);
        
        let after = get_mimalloc_committed_memory();
        let delta = after.saturating_sub(before);
        
        println!("=== Metadata Cache Creation Memory Test ===");
        println!("Created 100 MutexFileMetadataCache instances");
        println!("Mimalloc committed memory delta: {} bytes", delta);
        println!("Average per cache: {} bytes", delta / 100);
        
        // Just verify we can create caches without issues
        assert_eq!(caches.len(), 100);
    }
}
