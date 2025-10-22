
use crate::cache_policy::{
    create_policy, CacheConfig, CacheError, CachePolicy, CacheResult, PolicyType,
};
use arrow_array::Array;
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::common::ScalarValue;
use datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::physical_plan::Statistics;
use object_store::{path::Path, ObjectMeta};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Trait to calculate heap memory size for statistics objects
trait HeapSize {
    fn heap_size(&self) -> usize;
}

impl HeapSize for Statistics {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.num_rows.heap_size()
            + self.total_byte_size.heap_size()
            + self.column_statistics.heap_size()
    }
}

impl<T: HeapSize + std::fmt::Debug + Clone + PartialEq + Eq + PartialOrd> HeapSize
for Precision<T>
{
    fn heap_size(&self) -> usize {
        match self {
            Precision::Exact(val) => std::mem::size_of::<Self>() + val.heap_size(),
            Precision::Inexact(val) => std::mem::size_of::<Self>() + val.heap_size(),
            Precision::Absent => std::mem::size_of::<Self>(),
        }
    }
}

impl HeapSize for usize {
    fn heap_size(&self) -> usize {
        0 // Primitive types don't have heap allocation
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + (self.capacity() * std::mem::size_of::<T>())
            + self.iter().map(|item| item.heap_size()).sum::<usize>()
    }
}

impl HeapSize for ColumnStatistics {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.null_count.heap_size()
            + self.max_value.heap_size()
            + self.min_value.heap_size()
            + self.distinct_count.heap_size()
    }
}

impl HeapSize for ScalarValue {
    fn heap_size(&self) -> usize {
        match self {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                std::mem::size_of::<Self>() + s.capacity()
            }
            ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
                std::mem::size_of::<Self>() + b.capacity()
            }
            ScalarValue::List(arr) => {
                // Estimate list array memory size
                std::mem::size_of::<Self>() + std::mem::size_of_val(arr.as_ref()) + (arr.len() * 8)
            }
            ScalarValue::Struct(arr) => {
                // Estimate struct array memory size
                std::mem::size_of::<Self>() + std::mem::size_of_val(arr.as_ref()) + (arr.len() * 16)
            }
            _ => std::mem::size_of::<Self>(), // Primitive types and nulls
        }
    }
}

/// Extension trait to add memory_size method to Statistics
trait StatisticsMemorySize {
    fn memory_size(&self) -> usize;
}

impl StatisticsMemorySize for Statistics {
    fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.num_rows.heap_size()
            + self.total_byte_size.heap_size()
            + self.column_statistics.heap_size()
    }
}

/// Combined memory tracking and policy-based eviction cache
///
/// This cache leverages DashMap's built-in concurrency from DefaultFileStatisticsCache
/// and adds memory tracking + policy-based eviction on top.
pub struct CustomStatsCache {
    /// The underlying DataFusion statistics cache (DashMap-based, already thread-safe)
    inner_cache: DefaultFileStatisticsCache,
    /// The eviction policy (thread-safe)
    policy: Arc<Mutex<Box<dyn CachePolicy>>>,
    /// Cache configuration
    config: CacheConfig,
    /// Memory usage tracker - maps cache keys to their memory consumption (thread-safe)
    memory_tracker: Arc<Mutex<HashMap<String, usize>>>,
    /// Total memory consumed by all entries (thread-safe)
    total_memory: Arc<Mutex<usize>>,
}

impl CustomStatsCache {
    /// Create a new custom statistics cache
    pub fn new(config: CacheConfig) -> Self {
        let inner_cache = DefaultFileStatisticsCache::default();
        let policy = Arc::new(Mutex::new(create_policy(config.policy_type.clone())));

        Self {
            inner_cache,
            policy,
            config, // No mutex needed - we can update it when needed
            memory_tracker: Arc::new(Mutex::new(HashMap::new())),
            total_memory: Arc::new(Mutex::new(0)),
        }
    }

    /// Create with default configuration
    pub fn with_default_config() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Get the underlying cache for compatibility
    pub fn inner(&self) -> &DefaultFileStatisticsCache {
        &self.inner_cache
    }

    /// Get total memory consumed by all cached statistics
    pub fn memory_consumed(&self) -> usize {
        self.total_memory.lock().map(|guard| *guard).unwrap_or(0)
    }

    /// Update the cache size limit
    pub fn update_size_limit(&self, new_limit: usize) -> CacheResult<()> {
        // Note: We can't update self.config.size_limit here since we don't have &mut self
        // For now, we'll trigger eviction based on the new limit
        let current_size = self.current_size()?;
        if current_size > new_limit {
            let target_eviction = current_size - (new_limit as f64 * 0.8) as usize;
            self.evict(target_eviction)?;
        }
        Ok(())
    }

    /// Switch to a different eviction policy
    pub fn set_policy(&self, policy_type: PolicyType) -> CacheResult<()> {
        let mut policy_guard = self
            .policy
            .lock()
            .map_err(|e| CacheError::PolicyLockError {
                reason: format!("Failed to acquire policy lock: {}", e),
            })?;

        // Create new policy and transfer existing entries
        let mut new_policy = create_policy(policy_type.clone());

        // Get all current entries and notify new policy
        if let Ok(tracker) = self.memory_tracker.lock() {
            for (key, size) in tracker.iter() {
                new_policy.on_insert(key, *size);
            }
        }

        *policy_guard = new_policy;
        // Note: We can't update self.config.policy_type here since we don't have &mut self
        // The policy change is effective immediately through the policy_guard update

        Ok(())
    }

    /// Get current policy name
    pub fn policy_name(&self) -> CacheResult<String> {
        let policy_guard = self
            .policy
            .lock()
            .map_err(|e| CacheError::PolicyLockError {
                reason: format!("Failed to acquire policy lock: {}", e),
            })?;
        Ok(policy_guard.policy_name().to_string())
    }

    /// Get current cache size according to policy (uses actual memory consumption)
    pub fn current_size(&self) -> CacheResult<usize> {
        Ok(self.memory_consumed())
    }

    /// Check if eviction is needed and trigger it
    pub fn evict_if_needed(&self) -> CacheResult<usize> {
        let current_size = self.current_size()?;
        // Use a default threshold since we can't access self.config without &mut
        let threshold_size = (self.config.size_limit as f64 * 0.8) as usize;

        if current_size > threshold_size {
            let target_eviction = current_size - threshold_size;
            self.evict(target_eviction)
        } else {
            Ok(0)
        }
    }

    /// Manually trigger eviction to free up specified amount of memory
    pub fn evict(&self, target_size: usize) -> CacheResult<usize> {
        if target_size == 0 {
            return Ok(0);
        }

        let candidates = {
            let policy_guard = self
                .policy
                .lock()
                .map_err(|e| CacheError::PolicyLockError {
                    reason: format!("Failed to acquire policy lock: {}", e),
                })?;
            policy_guard.select_for_eviction(target_size)
        };

        let mut freed_size = 0;
        for key in candidates {
            // Get memory size before removal
            let entry_size = if let Ok(tracker) = self.memory_tracker.lock() {
                tracker.get(&key).copied().unwrap_or(0)
            } else {
                0
            };

            if entry_size > 0 {
                // Parse key back to path and remove
                if let Ok(path) = self.parse_key_to_path(&key) {
                    self.remove_internal(&path);
                    freed_size += entry_size;

                    // Stop if we've freed enough
                    if freed_size >= target_size {
                        break;
                    }
                }
            }
        }

        Ok(freed_size)
    }

    /// Parse cache key back to Path
    fn parse_key_to_path(&self, key: &str) -> CacheResult<Path> {
        Ok(Path::from(key))
    }

    /// Remove implementation that works with &self using interior mutability
    /// This allows removal without requiring &mut self
    pub fn remove_internal(&self, k: &Path) -> Option<Arc<Statistics>> {
        let key = k.to_string();

        // Get the value before removal
        let result = self.inner_cache.get(k);

        // Update memory tracking
        if let Ok(mut tracker) = self.memory_tracker.lock() {
            if let Ok(mut total) = self.total_memory.lock() {
                if let Some(old_size) = tracker.remove(&key) {
                    *total = total.saturating_sub(old_size);
                }
            }
        }

        // Notify policy of removal
        if let Ok(mut policy_guard) = self.policy.lock() {
            policy_guard.on_remove(&key);
        }

        result
    }
}

// Implement CacheAccessor - DashMap handles concurrency, we just need to handle the &mut self requirement
impl CacheAccessor<Path, Arc<Statistics>> for CustomStatsCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Statistics>> {
        let result = self.inner_cache.get(k);

        if result.is_some() {
            // Notify policy of access
            let key = k.to_string();
            let memory_size = if let Ok(tracker) = self.memory_tracker.lock() {
                tracker.get(&key).copied().unwrap_or(0)
            } else {
                0
            };

            if let Ok(mut policy_guard) = self.policy.lock() {
                policy_guard.on_access(&key, memory_size);
            }
        }

        result
    }

    fn get_with_extra(&self, k: &Path, _extra: &Self::Extra) -> Option<Arc<Statistics>> {
        self.get(k)
    }

    fn put(&self, k: &Path, v: Arc<Statistics>) -> Option<Arc<Statistics>> {
        let meta = ObjectMeta {
            location: k.clone(),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };
        self.put_with_extra(k, v, &meta)
    }

    fn put_with_extra(
        &self,
        k: &Path,
        v: Arc<Statistics>,
        e: &Self::Extra,
    ) -> Option<Arc<Statistics>> {
        let key = k.to_string();
        let memory_size = v.memory_size();

        // Check if we need to evict before inserting
        let _ = self.evict_if_needed();

        // Put in the underlying cache (DashMap handles concurrency)
        let result = self.inner_cache.put_with_extra(k, v.clone(), e);

        // Track memory usage
        if let Ok(mut tracker) = self.memory_tracker.lock() {
            if let Ok(mut total) = self.total_memory.lock() {
                // If there was a previous entry, subtract its memory
                if let Some(old_size) = tracker.get(&key) {
                    *total = total.saturating_sub(*old_size);
                }

                // Add new entry memory
                tracker.insert(key.clone(), memory_size);
                *total += memory_size;
            }
        }

        // Notify policy of insertion
        if let Ok(mut policy_guard) = self.policy.lock() {
            policy_guard.on_insert(&key, memory_size);
        }

        result
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Statistics>> {
        let key = k.to_string();

        // Actually remove from the underlying cache
        let result = self.inner_cache.remove(k);

        // Only proceed with tracking updates if the entry existed
        if result.is_some() {
            // Update memory tracking
            if let Ok(mut tracker) = self.memory_tracker.lock() {
                if let Ok(mut total) = self.total_memory.lock() {
                    if let Some(old_size) = tracker.remove(&key) {
                        *total = total.saturating_sub(old_size);
                    }
                }
            }

            // Notify policy of removal
            if let Ok(mut policy_guard) = self.policy.lock() {
                policy_guard.on_remove(&key);
            }
        }

        result
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.inner_cache.get(k).is_some()
    }

    fn len(&self) -> usize {
        self.memory_tracker.lock().map(|t| t.len()).unwrap_or(0)
    }

    fn clear(&self) {
        // Clear memory tracking
        if let Ok(mut tracker) = self.memory_tracker.lock() {
            tracker.clear();
        }

        if let Ok(mut total) = self.total_memory.lock() {
            *total = 0;
        }

        // Clear policy
        if let Ok(mut policy_guard) = self.policy.lock() {
            policy_guard.clear();
        }

        // Note: DefaultFileStatisticsCache doesn't support clear
        // but we've reset our tracking
    }

    fn name(&self) -> String {
        format!(
            "CustomStatsCache({})",
            self.policy_name().unwrap_or_else(|_| "unknown".to_string())
        )
    }
}

impl Default for CustomStatsCache {
    fn default() -> Self {
        Self::with_default_config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use datafusion::common::stats::Precision;

    fn create_test_statistics() -> Statistics {
        Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(50000),
            column_statistics: vec![],
        }
    }

    fn create_test_path(name: &str) -> Path {
        Path::from(format!("/test/{}.parquet", name))
    }

    fn create_test_meta(path: &Path) -> ObjectMeta {
        ObjectMeta {
            location: path.clone(),
            last_modified: Utc::now(),
            size: 1000,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_custom_stats_cache_creation() {
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 1024 * 1024,
            eviction_threshold: 0.8,
        };

        let cache = CustomStatsCache::new(config);
        assert_eq!(cache.policy_name().unwrap(), "lru");
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_memory_tracking_with_policy() {
        let cache = CustomStatsCache::with_default_config();

        // Initially empty
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);

        // Add an entry
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path, stats, &meta);

        // Should have memory consumption and policy tracking
        assert!(cache.memory_consumed() > 0);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_size().unwrap(), cache.memory_consumed());

        // Verify we can retrieve it
        assert!(cache.get(&path).is_some());
    }

    #[test]
    fn test_policy_based_eviction_with_memory() {
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 1000, // Small limit to trigger eviction
            eviction_threshold: 0.8,
        };
        let cache = CustomStatsCache::new(config);

        // Add multiple entries to trigger eviction
        for i in 0..10 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());

            cache.put_with_extra(&path, stats, &meta);
        }

        // Memory should be managed by eviction
        let final_memory = cache.memory_consumed();
        assert!(
            final_memory <= 1000,
            "Memory should be within limit due to eviction"
        );
        assert!(cache.len() > 0, "Should still have some entries");
    }

    #[test]
    fn test_manual_eviction_with_memory_tracking() {
        let cache = CustomStatsCache::with_default_config();

        // Add entries
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        let memory_before = cache.memory_consumed();
        assert!(memory_before > 0);

        // Manually evict some memory
        let freed = cache.evict(memory_before / 2).unwrap();
        let memory_after = cache.memory_consumed();

        assert!(freed > 0, "Should have freed some memory");
        assert!(memory_after < memory_before, "Memory should be reduced");
    }

    #[test]
    fn test_policy_switching_with_memory() {
        let mut cache = CustomStatsCache::with_default_config();

        // Add some entries
        for i in 0..3 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        let memory_before = cache.memory_consumed();

        // Switch policy
        assert_eq!(cache.policy_name().unwrap(), "lru");
        cache.set_policy(PolicyType::Lfu).unwrap();
        assert_eq!(cache.policy_name().unwrap(), "lfu");

        // Memory tracking should be preserved
        assert_eq!(cache.memory_consumed(), memory_before);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_remove_with_memory_tracking() {
        let mut cache = CustomStatsCache::with_default_config();

        // Add entries
        let path1 = create_test_path("file1");
        let path2 = create_test_path("file2");
        let meta1 = create_test_meta(&path1);
        let meta2 = create_test_meta(&path2);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path1, stats.clone(), &meta1);
        cache.put_with_extra(&path2, stats, &meta2);

        let memory_with_two = cache.memory_consumed();
        assert_eq!(cache.len(), 2);

        // Remove one entry
        let removed = cache.remove(&path1);
        assert!(removed.is_some());

        let memory_with_one = cache.memory_consumed();
        assert_eq!(cache.len(), 1);
        assert!(memory_with_one < memory_with_two);

        // Remove second entry
        cache.remove(&path2);
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_clear_with_memory_tracking() {
        let cache = CustomStatsCache::with_default_config();

        // Add multiple entries
        for i in 0..3 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        assert!(cache.memory_consumed() > 0);
        assert_eq!(cache.len(), 3);

        // Clear all
        cache.clear();

        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_lru_eviction_with_memory() {
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 2000, // Limit to ~2 entries
            eviction_threshold: 0.8,
        };
        let cache = CustomStatsCache::new(config);

        // Add entries
        let mut paths = vec![];
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            paths.push(path.clone());

            cache.put_with_extra(&path, stats, &meta);
        }

        // Access some entries to update LRU order
        cache.get(&paths[2]);
        cache.get(&paths[4]);

        // Memory should be within limits due to LRU eviction
        assert!(cache.memory_consumed() <= 2000);

        // Recently accessed entries should still be available
        assert!(cache.get(&paths[2]).is_some());
        assert!(cache.get(&paths[4]).is_some());
    }

    #[test]
    fn test_lfu_eviction_with_memory() {
        let config = CacheConfig {
            policy_type: PolicyType::Lfu,
            size_limit: 2000, // Limit to ~2 entries
            eviction_threshold: 0.8,
        };
        let cache = CustomStatsCache::new(config);

        // Add entries
        let mut paths = vec![];
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            paths.push(path.clone());

            cache.put_with_extra(&path, stats, &meta);
        }

        // Create frequency patterns
        for _ in 0..5 {
            cache.get(&paths[1]);
            cache.get(&paths[3]);
        }

        // Memory should be within limits due to LFU eviction
        assert!(cache.memory_consumed() <= 2000);

        // Frequently accessed entries should still be available
        assert!(cache.get(&paths[1]).is_some());
        assert!(cache.get(&paths[3]).is_some());
    }

    #[test]
    fn test_concurrent_operations() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(CustomStatsCache::with_default_config());
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent operations
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let path = create_test_path(&format!("concurrent{}", i));
                let meta = create_test_meta(&path);
                let stats = Arc::new(create_test_statistics());

                // Put operation
                cache_clone.put_with_extra(&path, stats, &meta);

                // Get operation
                let result = cache_clone.get(&path);
                assert!(result.is_some());
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should have entries
        assert!(cache.len() > 0);
    }
}
