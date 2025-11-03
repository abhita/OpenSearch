
use std::sync::{Arc, Mutex};
use jni::JNIEnv;

use datafusion::execution::cache::cache_manager::{FileMetadataCache};
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use object_store::ObjectMeta;

// Helper function to handle cache errors
fn handle_cache_error(env: &mut JNIEnv, operation: &str, error: &str) {
    let msg = format!("Cache {} failed: {}", operation, error);
    let _ = env.throw_new("java/lang/RuntimeException", &msg);
}

/*
DefaultFilesMetadataCache of datafusion is internally wrapped in a Mutex and requires mut access for operations like remove.
Refer: https://github.com/apache/datafusion/blob/main/datafusion/execution/src/cache/cache_unit.rs#L312-L315
https://github.com/apache/datafusion/blob/main/datafusion/execution/src/cache/cache_unit.rs#L402

Having multiple references to Cache, trying to acquire a mutable reference for methods like remove
would lead to failures.
Hence explicit handling of mutable references is required for which MutexFileMetadataCache is introduced
*/

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
}

// Implement CacheAccessor which is required by FileMetadataCache
impl CacheAccessor<ObjectMeta, Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> for MutexFileMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(cache) => cache.get(k),
            Err(_) => None, // Return None on lock failure
        }
    }

    fn get_with_extra(&self, k: &ObjectMeta, extra: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(cache) => cache.get_with_extra(k, extra),
            Err(_) => None,
        }
    }

    fn put(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.put(k, v),
            Err(_) => None,
        }
    }

    fn put_with_extra(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>, e: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.put_with_extra(k, v, e),
            Err(_) => None,
        }
    }

    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.remove(k),
            Err(_) => None,
        }
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        match self.inner.lock() {
            Ok(cache) => cache.contains_key(k),
            Err(_) => false,
        }
    }

    fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.len(),
            Err(_) => 0,
        }
    }

    fn clear(&self) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.clear();
        }
    }

    fn name(&self) -> String {
        match self.inner.lock() {
            Ok(cache) => cache.name(),
            Err(_) => "cache_error".to_string(),
        }
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.cache_limit(),
            Err(_) => 0,
        }
    }

    fn update_cache_limit(&self, limit: usize) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.update_cache_limit(limit);
        }
    }

    fn list_entries(&self) -> std::collections::HashMap<object_store::path::Path, datafusion::execution::cache::cache_manager::FileMetadataCacheEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.list_entries(),
            Err(_) => std::collections::HashMap::new(),
        }
    }
}

// JNI wrapper functions for cache operations
use jni::objects::JClass;
use jni::sys::jlong;

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_cacheGet(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    key_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const MutexFileMetadataCache) };
    let key = unsafe { &*(key_ptr as *const ObjectMeta) };

    match cache.inner.lock() {
        Ok(cache_guard) => {
            match cache_guard.get(key) {
                Some(metadata) => Box::into_raw(Box::new(metadata)) as jlong,
                None => 0,
            }
        }
        Err(e) => {
            handle_cache_error(&mut env, "get", &e.to_string());
            -1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_cachePut(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    key_ptr: jlong,
    value_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const MutexFileMetadataCache) };
    let key = unsafe { &*(key_ptr as *const ObjectMeta) };
    let value = unsafe { Box::from_raw(value_ptr as *mut Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>) };

    match cache.inner.lock() {
        Ok(mut cache_guard) => {
            match cache_guard.put(key, *value) {
                Some(old_value) => Box::into_raw(Box::new(old_value)) as jlong,
                None => 0,
            }
        }
        Err(e) => {
            handle_cache_error(&mut env, "put", &e.to_string());
            -1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_cacheRemove(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    key_ptr: jlong,
) -> jlong {
    let cache = unsafe { &mut *(cache_ptr as *mut MutexFileMetadataCache) };
    let key = unsafe { &*(key_ptr as *const ObjectMeta) };

    match cache.remove(key) {
        Some(metadata) => Box::into_raw(Box::new(metadata)) as jlong,
        None => {
            let _ = env.throw_new("java/util/NoSuchElementException", "Key not found in cache");
            -1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_cacheClear(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) {
    let cache = unsafe { &*(cache_ptr as *const MutexFileMetadataCache) };

    match cache.inner.lock() {
        Ok(mut cache_guard) => cache_guard.clear(),
        Err(e) => handle_cache_error(&mut env, "clear", &e.to_string()),
    }
}
