/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::ptr::addr_of_mut;
use datafusion_expr::expr_rewriter::unalias;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use std::sync::{Arc, Mutex};
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::DataType;
use arrow_schema::ffi::FFI_ArrowSchema;
use std::time::Instant;

mod util;
mod row_id_optimizer;
mod listing_table;
mod metadata_cache;
mod statistics_cache;
mod cache_policy;

use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::Statistics;
use datafusion::common::stats::Precision;

use crate::cache_policy::{CacheConfig, PolicyType};
use crate::statistics_cache::CustomStatisticsCache;
use crate::util::{
    construct_file_metadata, create_object_meta_from_file, create_object_meta_from_filenames,
    parse_string_arr, set_object_result_error, set_object_result_ok,
};
use datafusion::prelude::SessionConfig;
use datafusion::DATAFUSION_VERSION;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::TryStreamExt;
use jni::objects::{JObjectArray, JString};
use object_store::ObjectMeta;
use prost::Message;
use tokio::runtime::Runtime;
use crate::listing_table::{ListingOptions, ListingTable, ListingTableConfig};
use crate::row_id_optimizer::FilterRowIdOptimizer;
use crate::metadata_cache::{MutexFileMetadataCache};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::execution::cache::cache_manager::{self, CacheManagerConfig, FileMetadataCache};
use datafusion::execution::cache::cache_unit::{
    DefaultFileStatisticsCache, DefaultFilesMetadataCache, DefaultListFilesCache,
};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileMeta;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_datasource::ListingTableUrl;


/// Create a new DataFusion session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config(config);
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

/// Close and cleanup a DataFusion context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
}

/// Get version information
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#, DATAFUSION_VERSION);
    env.new_string(version_info).expect("Couldn't create Java string").as_raw()
}

/// Get version information (legacy method name)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    env.new_string(DATAFUSION_VERSION).expect("Couldn't create Java string").as_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let rt = Runtime::new().unwrap();
    let ctx = Box::into_raw(Box::new(rt)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    cache_config_ptr: jlong,
) -> jlong {
    let cache_manager_config = unsafe { Box::from_raw(cache_config_ptr as *mut CacheManagerConfig) };

    let runtime_env = RuntimeEnvBuilder::default()
        .with_cache_manager(*cache_manager_config)
        .build();

    Box::into_raw(Box::new(runtime_env)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    let runtimeEnv = unsafe { &mut *(runtime_id as *mut RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtimeEnv.clone()));
    let ctx = Box::into_raw(Box::new(context)) as jlong;
    ctx
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray
) -> jlong {

    let table_path: String = env.get_string(&table_path).expect("Couldn't get java string!").into();
    let files: Vec<String> = parse_string_arr(&mut env, files).expect("Expected list of files");
    let files_meta = create_object_meta_from_filenames(&table_path, files);

    let table_path = ListingTableUrl::parse(table_path).unwrap();
    let shard_view = ShardView::new(table_path, files_meta);
    Box::into_raw(Box::new(shard_view)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_closeDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_meta: Arc<Vec<ObjectMeta>>
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_meta: Vec<ObjectMeta>) -> Self {
        let files_meta = Arc::new(files_meta);
        ShardView {
            table_path,
            files_meta
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_meta(&self) -> Arc<Vec<ObjectMeta>> {
        self.files_meta.clone()
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_executeSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    global_runtime_env_ptr: jlong,
    tokio_runtime_env_ptr: jlong,
    // callback: JObject,
) -> jlong {
    let overall = Instant::now();
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime_ptr = unsafe { &*(tokio_runtime_env_ptr as *const Runtime)};
    let table_name: String = env.get_string(&table_name).expect("Couldn't get java string!").into();
    let runtime_env = unsafe { &*(global_runtime_env_ptr as *const RuntimeEnv) };

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    println!("Table path: {}", table_path);
    println!("Files: {:?}", files_meta);

    // TODO: get config from CSV DataFormat
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 1;

    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime_env.clone()))
        .with_default_features()
        // .with_optimizer_rule(Arc::new(OptimizeRowId))
        // .with_physical_optimizer_rule(Arc::new(FilterRowIdOptimizer)) // TODO: enable only for query phase
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet"); // TODO: take this as parameter
        // .with_table_partition_cols(vec![("row_base".to_string(), DataType::Int32)]); // TODO: enable only for query phase

    // Ideally the executor will give this
    runtime_ptr.block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table(table_name, provider)
            .expect("Failed to attach the Table");

    });

    let start = Instant::now();
    // TODO : how to close ctx ?
    // Convert Java byte array to Rust Vec<u8>
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to convert plan bytes: {}", e);
            env.throw_new("java/lang/Exception", error_msg);
            return 0;
        }
    };

    let substrait_plan = match Plan::decode(plan_bytes_vec.as_slice()) {
        Ok(plan) => {
            // println!("SUBSTRAIT rust: Decoding is successful, Plan has {} relations", plan.relations.len());
            plan
        },
        Err(e) => {
            return 0;
        }
    };

    //let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    runtime_ptr.block_on(async {

        let logical_plan = match from_substrait_plan(&ctx.state(), &substrait_plan).await {
            Ok(plan) => {
                // println!("SUBSTRAIT Rust: LogicalPlan: {:?}", plan);
                  let duration = start.elapsed();
                         println!("Rust: Substrait decoding time in milliseconds: {}", duration.as_millis());
                plan
            },
            Err(e) => {
                println!("SUBSTRAIT Rust: Failed to convert Substrait plan: {}", e);
                return 0;
            }
        };
        let dataframe = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let stream = dataframe.execute_stream().await.unwrap();
        let stream_ptr = Box::into_raw(Box::new(stream)) as jlong;
        // println!("The memory used currently right now: {:?}", jemalloc_stats::refresh_allocated());
        let duration1 = overall.elapsed();
        println!("Rust: Overall query setup time in milliseconds: {}", duration1.as_millis());

        stream_ptr

    })
}

// If we need to create session context separately
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_nativeCreateSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    shard_view_ptr: jlong,
    global_runtime_env_ptr: jlong,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_meta();

    // Will use it once the global RunTime is defined
    // let runtime_arc = unsafe {
    //     let boxed = &*(runtime_env_ptr as *const Pin<Arc<RuntimeEnv>>);
    //     (**boxed).clone()
    // };

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(table_path.prefix(), files_meta);

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default()
            .with_list_files_cache(Some(list_file_cache))).build().unwrap();



    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), Arc::new(runtime_env));


    // Create default parquet options
    let file_format = CsvFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".csv");


    // let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };
    let mut session_context_ptr = 0;

    // Ideally the executor will give this
    Runtime::new().expect("Failed to create Tokio Runtime").block_on(async {
        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path.clone())
            .await.unwrap();


        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        // Create a new TableProvider
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let shard_id = table_path.prefix().filename().expect("error in fetching Path");
        ctx.register_table(shard_id, provider)
            .expect("Failed to attach the Table");

        // Return back after wrapping in Box
        session_context_ptr = Box::into_raw(Box::new(ctx)) as jlong
    });

    session_context_ptr
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime_ptr as *mut Runtime) };

    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        //let fetch_start = std::time::Instant::now();
        let next = stream.try_next().await;
        //let fetch_time = fetch_start.elapsed();
        match next {
            Ok(Some(batch)) => {
                //let convert_start = std::time::Instant::now();
                // Convert to struct array for compatibility with FFI
                //println!("Num rows : {}", batch.num_rows());
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                //let convert_time = convert_start.elapsed();
                // ffi_array must remain alive until after the callback is called
                // let callback_start = std::time::Instant::now();
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
                // let callback_time = callback_start.elapsed();
                // println!("Fetch: {:?}, Convert: {:?}, Callback: {:?}",
                //          fetch_time, convert_time, callback_time);
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
        //println!("Total time: {:?}", start.elapsed());
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_RecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_initCacheManagerConfig(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = CacheManagerConfig::default();
    Box::into_raw(Box::new(config)) as jlong
}


/// Create a metadata cache and add it to the CacheManagerConfig
/// The config_ptr remains the same, only the contents are updated
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createMetadataCache(
    _env: JNIEnv,
    _class: JClass,
    config_ptr: jlong,
    size_limit: jlong,
) -> jlong {
    // Create cache with wrapper that implements FileMetadataCache
    let inner_cache = DefaultFilesMetadataCache::new(size_limit.try_into().unwrap());
    let wrapped_cache = Arc::new(MutexFileMetadataCache::new(inner_cache));

    // Update the CacheManagerConfig at the same memory location
    if config_ptr != 0 {
        let cache_manager_config = unsafe { &mut *(config_ptr as *mut CacheManagerConfig) };
        *cache_manager_config = cache_manager_config.clone()
            .with_file_metadata_cache(Some(wrapped_cache.clone()));
    }

    // Return the Arc<MutexFileMetadataCache> pointer for JNI operations
    Box::into_raw(Box::new(wrapped_cache)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCachePut(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false
    };
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let data_format = if file_path.to_lowercase().ends_with(".parquet") {
        "parquet"
    } else {
        return false; // Skip unsupported formats
    };

    let object_meta = create_object_meta_from_file(&file_path);
    let store = Arc::new(object_store::local::LocalFileSystem::new());

    // Use Runtime to block on the async operation
    let metadata = Runtime::new()
        .expect("Failed to create Tokio Runtime")
        .block_on(async {
            construct_file_metadata(store.as_ref(), &object_meta, data_format)
                .await
                .expect("Failed to construct file metadata")
        });

    let len_before = cache.len();
    let old_metadata = cache.put(&object_meta, metadata);
    let len_after = cache.len();

    if len_after > len_before {
        println!("Successfully cached new metadata for: {} (cache: {} -> {})", file_path, len_before, len_after);
        true
    } else if old_metadata.is_some() {
        println!("Successfully updated existing metadata for: {} (cache: {})", file_path, len_after);
        true
    } else {
        println!("Failed to cache metadata for: {} (cache: {})", file_path, len_after);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheRemove(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let object_meta = create_object_meta_from_file(&file_path);

    // Lock the mutex and remove
    if let Some(_cache_obj) = cache.inner.lock().unwrap().remove(&object_meta) {
        println!("Cache removed for: {}", file_path);
        true
    } else {
        println!("Item not found in cache: {}", file_path);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheGet(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };

    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let object_meta = create_object_meta_from_file(&file_path);

    match cache.get(&object_meta) {
        Some(metadata) => {
            println!("Retrieved metadata for: {} - size: {:?}", file_path, metadata.memory_size());
            true
        },
        None => {
            println!("No metadata found for: {}", file_path);
            false
        },
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheGetEntries(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jni::sys::jobjectArray {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };

    // Get all entries from the cache
    let entries = cache.list_entries();

    println!("Retrieved {} cache entries", entries.len());

    // Create String array class
    let string_class = match env.find_class("java/lang/String") {
        Ok(cls) => cls,
        Err(e) => {
            println!("Failed to find String class: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // Create array with size = number of entries * 3 (path, size, hit_count for each entry)
    let array_size = (entries.len() * 3) as i32;
    let result_array = match env.new_object_array(array_size, &string_class, JObject::null()) {
        Ok(arr) => arr,
        Err(e) => {
            println!("Failed to create object array: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // Fill the array with entries
    let mut index = 0;
    for (path, entry) in entries.iter() {
        // Add path
        if let Ok(path_str) = env.new_string(path.as_ref()) {
            let _ = env.set_object_array_element(&result_array, index, path_str);
        }
        index += 1;

        // Add size
        if let Ok(size_str) = env.new_string(entry.size_bytes.to_string()) {
            let _ = env.set_object_array_element(&result_array, index, size_str);
        }
        index += 1;

        // Add hit_count
        if let Ok(hit_count_str) = env.new_string(entry.hits.to_string()) {
            let _ = env.set_object_array_element(&result_array, index, hit_count_str);
        }
        index += 1;
    }

    result_array.as_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheGetSize(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong
) -> usize {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    cache.inner.lock().unwrap().memory_used()
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheContainsFile(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false
    };
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    let object_meta = create_object_meta_from_file(&file_path);
    cache.inner.lock().unwrap().contains_key(&object_meta)
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheUpdateSizeLimit(
     env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    new_size_limit: usize
) -> bool {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    cache.inner.lock().unwrap().update_cache_limit(new_size_limit);
    if cache.inner.lock().unwrap().cache_limit() == new_size_limit {
        println!("Cache size limit updated to: {}", new_size_limit);
        true
    } else {
        println!("Failed to update cache size limit to: {}", new_size_limit);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_metadataCacheClear(
     env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong
) {
    let cache = unsafe { &*(cache_ptr as *const Arc<MutexFileMetadataCache>) };
    cache.inner.lock().unwrap().clear();
}

// STATISTICS CACHE METHODS

/// Create a statistics cache and add it to the CacheManagerConfig
/// The config_ptr remains the same, only the contents are updated
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createStatisticsCache(
    _env: JNIEnv,
    _class: JClass,
    config_ptr: jlong,
    size_limit: jlong,
) -> jlong {
    // Create memory-aware policy cache with default LRU policy
    let config = CacheConfig {
        policy_type: PolicyType::Lru,
        size_limit: size_limit as usize,
        eviction_threshold: 0.8,
    };
    let memory_aware_cache = CustomStatisticsCache::new(config);

    // Create a new DefaultFileStatisticsCache for the CacheManagerConfig
    let stats_cache = Arc::new(DefaultFileStatisticsCache::default());

    // Update the CacheManagerConfig at the same memory location
    if config_ptr != 0 {
        let cache_manager_config = unsafe { &mut *(config_ptr as *mut CacheManagerConfig) };
        *cache_manager_config = cache_manager_config
            .clone()
            .with_files_statistics_cache(Some(stats_cache));
    }

    // Return the CustomStatisticsCache pointer for JNI operations
    Box::into_raw(Box::new(memory_aware_cache)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCachePut(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };

    let cache =  unsafe { &mut *(cache_ptr as *mut CustomStatisticsCache) };
    let data_format = if file_path.to_lowercase().ends_with(".parquet") {
        "parquet"
    } else {
        return false; // Skip unsupported formats
    };

    // Use DataFusion's parquet statistics construction
    let statistics = Runtime::new()
        .expect("Failed to create Tokio Runtime")
        .block_on(async {
            use datafusion::common::stats::ColumnStatistics;
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            use parquet::file::reader::{FileReader, SerializedFileReader};
            use std::fs::File;

            match File::open(&file_path) {
                Ok(file) => {
                    match SerializedFileReader::new(file) {
                        Ok(reader) => {
                            let metadata = reader.metadata();

                            // Extract row count from all row groups
                            let mut num_rows = 0;
                            let mut total_byte_size = 0;
                            for rg in metadata.row_groups() {
                                num_rows += rg.num_rows();
                                total_byte_size += rg.total_byte_size();
                            }

                            // Build column statistics from parquet metadata
                            let mut column_stats = Vec::new();
                            let num_columns = metadata.file_metadata().schema_descr().num_columns();

                            for col_idx in 0..num_columns {
                                let mut col_null_count = 0;
                                let mut col_distinct_count = None;
                                let mut has_min_max = false;

                                // Aggregate statistics across all row groups for this column
                                for rg in metadata.row_groups() {
                                    if let Some(col_chunk) = rg.columns().get(col_idx) {
                                        if let Some(stats) = col_chunk.statistics() {
                                            if let Some(null_count) = stats.null_count_opt() {
                                                col_null_count += null_count;
                                            }
                                            if let Some(distinct) = stats.distinct_count() {
                                                col_distinct_count = Some(distinct);
                                            }
                                            has_min_max = stats.has_min_max_set();
                                        }
                                    }
                                }

                                // Create column statistics
                                column_stats.push(ColumnStatistics {
                                    null_count: Precision::Exact(col_null_count as usize),
                                    max_value: Precision::Absent,
                                    min_value: Precision::Absent,
                                    distinct_count: col_distinct_count
                                        .map(|count| Precision::Exact(count as usize))
                                        .unwrap_or(Precision::Absent),
                                    sum_value: Precision::Absent,
                                });
                            }

                            Arc::new(Statistics {
                                num_rows: Precision::Exact(num_rows as usize),
                                total_byte_size: Precision::Exact(total_byte_size as usize),
                                column_statistics: column_stats,
                            })
                        }
                        Err(_) => Arc::new(Statistics::new_unknown(&arrow_schema::Schema::empty())),
                    }
                }
                Err(_) => Arc::new(Statistics::new_unknown(&arrow_schema::Schema::empty())),
            }
        });

    let path_obj = match object_store::path::Path::from_url_path(&file_path) {
        Ok(path) => path,
        Err(_) => {
            println!("Failed to create path object from: {}", file_path);
            return false;
        }
    };

    let object_meta = create_object_meta_from_file(&file_path);

    let len_before = cache.len();
    let result = cache.put_with_extra(&path_obj, statistics, &object_meta);
    let len_after = cache.len();

    if len_after > len_before {
        println!("Successfully cached new stats for: {} (cache: {} -> {})", file_path, len_before, len_after);
        true
    } else if result.is_some() {
        println!("Successfully updated existing stats for: {} (cache: {})", file_path, len_after);
        true
    } else {
        println!("Failed to cache stats for: {} (cache: {})", file_path, len_after);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheRemove(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };

    let path_obj = match object_store::path::Path::from_url_path(&file_path) {
        Ok(path) => path,
        Err(_) => {
            println!("Failed to create path object from: {}", file_path);
            return false;
        }
    };

    let cache = unsafe { &mut *(cache_ptr as *mut CustomStatisticsCache) };

    if let Some(_cache_obj) = cache.remove(&path_obj){
        println!("Cache removed for: {}", file_path);
        true
    } else {
        println!("Item not found in cache: {}", file_path);
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGet(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString,
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };

    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    let path_obj = match object_store::path::Path::from_url_path(&file_path) {
        Ok(path) => path,
        Err(_) => {
            println!("Failed to create path object from: {}", file_path);
            return false;
        }
    };

    match cache.get(&path_obj) {
        Some(statistics) => {
            println!("Retrieved statistics for: {}", file_path);
            true
        }
        None => {
            println!("No statistics found for: {}", file_path);
            false
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetEntries(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jni::sys::jobjectArray {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };

    // Get all entries from the DashMap
    let entries: Vec<_> = cache.inner().iter().collect();

    println!("Retrieved {} statistics cache entries", entries.len());

    // Create String array class
    let string_class = match env.find_class("java/lang/String") {
        Ok(cls) => cls,
        Err(e) => {
            println!("Failed to find String class: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // Create array with just the paths
    let array_size = entries.len() as i32;
    let result_array = match env.new_object_array(array_size, &string_class, JObject::null()) {
        Ok(arr) => arr,
        Err(e) => {
            println!("Failed to create object array: {:?}", e);
            return std::ptr::null_mut();
        }
    };

    // Fill the array with entry paths
    for (index, entry) in entries.iter().enumerate() {
        let path_str = entry.key().to_string();
        if let Ok(jstr) = env.new_string(path_str) {
            let _ = env.set_object_array_element(&result_array, index as i32, jstr);
        }
    }

    result_array.as_raw()
}

/// Get current memory usage from statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetSize(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };

    // Return actual memory consumed by the cache entries
    cache.memory_consumed() as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheContainsFile(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };

    let path_obj = match object_store::path::Path::from_url_path(&file_path) {
        Ok(path) => path,
        Err(_) => {
            println!("Failed to create path object from: {}", file_path);
            return false;
        }
    };
    cache.inner().contains_key(&path_obj)
}

/// Update size limit for statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheUpdateSizeLimit(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    new_size_limit: jlong,
) -> bool {
    let cache = unsafe { &mut *(cache_ptr as *mut CustomStatisticsCache) };

    // Update size limit using the policy cache's functionality
    match cache.update_size_limit(new_size_limit as usize) {
        Ok(_) => {
            println!("Statistics cache size limit updated to: {}", new_size_limit);
        }
        Err(_) => {
            println!("Failed to update statistics cache size limit to: {}", new_size_limit);
        }
    }

    true
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheClear(
    env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.clear();
    println!("Statistics cache cleared");
}

/// Get hit count from statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetHitCount(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.hit_count() as jlong
}

/// Get miss count from statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetMissCount(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.miss_count() as jlong
}

/// Get hit rate from statistics cache (returns value between 0.0 and 1.0)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetHitRate(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> f64 {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.hit_rate()
}

/// Reset hit and miss counters in statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheResetStats(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.reset_stats();
    println!("Statistics cache hit/miss counters reset");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_concurrent_file_statistics_cache_operations() {
        // Create a statistics cache
        let stats_cache = Arc::new(DefaultFileStatisticsCache::default());

        // Create some test paths
        let test_paths: Vec<object_store::path::Path> = (0..10)
            .map(|i| object_store::path::Path::from(format!("/test/file{}.parquet", i)))
            .collect();

        // Pre-populate cache with some statistics
        for (i, path) in test_paths.iter().enumerate() {
            let stats = Arc::new(Statistics {
                num_rows: Precision::Exact(i * 100),
                total_byte_size: Precision::Exact(i * 1000),
                column_statistics: vec![],
            });
            // DefaultFileStatisticsCache requires put_with_extra
            let meta = ObjectMeta {
                location: path.clone(),
                last_modified: chrono::Utc::now(),
                size: (i * 1000) as u64,
                e_tag: None,
                version: None,
            };
            stats_cache.put_with_extra(path, stats, &meta);
        }

        println!("Initial cache size: {}", stats_cache.len());

        // Spawn multiple reader threads
        let mut reader_handles = vec![];
        for thread_id in 0..5 {
            let cache_clone = stats_cache.clone();
            let paths_clone = test_paths.clone();

            let handle = thread::spawn(move || {
                for _ in 0..20 {
                    for path in &paths_clone {
                        if let Some(stats) = cache_clone.get(path) {
                            // Verify we can read the statistics
                            assert!(stats.num_rows != Precision::Absent);
                        }
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                println!("Reader thread {} completed", thread_id);
            });

            reader_handles.push(handle);
        }

        // Spawn multiple writer threads that add new entries
        let mut writer_handles = vec![];
        for thread_id in 0..3 {
            let cache_clone = stats_cache.clone();

            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let path = object_store::path::Path::from(
                        format!("/test/writer{}_file{}.parquet", thread_id, i)
                    );
                    let stats = Arc::new(Statistics {
                        num_rows: Precision::Exact(thread_id * 1000 + i),
                        total_byte_size: Precision::Exact(thread_id * 10000 + i * 100),
                        column_statistics: vec![],
                    });
                    let meta = ObjectMeta {
                        location: path.clone(),
                        last_modified: chrono::Utc::now(),
                        size: (thread_id * 10000 + i * 100) as u64,
                        e_tag: None,
                        version: None,
                    };
                    cache_clone.put_with_extra(&path, stats, &meta);
                    thread::sleep(Duration::from_millis(2));
                }
                println!("Writer thread {} completed", thread_id);
            });

            writer_handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in reader_handles {
            handle.join().expect("Reader thread panicked");
        }

        for handle in writer_handles {
            handle.join().expect("Writer thread panicked");
        }

        println!("Final cache size: {}", stats_cache.len());

        // Verify cache contains expected entries
        assert!(stats_cache.len() >= 10, "Cache should contain at least the initial 10 entries");

        // Verify we can still read from cache
        for path in &test_paths {
            assert!(stats_cache.contains_key(path), "Original paths should still be in cache");
        }

        println!("✓ Concurrent operations test passed!");
    }

    #[test]
    fn test_arc_get_mut_behavior() {
        // Test the Arc::get_mut behavior used in statisticsCacheRemove
        let path = object_store::path::Path::from("/test/single.parquet");

        // Create a single Arc reference
        let mut cache_arc = Arc::new(DefaultFileStatisticsCache::default());
        let meta = ObjectMeta {
            location: path.clone(),
            last_modified: chrono::Utc::now(),
            size: 2000,
            e_tag: None,
            version: None,
        };
        cache_arc.put_with_extra(&path, Arc::new(Statistics {
            num_rows: Precision::Exact(200),
            total_byte_size: Precision::Exact(2000),
            column_statistics: vec![],
        }), &meta);

        assert!(cache_arc.contains_key(&path), "Path should be in cache");

        // With single reference, Arc::get_mut should succeed
        if let Some(cache_mut) = Arc::get_mut(&mut cache_arc) {
            let _ = cache_mut.remove(&path);
            println!("✓ Successfully removed with single Arc reference");
        } else {
            panic!("Arc::get_mut failed with single reference");
        }

        assert!(!cache_arc.contains_key(&path), "Path should be removed");

        // Test with multiple references
        let mut cache_arc2 = Arc::new(DefaultFileStatisticsCache::default());
        let meta2 = ObjectMeta {
            location: path.clone(),
            last_modified: chrono::Utc::now(),
            size: 3000,
            e_tag: None,
            version: None,
        };
        cache_arc2.put_with_extra(&path, Arc::new(Statistics {
            num_rows: Precision::Exact(300),
            total_byte_size: Precision::Exact(3000),
            column_statistics: vec![],
        }), &meta2);

        let _clone = cache_arc2.clone(); // Create second reference

        // With multiple references, Arc::get_mut should fail
        if Arc::get_mut(&mut cache_arc2).is_some() {
            panic!("Arc::get_mut should fail with multiple references");
        } else {
            println!("✓ Arc::get_mut correctly failed with multiple references");
        }

        println!("✓ Arc::get_mut behavior test passed!");
    }
}
