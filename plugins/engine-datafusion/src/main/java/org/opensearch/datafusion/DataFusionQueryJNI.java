/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.index.engine.exec.FileMetadata;

import java.util.Collection;

/**
 * JNI wrapper for DataFusion operations
 */
public class DataFusionQueryJNI {

    private static boolean libraryLoaded = false;

    static {
        loadNativeLibrary();
    }

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private DataFusionQueryJNI() {
        // Utility class
    }

    /**
     * Load the native library from resources
     */
    private static synchronized void loadNativeLibrary() {
        if (libraryLoaded) {
            return;
        }

        try {
            // Try to load the library directly
            System.loadLibrary("opensearch_datafusion_jni");
            libraryLoaded = true;
        } catch (UnsatisfiedLinkError e) {
            // Try loading from resources
            try {
                String osName = System.getProperty("os.name").toLowerCase();
                String libExtension = osName.contains("windows") ? ".dll" : (osName.contains("mac") ? ".dylib" : ".so");
                String libName = "libopensearch_datafusion_jni" + libExtension;

                java.io.InputStream is = DataFusionQueryJNI.class.getResourceAsStream("/native/" + libName);
                if (is != null) {
                    java.io.File tempFile = java.io.File.createTempFile("libopensearch_datafusion_jni", libExtension);
                    tempFile.deleteOnExit();

                    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            fos.write(buffer, 0, bytesRead);
                        }
                    }

                    System.load(tempFile.getAbsolutePath());
                    libraryLoaded = true;
                } else {
                    throw new RuntimeException("Native library not found: " + libName, e);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load native library", ex);
            }
        }
    }

    /**
     * Create a new global runtime environment
     * @return runtime env pointer for subsequent operations
     */
    public static native long createGlobalRuntime(long cacheManagerPtr);

    /**
     * Create a default global runtime environment with default configuration
     * @return runtime environment pointer
     */
    public static native long createDefaultGlobalRuntimeEnv();

    /**
     * Create a new Tokio async runtime for executing async operations
     * @return tokio runtime pointer
     */
    public static native long createTokioRuntime();

    /**
     * Closes global runtime environment
     * @param pointer the runtime environment pointer to close
     * @return status code
     */
    public static native long closeGlobalRuntime(long pointer);

    /**
     * Get version information
     * @return JSON string with version information
     */
    public static native String getVersionInfo();

    /**
     * Create a new DataFusion session context
     * @param runtimeId the global runtime environment ID
     * @return context ID for subsequent operations
     */
    public static native long createSessionContext(long runtimeId);

    /**
     * Close and cleanup a DataFusion context
     * @param contextId the context ID to close
     */
    public static native void closeSessionContext(long contextId);

    /**
     * Execute a Substrait query plan
     * @param cachePtr the session context ID
     * @param tableName the name of the table to query
     * @param substraitPlan the serialized Substrait query plan
     * @param tokioRuntimePtr pointer to the Tokio runtime
     * @param runtimeEnvPtr pointer to the runtime environment
     * @return stream pointer for result iteration
     */
    public static native long executeQueryPhase(long cachePtr, String tableName, byte[] substraitPlan, long tokioRuntimePtr, long runtimeEnvPtr);

    /**
     * Execute a fetch phase to retrieve specific rows by their IDs
     * @param cachePtr the session context ID
     * @param rowIds array of row IDs to fetch
     * @param projections array of column names to project
     * @param tokioRuntimePtr pointer to the Tokio runtime
     * @param runtimeEnvPtr pointer to the runtime environment
     * @return stream pointer for result iteration
     */
    public static native long executeFetchPhase(long cachePtr, long[] rowIds, String[] projections, long tokioRuntimePtr, long runtimeEnvPtr);

    /**
     * Create a DataFusion reader for reading parquet files
     * @param path the base path for the files
     * @param files array of file names to read
     * @return reader pointer
     */
    public static native long createDatafusionReader(String path, String[] files);

    /**
     * Close and cleanup a DataFusion reader
     * @param ptr the reader pointer to close
     */
    public static native void closeDatafusionReader(long ptr);

    /**
     * Register a directory with CSV files
     * @param contextId the session context ID
     * @param tableName the table name to register
     * @param directoryPath the directory path containing CSV files
     * @param fileNames array of file names to register
     * @return status code
     */
    public static native int registerCsvDirectory(long contextId, String tableName, String directoryPath, String[] fileNames);

    /**
     * Check if stream has more data
     * @param streamPtr the stream pointer
     * @return true if more data available
     */
    public static native boolean streamHasNext(long streamPtr);

    /**
     * Get next batch from stream
     * @param streamPtr the stream pointer
     * @return byte array containing the next batch, or null if no more data
     */
    public static native byte[] streamNext(long streamPtr);

    /**
     * Close and cleanup a result stream
     * @param streamPtr the stream pointer to close
     */
    public static native void closeStream(long streamPtr);


    /**
     * Initialize cache manager config (creates empty config)
     * @return cache manager config pointer
     */
    public static native long initCacheManagerConfig();

    // Cache creation methods

    /**
     * Generic cache creation method that handles all cache types
     * @param cacheManagerConfigPointer cache manager config pointer
     * @param cacheType cache type name (e.g., "METADATA", "STATS")
     * @param sizeLimit size limit for cache in bytes
     * @param evictionType eviction policy (e.g., "LRU", "LFU", "FIFO")
     * @return cache pointer
     */
    public static native long createCache(long cacheManagerConfigPointer, String cacheType, long sizeLimit, String evictionType);

    /**
     * Create a new empty CustomCacheManager
     * @return custom cache manager pointer
     */
    public static native long createCustomCacheManager();

    /**
     * Destroy CustomCacheManager and free associated resources
     * @throws DataFusionException if destruction fails
     */
    public static native void destroyCustomCacheManager();

    /**
     * Add multiple files to the cache manager for caching metadata
     * @param filePaths array of file paths to add to the cache
     * @throws DataFusionException if adding files fails
     */
    public static native void cacheManagerAddFiles(String[] filePaths);

    /**
     * Remove multiple files from the cache manager
     * @param filePaths array of file paths to remove from the cache
     * @throws DataFusionException if removing files fails
     */
    public static native void cacheManagerRemoveFiles(String[] filePaths);

    /**
     * Clear all entries from a specific cache type
     * @param cacheType the type of cache to clear (e.g., "METADATA", "STATS")
     * @throws DataFusionException if clearing cache fails
     */
    public static native void cacheManagerClearByCacheType(String cacheType);

    /**
     * Clear all entries from all cache types
     * @throws DataFusionException if clearing cache fails
     */
    public static native void cacheManagerClear();

    /**
     * Update the size limit for a specific cache type
     * @param cacheType the type of cache to update (e.g., "METADATA", "STATS")
     * @param sizeLimit new size limit in bytes
     * @throws DataFusionException if updating size limit fails
     */
    public static native void cacheManagerUpdateSizeLimitForCacheType(String cacheType, long sizeLimit);

    /**
     * Get the memory consumed by a specific cache type
     * @param cacheType the type of cache to query (e.g., "METADATA", "STATS")
     * @return memory consumed in bytes
     * @throws DataFusionException if querying memory fails
     */
    public static native long cacheManagerGetMemoryConsumedForCacheType(String cacheType);

    /**
     * Check if a specific file exists in a cache type
     * @param cacheType the type of cache to check (e.g., "METADATA", "STATS")
     * @param filePath the file path to check
     * @return true if the file exists in the cache, false otherwise
     * @throws DataFusionException if checking cache fails
     */
    public static native boolean cacheManagerGetItemByCacheType(String cacheType, String filePath);

    /**
     * Get the total memory consumed by all cache types
     * @return total memory consumed in bytes across all caches
     * @throws DataFusionException if querying memory fails
     */
    public static native long cacheManagerGetTotalMemoryConsumed();

}
