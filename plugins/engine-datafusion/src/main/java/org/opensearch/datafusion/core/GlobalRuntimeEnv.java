/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;

import static org.opensearch.datafusion.DataFusionQueryJNI.closeGlobalRuntime;
import static org.opensearch.datafusion.DataFusionQueryJNI.createDefaultGlobalRuntimeEnv;
import static org.opensearch.datafusion.DataFusionQueryJNI.createGlobalRuntime;
import static org.opensearch.datafusion.DataFusionQueryJNI.createTokioRuntime;

/**
 * Global runtime environment for DataFusion operations.
 * Manages the lifecycle of the native DataFusion runtime.
 */
public class GlobalRuntimeEnv implements AutoCloseable {
    // ptr to runtime environment in df
    private long ptr;
    private long tokio_runtime_ptr;
    private CacheManager cacheManager;
    private static final Logger logger = LogManager.getLogger(GlobalRuntimeEnv.class);


    public GlobalRuntimeEnv(ClusterSettings clusterSettings) {

        try {
            // Create cache configuration
            long cacheConfigPtr = CacheUtils.createCacheConfig(clusterSettings);
            // Create global runtime with cache config
            this.ptr = createGlobalRuntime(cacheConfigPtr);
            this.cacheManager = new CacheManager();
            this.tokio_runtime_ptr = createTokioRuntime();
        } catch (Exception e) {
            logger.error("Failed to create global runtime environment. Using DefaultGlobalRuntimeEnv", e);
            useDefaultRuntime();

        }
    }

    private void useDefaultRuntime() {
        this.ptr = createDefaultGlobalRuntimeEnv();
        this.cacheManager = null;
        this.tokio_runtime_ptr = createTokioRuntime();
    }

    /**
     * Gets the native pointer to the runtime environment.
     * @return the native pointer
     */
    public long getPointer() {
        return ptr;
    }

    public long getTokioRuntimePtr() {
        return tokio_runtime_ptr;
    }

    @Override
    public void close() throws IOException {
        if (cacheManager != null) {
            cacheManager.close();
        }
        if (this.ptr == -1) {
            logger.info("Global runtime environment already closed");
            return;
        }
        closeGlobalRuntime(this.ptr);
        this.ptr = -1;

    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
