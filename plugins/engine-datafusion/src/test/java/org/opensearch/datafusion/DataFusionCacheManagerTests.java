/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.datafusion.search.cache.CacheAccessor;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheType;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATS_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATS_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATS_CACHE_SIZE_LIMIT;

public class DataFusionCacheManagerTests extends OpenSearchTestCase {
    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings mockSettings = Settings.builder().put("path.data", "/tmp/test-data").build();

        when(mockEnvironment.settings()).thenReturn(mockSettings);
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(STATS_CACHE_ENABLED);
        clusterSettingsToAdd.add(STATS_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(STATS_CACHE_EVICTION_TYPE);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);

        service = new DataFusionService(Collections.emptyMap(), clusterSettings);
        service.doStart();
    }

    public void testAddFileToCache() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        CacheAccessor statisticsCache = cacheManager.getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addToCache(List.of(fileName));

        assertTrue((Boolean) metadataCache.get(fileName));
        assertTrue((Boolean) statisticsCache.get(fileName));
        assertTrue(metadataCache.containsFile(fileName));
        assertTrue(statisticsCache.containsFile(fileName));
        assertTrue(metadataCache.getMemoryConsumed() > 0);
    }

    public void testRemoveFileFromCache() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        CacheAccessor statisticsCache = cacheManager.getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        cacheManager.addToCache(List.of(fileName));
        assertTrue(metadataCache.containsFile(fileName));
        assertTrue(statisticsCache.containsFile(fileName));

        boolean removed = cacheManager.removeFiles(List.of(fileName));

        assertTrue(removed);
        assertFalse(metadataCache.containsFile(fileName));
        assertFalse(statisticsCache.containsFile(fileName));
    }

    public void testCacheSizeLimitEviction() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        metadataCache.put(fileName);
        assertTrue(metadataCache.containsFile(fileName));

        metadataCache.setSizeLimit(new ByteSizeValue(40));

        assertFalse(metadataCache.containsFile(fileName));
        assertEquals(0, metadataCache.getEntries().size());
    }

    public void testCachePutWithIncreasedSizeLimit() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        metadataCache.setSizeLimit(new ByteSizeValue(500000));
        metadataCache.put(fileName);

        assertTrue(metadataCache.containsFile(fileName));
        logger.info("Entries: {}", metadataCache.getEntries());
        //(we print 3 elements per entry : filePath, memorySize, HitCount)
        assertEquals(1*3, metadataCache.getEntries().size());
    }

    public void testCacheClear() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String fileName = getResourceFile("hits1.parquet").getPath();

        metadataCache.put(fileName);
        assertTrue(metadataCache.containsFile(fileName));

        metadataCache.clear();

        assertFalse(metadataCache.containsFile(fileName));
        assertEquals(0, metadataCache.getEntries().size());
    }

    public void testAddMultipleFilesToCache() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        List<String> fileNames = List.of(
            getResourceFile("hits1.parquet").getPath(),
            getResourceFile("hits2.parquet").getPath()
        );

        cacheManager.addToCache(fileNames);
        // 3 elements per cache entry displayed
        assertEquals(2*3, metadataCache.getEntries().size());
        fileNames.forEach(fileName -> assertTrue(metadataCache.containsFile(fileName)));
    }

    public void testRemoveNonExistentFile() {
        CacheManager cacheManager = service.getCacheManager();
        String nonExistentFile = "/path/nonexistent.parquet";

        boolean removed = cacheManager.removeFiles(List.of(nonExistentFile));

        assertFalse(removed);
    }

    public void testGetNonExistentFile() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);
        String nonExistentFile = "/path/nonexistent.parquet";

        Object result = metadataCache.get(nonExistentFile);

//        assertNull(result);
        assertFalse(metadataCache.containsFile(nonExistentFile));
    }

    public void testAddEmptyFileList() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);

        cacheManager.addToCache(Collections.emptyList());

        assertEquals(0, metadataCache.getEntries().size());
    }

    public void testStatisticsCacheOperations() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor statisticsCache = cacheManager.getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        statisticsCache.put(fileName);
        assertTrue(statisticsCache.containsFile(fileName));
        assertTrue(statisticsCache.getMemoryConsumed() > 0);

        statisticsCache.remove(fileName);
        assertFalse(statisticsCache.containsFile(fileName));
    }

    public void testStatisticsCacheSizeLimit() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        statisticsCache.put(fileName);
        logger.info(statisticsCache.getEntries());
        assertTrue(statisticsCache.containsFile(fileName));

        statisticsCache.setSizeLimit(new ByteSizeValue(40));
        assertFalse(statisticsCache.containsFile(fileName));
    }

    public void testStatisticsCacheClear() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        statisticsCache.put(fileName);
        assertTrue(statisticsCache.containsFile(fileName));

        statisticsCache.clear();
        assertFalse(statisticsCache.containsFile(fileName));
    }

    public void testBothCacheTypesMemoryTracking() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);
        CacheAccessor statisticsCache = cacheManager.getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        long initialTotal = cacheManager.getTotalUsedBytes();

        metadataCache.put(fileName);
        statisticsCache.put(fileName);

        long afterBothAdded = cacheManager.getTotalUsedBytes();
        assertTrue(afterBothAdded > initialTotal);

        long metadataMemory = metadataCache.getMemoryConsumed();
        long statisticsMemory = statisticsCache.getMemoryConsumed();
        assertEquals(afterBothAdded, initialTotal + metadataMemory + statisticsMemory);
    }

    public void testCacheManagerTotalMemoryTracking() {
        CacheManager cacheManager = service.getCacheManager();
        String fileName = getResourceFile("hits1.parquet").getPath();

        long initialMemory = cacheManager.getTotalUsedBytes();
        cacheManager.addToCache(List.of(fileName));
        long afterAddMemory = cacheManager.getTotalUsedBytes();

        assertTrue(afterAddMemory > initialMemory);

        cacheManager.removeFiles(List.of(fileName));
        long afterRemoveMemory = cacheManager.getTotalUsedBytes();

        assertEquals(initialMemory, afterRemoveMemory);
    }

    public void testCacheSizeLimits() {
        CacheManager cacheManager = service.getCacheManager();
        CacheAccessor metadataCache = cacheManager.getCacheAccessor(CacheType.METADATA);

        long configuredLimit = metadataCache.getConfiguredSizeLimit();
        long totalLimit = cacheManager.getTotalSizeLimit();

        assertTrue(configuredLimit > 0);
        assertTrue(totalLimit > 0);
    }

    public void testStatisticsCacheHitCount() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        // Initially zero
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        // Add entry
        statisticsCache.put(fileName);
        assertTrue(statisticsCache.containsFile(fileName));

        // Get the entry - should increment hit count
        statisticsCache.get(fileName);
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        // Get it again - should increment hit count again
        statisticsCache.get(fileName);
        assertEquals(2, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
    }

    public void testStatisticsCacheMissCount() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String nonExistentFile = "/nonexistent/file.parquet";

        // Initially zero
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        // Try to get non-existent entry - should increment miss count
        statisticsCache.get(nonExistentFile);
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        // Try again - should increment miss count again
        statisticsCache.get(nonExistentFile);
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(2, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
    }

    public void testStatisticsCacheHitMissedMixed() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();
        String nonExistentFile = "/nonexistent/file.parquet";

        // Add entry
        statisticsCache.put(fileName);

        // Mix of hits and misses
        statisticsCache.get(fileName); // hit
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        statisticsCache.get(nonExistentFile); // miss
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        statisticsCache.get(fileName); // hit
        assertEquals(2, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        statisticsCache.get(nonExistentFile); // miss
        assertEquals(2, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(2, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
    }

    public void testStatisticsCacheHitRate() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();
        String nonExistentFile = "/nonexistent/file.parquet";

        // Initially 0.0 (no operations)
        assertEquals(0.0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitRate(), 0.001);

        // Add entry
        statisticsCache.put(fileName);

        // 2 hits, 0 misses = 100% hit rate
        statisticsCache.get(fileName);
        statisticsCache.get(fileName);
        assertEquals(1.0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitRate(), 0.001);

        // 2 hits, 1 miss = 66.67% hit rate
        statisticsCache.get(nonExistentFile);
        assertEquals(0.6666, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitRate(), 0.001);

        // 3 hits, 1 miss = 75% hit rate
        statisticsCache.get(fileName);
        assertEquals(0.75, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitRate(), 0.001);
    }

    public void testStatisticsCacheResetStats() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();
        String nonExistentFile = "/nonexistent/file.parquet";

        // Add entry and generate some hits/misses
        statisticsCache.put(fileName);
        statisticsCache.get(fileName); // hit
        statisticsCache.get(nonExistentFile); // miss

        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
        assertEquals(0.5, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitRate(), 0.001);

        // Reset stats
        ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).resetStats();

        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
        assertEquals(0.0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitRate(), 0.001);

        // Cache entries should still exist
        assertTrue(statisticsCache.containsFile(fileName));

        // After reset, new operations should start counting from zero
        statisticsCache.get(fileName);
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
    }

    public void testStatisticsCacheClearResetsStats() {
        CacheAccessor statisticsCache = service.getCacheManager().getCacheAccessor(CacheType.STATISTICS);
        String fileName = getResourceFile("hits1.parquet").getPath();

        // Add entry and generate hits/misses
        statisticsCache.put(fileName);
        statisticsCache.get(fileName); // hit
        statisticsCache.get("/nonexistent/file.parquet"); // miss

        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(1, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());

        // Clear should reset stats
        statisticsCache.clear();

        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getHitCount());
        assertEquals(0, ((org.opensearch.datafusion.search.cache.StatisticsCacheAccessor) statisticsCache).getMissCount());
        assertFalse(statisticsCache.containsFile(fileName));
    }

    private File getResourceFile(String fileName) {
        URL resource = getClass().getClassLoader().getResource(fileName);
        assertNotNull("Resource file not found: " + fileName, resource);
        return new File(resource.getFile());
    }
}
