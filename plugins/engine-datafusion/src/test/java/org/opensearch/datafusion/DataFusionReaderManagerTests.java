/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.datafusion.search.DatafusionReader;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.env.Environment;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.vectorized.execution.search.DataFormat;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.index.engine.Engine.SearcherScope.INTERNAL;

public class DataFusionReaderManagerTests extends OpenSearchTestCase {

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

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);

        service = new DataFusionService(Collections.emptyMap(), clusterSettings);
        //service = new DataFusionService(Map.of());
        service.doStart();
    }

    public void testInitialReaderCreation() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "hits3.parquet", "hits1.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/hits3.parquet");
        writerFileSet.add(shardPath.getDataPath() + "/hits1.parquet");

        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher = engine.acquireSearcher("test");
        DatafusionReader reader = searcher.getReader();

        assertEquals(2, reader.files.stream().toList().get(0).getFiles().size());
        assertNotEquals(-1, reader.cachePtr);

        searcher.close();
    }

    public void testMultipleSearchersShareSameReader() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "hits3.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/hits3.parquet");

        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("test1");
        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");

        // Both searchers should share the same reader instance
        assertSame(searcher1.getReader(), searcher2.getReader());

        searcher1.close();
        searcher2.close();
    }

    public void testReaderSurvivesPartialSearcherClose() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "hits3.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/hits3.parquet");

        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("test1");
        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");
        DatafusionReader reader = searcher1.getReader();

        // Close first searcher - reader should stay alive
        searcher1.close();
        assertNotEquals(-1, reader.cachePtr);

        // Close second searcher - reader should not be closed
        searcher2.close();
        assertNotEquals(-1, reader.cachePtr);
    }

    public void testRefreshCreatesNewReader() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "hits3.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        // Initial refresh
        RefreshResult refreshResult1 = new RefreshResult();
        WriterFileSet writerFileSet1 = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet1.add(shardPath.getDataPath() + "/hits3.parquet");
        refreshResult1.add(getMockDataFormat(), List.of(writerFileSet1));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult1, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("test1");
        DatafusionReader reader1 = searcher1.getReader();

        // Add new file and refresh
        // addResourceFilesToShardPath(shardPath, "hits1.parquet");
        addFilesToShardPath(shardPath, "hits1.parquet");
        RefreshResult refreshResult2 = new RefreshResult();
        WriterFileSet writerFileSet2 = new WriterFileSet(shardPath.getDataPath(), 2);
        writerFileSet2.add(shardPath.getDataPath() + "/hits3.parquet");
        writerFileSet2.add(shardPath.getDataPath() + "/hits1.parquet");
        refreshResult2.add(getMockDataFormat(), List.of(writerFileSet2));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult2, 2));

        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");
        DatafusionReader reader2 = searcher2.getReader();

        // Should have different readers
        assertNotSame(reader1, reader2);
        assertEquals(1, reader1.files.stream().toList().getFirst().getFiles().size());
        assertEquals(2, reader2.files.stream().toList().getFirst().getFiles().size());

        searcher1.close();
        searcher2.close();
    }

    public void testDecRefAfterCloseThrowsException() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "hits3.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/hits3.parquet");
        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher = engine.acquireSearcher("test");
        DatafusionReader reader = searcher.getReader();

        searcher.close();
        reader.decRef();
        assertEquals(-1, reader.cachePtr);

        // Calling decRef on closed reader should throw
        assertThrows(IllegalStateException.class, reader::decRef);
    }

// R1 -> f1,f2,f3
    // S1 -> f1, f2, f3
    // S2 -> f1, f2, f3
    //R2 -> f2,f3
    //S3 -> f2,f3

    public void testReaderManagerClosesAfterSearchRelease() throws IOException {
        Map<String, Object[]> finalRes = new HashMap<>();
        DatafusionSearcher datafusionSearcher = null;

        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "hits3.parquet", "hits1.parquet");

        try {
            DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
            DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

            RefreshResult refreshResult = new RefreshResult();
            WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
            writerFileSet.add(shardPath.getDataPath() + "/hits3.parquet");
            writerFileSet.add(shardPath.getDataPath() + "/hits1.parquet");
            List<WriterFileSet> writerFiles = List.of(writerFileSet);

            refreshResult.add(getMockDataFormat(), writerFiles);
            readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

            // DatafusionReader readerR1 = readerManager.acquire();
            DatafusionSearcher datafusionSearcherS1 = engine.acquireSearcher("Search");
            DatafusionReader readerR1 = datafusionSearcherS1.getReader();
            assertEquals(readerR1.files.size(), datafusionSearcherS1.getReader().files.size());

            DatafusionSearcher datafusionSearcher1v2 = engine.acquireSearcher("Search");
            DatafusionReader readerR1v2 = datafusionSearcher1v2.getReader();
            assertEquals(readerR1v2.files.size(), datafusionSearcher1v2.getReader().files.size());

            assertEquals(readerR1v2, readerR1);

            addFilesToShardPath(shardPath, "hits2.parquet");
            // now trigger refresh to have new Reader with F2, F3
            RefreshResult refreshResultR2 = new RefreshResult();
            WriterFileSet writerFileSet2 = new WriterFileSet(shardPath.getDataPath(), 2);
            writerFileSet2.add(shardPath.getDataPath() + "/hits2.parquet");
            writerFileSet2.add(shardPath.getDataPath() + "/hits1.parquet");
            List<WriterFileSet> writerFiles2 = List.of(writerFileSet2);

            refreshResultR2.add(getMockDataFormat(), writerFiles2);
            readerManager.afterRefresh(true, new CatalogSnapshot(refreshResultR2, 2));

            // now check if new Reader is created with F2, F3
            // DatafusionReader readerR2 = readerManager.acquire();
            DatafusionSearcher datafusionSearcherS2 = engine.acquireSearcher("Search");
            DatafusionReader readerR2 = datafusionSearcherS2.getReader();
            assertEquals(readerR2.files.size(), datafusionSearcherS2.getReader().files.size());

            //now we close S1 and automatically R1 will be closed
            datafusionSearcherS1.close();
            // 1 for SearcherS1v2
            assertEquals(1, getRefCount(readerR1));
            // 1 for SearcherS2 and 1 for CatalogSnapshot
            assertEquals(2, getRefCount(readerR2));
            assertNotEquals(-1, readerR1.cachePtr);
            datafusionSearcher1v2.close();
            assertEquals(-1, readerR1v2.cachePtr);

            assertThrows(IllegalStateException.class, () -> readerR1.decRef());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (datafusionSearcher != null) {
                datafusionSearcher.close();
            }
        }
    }

    private int getRefCount(DatafusionReader reader) {
        try {
            java.lang.reflect.Field refCountField = DatafusionReader.class.getDeclaredField("refCount");
            refCountField.setAccessible(true);
            return ((java.util.concurrent.atomic.AtomicInteger) refCountField.get(reader)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File getResourceFile(String fileName) {
        URL resourceUrl = getClass().getClassLoader().getResource(fileName);
        if (resourceUrl == null) {
            throw new IllegalArgumentException("Resource not found: " + fileName);
        }
        return new File(resourceUrl.getPath());
    }

    private org.opensearch.index.engine.exec.DataFormat getMockDataFormat() {
        return new org.opensearch.index.engine.exec.DataFormat() {
            @Override
            public Setting<Settings> dataFormatSettings() {
                return null;
            }

            @Override
            public Setting<Settings> clusterLeveldataFormatSettings() {
                return null;
            }

            @Override
            public String name() {
                return "parquet";
            }

            @Override
            public void configureStore() {

            }
        };
    }

    private ShardPath createCustomShardPath(String indexName, int shardId) {
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardId shId = new ShardId(index, shardId);
        Path dataPath = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(shardId));
        return new ShardPath(false, dataPath, dataPath, shId);
    }

    private void addFilesToShardPath(ShardPath shardPath, String... fileNames) throws IOException {
        for (String resourceFileName : fileNames) {
            try (InputStream is = getClass().getResourceAsStream("/" + resourceFileName)) {
                Path targetPath = shardPath.getDataPath().resolve(resourceFileName);
                java.nio.file.Files.createDirectories(targetPath.getParent());
                if (is != null) {
                    java.nio.file.Files.copy(is, targetPath);
                } else {
                    java.nio.file.Files.createFile(targetPath);
                }
            }
        }
    }

    private ShardPath createShardPathWithResourceFiles(String indexName, int shardId, String... resourceFileNames) throws IOException {
        ShardPath shardPath = createCustomShardPath(indexName, shardId);

        for (String resourceFileName : resourceFileNames) {
            try (InputStream is = getClass().getResourceAsStream("/" + resourceFileName)) {
                Path targetPath = shardPath.getDataPath().resolve(resourceFileName);
                java.nio.file.Files.createDirectories(targetPath.getParent());
                if (is != null) {
                    java.nio.file.Files.copy(is, targetPath);
                } else {
                    java.nio.file.Files.createFile(targetPath);
                }
            }
        }

        return shardPath;
    }

}
