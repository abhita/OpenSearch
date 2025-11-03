/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.datafusion.DataFusionQueryJNI;
import org.opensearch.datafusion.DataFusionService;
import org.opensearch.datafusion.core.DefaultRecordBatchStream;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DatafusionSearcher implements EngineSearcher<DatafusionQuery, RecordBatchStream> {
    private final String source;
    private DatafusionReader reader;
    private Long tokioRuntimePtr;
    private Long globalRuntimeEnvId;
    private Closeable closeable;
    private boolean readerReleased = false;

    public DatafusionSearcher(String source, DatafusionReader reader, Long tokioRuntimePtr, Long globalRuntimeEnvId, Closeable close) {
        this.source = source;
        this.reader = reader;
        this.tokioRuntimePtr = tokioRuntimePtr;
        this.globalRuntimeEnvId = globalRuntimeEnvId;
        this.closeable = close;
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public void search(DatafusionQuery datafusionQuery, List<SearchResultsCollector<RecordBatchStream>> collectors) throws IOException {
        // TODO : call search here to native
        // TODO : change RunTimePtr
        long nativeStreamPtr = DataFusionQueryJNI.executeSubstraitQuery(reader.getCachePtr(), datafusionQuery.toString(), datafusionQuery.getSubstraitBytes(), globalRuntimeEnvId, tokioRuntimePtr);
        RecordBatchStream stream = new DefaultRecordBatchStream(nativeStreamPtr);
        while(stream.hasNext()) {
            for(SearchResultsCollector<RecordBatchStream> collector : collectors) {
                collector.collect(stream);
            }
        }
    }

//    @Override
//    public long search(DatafusionQuery datafusionQuery, Long contextPtr) {
//        return DataFusionQueryJNI.executeSubstraitQuery(reader.getCachePtr(), datafusionQuery.getIndexName(), datafusionQuery.getSubstraitBytes(), contextPtr);
//    }

    @Override
    public long search(DatafusionQuery datafusionQuery, Long contextPtr, Long globalRuntimeEnvId, Long tokioRuntimePtr) {
        return DataFusionQueryJNI.executeSubstraitQuery(reader.getCachePtr(), datafusionQuery.getIndexName(), datafusionQuery.getSubstraitBytes(), globalRuntimeEnvId, tokioRuntimePtr);
    }

    public DatafusionReader getReader() {
        return reader;
    }

    @Override
    public void close() {
        try {
            // Use IOUtils.close for exception safety - only close other resources
            // Reader reference is managed by the ReaderManager
            IOUtils.close(closeable);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close searcher", e);
        } catch (AlreadyClosedException e) {
            throw new AssertionError(e);
        }
    }
}
