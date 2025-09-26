/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.EngineReaderManager;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader>, CatalogSnapshotAwareRefreshListener {
    private DatafusionReader current;
    private String path;
    private String dataFormat;
    private Consumer<List<String>> onFilesAdded;
    private Consumer<List<String>> onFilesRemoved;

//    private final Lock refreshLock = new ReentrantLock();
//    private final List<ReferenceManager.RefreshListener> refreshListeners = new CopyOnWriteArrayList();

    public DatafusionReaderManager(String path, Collection<FileMetadata> files) throws IOException {
        this.current = new DatafusionReader(path, files);
        this.path = path;
        this.dataFormat = dataFormat;
    }

    /**
     * Set callback for when files are added during refresh
     */
    public void setOnFilesAdded(Consumer<List<String>> onFilesAdded) {
        this.onFilesAdded = onFilesAdded;
    }

    @Override
    public DatafusionReader acquire() throws IOException {
        if (current == null) {
            throw new RuntimeException("Invalid state for datafusion reader");
        }
        current.incRef();
        return current;
    }

    @Override
    public void release(DatafusionReader reference) throws IOException {
        assert reference != null : "Shard view can't be null";
        reference.decRef();
    }

    @Override
    public void beforeRefresh() throws IOException {
        // no op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh && catalogSnapshot != null) {
            DatafusionReader old = this.current;
            Collection<FileMetadata> newFiles = catalogSnapshot.getSearchableFiles(dataFormat);

            processFileChanges(old.files, newFiles);

            release(old);
            this.current = new DatafusionReader(this.path, newFiles);
            this.current.incRef();
        }
    }

    private void processFileChanges(Collection<FileMetadata> oldFiles, Collection<FileMetadata> newFiles) {
        Set<String> oldFilePaths = extractFilePaths(oldFiles);
        Set<String> newFilePaths = extractFilePaths(newFiles);

        Set<String> filesToAdd = new HashSet<>(newFilePaths);
        filesToAdd.removeAll(oldFilePaths);

        Set<String> filesToRemove = new HashSet<>(oldFilePaths);
        filesToRemove.removeAll(newFilePaths);

        if (!filesToAdd.isEmpty() && onFilesAdded != null) {
            onFilesAdded.accept(List.copyOf(filesToAdd));
        }
    }

    private Set<String> extractFilePaths(Collection<FileMetadata> files) {
        Set<String> paths = new HashSet<>();
        for (FileMetadata file : files) {
            paths.add(this.path.concat(file.fileName()));
        }
        return paths;
    }

}
