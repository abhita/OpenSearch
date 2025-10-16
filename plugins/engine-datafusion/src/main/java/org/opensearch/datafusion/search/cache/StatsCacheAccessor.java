/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.datafusion.DataFusionQueryJNI.createStatisticsCache;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheClear;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheContainsFile;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheGet;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheGetEntries;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheGetSize;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCachePut;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheRemove;
import static org.opensearch.datafusion.DataFusionQueryJNI.statisticsCacheUpdateSizeLimit;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATS_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATS_CACHE_SIZE_LIMIT;
import static org.opensearch.datafusion.search.cache.CacheSettings.STATS_CACHE_SIZE_LIMIT_KEY;

public class StatsCacheAccessor extends CacheAccessor{

    private CachePolicy cachePolicy;

    public void setCachePolicy(String cachePolicy) {
        this.cachePolicy = CachePolicy.valueOf(cachePolicy);
    }


    public StatsCacheAccessor(long cacheManagerPointer, ClusterSettings cacheSettings, CacheType name) {
        super(cacheManagerPointer, cacheSettings, name);
    }

    @Override
    protected Map<String, Object> extractSettings(ClusterSettings clusterSettings) {
        Map<String, Object> properties = new HashMap<>();

        clusterSettings.addSettingsUpdateConsumer(STATS_CACHE_SIZE_LIMIT, this::setSizeLimit);
        setSizeLimit(clusterSettings.get(STATS_CACHE_SIZE_LIMIT));
        properties.put(STATS_CACHE_SIZE_LIMIT_KEY,this.sizeLimit);

        clusterSettings.addSettingsUpdateConsumer(STATS_CACHE_EVICTION_TYPE, this::setCachePolicy);
        setCachePolicy(clusterSettings.get(STATS_CACHE_EVICTION_TYPE));
        properties.put(STATS_CACHE_EVICTION_TYPE.getKey(),this.cachePolicy);

        return properties;
    }

    @Override
    public long createCache(long cacheManagerPointer, Map<String, Object> properties) {
        return createStatisticsCache(cacheManagerPointer, this.sizeLimit);
    }

    @Override
    public boolean put(String filePath) {
        return statisticsCachePut(this.getPointer(), filePath);
    }

    @Override
    public Object get(String filePath) {
        return statisticsCacheGet(this.pointer, filePath);
    }

    @Override
    public boolean remove(String filePath) {
        return statisticsCacheRemove(this.pointer, filePath);
    }

    @Override
    public void evict() {
        return;
    }

    @Override
    public void clear() {
        statisticsCacheClear(this.pointer);
    }

    @Override
    public long getMemoryConsumed() {
        return statisticsCacheGetSize(this.pointer);
    }

    @Override
    public boolean containsFile(String filePath) {
        return statisticsCacheContainsFile(this.pointer, filePath);
    }

    // TODO: Replace the logic with optimized version to check if it is update or set limit call
    @Override
    public void setSizeLimit(ByteSizeValue limit) {
        if(this.sizeLimit == 0){
            this.sizeLimit = limit.getBytes();
        } else{
            statisticsCacheUpdateSizeLimit(this.pointer, limit.getBytes());
            this.sizeLimit = limit.getBytes();
        }
    }

    @Override
    public List<String> getEntries() {
        return List.of(statisticsCacheGetEntries(this.pointer));
    }
}
