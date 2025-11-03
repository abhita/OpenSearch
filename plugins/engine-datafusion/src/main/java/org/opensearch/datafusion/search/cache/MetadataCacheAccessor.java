package org.opensearch.datafusion.search.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.datafusion.DataFusionQueryJNI.createMetadataCache;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheClear;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheContainsFile;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheGet;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheGetEntries;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheGetSize;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCachePut;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheRemove;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheUpdateSizeLimit;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT_KEY;

public class MetadataCacheAccessor extends CacheAccessor {
    private static final Logger logger = LogManager.getLogger(MetadataCacheAccessor.class);
    private CachePolicy cachePolicy;

    public MetadataCacheAccessor(long cacheManagerPointer, ClusterSettings settings, CacheType type) {
        super(cacheManagerPointer, settings,type);
    }

    public void setCachePolicy(String cachePolicy) {
        this.cachePolicy = CachePolicy.valueOf(cachePolicy);
    }

    @Override
    protected Map<String, Object> extractSettings(ClusterSettings clusterSettings) {
        Map<String, Object> properties = new HashMap<>();

        clusterSettings.addSettingsUpdateConsumer(METADATA_CACHE_SIZE_LIMIT, this::setSizeLimit);
        setSizeLimit(clusterSettings.get(METADATA_CACHE_SIZE_LIMIT));
        properties.put(METADATA_CACHE_SIZE_LIMIT_KEY,this.sizeLimit);

        clusterSettings.addSettingsUpdateConsumer(METADATA_CACHE_EVICTION_TYPE, this::setCachePolicy);
        setCachePolicy(clusterSettings.get(METADATA_CACHE_EVICTION_TYPE));
        properties.put(METADATA_CACHE_EVICTION_TYPE.getKey(),this.cachePolicy);

        return properties;
    }

    @Override
    public long createCache(long cacheManagerPointer, Map<String, Object> properties) {
        return createMetadataCache(cacheManagerPointer, this.sizeLimit);
    }

    @Override
    public boolean put(String filePath) {
        try {
            return metadataCachePut(this.getPointer(), filePath);
        } catch (RuntimeException e) {
            logger.error("Failed to put file [{}] in metadata cache: {}", filePath, e.getMessage());
            return false;
        }
    }

    @Override
    public Object get(String filePath) {
        try {
            return metadataCacheGet(this.pointer, filePath);
        } catch (RuntimeException e) {
            logger.error("Failed to get file [{}] from metadata cache: {}", filePath, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean remove(String filePath) {
        try {
            return metadataCacheRemove(this.pointer, filePath);
        } catch (RuntimeException e) {
            logger.error("Failed to remove file [{}] from metadata cache: {}", filePath, e.getMessage());
            return false;
        }
    }

    @Override
    public void evict() {
        throw new UnsupportedOperationException("Explicit Eviction Not Supported");
    }

    @Override
    public void clear() {
        try {
            metadataCacheClear(this.pointer);
        } catch (RuntimeException e) {
            logger.error("Failed to clear metadata cache: {}", e.getMessage());
        }
    }

    @Override
    public long getMemoryConsumed() {
        try {
            return metadataCacheGetSize(this.pointer);
        } catch (RuntimeException e) {
            logger.warn("Failed to get metadata cache size: {}", e.getMessage());
            return 0L;
        }
    }

    @Override
    public boolean containsFile(String filePath) {
        try {
            return metadataCacheContainsFile(this.pointer, filePath);
        } catch (RuntimeException e) {
            logger.error("Failed to check if metadata cache contains file [{}]: {}", filePath, e.getMessage());
            return false;
        }
    }

    @Override
    public void setSizeLimit(ByteSizeValue limit) {
        if(this.sizeLimit == 0){
            this.sizeLimit = limit.getBytes();
        } else{
            try {
                metadataCacheUpdateSizeLimit(this.pointer, limit.getBytes());
                this.sizeLimit = limit.getBytes();
            } catch (RuntimeException e) {
                logger.error("Failed to update metadata cache size limit to [{}]: {}", limit, e.getMessage());
            }
        }
    }

    @Override
    public List<String> getEntries() {
        try {
            return List.of(metadataCacheGetEntries(this.pointer));
        } catch (RuntimeException e) {
            logger.error("Failed to get metadata cache entries: {}", e.getMessage());
            return List.of();
        }
    }

}
