/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search.cache;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

public class CacheSettings {

    public static final String STATS_CACHE_SIZE_LIMIT_KEY = "statistics.cache.size.limit";
    public static final String METADATA_CACHE_SIZE_LIMIT_KEY = "datafusion.metadata.cache.size.limit";
    public static final Setting<ByteSizeValue> METADATA_CACHE_SIZE_LIMIT =
        new Setting<>(METADATA_CACHE_SIZE_LIMIT_KEY, "50mb",
            (s) -> ByteSizeValue.parseBytesSizeValue(s, new ByteSizeValue(1000, ByteSizeUnit.KB),METADATA_CACHE_SIZE_LIMIT_KEY), Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<ByteSizeValue> STATS_CACHE_SIZE_LIMIT =
        new Setting<>(STATS_CACHE_SIZE_LIMIT_KEY, "10mb",
            (s) -> ByteSizeValue.parseBytesSizeValue(s, new ByteSizeValue(1000, ByteSizeUnit.KB),STATS_CACHE_SIZE_LIMIT_KEY), Setting.Property.NodeScope, Setting.Property.Dynamic);


    public static final Setting<String> METADATA_CACHE_EVICTION_TYPE = new Setting<String>(
        "datafusion.metadata.cache.eviction.type",
        "LRU",
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> STATS_CACHE_EVICTION_TYPE = new Setting<String>(
        "stats.cache.eviction.type",
        "LFU",
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );


    public static final String METADATA_CACHE_ENABLED_KEY = "datafusion.metadata.cache.enabled";
    public static final Setting<Boolean> METADATA_CACHE_ENABLED =
        Setting.boolSetting(METADATA_CACHE_ENABLED_KEY, true, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final String STATS_CACHE_ENABLED_KEY = "stats.cache.enabled";
    public static final Setting<Boolean> STATS_CACHE_ENABLED =
        Setting.boolSetting(STATS_CACHE_ENABLED_KEY, true, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final List<Setting<?>> CACHE_SETTINGS = Arrays.asList(
        METADATA_CACHE_SIZE_LIMIT,
        METADATA_CACHE_EVICTION_TYPE,
        STATS_CACHE_SIZE_LIMIT,
        STATS_CACHE_EVICTION_TYPE
    );

    public static final List<Setting<?>> CACHE_ENABLED = Arrays.asList(
        METADATA_CACHE_ENABLED,
        STATS_CACHE_ENABLED
    );
}
