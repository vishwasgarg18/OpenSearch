/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * SPI contract for any cache that participates in the unified stats aggregation.
 *
 * <p>Both {@link FileCache} (Java LRU for Lucene blocks) and
 * {@code org.opensearch.blockcache.BlockCacheHandle} (Rust Foyer cache for
 * columnar byte ranges) implement this interface. {@link UnifiedCacheService}
 * calls {@link #cacheStats()} on each registered cache when assembling the
 * {@code aggregate_file_cache} section of {@code _nodes/stats}.
 *
 * <p>Implementations in {@code server/} (e.g. {@link FileCache}) read their stats
 * directly from in-memory counters. Implementations in {@code sandbox/libs/block-cache}
 * make an FFM (Foreign Function &amp; Memory) call to read Rust atomic counters from
 * the native Foyer cache. Both return the same {@link CacheStats} shape, which
 * {@link UnifiedCacheService} can sum element-wise via {@link CacheStats#add}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CacheStatsProvider {

    /**
     * Snapshots the current cache statistics.
     *
     * <p>Implementations must be:
     * <ul>
     *   <li><b>Fast</b> — called on every {@code _nodes/stats} request; must not
     *       perform I/O or blocking operations.</li>
     *   <li><b>Non-null</b> — must return {@link CacheStats#EMPTY} rather than
     *       {@code null} when the cache is unavailable or not configured.</li>
     *   <li><b>Thread-safe</b> — may be called concurrently.</li>
     * </ul>
     *
     * @return a point-in-time snapshot of this cache's stats; never {@code null}
     */
    CacheStats cacheStats();
}
