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
 * Flat, cache-agnostic snapshot of a single cache's effectiveness.
 *
 * <p>Used by {@link UnifiedCacheService} to aggregate stats across all registered
 * caches (FileCache + optional block cache) without coupling to any specific
 * cache implementation. Both {@link FileCache} and
 * {@code org.opensearch.blockcache.BlockCacheHandle} produce this type via
 * {@link CacheStatsProvider#cacheStats()}.
 *
 * <p>Instances are immutable. Use {@link #add(CacheStats, CacheStats)} to combine
 * stats from multiple caches for cross-cache rollups.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CacheStats {

    /** Number of reads served from this cache (no remote I/O needed). */
    public final long hits;

    /**
     * Bytes served from cache across all hits.
     *
     * <p>For variable-size caches (e.g. Parquet column chunks in the block cache),
     * byte-based metrics are more meaningful than count-based: a high count hit rate
     * on small metadata entries may mask poor effectiveness on large row-group reads.
     * For fixed-size caches (e.g. Lucene ~8 MB blocks in FileCache), this is
     * approximately {@code hits × blockSize}.
     */
    public final long hitBytes;

    /** Number of reads that missed this cache and required remote I/O. */
    public final long misses;

    /**
     * Bytes that required remote I/O due to cache misses.
     *
     * <p>For the block cache, this is known from the key's byte range at miss time.
     * For FileCache, this is approximately {@code misses × blockSize}.
     * Together with {@link #hitBytes}, gives the true byte-level cache effectiveness:
     * {@code hitBytes / (hitBytes + missBytes)}.
     */
    public final long missBytes;

    /** Bytes currently resident in this cache (on SSD or in DRAM). */
    public final long usedBytes;

    /** Bytes evicted by LRU pressure since node start. */
    public final long evictedBytes;

    /**
     * Bytes explicitly removed by lifecycle events (e.g. shard closed, relocated).
     * Distinct from {@link #evictedBytes} which are LRU-pressure-driven.
     */
    public final long removedBytes;

    /** Total configured capacity of this cache in bytes. */
    public final long totalBytes;

    public CacheStats(
        long hits, long hitBytes,
        long misses, long missBytes,
        long usedBytes, long evictedBytes, long removedBytes, long totalBytes
    ) {
        this.hits = hits;
        this.hitBytes = hitBytes;
        this.misses = misses;
        this.missBytes = missBytes;
        this.usedBytes = usedBytes;
        this.evictedBytes = evictedBytes;
        this.removedBytes = removedBytes;
        this.totalBytes = totalBytes;
    }

    /**
     * Empty/zero stats sentinel. Used when a cache is not configured or not available.
     */
    public static final CacheStats EMPTY = new CacheStats(0, 0, 0, 0, 0, 0, 0, 0);

//    /**
//     * Element-wise sum of two {@code CacheStats} snapshots.
//     *
//     * <p>Used by {@link UnifiedCacheService#aggregateStats()} to produce the
//     * cross-cache rollup that appears in the top-level fields of
//     * {@code aggregate_file_cache} in {@code _nodes/stats}.
//     *
//     * @param a stats from one cache
//     * @param b stats from another cache
//     * @return a new {@code CacheStats} with all fields summed
//     */
    public static CacheStats add(CacheStats a, CacheStats b) {
        return new CacheStats(
            a.hits + b.hits,
            a.hitBytes + b.hitBytes,
            a.misses + b.misses,
            a.missBytes + b.missBytes,
            a.usedBytes + b.usedBytes,
            a.evictedBytes + b.evictedBytes,
            a.removedBytes + b.removedBytes,
            a.totalBytes + b.totalBytes
        );
    }
}
