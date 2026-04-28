/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

/**
 * Read-only snapshot of block cache performance statistics.
 *
 * <p>Analogous to
 * {@link org.opensearch.index.store.remote.utils.cache.stats.IRefCountedCacheStats}
 * for the Java {@link org.opensearch.index.store.remote.filecache.FileCache}.
 *
 * <p>Implementations are immutable snapshots produced by
 * {@link BlockCacheStatsCounter#snapshot()}. Snapshots are taken at most once
 * per {@code _nodes/stats} request; the counters themselves are maintained as
 * Rust atomics inside the native Foyer cache and read via FFM.
 *
 * @opensearch.experimental
 */
public interface IBlockCacheStats {

    /**
     * Number of {@code get()} calls that returned a cached value.
     */
    long hitCount();

    /**
     * Bytes served from cache across all hits.
     *
     * <p>For variable-size entries (e.g. Parquet column chunks spanning 1 KB – 64 MB),
     * this is more meaningful than {@link #hitCount()} alone: a high count hit rate
     * on small metadata entries may mask poor byte-level effectiveness if large
     * row-group reads consistently miss. Comparable to
     * {@link org.opensearch.index.store.remote.utils.cache.stats.IRefCountedCacheStats#evictionWeight()}
     * in the FileCache stats hierarchy.
     */
    long hitBytes();

    /**
     * Number of {@code get()} calls that returned no value (cache miss).
     */
    long missCount();

    /**
     * Bytes that required a remote fetch due to cache misses.
     *
     * <p>For range-keyed entries the requested byte range is encoded in the
     * cache key (format {@code "path\x1Fstart-end"}), so this value is known
     * at miss time — no remote fetch callback is required.
     * Together with {@link #hitBytes()}, this gives the true byte-level hit rate:
     * {@code hitBytes / (hitBytes + missBytes)}.
     */
    long missBytes();

    /**
     * Total number of requests ({@code hitCount + missCount}).
     */
    default long requestCount() {
        return hitCount() + missCount();
    }

    /**
     * Hit rate: {@code hitCount / requestCount}, or {@code 1.0} when
     * {@code requestCount == 0}.
     */
    default double hitRate() {
        long total = requestCount();
        return total == 0 ? 1.0 : (double) hitCount() / total;
    }

    /**
     * Miss rate: {@code missCount / requestCount}, or {@code 0.0} when
     * {@code requestCount == 0}.
     */
    default double missRate() {
        long total = requestCount();
        return total == 0 ? 0.0 : (double) missCount() / total;
    }

    /**
     * Number of entries removed from the cache by LRU pressure.
     */
    long evictionCount();

    /**
     * Total bytes removed from the cache by LRU pressure.
     *
     * <p>Variable-size entries (Parquet column chunks) are tracked by byte weight,
     * not entry count, because a single Parquet row group can be 1 KB – 64 MB.
     */
    long evictionBytes();

    /**
     * Current bytes resident in the block cache on disk.
     */
    long usedBytes();

    /**
     * Configured capacity of the block cache in bytes.
     */
    long capacityBytes();

    /**
     * Used / capacity ratio, or {@code 0.0} when capacity is zero.
     */
    default double usedPercent() {
        return capacityBytes() == 0 ? 0.0 : (double) usedBytes() / capacityBytes();
    }
}
