/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

/**
 * Produces point-in-time {@link IBlockCacheStats} snapshots from an underlying
 * counter store.
 *
 * <p>Analogous to
 * {@link org.opensearch.index.store.remote.utils.cache.stats.StatsCounter}
 * for the Java {@link org.opensearch.index.store.remote.filecache.FileCache}.
 *
 * <p>Unlike {@code StatsCounter}, this interface does <em>not</em> define
 * mutation methods ({@code recordHit}, {@code recordEviction}, etc.) because
 * all counter mutations happen inside the native Rust Foyer runtime — Java
 * cannot intercept hot-path operations without FFM overhead on every
 * {@code get()}/{@code put()}/eviction.
 *
 * <p>The single {@link #snapshot()} method is the only entry point. It performs
 * an FFM call to read the Rust atomic counters at most once per
 * {@code _nodes/stats} request.
 *
 * @opensearch.experimental
 */
public interface BlockCacheStatsCounter {

    /**
     * Produces an immutable aggregate snapshot of the current block cache statistics.
     *
     * <p>Returns an {@link AggregateBlockCacheStats} containing both the cross-tier
     * overall rollup and the disk-tier (block-level) breakdown — mirroring the shape
     * of {@link org.opensearch.index.store.remote.utils.cache.stats.AggregateRefCountedCacheStats}
     * for the Java FileCache.
     *
     * <p>Implementations may perform an FFM call to read Rust atomic counters.
     * Callers should invoke this method at most once per stats collection cycle
     * (i.e. once per {@code _nodes/stats} request) to avoid unnecessary FFM
     * overhead.
     *
     * @return an immutable {@link AggregateBlockCacheStats} snapshot; never {@code null}
     */
    AggregateBlockCacheStats snapshot();
}
