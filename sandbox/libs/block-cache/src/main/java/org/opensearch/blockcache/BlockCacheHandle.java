/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.blockcache.foyer.FoyerBlockCache;
import org.opensearch.blockcache.stats.AggregateBlockCacheStats;
import org.opensearch.blockcache.stats.BlockCacheStatsCounter;
import org.opensearch.blockcache.stats.FoyerBlockCacheStatsCounter;
import org.opensearch.index.store.remote.filecache.CacheStats;
import org.opensearch.index.store.remote.filecache.CacheStatsProvider;

import java.io.Closeable;
import java.util.Objects;

/**
 * Lifecycle owner of a node-level {@link BlockCache}.
 *
 * <p>Created once at node startup and closed after all plugins have stopped.
 * Provides a stable, named type for dependency injection — callers inject
 * {@code BlockCacheHandle} rather than the raw {@link BlockCache} interface,
 * which avoids ambiguity when multiple {@link BlockCache} bindings exist.
 *
 * <p>The concrete {@link BlockCache} implementation is injected at construction
 * time, keeping this class entirely decoupled from any specific backend:
 *
 * <pre>{@code
 * // Foyer-backed (native):
 * BlockCacheHandle handle = new BlockCacheHandle(new FoyerBlockCache(diskBytes, diskDir));
 *
 * // No-op (tests / cache disabled):
 * BlockCacheHandle handle = new BlockCacheHandle(new NoOpBlockCache());
 * }</pre>
 *
 * <p>Implements {@link CacheStatsProvider} — the {@link #cacheStats()} method
 * reads stats via a {@link BlockCacheStatsCounter} whose implementation
 * ({@link FoyerBlockCacheStatsCounter}) performs an FFM call to read Rust
 * atomic counters from the native Foyer runtime. This mirrors the Java
 * {@link org.opensearch.index.store.remote.utils.cache.stats.StatsCounter}
 * → {@link org.opensearch.index.store.remote.utils.cache.stats.IRefCountedCacheStats}
 * pattern used by {@code LRUCache}.
 *
 * <p>Thread-safe. The {@link BlockCache} and {@link BlockCacheStatsCounter}
 * references are {@code final}; per JLS §17.5, {@code final} fields have
 * safe-publication semantics.
 *
 * @opensearch.experimental
 */
public final class BlockCacheHandle implements Closeable, CacheStatsProvider {

    private static final Logger logger = LogManager.getLogger(BlockCacheHandle.class);

    private final BlockCache cache;

    /**
     * Stats counter for this cache. Analogous to
     * {@link org.opensearch.index.store.remote.utils.cache.stats.StatsCounter}
     * in the {@code LRUCache} pattern. For Foyer-backed caches this is a
     * {@link FoyerBlockCacheStatsCounter} that reads Rust atomic counters via
     * FFM. For non-Foyer caches (e.g. tests) this returns
     * {@link CacheStats#EMPTY}.
     */
    private final BlockCacheStatsCounter statsCounter;

    /**
     * Creates a {@code BlockCacheHandle} wrapping the given {@link BlockCache}.
     *
     * <p>If {@code cache} is a {@link FoyerBlockCache}, a
     * {@link FoyerBlockCacheStatsCounter} is wired automatically so that
     * {@link #cacheStats()} performs real FFM stats reads. For other
     * implementations, a no-op counter is used.
     *
     * @param cache the {@link BlockCache} implementation to manage; must not be null
     * @throws NullPointerException if {@code cache} is null
     */
    public BlockCacheHandle(BlockCache cache) {
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
        if (cache instanceof FoyerBlockCache foyer) {
            this.statsCounter = new FoyerBlockCacheStatsCounter(foyer);
        } else {
            // Non-Foyer cache (e.g. test stubs) — return empty aggregate stats
            this.statsCounter = () -> AggregateBlockCacheStats.EMPTY;
        }
    }

    /**
     * Returns the {@link BlockCache} managed by this handle.
     *
     * <p>Callers that need to hand the cache to a native layer should
     * pattern-match against the concrete implementation:
     * <pre>{@code
     * BlockCache bc = handle.getCache();
     * if (bc instanceof FoyerBlockCache foyer) {
     *     nativeRuntime.attachCache(foyer.nativeCachePtr());
     * }
     * }</pre>
     *
     * @return the {@link BlockCache} instance; never {@code null}
     */
    public BlockCache getCache() {
        return cache;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns a {@link CacheStats} snapshot for this block cache.
     *
     * <p>For Foyer-backed caches, delegates to
     * {@link FoyerBlockCacheStatsCounter#snapshot()} which performs a single
     * FFM call ({@code foyer_snapshot_stats}) to read five Rust
     * {@code AtomicI64} counters from the native Foyer runtime. Analogous to
     * calling {@code DefaultStatsCounter.snapshot()} in the Java
     * {@link org.opensearch.index.store.remote.filecache.FileCache} pattern.
     *
     * <p>Called at most once per {@code _nodes/stats} request by
     * {@link org.opensearch.index.store.remote.filecache.UnifiedCacheService#aggregateStats()}.
     *
     * @return a point-in-time stats snapshot; never {@code null}
     */
    @Override
    public CacheStats cacheStats() {
        var s = statsCounter.snapshot();
        // Use overall stats (cross-tier rollup; today == block_level for single-tier Foyer)
        var overall = s.overallStats();
        return new CacheStats(
            overall.hitCount(),
            overall.hitBytes(),   // bytes served from cache — critical for variable-size entries
            overall.missCount(),
            overall.missBytes(),  // bytes fetched remotely — known from key range at miss time
            overall.usedBytes(),
            overall.evictionBytes(),
            0L,                   // removedBytes: lifecycle removes tracked separately (evict_prefix)
            overall.capacityBytes()
        );
    }

    /**
     * Closes the underlying cache. Delegates to {@link BlockCache#close()}.
     *
     * <p>Idempotency is enforced by the {@link BlockCache} implementation.
     */
    @Override
    public void close() {
        cache.close();
        logger.info("Block cache closed");
    }
}
