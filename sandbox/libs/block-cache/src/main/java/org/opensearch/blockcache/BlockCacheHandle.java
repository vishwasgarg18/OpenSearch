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
import org.opensearch.index.store.remote.filecache.CacheStatsProvider;
import org.opensearch.index.store.remote.filecache.NodeCacheStats;

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
 * <p>Implements {@link CacheStatsProvider} by delegating to
 * {@link BlockCache#cacheStats()}. Each {@link BlockCache} implementation is
 * responsible for its own stats collection; this handle is purely a lifecycle
 * and injection boundary.
 *
 * <p>Thread-safe. The {@link BlockCache} reference is {@code final}.
 *
 * @opensearch.experimental
 */
public final class BlockCacheHandle implements Closeable, CacheStatsProvider {

    private static final Logger logger = LogManager.getLogger(BlockCacheHandle.class);

    private final BlockCache cache;

    /**
     * Creates a {@code BlockCacheHandle} wrapping the given {@link BlockCache}.
     *
     * @param cache the {@link BlockCache} implementation to manage; must not be null
     */
    public BlockCacheHandle(BlockCache cache) {
        this.cache = Objects.requireNonNull(cache, "cache must not be null");
    }

    /**
     * Returns the {@link BlockCache} managed by this handle.
     */
    public BlockCache getCache() {
        return cache;
    }

    /**
     * Returns a live snapshot of this cache's statistics.
     * Delegates to {@link BlockCache#cacheStats()}.
     */
    @Override
    public NodeCacheStats cacheStats() {
        return cache.cacheStats();
    }

    /**
     * Closes the underlying cache. Idempotency is enforced by {@link BlockCache#close()}.
     */
    @Override
    public void close() {
        cache.close();
        logger.info("Block cache closed");
    }
}
