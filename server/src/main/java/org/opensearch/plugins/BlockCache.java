/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * Node-scoped block cache contract — backend-neutral.
 *
 * <p>This interface deliberately carries only lifecycle and observability
 * methods. Backend-specific surface (e.g. Caffeine's pin/unpin reference
 * counting, or Foyer's native cache pointer) lives on concrete subtypes and
 * is consumed by code that explicitly knows which backend it is talking to.
 * Core only ever uses the two methods declared here.
 *
 * <p>A block cache stores variable-size contiguous byte ranges (file ranges,
 * Parquet column chunks, remote-object ranges, etc.). The exact key and
 * value shape is an implementation detail and is not part of this interface
 * — different backends may use path-and-offset keys, repository-and-range
 * keys, native pointers, or anything else.
 *
 * <p>Implementations must be thread-safe and idempotent on {@link #close()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BlockCache extends Closeable {

    /**
     * Release all resources held by this cache. Idempotent: calling more than
     * once must be a no-op.
     */
    @Override
    void close();

    /**
     * Returns a point-in-time snapshot of cache counters.
     *
     * <p>Implementations that do not track a particular metric should return
     * zero for that field rather than throwing. The snapshot is not
     * guaranteed to be internally consistent across concurrent cache
     * activity.
     *
     * @return counter snapshot; never {@code null}
     */
    BlockCacheStats stats();

    /**
     * Returns the unique name of this cache backend, or an empty string if unnamed.
     * Used by {@link BlockCacheRegistry} to look up caches by name.
     *
     * @return cache name; never {@code null}
     */
    default String cacheName() {
        return "";
    }

    /**
     * Returns an opaque native pointer for this cache, or {@code 0L} if this cache
     * has no native backing.
     *
     * <p>Native-backed implementations return a pointer that native storage components
     * can use to integrate with this cache directly, without depending on the concrete
     * cache type. The exact pointer type and lifetime contract are defined by the
     * implementation.
     *
     * <p>The pointer is owned by this {@code BlockCache} instance and is valid for its
     * entire lifetime. Callers must <em>not</em> free it — the implementation frees it
     * in {@link #close()}.
     *
     * <p>Pure-Java implementations return {@code 0L}. Native storage components that
     * receive {@code 0L} must fall back to their default (non-cached) behaviour.
     *
     * @return native pointer for this cache, or {@code 0L} if not applicable
     */
    default long nativeBlockCachePtr() {
        return 0L;
    }

    /**
     * Evict all cache entries whose key starts with the given path prefix.
     *
     * <p>Used by {@link org.opensearch.index.store.remote.filecache.NodeCacheOrchestratorCleaner}
     * to deterministically remove all cached byte-range entries for a shard or index
     * when it is deleted.
     *
     * <p>The default implementation is a no-op — pure-Java caches that do not
     * support prefix eviction or have their own eviction mechanism may leave this unimplemented.
     *
     * @param prefix absolute path prefix; all entries whose key starts with this string are removed
     */
    default void evictPrefix(String prefix) {}
}
