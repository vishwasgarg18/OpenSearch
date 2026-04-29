/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache;

import org.opensearch.blockcache.stats.AggregateBlockCacheStats;
import org.opensearch.blockcache.stats.BlockCacheStats;

import java.io.Closeable;

/**
 * A node-level cache for variable-size blocks of data read from remote storage.
 *
 * <p>Sits at the SSD tier to reduce repeated fetches from object storage.
 * "Block" here is used in the storage sense — a contiguous byte range read
 * as an indivisible I/O unit — not a fixed-size disk sector.
 * Entry granularity is determined by the calling layer and may vary from
 * kilobytes to tens of megabytes.
 *
 * <p>This interface is agnostic of backing implementation; the current
 * implementation delegates to the Foyer Rust library via FFM.
 *
 * <p>Implementations manage the lifecycle of underlying native resources.
 * {@link #close()} must be called at node shutdown and must be idempotent.
 *
 * @opensearch.experimental
 */
public interface BlockCache extends Closeable {

    /**
     * Snapshots the current cache statistics.
     *
     * <p>Implementations that do not track statistics return all-zero stats.
     *
     * @return a point-in-time stats snapshot; never {@code null}
     */
    default AggregateBlockCacheStats cacheStats() {
        return AggregateBlockCacheStats.EMPTY;
    }

    /**
     * Returns the disk capacity in bytes allocated to this cache.
     * Used by {@code UnifiedCacheService} for disk watermark calculations.
     *
     * <p>Implementations that do not use a disk tier may return {@code 0}.
     */
    default long diskCapacityBytes() {
        return 0L;
    }

    /**
     * Release all resources held by this cache.
     *
     * <p>Must be idempotent: calling {@code close()} more than once is a no-op.
     */
    @Override
    void close();
}
