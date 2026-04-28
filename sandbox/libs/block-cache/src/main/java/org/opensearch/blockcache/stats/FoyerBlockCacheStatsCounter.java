/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

import org.opensearch.blockcache.foyer.FoyerBridge;
import org.opensearch.blockcache.foyer.FoyerBlockCache;

import java.util.Objects;

/**
 * {@link BlockCacheStatsCounter} backed by Foyer's native Rust atomics.
 *
 * <p>Analogous to
 * {@link org.opensearch.index.store.remote.utils.cache.stats.DefaultStatsCounter}
 * for the Java {@link org.opensearch.index.store.remote.filecache.FileCache},
 * but instead of maintaining Java-side {@code long} fields, this class reads
 * Rust atomic counters via a single FFM call to
 * {@link FoyerBridge#snapshotStats(long)}.
 *
 * <p>Counter mutations (hit, miss, eviction) are performed entirely in Rust
 * on every {@code get()}/{@code put()}/LRU-eviction event. This implementation
 * simply reads them at snapshot time — at most once per {@code _nodes/stats}
 * request.
 *
 * <p>Returns an {@link AggregateBlockCacheStats} mirroring the structure of
 * {@link org.opensearch.index.store.remote.utils.cache.stats.AggregateRefCountedCacheStats}:
 * <ul>
 *   <li><b>overallStats</b> — cross-tier rollup (first section of the FFM buffer)</li>
 *   <li><b>blockLevelStats</b> — disk-tier stats (second section of the FFM buffer)</li>
 * </ul>
 * Today Foyer is single-tier so both sections are identical.
 *
 * <p>Thread-safe: all state is in the Rust atomics; only two {@code long}
 * fields are held here and they are {@code final}.
 *
 * @opensearch.experimental
 */
public final class FoyerBlockCacheStatsCounter implements BlockCacheStatsCounter {


    private final long nativeCachePtr;
    private final long capacityBytes;

    /**
     * Creates a stats counter for the given Foyer cache handle.
     *
     * @param foyer the Foyer block cache whose atomics will be read on
     *              {@link #snapshot()}; must not be null
     */
    public FoyerBlockCacheStatsCounter(FoyerBlockCache foyer) {
        Objects.requireNonNull(foyer, "foyer must not be null");
        this.nativeCachePtr = foyer.nativeCachePtr();
        this.capacityBytes  = foyer.diskCapacityBytes();
    }

    @Override
    public AggregateBlockCacheStats snapshot() {
        long[] raw = FoyerBridge.snapshotStats(nativeCachePtr);
        return new AggregateBlockCacheStats(
            toBlockCacheStats(raw, 0),                          // overall section
            toBlockCacheStats(raw, BlockCacheStats.Field.COUNT) // block-level section
        );
    }

    /**
     * Extracts a {@link BlockCacheStats} from one section of the FFM buffer.
     *
     * @param raw    the buffer returned by {@link FoyerBridge#snapshotStats}
     * @param offset the start index of this section ({@code 0} for overall,
     *               {@code Field.COUNT} for block-level)
     */
    private BlockCacheStats toBlockCacheStats(long[] raw, int offset) {
        return new BlockCacheStats(
            raw[offset + BlockCacheStats.Field.HIT_COUNT.ordinal()],
            raw[offset + BlockCacheStats.Field.HIT_BYTES.ordinal()],
            raw[offset + BlockCacheStats.Field.MISS_COUNT.ordinal()],
            raw[offset + BlockCacheStats.Field.MISS_BYTES.ordinal()],
            raw[offset + BlockCacheStats.Field.EVICTION_COUNT.ordinal()],
            raw[offset + BlockCacheStats.Field.EVICTION_BYTES.ordinal()],
            raw[offset + BlockCacheStats.Field.USED_BYTES.ordinal()],
            capacityBytes
        );
    }
}
