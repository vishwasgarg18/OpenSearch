/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Aggregate snapshot of block cache statistics, mirroring the structure of
 * {@link org.opensearch.index.store.remote.utils.cache.stats.AggregateRefCountedCacheStats}
 * for uniformity.
 *
 * <p>Contains two {@link BlockCacheStats} sub-sections:
 * <ul>
 *   <li><b>{@code overallStats}</b> — cross-tier rollup. Today Foyer is a single-tier
 *       disk cache (no in-memory tier), so {@code overallStats} is always identical to
 *       {@code blockLevelStats}. The separation exists so that future multi-tier Foyer
 *       configurations can populate the two independently without a breaking API change.</li>
 *   <li><b>{@code blockLevelStats}</b> — disk-tier (block-level) stats.</li>
 * </ul>
 *
 * <p>Produced by {@link FoyerBlockCacheStatsCounter#snapshot()} from the
 * {@code long[10]} array returned by {@link org.opensearch.blockcache.foyer.FoyerBridge#snapshotStats(long)}.
 * The first 5 elements map to {@code overallStats} and the next 5 to {@code blockLevelStats}
 * (using the same field order as {@link BlockCacheStats}).
 *
 * @opensearch.experimental
 */
public final class AggregateBlockCacheStats implements Writeable, ToXContentFragment {

    /**
     * Empty aggregate returned when no block cache is configured.
     */
    public static final AggregateBlockCacheStats EMPTY = new AggregateBlockCacheStats(
        BlockCacheStats.EMPTY,
        BlockCacheStats.EMPTY
    );

    private final BlockCacheStats overallStats;
    private final BlockCacheStats blockLevelStats;

    /**
     * Creates an aggregate snapshot.
     *
     * @param overallStats    cross-tier rollup; must not be null
     * @param blockLevelStats disk-tier stats; must not be null
     */
    public AggregateBlockCacheStats(BlockCacheStats overallStats, BlockCacheStats blockLevelStats) {
        this.overallStats    = Objects.requireNonNull(overallStats, "overallStats must not be null");
        this.blockLevelStats = Objects.requireNonNull(blockLevelStats, "blockLevelStats must not be null");
    }

    /**
     * Returns the cross-tier rollup stats.
     *
     * <p>Today identical to {@link #blockLevelStats()} — Foyer has only a disk tier.
     */
    public BlockCacheStats overallStats() {
        return overallStats;
    }

    /**
     * Returns the disk-tier (block-level) stats.
     */
    public BlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }

    /**
     * Convenience accessor: overall hit count.
     */
    public long hitCount() {
        return overallStats.hitCount();
    }

    /**
     * Convenience accessor: overall miss count.
     */
    public long missCount() {
        return overallStats.missCount();
    }

    /**
     * Convenience accessor: overall eviction bytes.
     */
    public long evictionBytes() {
        return overallStats.evictionBytes();
    }

    /**
     * Convenience accessor: overall used bytes.
     */
    public long usedBytes() {
        return overallStats.usedBytes();
    }

    /**
     * Convenience accessor: overall capacity bytes.
     */
    public long capacityBytes() {
        return overallStats.capacityBytes();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Writeable — StreamInput constructor + writeTo
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Deserialises an {@code AggregateBlockCacheStats} from the wire by reading
     * the two inner {@link BlockCacheStats} objects in order.
     */
    public AggregateBlockCacheStats(StreamInput in) throws IOException {
        this.overallStats    = new BlockCacheStats(in);
        this.blockLevelStats = new BlockCacheStats(in);
    }

    /**
     * Serialises this aggregate to the wire by writing the two inner
     * {@link BlockCacheStats} objects in order.
     * Order matches {@link #AggregateBlockCacheStats(StreamInput)}.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        overallStats.writeTo(out);
        blockLevelStats.writeTo(out);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ToXContentFragment
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Writes this aggregate as two named nested JSON objects:
     * <pre>{@code
     * "overall_stats":     { ... },
     * "block_level_stats": { ... }
     * }</pre>
     * Each nested object is rendered by {@link BlockCacheStats#toXContent}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("overall_stats");
        overallStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("block_level_stats");
        blockLevelStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Object
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public String toString() {
        return "AggregateBlockCacheStats{overall=" + overallStats + ", blockLevel=" + blockLevelStats + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregateBlockCacheStats)) return false;
        AggregateBlockCacheStats that = (AggregateBlockCacheStats) o;
        return overallStats.equals(that.overallStats) && blockLevelStats.equals(that.blockLevelStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(overallStats, blockLevelStats);
    }
}
