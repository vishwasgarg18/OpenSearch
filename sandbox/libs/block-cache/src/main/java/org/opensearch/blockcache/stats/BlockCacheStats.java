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
 * Immutable snapshot of block cache statistics.
 *
 * <p>Analogous to
 * {@link org.opensearch.index.store.remote.utils.cache.stats.RefCountedCacheStats}
 * for the Java {@link org.opensearch.index.store.remote.filecache.FileCache},
 * with the addition of {@link #hitBytes()} and {@link #missBytes()} which are
 * particularly important for variable-size entries (Parquet column chunks, 1 KB –
 * 64 MB). For fixed-size caches (e.g. Lucene ~8 MB blocks), count-based hit rate
 * is a reliable proxy for byte effectiveness; for variable-size caches it is not.
 *
 * <p>Produced by {@link BlockCacheStatsCounter#snapshot()} and passed up the
 * stack to {@link org.opensearch.blockcache.BlockCacheHandle#cacheStats()},
 * which converts it to the cross-cache
 * {@link org.opensearch.index.store.remote.filecache.CacheStats} shape used
 * by {@link org.opensearch.index.store.remote.filecache.AggregateFileCacheStats#merge}.
 *
 * @opensearch.experimental
 */
public final class BlockCacheStats implements IBlockCacheStats, Writeable, ToXContentFragment {

    /**
     * Ordered fields of a single stats section in the FFM transfer buffer.
     *
     * <p>The ordinal of each constant is its index within one section.
     * The ordinal order <strong>must match</strong> the field order in
     * {@code BlockCacheStats::snapshot()} in Rust.
     *
     * <p>To add a field: add a constant here in the correct position AND add the
     * corresponding value to the Rust {@code snapshot()} array at the same offset.
     * {@link #COUNT} then updates automatically — buffer sizes and section offsets
     * stay correct without further changes.
     */
    public enum Field {
        HIT_COUNT,
        HIT_BYTES,
        MISS_COUNT,
        MISS_BYTES,
        EVICTION_COUNT,
        EVICTION_BYTES,
        USED_BYTES;

        /** Number of fields per section — derived from the enum, not a separate constant. */
        public static final int COUNT = values().length;
    }

    /** Sentinel returned when no block cache is configured. */
    public static final BlockCacheStats EMPTY = new BlockCacheStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    private final long hitCount;
    /** Bytes served from cache across all hits — see {@link IBlockCacheStats#hitBytes()}. */
    private final long hitBytes;
    private final long missCount;
    /** Bytes that required remote fetch — see {@link IBlockCacheStats#missBytes()}. */
    private final long missBytes;
    private final long evictionCount;
    private final long evictionBytes;
    private final long usedBytes;
    private final long capacityBytes;

    /**
     * Creates an immutable stats snapshot.
     *
     * @param hitCount      number of {@code get()} calls that returned a cached value
     * @param hitBytes      bytes served from cache across all hits
     * @param missCount     number of {@code get()} calls that returned no value
     * @param missBytes     bytes that required remote fetch due to misses (from key range)
     * @param evictionCount number of entries removed by LRU pressure
     * @param evictionBytes total bytes removed by LRU pressure
     * @param usedBytes     current bytes resident in the cache on disk
     * @param capacityBytes configured capacity in bytes
     */
    public BlockCacheStats(
        long hitCount,
        long hitBytes,
        long missCount,
        long missBytes,
        long evictionCount,
        long evictionBytes,
        long usedBytes,
        long capacityBytes
    ) {
        this.hitCount = hitCount;
        this.hitBytes = hitBytes;
        this.missCount = missCount;
        this.missBytes = missBytes;
        this.evictionCount = evictionCount;
        this.evictionBytes = evictionBytes;
        this.usedBytes = usedBytes;
        this.capacityBytes = capacityBytes;
    }

    @Override public long hitCount()      { return hitCount;      }
    @Override public long hitBytes()      { return hitBytes;      }
    @Override public long missCount()     { return missCount;     }
    @Override public long missBytes()     { return missBytes;     }
    @Override public long evictionCount() { return evictionCount; }
    @Override public long evictionBytes() { return evictionBytes; }
    @Override public long usedBytes()     { return usedBytes;     }
    @Override public long capacityBytes() { return capacityBytes; }

    // ─────────────────────────────────────────────────────────────────────────
    // Writeable — StreamInput constructor + writeTo
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Deserialises a {@code BlockCacheStats} from the wire.
     * Field order matches {@link #writeTo(StreamOutput)}.
     */
    public BlockCacheStats(StreamInput in) throws IOException {
        this.hitCount      = in.readLong();
        this.hitBytes      = in.readLong();
        this.missCount     = in.readLong();
        this.missBytes     = in.readLong();
        this.evictionCount = in.readLong();
        this.evictionBytes = in.readLong();
        this.usedBytes     = in.readLong();
        this.capacityBytes = in.readLong();
    }

    /**
     * Serialises this snapshot to the wire.
     * Field order matches {@link #BlockCacheStats(StreamInput)}.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(hitCount);
        out.writeLong(hitBytes);
        out.writeLong(missCount);
        out.writeLong(missBytes);
        out.writeLong(evictionCount);
        out.writeLong(evictionBytes);
        out.writeLong(usedBytes);
        out.writeLong(capacityBytes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ToXContentFragment
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Writes this snapshot as inline JSON fields.
     *
     * <p>Output fields:
     * <pre>{@code
     *   "hit_count":         1000000,
     *   "hit_bytes":         8589934592,
     *   "miss_count":        250000,
     *   "miss_bytes":        2147483648,
     *   "byte_hit_rate":     0.8,
     *   "hit_rate":          0.8,
     *   "eviction_count":    5000,
     *   "eviction_bytes":    327680000,
     *   "used_in_bytes":     4294967296,
     *   "capacity_in_bytes": 68719476736,
     *   "used_percent":      0.0625
     * }</pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("hit_count", hitCount);
        builder.field("hit_bytes", hitBytes);
        builder.field("miss_count", missCount);
        builder.field("miss_bytes", missBytes);
        // byte_hit_rate: true effectiveness for variable-size entries
        long totalBytes = hitBytes + missBytes;
        builder.field("byte_hit_rate", totalBytes == 0 ? 1.0 : (double) hitBytes / totalBytes);
        builder.field("hit_rate", hitRate());
        builder.field("eviction_count", evictionCount);
        builder.field("eviction_bytes", evictionBytes);
        builder.field("used_in_bytes", usedBytes);
        builder.field("capacity_in_bytes", capacityBytes);
        builder.field("used_percent", usedPercent());
        return builder;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Object
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public String toString() {
        return "BlockCacheStats{"
            + "hits=" + hitCount
            + ", hitBytes=" + hitBytes
            + ", misses=" + missCount
            + ", missBytes=" + missBytes
            + ", hitRate=" + String.format("%.2f", hitRate())
            + ", evictionCount=" + evictionCount
            + ", evictionBytes=" + evictionBytes
            + ", usedBytes=" + usedBytes
            + ", capacityBytes=" + capacityBytes
            + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BlockCacheStats)) return false;
        BlockCacheStats that = (BlockCacheStats) o;
        return hitCount == that.hitCount
            && hitBytes == that.hitBytes
            && missCount == that.missCount
            && missBytes == that.missBytes
            && evictionCount == that.evictionCount
            && evictionBytes == that.evictionBytes
            && usedBytes == that.usedBytes
            && capacityBytes == that.capacityBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hitCount, hitBytes, missCount, missBytes,
                            evictionCount, evictionBytes, usedBytes, capacityBytes);
    }
}
