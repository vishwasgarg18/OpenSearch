/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FoyerAggregatedStats}.
 *
 * <p>These tests exercise the FFM buffer parsing logic directly without
 * requiring the native library. The {@code snapshot(long[], long)} factory
 * reads from a manually constructed buffer, so any field-ordinal bug or
 * off-by-one in the section offset would be caught here.
 *
 * <h3>Buffer layout under test</h3>
 * The 14-element {@code long[]} has two 7-field sections:
 * <pre>
 *   [0] HIT_COUNT   [1] HIT_BYTES   [2] MISS_COUNT  [3] MISS_BYTES
 *   [4] EVICTION_COUNT [5] EVICTION_BYTES [6] USED_BYTES
 *   — then the same 7 fields repeated at [7..13] for block-level —
 * </pre>
 */
public class FoyerAggregatedStatsTests extends OpenSearchTestCase {

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Builds a 14-element FFM stats buffer.
     * Section 0 (overall) at indices 0–6, section 1 (block-level) at 7–13.
     */
    private static long[] makeBuffer(
        long hitCount0, long hitBytes0, long missCount0, long missBytes0,
        long evictionCount0, long evictionBytes0, long usedBytes0,
        long hitCount1, long hitBytes1, long missCount1, long missBytes1,
        long evictionCount1, long evictionBytes1, long usedBytes1
    ) {
        return new long[] {
            hitCount0, hitBytes0, missCount0, missBytes0,
            evictionCount0, evictionBytes0, usedBytes0,
            hitCount1, hitBytes1, missCount1, missBytes1,
            evictionCount1, evictionBytes1, usedBytes1
        };
    }

    /** Builds a buffer where both sections are identical. */
    private static long[] uniformBuffer(
        long hitCount, long hitBytes, long missCount, long missBytes,
        long evictionCount, long evictionBytes, long usedBytes
    ) {
        return makeBuffer(
            hitCount, hitBytes, missCount, missBytes, evictionCount, evictionBytes, usedBytes,
            hitCount, hitBytes, missCount, missBytes, evictionCount, evictionBytes, usedBytes
        );
    }

    // ── Non-null guarantees ───────────────────────────────────────────────────

    public void testSnapshotReturnsNonNull() {
        long[] raw = new long[14];
        assertNotNull(FoyerAggregatedStats.snapshot(raw, 0L));
    }

    public void testOverallStatsAccessorReturnsNonNull() {
        long[] raw = new long[14];
        assertNotNull(FoyerAggregatedStats.snapshot(raw, 0L).overallStats());
    }

    public void testBlockLevelStatsAccessorReturnsNonNull() {
        long[] raw = new long[14];
        assertNotNull(FoyerAggregatedStats.snapshot(raw, 0L).blockLevelStats());
    }

    // ── overallStats field mapping (each field one-hot) ───────────────────────

    public void testOverallStatsHitCountMappedFromIndex0() {
        long[] raw = makeBuffer(42, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(42L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().hits());
    }

    public void testOverallStatsHitBytesMappedFromIndex1() {
        long[] raw = makeBuffer(0, 1024, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(1024L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().hitBytes());
    }

    public void testOverallStatsMissCountMappedFromIndex2() {
        long[] raw = makeBuffer(0, 0, 7, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(7L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().misses());
    }

    public void testOverallStatsMissBytesMappedFromIndex3() {
        long[] raw = makeBuffer(0, 0, 0, 512, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(512L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().missBytes());
    }

    public void testOverallStatsEvictionCountMappedFromIndex4() {
        long[] raw = makeBuffer(0, 0, 0, 0, 3, 0, 0,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(3L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().evictions());
    }

    public void testOverallStatsEvictionBytesMappedFromIndex5() {
        long[] raw = makeBuffer(0, 0, 0, 0, 0, 2048, 0,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(2048L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().evictionBytes());
    }

    public void testOverallStatsUsedBytesMappedFromIndex6() {
        long[] raw = makeBuffer(0, 0, 0, 0, 0, 0, 999,  0, 0, 0, 0, 0, 0, 0);
        assertEquals(999L, FoyerAggregatedStats.snapshot(raw, 1L).overallStats().diskBytesUsed());
    }

    // ── blockLevelStats section isolation ─────────────────────────────────────

    public void testBlockLevelStatsReadFromSection1NotSection0() {
        long[] raw = makeBuffer(10, 0, 0, 0, 0, 0, 0,  99, 0, 0, 0, 0, 0, 0);
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(raw, 1L);
        assertEquals(10L, s.overallStats().hits());
        assertEquals(99L, s.blockLevelStats().hits());
    }

    public void testBlockLevelStatsAllFieldsMappedCorrectly() {
        long[] raw = makeBuffer(
            0, 0, 0, 0, 0, 0, 0,
            11, 22, 33, 44, 55, 66, 77
        );
        BlockCacheStats bl = FoyerAggregatedStats.snapshot(raw, 100L).blockLevelStats();
        assertEquals(11L, bl.hits());
        assertEquals(22L, bl.hitBytes());
        assertEquals(33L, bl.misses());
        assertEquals(44L, bl.missBytes());
        assertEquals(55L, bl.evictions());
        assertEquals(66L, bl.evictionBytes());
        assertEquals(77L, bl.diskBytesUsed());
    }

    // ── capacityBytes (totalBytes) ────────────────────────────────────────────

    public void testCapacityBytesPassedThroughToOverallStats() {
        long capacity = 1_073_741_824L; // 1 GiB
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(new long[14], capacity);
        assertEquals(capacity, s.overallStats().totalBytes());
    }

    public void testCapacityBytesPassedThroughToBlockLevelStats() {
        long capacity = 1_073_741_824L;
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(new long[14], capacity);
        assertEquals(capacity, s.blockLevelStats().totalBytes());
    }

    public void testZeroCapacityBytes() {
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(new long[14], 0L);
        assertEquals(0L, s.overallStats().totalBytes());
    }

    // ── Foyer-specific constant zero fields ───────────────────────────────────

    public void testOverallStatsMemoryBytesUsedIsZero() {
        long[] raw = uniformBuffer(10, 100, 5, 50, 2, 20, 500);
        assertEquals(0L, FoyerAggregatedStats.snapshot(raw, 1000L).overallStats().memoryBytesUsed());
    }

    public void testBlockLevelStatsMemoryBytesUsedIsZero() {
        long[] raw = uniformBuffer(10, 100, 5, 50, 2, 20, 500);
        assertEquals(0L, FoyerAggregatedStats.snapshot(raw, 1000L).blockLevelStats().memoryBytesUsed());
    }

    public void testOverallStatsRemovedIsZero() {
        long[] raw = uniformBuffer(10, 100, 5, 50, 2, 20, 500);
        assertEquals(0L, FoyerAggregatedStats.snapshot(raw, 1000L).overallStats().removed());
    }

    public void testOverallStatsRemovedBytesIsZero() {
        long[] raw = uniformBuffer(10, 100, 5, 50, 2, 20, 500);
        assertEquals(0L, FoyerAggregatedStats.snapshot(raw, 1000L).overallStats().removedBytes());
    }

    public void testBlockLevelStatsRemovedIsZero() {
        long[] raw = uniformBuffer(10, 100, 5, 50, 2, 20, 500);
        assertEquals(0L, FoyerAggregatedStats.snapshot(raw, 1000L).blockLevelStats().removed());
    }

    public void testBlockLevelStatsRemovedBytesIsZero() {
        long[] raw = uniformBuffer(10, 100, 5, 50, 2, 20, 500);
        assertEquals(0L, FoyerAggregatedStats.snapshot(raw, 1000L).blockLevelStats().removedBytes());
    }

    // ── All-fields complete projection ────────────────────────────────────────

    public void testCompleteOverallStatsProjection() {
        long[] raw = makeBuffer(100, 1000, 10, 200, 5, 500, 4096,  0, 0, 0, 0, 0, 0, 0);
        BlockCacheStats bc = FoyerAggregatedStats.snapshot(raw, 8192L).overallStats();
        assertEquals(100L,  bc.hits());
        assertEquals(1000L, bc.hitBytes());
        assertEquals(10L,   bc.misses());
        assertEquals(200L,  bc.missBytes());
        assertEquals(5L,    bc.evictions());
        assertEquals(500L,  bc.evictionBytes());
        assertEquals(4096L, bc.diskBytesUsed());
        assertEquals(8192L, bc.totalBytes());
        assertEquals(0L,    bc.memoryBytesUsed());
        assertEquals(0L,    bc.removed());
        assertEquals(0L,    bc.removedBytes());
    }

    // ── All-zeros buffer ──────────────────────────────────────────────────────

    public void testAllZeroBufferProducesZeroStats() {
        BlockCacheStats overall = FoyerAggregatedStats.snapshot(new long[14], 0L).overallStats();
        assertEquals(0L, overall.hits());
        assertEquals(0L, overall.hitBytes());
        assertEquals(0L, overall.misses());
        assertEquals(0L, overall.missBytes());
        assertEquals(0L, overall.evictions());
        assertEquals(0L, overall.evictionBytes());
        assertEquals(0L, overall.diskBytesUsed());
        assertEquals(0L, overall.totalBytes());
    }

    // ── Section independence ──────────────────────────────────────────────────

    public void testBothSectionsCanHaveDifferentValues() {
        long[] raw = makeBuffer(
            1, 2, 3, 4, 5, 6, 7,
            10, 20, 30, 40, 50, 60, 70
        );
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(raw, 0L);
        assertEquals(1L,  s.overallStats().hits());
        assertEquals(10L, s.blockLevelStats().hits());
        assertEquals(7L,  s.overallStats().diskBytesUsed());
        assertEquals(70L, s.blockLevelStats().diskBytesUsed());
    }

    public void testSection0ChangesDoNotAffectSection1() {
        // Put a distinctive value only in section 0 field 4 (eviction_count)
        long[] raw = makeBuffer(0, 0, 0, 0, 999, 0, 0,  0, 0, 0, 0, 0, 0, 0);
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(raw, 0L);
        assertEquals(999L, s.overallStats().evictions());
        assertEquals(0L,   s.blockLevelStats().evictions()); // section 1 unaffected
    }

    // ── Large and boundary values ─────────────────────────────────────────────

    public void testLargeCounterValuesPassThroughWithoutCorruption() {
        long large = Long.MAX_VALUE / 2;
        long[] raw = uniformBuffer(large, large, large, large, large, large, large);
        BlockCacheStats bc = FoyerAggregatedStats.snapshot(raw, large).overallStats();
        assertEquals(large, bc.hits());
        assertEquals(large, bc.hitBytes());
        assertEquals(large, bc.misses());
        assertEquals(large, bc.missBytes());
        assertEquals(large, bc.evictions());
        assertEquals(large, bc.evictionBytes());
        assertEquals(large, bc.diskBytesUsed());
        assertEquals(large, bc.totalBytes());
    }

    public void testMaxLongCapacityPassesThrough() {
        long max = Long.MAX_VALUE;
        FoyerAggregatedStats s = FoyerAggregatedStats.snapshot(new long[14], max);
        assertEquals(max, s.overallStats().totalBytes());
        assertEquals(max, s.blockLevelStats().totalBytes());
    }
}
