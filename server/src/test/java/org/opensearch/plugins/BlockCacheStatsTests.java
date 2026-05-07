/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link BlockCacheStats}.
 *
 * <p>Verifies that the record carries all 11 fields correctly and that
 * the accessor names match the declared field order, which is the FFM
 * wire contract between the server and Foyer plugin.
 */
public class BlockCacheStatsTests extends OpenSearchTestCase {

    // ── Field order and accessor correctness ─────────────────────────────────

    public void testAllFieldsStoredAndReturnedInDeclarationOrder() {
        BlockCacheStats s = new BlockCacheStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        assertEquals(1L,  s.hits());
        assertEquals(2L,  s.misses());
        assertEquals(3L,  s.hitBytes());
        assertEquals(4L,  s.missBytes());
        assertEquals(5L,  s.evictions());
        assertEquals(6L,  s.evictionBytes());
        assertEquals(7L,  s.removed());
        assertEquals(8L,  s.removedBytes());
        assertEquals(9L,  s.memoryBytesUsed());
        assertEquals(10L, s.diskBytesUsed());
        assertEquals(11L, s.totalBytes());
    }

    public void testAllZeroRecord() {
        BlockCacheStats s = new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        assertEquals(0L, s.hits());
        assertEquals(0L, s.misses());
        assertEquals(0L, s.hitBytes());
        assertEquals(0L, s.missBytes());
        assertEquals(0L, s.evictions());
        assertEquals(0L, s.evictionBytes());
        assertEquals(0L, s.removed());
        assertEquals(0L, s.removedBytes());
        assertEquals(0L, s.memoryBytesUsed());
        assertEquals(0L, s.diskBytesUsed());
        assertEquals(0L, s.totalBytes());
    }

    public void testMaxLongValuesStoredWithoutCorruption() {
        long max = Long.MAX_VALUE;
        BlockCacheStats s = new BlockCacheStats(max, max, max, max, max, max, max, max, max, max, max);
        assertEquals(max, s.hits());
        assertEquals(max, s.diskBytesUsed());
        assertEquals(max, s.totalBytes());
    }

    // ── Equality (record identity) ────────────────────────────────────────────

    public void testTwoRecordsWithSameValuesAreEqual() {
        BlockCacheStats a = new BlockCacheStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        BlockCacheStats b = new BlockCacheStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testTwoRecordsWithDifferentValuesAreNotEqual() {
        BlockCacheStats a = new BlockCacheStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        BlockCacheStats b = new BlockCacheStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 99);
        assertNotEquals(a, b);
    }

    // ── Typical Foyer-style construction (disk-only, no memory tier) ──────────

    public void testTypicalFoyerStats() {
        // Foyer is disk-only: memoryBytesUsed=0, removed=0, removedBytes=0
        BlockCacheStats s = new BlockCacheStats(
            100L,   // hits
            10L,    // misses
            1024L,  // hitBytes
            512L,   // missBytes
            3L,     // evictions
            256L,   // evictionBytes
            0L,     // removed
            0L,     // removedBytes
            0L,     // memoryBytesUsed — disk-only
            4096L,  // diskBytesUsed
            8192L   // totalBytes (configured capacity)
        );
        assertEquals(100L,  s.hits());
        assertEquals(10L,   s.misses());
        assertEquals(1024L, s.hitBytes());
        assertEquals(512L,  s.missBytes());
        assertEquals(3L,    s.evictions());
        assertEquals(256L,  s.evictionBytes());
        assertEquals(0L,    s.removed());
        assertEquals(0L,    s.removedBytes());
        assertEquals(0L,    s.memoryBytesUsed());
        assertEquals(4096L, s.diskBytesUsed());
        assertEquals(8192L, s.totalBytes());
    }

    // ── toString contains field values (useful for debugging) ─────────────────

    public void testToStringContainsFieldValues() {
        BlockCacheStats s = new BlockCacheStats(42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        assertTrue("toString should contain hits value", s.toString().contains("42"));
    }
}
