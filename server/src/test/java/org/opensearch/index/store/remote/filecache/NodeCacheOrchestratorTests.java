/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link NodeCacheOrchestrator}.
 *
 * <p>These tests exercise the pure-Java logic of the orchestrator — budget
 * validation, block cache registration, stats aggregation — without requiring
 * a real {@link FileCache} instance from disk.
 */
public class NodeCacheOrchestratorTests extends OpenSearchTestCase {

    // ── validate() — static budget checks ────────────────────────────────────

    public void testValidatePassesWithLegalValues() {
        // Must not throw
        NodeCacheOrchestrator.validate(600L, 200L, 1000L);
    }

    public void testValidatePassesWithZeroBlockCache() {
        NodeCacheOrchestrator.validate(800L, 0L, 1000L);
    }

    public void testValidateThrowsOnZeroTotalSSD() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(600L, 200L, 0L)
        );
        assertTrue(ex.getMessage().contains("SSD capacity"));
    }

    public void testValidateThrowsOnNegativeTotalSSD() {
        expectThrows(
            IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(600L, 200L, -1L)
        );
    }

    public void testValidateThrowsOnNegativeBlockCacheBytes() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(800L, -1L, 1000L)
        );
        assertTrue(ex.getMessage().contains("block cache allocation") || ex.getMessage().contains("blockCacheBytes"));
    }

    public void testValidateThrowsWhenFileCacheBytesIsZero() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(0L, 1000L, 1000L)
        );
        assertTrue(ex.getMessage().contains("FileCache") || ex.getMessage().contains("no SSD"));
    }

    public void testValidateThrowsWhenFileCacheBytesIsNegative() {
        expectThrows(
            IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(-1L, 600L, 1000L)
        );
    }

    public void testValidateThrowsWhenSumExceedsTotalSSD() {
        // fileCacheBytes=700 + blockCacheBytes=400 = 1100 > totalSSD=1000
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(700L, 400L, 1000L)
        );
        assertTrue(ex.getMessage().contains("exceeds available capacity") || ex.getMessage().contains("Reduce"));
    }

    public void testValidatePassesWhenSumExactlyEqualsTotalSSD() {
        // Edge case: sum == totalSSD should be allowed (tight but valid)
        NodeCacheOrchestrator.validate(600L, 400L, 1000L); // must not throw
    }

    // ── addBlockCache and accessors ───────────────────────────────────────────

    public void testAddNullBlockCacheIsIgnored() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(null); // must not throw
        assertEquals(0, orc.blockCaches().size());
    }

    public void testAddBlockCacheRegistersIt() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        BlockCache bc = mockBlockCache(0, 0, 0, 0, 1_000_000L);
        orc.addBlockCache(bc);
        assertEquals(1, orc.blockCaches().size());
        assertSame(bc, orc.blockCaches().get(0));
    }

    public void testAddMultipleBlockCaches() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 500L));
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 500L));
        assertEquals(2, orc.blockCaches().size());
    }

    public void testBlockCachesReturnsUnmodifiableView() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 1L));
        expectThrows(UnsupportedOperationException.class, () -> orc.blockCaches().add(null));
    }

    public void testFileCacheAccessorReturnsInjectedInstance() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        assertSame(fc, orc.fileCache());
    }

    // ── blockCacheCapacityBytes ───────────────────────────────────────────────

    public void testBlockCacheCapacityBytesWithNoBlockCachesIsZero() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        assertEquals(0L, orc.blockCacheCapacityBytes());
    }

    public void testBlockCacheCapacityBytesSumsAcrossAllCaches() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 100L, 400L));
        orc.addBlockCache(mockBlockCache(0, 0, 0, 200L, 600L));
        // totalBytes: 400 + 600 = 1000
        assertEquals(1000L, orc.blockCacheCapacityBytes());
    }

    // ── cacheUtilizedBytes ────────────────────────────────────────────────────

    public void testCacheUtilizedBytesIncludesFileCacheUsage() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(500L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        assertEquals(500L, orc.cacheUtilizedBytes());
    }

    public void testCacheUtilizedBytesIncludesBlockCacheDiskUsed() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(100L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 300L, 0, 0));
        assertEquals(400L, orc.cacheUtilizedBytes()); // 100 fileCache + 300 blockCache disk
    }

    public void testCacheUtilizedBytesIncludesBlockCacheMemoryUsed() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        BlockCacheStats stats = new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 50L, 200L, 0L);
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        orc.addBlockCache(bc);
        assertEquals(250L, orc.cacheUtilizedBytes()); // 50 memory + 200 disk
    }

    // ── blockCache() convenience accessor ────────────────────────────────────

    public void testBlockCacheConvenienceAccessorReturnsNullWhenNoneRegistered() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        assertNull(orc.blockCache());
    }

    public void testBlockCacheConvenienceAccessorReturnsFirstRegistered() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        BlockCache first  = mockBlockCache(0, 0, 0, 0, 100L);
        BlockCache second = mockBlockCache(0, 0, 0, 0, 200L);
        orc.addBlockCache(first);
        orc.addBlockCache(second);
        assertSame(first, orc.blockCache());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Creates a mock BlockCache that returns a BlockCacheStats with the given values.
     */
    private BlockCache mockBlockCache(long hits, long misses, long diskBytesUsed,
                                      long memoryBytesUsed, long totalBytes) {
        BlockCacheStats stats = new BlockCacheStats(
            hits, misses, 0L, 0L, 0L, 0L, 0L, 0L,
            memoryBytesUsed, diskBytesUsed, totalBytes
        );
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        return bc;
    }
}
