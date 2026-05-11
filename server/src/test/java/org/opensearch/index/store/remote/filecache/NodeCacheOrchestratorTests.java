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
 * Unit tests for {@link NodeCacheOrchestrator}: budget validation, addBlockCache,
 * capacity and utilization accessors — all without real FileCache from disk.
 */
public class NodeCacheOrchestratorTests extends OpenSearchTestCase {

    // ── validate() ────────────────────────────────────────────────────────────

    public void testValidatePassesLegalValues() {
        NodeCacheOrchestrator.validate(600L, 200L, 1000L);
    }

    public void testValidatePassesZeroBlockCache() {
        NodeCacheOrchestrator.validate(800L, 0L, 1000L);
    }

    public void testValidatePassesSumEqualsTotalSSD() {
        NodeCacheOrchestrator.validate(600L, 400L, 1000L);
    }

    public void testValidateThrowsZeroTotalSSD() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(600L, 200L, 0L));
        assertTrue(ex.getMessage().contains("SSD capacity"));
    }

    public void testValidateThrowsNegativeTotalSSD() {
        expectThrows(IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(600L, 200L, -1L));
    }

    public void testValidateThrowsNegativeBlockCacheBytes() {
        expectThrows(IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(800L, -1L, 1000L));
    }

    public void testValidateThrowsFileCacheBytesZero() {
        expectThrows(IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(0L, 1000L, 1000L));
    }

    public void testValidateThrowsFileCacheBytesNegative() {
        expectThrows(IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(-1L, 600L, 1000L));
    }

    public void testValidateThrowsSumExceedsTotalSSD() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> NodeCacheOrchestrator.validate(700L, 400L, 1000L));
        assertTrue(ex.getMessage().contains("exceeds") || ex.getMessage().contains("Reduce"));
    }

    // ── addBlockCache / blockCaches ───────────────────────────────────────────

    public void testAddNullBlockCacheIgnored() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(null);
        assertEquals(0, orc.blockCaches().size());
    }

    public void testAddBlockCacheRegistersIt() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        BlockCache bc = mockBlockCache(0, 0, 0, 0, 1000L);
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

    public void testBlockCachesListIsUnmodifiable() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 1L));
        expectThrows(UnsupportedOperationException.class, () -> orc.blockCaches().add(null));
    }

    public void testFileCacheAccessor() {
        FileCache fc = mock(FileCache.class);
        assertSame(fc, new NodeCacheOrchestrator(fc).fileCache());
    }

    // ── blockCacheCapacityBytes ───────────────────────────────────────────────

    public void testBlockCacheCapacityZeroWhenNone() {
        FileCache fc = mock(FileCache.class);
        assertEquals(0L, new NodeCacheOrchestrator(fc).blockCacheCapacityBytes());
    }

    public void testBlockCacheCapacitySumsAcrossCaches() {
        FileCache fc = mock(FileCache.class);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 400L));
        orc.addBlockCache(mockBlockCache(0, 0, 0, 0, 600L));
        assertEquals(1000L, orc.blockCacheCapacityBytes());
    }

    // ── cacheUtilizedBytes ────────────────────────────────────────────────────

    public void testCacheUtilizedIncludesFileCache() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(500L);
        assertEquals(500L, new NodeCacheOrchestrator(fc).cacheUtilizedBytes());
    }

    public void testCacheUtilizedIncludesBlockCacheDisk() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(100L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        orc.addBlockCache(mockBlockCache(0, 0, 300L, 0, 0));
        assertEquals(400L, orc.cacheUtilizedBytes());
    }

    public void testCacheUtilizedIncludesBlockCacheMemory() {
        FileCache fc = mock(FileCache.class);
        when(fc.usage()).thenReturn(0L);
        NodeCacheOrchestrator orc = new NodeCacheOrchestrator(fc);
        BlockCacheStats stats = new BlockCacheStats(0,0,0,0,0,0,0,0,50L,200L,0L);
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        orc.addBlockCache(bc);
        assertEquals(250L, orc.cacheUtilizedBytes());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private BlockCache mockBlockCache(long hits, long misses, long diskUsed, long memUsed, long total) {
        BlockCacheStats stats = new BlockCacheStats(hits, misses, 0,0,0,0,0,0, memUsed, diskUsed, total);
        BlockCache bc = mock(BlockCache.class);
        when(bc.stats()).thenReturn(stats);
        return bc;
    }
}
