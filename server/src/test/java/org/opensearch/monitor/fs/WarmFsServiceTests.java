/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.NodeCacheOrchestrator;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WarmFsService}.
 *
 * <p>The constructor takes a {@link NodeCacheOrchestrator} (not a raw {@link FileCache}) and a
 * pre-computed {@code virtualBlockCacheBytes} value. Virtual total capacity is:
 * <pre>
 *   total = (dataToFileCacheRatio × fileCacheCapacity) + virtualBlockCacheBytes
 * </pre>
 */
public class WarmFsServiceTests extends OpenSearchTestCase {

    private Settings settings;
    private FileCacheSettings fileCacheSettings;
    private IndicesService indicesService;
    private FileCache fileCache;
    private NodeCacheOrchestrator orchestrator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.EMPTY;
        fileCacheSettings = mock(FileCacheSettings.class);
        indicesService = mock(IndicesService.class);
        fileCache = mock(FileCache.class);
        orchestrator = mock(NodeCacheOrchestrator.class);
        when(orchestrator.fileCache()).thenReturn(fileCache);
    }

    // ── Normal operation ──────────────────────────────────────────────────────

    public void testStatsWithNormalOperationNoBlockCache() throws Exception {
        double fileCacheRatio = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024; // 100 MB
        long fileCacheUsage    = 20L  * 1024 * 1024; // 20 MB
        long shard1Size        = 50L  * 1024 * 1024; // 50 MB
        long shard2Size        = 30L  * 1024 * 1024; // 30 MB
        long blockCacheCapacity = 0L;
        long virtualBlockCacheBytes = 0L;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(blockCacheCapacity);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(fileCacheUsage);

        IndexService indexService = mockIndexService(shard1Size, shard2Size);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, virtualBlockCacheBytes);
            FsInfo fsInfo = svc.stats();

            assertNotNull(fsInfo);
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            assertEquals("/warm", warmPath.path);
            assertEquals("warm", warmPath.mount);
            assertEquals("warm", warmPath.type);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity) + virtualBlockCacheBytes;
            long expectedUsed  = shard1Size + shard2Size;
            long expectedFree  = expectedTotal - expectedUsed;

            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedFree,  warmPath.free);
            assertEquals(expectedFree,  warmPath.available);
            assertEquals(fileCacheCapacity + blockCacheCapacity, warmPath.fileCacheReserved);
            assertEquals(fileCacheUsage, warmPath.fileCacheUtilized);
        }
    }

    public void testStatsIncludesVirtualBlockCacheBytes() throws Exception {
        double fileCacheRatio = 5.0;
        long fileCacheCapacity      = 100L * 1024 * 1024; // 100 MB
        long blockCacheCapacity     = 200L * 1024 * 1024; // 200 MB physical
        long virtualBlockCacheBytes = 1000L * 1024 * 1024; // 1 GB virtual (e.g. 5× ratio)

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(blockCacheCapacity);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);
        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, virtualBlockCacheBytes);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity) + virtualBlockCacheBytes;
            assertEquals(expectedTotal, warmPath.total);
            // fileCacheReserved = fileCache + blockCache physical capacity
            assertEquals(fileCacheCapacity + blockCacheCapacity, warmPath.fileCacheReserved);
        }
    }

    public void testStatsWithNullIndicesService() throws IOException {
        double fileCacheRatio    = 5.0;
        long fileCacheCapacity   = 100L * 1024 * 1024;
        long fileCacheUsage      = 20L  * 1024 * 1024;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(fileCacheUsage);

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, null,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();

            FsInfo.Path warmPath = getSinglePath(fsInfo);
            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity);
            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedTotal, warmPath.free);     // no shards
            assertEquals(expectedTotal, warmPath.available);
        }
    }

    public void testStatsWithNonPrimaryShards() throws Exception {
        double fileCacheRatio  = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        IndexShard primaryShard = mockShard(true,  true,  50L * 1024 * 1024);
        IndexShard replicaShard = mockShard(false, true,  30L * 1024 * 1024);
        IndexService indexService = mockIndexService(primaryShard, replicaShard);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity);
            long expectedUsed  = 50L * 1024 * 1024; // only primary
            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedTotal - expectedUsed, warmPath.free);

            verify(primaryShard.store()).stats(anyLong());
            verify(replicaShard.store(), never()).stats(anyLong());
        }
    }

    public void testStatsWithInactiveShards() throws Exception {
        double fileCacheRatio  = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        IndexShard activeShard   = mockShard(true, true,  50L * 1024 * 1024);
        IndexShard inactiveShard = mockShard(true, false, 30L * 1024 * 1024);
        IndexService indexService = mockIndexService(activeShard, inactiveShard);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity);
            long expectedUsed  = 50L * 1024 * 1024; // only active
            assertEquals(expectedTotal - expectedUsed, warmPath.free);

            verify(activeShard.store()).stats(anyLong());
            verify(inactiveShard.store(), never()).stats(anyLong());
        }
    }

    public void testStatsWithNullRoutingEntry() throws Exception {
        double fileCacheRatio  = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        IndexShard shard = mock(IndexShard.class);
        when(shard.routingEntry()).thenReturn(null);
        IndexService indexService = mockIndexService(shard);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity);
            assertEquals(expectedTotal, warmPath.total);
            assertEquals(expectedTotal, warmPath.free); // shard skipped

            verify(shard, never()).store();
        }
    }

    public void testStatsWithExceptionWhileGettingShardSize() throws Exception {
        double fileCacheRatio  = 5.0;
        long fileCacheCapacity = 100L * 1024 * 1024;
        long shard1Size        = 50L  * 1024 * 1024;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        IndexShard normalShard = mockShard(true, true, shard1Size);
        IndexShard errorShard  = mockShardWithError(true, true);
        IndexService indexService = mockIndexService(normalShard, errorShard);
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity);
            long expectedFree  = expectedTotal - shard1Size; // error shard skipped
            assertEquals(expectedFree, warmPath.free);
        }
    }

    public void testStatsWithUsedBytesExceedingTotalClampsToZero() throws Exception {
        double fileCacheRatio  = 1.0; // 1:1 ratio, small virtual space
        long fileCacheCapacity = 10L * 1024 * 1024;  // 10 MB virtual
        long shardSize         = 20L * 1024 * 1024;  // 20 MB used — exceeds total

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        IndexService indexService = mockIndexService(mockShard(true, true, shardSize));
        when(indicesService.iterator()).thenReturn(Collections.singletonList(indexService).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            assertEquals(0L, warmPath.free);     // Math.max(0, negative) = 0
            assertEquals(0L, warmPath.available);
        }
    }

    public void testStatsWithMultipleIndices() throws Exception {
        double fileCacheRatio  = 5.0;
        long fileCacheCapacity = 200L * 1024 * 1024;

        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(fileCacheRatio);
        when(fileCache.capacity()).thenReturn(fileCacheCapacity);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        IndexService idx1 = mockIndexService(
            mockShard(true, true, 50L * 1024 * 1024),
            mockShard(true, true, 30L * 1024 * 1024));
        IndexService idx2 = mockIndexService(
            mockShard(true, true, 20L * 1024 * 1024),
            mockShard(true, true, 10L * 1024 * 1024));
        when(indicesService.iterator()).thenReturn(List.of(idx1, idx2).iterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            FsInfo fsInfo = svc.stats();
            FsInfo.Path warmPath = getSinglePath(fsInfo);

            long expectedTotal = (long)(fileCacheRatio * fileCacheCapacity);
            long expectedUsed  = (50 + 30 + 20 + 10) * 1024 * 1024L;
            assertEquals(expectedTotal - expectedUsed, warmPath.free);
        }
    }

    public void testStatsReturnsExactlyOnePath() throws IOException {
        when(fileCacheSettings.getRemoteDataRatio()).thenReturn(1.0);
        when(fileCache.capacity()).thenReturn(1L);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);
        when(indicesService.iterator()).thenReturn(Collections.emptyIterator());

        try (var nodeEnv = newNodeEnvironment()) {
            WarmFsService svc = new WarmFsService(settings, nodeEnv, fileCacheSettings, indicesService,
                orchestrator, 0L);
            List<FsInfo.Path> paths = new ArrayList<>();
            for (FsInfo.Path p : svc.stats()) {
                paths.add(p);
            }
            assertEquals(1, paths.size());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private FsInfo.Path getSinglePath(FsInfo fsInfo) {
        List<FsInfo.Path> paths = new ArrayList<>();
        for (FsInfo.Path p : fsInfo) paths.add(p);
        assertEquals("expected exactly one path", 1, paths.size());
        return paths.get(0);
    }

    @SafeVarargs
    private IndexService mockIndexService(IndexShard... shards) {
        IndexService svc = mock(IndexService.class);
        when(svc.iterator()).thenReturn(List.of(shards).iterator());
        return svc;
    }

    private IndexService mockIndexService(long... shardSizes) throws Exception {
        List<IndexShard> shards = new ArrayList<>();
        for (long size : shardSizes) shards.add(mockShard(true, true, size));
        IndexService svc = mock(IndexService.class);
        when(svc.iterator()).thenReturn(shards.iterator());
        return svc;
    }

    private IndexShard mockShard(boolean isPrimary, boolean isActive, long sizeBytes) throws Exception {
        IndexShard shard = mock(IndexShard.class);
        ShardRouting routing = TestShardRouting.newShardRouting(
            new ShardId("test", "_na_", 0), "node1", isPrimary,
            isActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.shardId()).thenReturn(routing.shardId());

        Store store      = mock(Store.class);
        StoreStats stats = mock(StoreStats.class);
        when(stats.getSizeInBytes()).thenReturn(sizeBytes);
        when(store.stats(anyLong())).thenReturn(stats);
        when(shard.store()).thenReturn(store);
        return shard;
    }

    private IndexShard mockShardWithError(boolean isPrimary, boolean isActive) throws Exception {
        IndexShard shard = mock(IndexShard.class);
        ShardRouting routing = TestShardRouting.newShardRouting(
            new ShardId("test", "_na_", 1), "node1", isPrimary,
            isActive ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING
        );
        when(shard.routingEntry()).thenReturn(routing);
        when(shard.shardId()).thenReturn(routing.shardId());

        Store store = mock(Store.class);
        when(store.stats(anyLong())).thenThrow(new RuntimeException("Test exception"));
        when(shard.store()).thenReturn(store);
        return shard;
    }
}
