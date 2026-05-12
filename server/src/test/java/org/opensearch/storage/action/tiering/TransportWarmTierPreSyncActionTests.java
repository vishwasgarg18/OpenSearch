/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportWarmTierPreSyncAction}.
 *
 * <p>Tests the per-shard operation (flush → refresh → waitForRemoteStoreSync) and
 * the block-bypass behaviour of {@code checkRequestBlock}.
 */
public class TransportWarmTierPreSyncActionTests extends OpenSearchTestCase {

    private TransportWarmTierPreSyncAction action;
    private IndicesService indicesService;
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("WarmTierPreSyncActionTests");
        indicesService = mock(IndicesService.class);

        action = new TransportWarmTierPreSyncAction(
            mock(ClusterService.class),
            mock(TransportService.class),
            indicesService,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }


    public void testShardOperationCallsFlushRefreshAndWaitInOrder() throws IOException {
        IndexShard shard = mockStartedPrimaryShard("test-index", 0);

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test-index", "_na_"), 0),
            "node1", true, ShardRoutingState.STARTED
        );

        action.shardOperation(
            new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest("test-index"),
            shardRouting
        );

        org.mockito.InOrder inOrder = inOrder(shard);
        inOrder.verify(shard).flush(any(FlushRequest.class));
        inOrder.verify(shard).refresh(eq("pre_warm_tier_remote_upload"));
        inOrder.verify(shard).waitForRemoteStoreSync();
    }

    public void testShardOperationUsesForceFlush() throws IOException {
        IndexShard shard = mockStartedPrimaryShard("test-index", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test-index", "_na_"), 0),
            "node1", true, ShardRoutingState.STARTED
        );

        action.shardOperation(
            new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest("test-index"),
            shardRouting
        );

        org.mockito.ArgumentCaptor<FlushRequest> captor = org.mockito.ArgumentCaptor.forClass(FlushRequest.class);
        verify(shard).flush(captor.capture());
        assertTrue(captor.getValue().force());
        assertTrue(captor.getValue().waitIfOngoing());
    }

    public void testShardOperationPropagatesWaitForSyncIOException() throws IOException {
        IndexShard shard = mockStartedPrimaryShard("test-index", 0);
        doThrow(new IOException("remote upload timeout")).when(shard).waitForRemoteStoreSync();

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test-index", "_na_"), 0),
            "node1", true, ShardRoutingState.STARTED
        );

        expectThrows(IOException.class, () ->
            action.shardOperation(
                new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest("test-index"),
                shardRouting
            )
        );
    }

    public void testShardOperationReturnsEmptyResult() throws IOException {
        mockStartedPrimaryShard("test-index", 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test-index", "_na_"), 0),
            "node1", true, ShardRoutingState.STARTED
        );

        org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction.EmptyResult result =
            action.shardOperation(
                new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest("test-index"),
                shardRouting
            );

        assertNotNull(result);
    }


    public void testCheckRequestBlockAllowsIndexWithWriteBlock() {
        // We applied INDEX_READ_ONLY_ALLOW_DELETE_BLOCK ourselves — the action must NOT reject itself
        String indexName = "test-index";
        ClusterBlocks blocks = ClusterBlocks.builder()
            .addIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)
            .build();
        ClusterState state = buildMinimalClusterState(indexName, blocks);

        ClusterBlockException ex = action.checkRequestBlock(
            state,
            new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest(indexName),
            new String[]{ indexName }
        );

        // Must be null: write-level block is intentionally skipped
        assertNull(ex);
    }

    public void testCheckRequestBlockRejectsMetadataWriteBlock() {
        // A genuine cluster-level metadata write block must still be enforced
        String indexName = "test-index";
        ClusterBlocks blocks = ClusterBlocks.builder()
            .addGlobalBlock(org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK)
            .build();
        // Build state — global block results in checkGlobalBlock raising the exception
        ClusterState state = buildMinimalClusterState(indexName, blocks);

        // checkGlobalBlock (not checkRequestBlock) catches global METADATA_WRITE blocks
        ClusterBlockException ex = action.checkGlobalBlock(
            state,
            new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest(indexName)
        );

        assertNotNull(ex);
    }


    private IndexShard mockStartedPrimaryShard(String indexName, int shardNum) throws IOException {
        Index index = new Index(indexName, "_na_");
        ShardId shardId = new ShardId(index, shardNum);

        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);

        IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(anyInt())).thenReturn(shard);
        when(indicesService.indexServiceSafe(eq(index))).thenReturn(indexService);

        return shard;
    }

    private ClusterState buildMinimalClusterState(String indexName, ClusterBlocks blocks) {
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(blocks)
            .build();
    }
}
