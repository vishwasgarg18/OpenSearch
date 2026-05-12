/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.storage.common.tiering.TieringUtils.FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.opensearch.storage.common.tiering.TieringUtils.H2W_MAX_CONCURRENT_TIERING_REQUESTS;
import static org.opensearch.storage.common.tiering.TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TransportHotToWarmTierAction}.
 *
 * <p>Tests the pluggable-dataformat pre-migration sequence:
 * write-block application, primary shard validation, pre-sync dispatch, and
 * failure cleanup paths.
 */
public class TransportHotToWarmTierActionTests extends OpenSearchTestCase {

    private TransportHotToWarmTierAction action;
    private ClusterService clusterService;
    private HotToWarmTieringService hotToWarmTieringService;
    private Client client;
    private ThreadPool threadPool;
    private NodeEnvironment nodeEnvironment;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TransportHotToWarmTierActionTests");
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        nodeEnvironment = newNodeEnvironment();

        Settings defaultSettings = Settings.builder()
            .put(H2W_MAX_CONCURRENT_TIERING_REQUESTS.getKey(), 50)
            .put(JVM_USAGE_TIERING_THRESHOLD_PERCENT.getKey(), 99)
            .put(FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT.getKey(), 90)
            .build();

        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(H2W_MAX_CONCURRENT_TIERING_REQUESTS);
        clusterSettingsToAdd.add(JVM_USAGE_TIERING_THRESHOLD_PERCENT);
        clusterSettingsToAdd.add(FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT);
        clusterSettingsToAdd.add(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(defaultSettings, clusterSettingsToAdd);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        hotToWarmTieringService = new HotToWarmTieringService(
            defaultSettings,
            clusterService,
            mock(ClusterInfoService.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            nodeEnvironment,
            mock(ShardLimitValidator.class)
        );

        action = new TransportHotToWarmTierAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            hotToWarmTieringService,
            client
        );
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        org.opensearch.common.util.io.IOUtils.close(nodeEnvironment);
        super.tearDown();
    }


    public void testNonPluggableIndexDoesNotApplyWriteBlock() throws Exception {
        // Standard Lucene-only index (no pluggable dataformat setting)
        String indexName = "lucene-index";
        ClusterState state = buildClusterStateWithIndex(indexName, false, 1);
        when(clusterService.state()).thenReturn(state);

        AtomicReference<Boolean> failureCalled = new AtomicReference<>(false);
        action.clusterManagerOperation(
            new IndexTieringRequest("warm", indexName),
            state,
            new ActionListener<AcknowledgedResponse>() {
                @Override public void onResponse(AcknowledgedResponse r) {}
                @Override public void onFailure(Exception e) { failureCalled.set(true); }
            }
        );

        // No write block submitted for non-pluggable indexes
        verify(clusterService, never()).submitStateUpdateTask(
            contains("pre-warm-tier-write-block"),
            any(ClusterStateUpdateTask.class)
        );
    }


    public void testIndexNotFoundFails() throws Exception {
        String indexName = "missing-index";
        ClusterState emptyState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().build())
            .build();
        when(clusterService.state()).thenReturn(emptyState);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        action.clusterManagerOperation(
            new IndexTieringRequest("warm", indexName),
            emptyState,
            new ActionListener<AcknowledgedResponse>() {
                @Override public void onResponse(AcknowledgedResponse r) { fail("Should fail"); }
                @Override public void onFailure(Exception e) { caughtFailure.set(e); }
            }
        );

        assertNotNull(caughtFailure.get());
        assertTrue(caughtFailure.get() instanceof IllegalArgumentException);
        assertTrue(caughtFailure.get().getMessage().contains(indexName));
    }


    public void testPluggableIndexAppliesWriteBlock() throws Exception {
        String indexName = "composite-index";
        ClusterState state = buildClusterStateWithIndex(indexName, true, 1);
        when(clusterService.state()).thenReturn(state);

        action.clusterManagerOperation(
            new IndexTieringRequest("warm", indexName),
            state,
            mock(ActionListener.class)
        );

        ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService).submitStateUpdateTask(
            contains("pre-warm-tier-write-block[" + indexName + "]"),
            taskCaptor.capture()
        );

        // Verify the task actually adds the block
        ClusterState result = taskCaptor.getValue().execute(state);
        assertTrue(result.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK));
    }


    public void testPrimaryNotStartedRemovesWriteBlockAndFails() throws Exception {
        String indexName = "composite-index";
        // Build state with INITIALIZING primary
        ClusterState state = buildClusterStateWithIndexInitializingPrimary(indexName);
        when(clusterService.state()).thenReturn(state);

        ArgumentCaptor<ClusterStateUpdateTask> blockTaskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        AtomicReference<Exception> caughtFailure = new AtomicReference<>();

        // Capture clusterStateProcessed callback
        doAnswer(invocation -> {
            String source = invocation.getArgument(0);
            ClusterStateUpdateTask task = invocation.getArgument(1);
            // Simulate: execute adds block, then clusterStateProcessed fires
            ClusterState newState = task.execute(state);
            task.clusterStateProcessed(source, state, newState);
            return null;
        }).when(clusterService).submitStateUpdateTask(
            contains("pre-warm-tier-write-block"),
            blockTaskCaptor.capture()
        );

        action.clusterManagerOperation(
            new IndexTieringRequest("warm", indexName),
            state,
            new ActionListener<AcknowledgedResponse>() {
                @Override public void onResponse(AcknowledgedResponse r) { fail("Should fail"); }
                @Override public void onFailure(Exception e) { caughtFailure.set(e); }
            }
        );

        assertNotNull(caughtFailure.get());
        assertTrue(caughtFailure.get().getMessage().contains("STARTED"));

        // Write block must be removed on failure
        verify(clusterService).submitStateUpdateTask(
            contains("recover-stuck-pre-warm-tier-write-block[" + indexName + "]"),
            any(ClusterStateUpdateTask.class)
        );
    }


    public void testPreSyncFailureRemovesWriteBlock() throws Exception {
        String indexName = "composite-index";
        ClusterState state = buildClusterStateWithIndex(indexName, true, 1);
        when(clusterService.state()).thenReturn(state);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();

        // Simulate: block task completes, then WarmTierPreSyncAction fails
        doAnswer(invocation -> {
            String source = invocation.getArgument(0);
            ClusterStateUpdateTask task = invocation.getArgument(1);
            ClusterState newState = task.execute(state);
            task.clusterStateProcessed(source, state, newState);
            return null;
        }).when(clusterService).submitStateUpdateTask(
            contains("pre-warm-tier-write-block"),
            any(ClusterStateUpdateTask.class)
        );

        // Simulate pre-sync failure from client.execute
        doAnswer(invocation -> {
            ActionListener<BroadcastResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("upload failed"));
            return null;
        }).when(client).execute(eq(WarmTierPreSyncAction.INSTANCE), any(), any());

        action.clusterManagerOperation(
            new IndexTieringRequest("warm", indexName),
            state,
            new ActionListener<AcknowledgedResponse>() {
                @Override public void onResponse(AcknowledgedResponse r) { fail("Should fail"); }
                @Override public void onFailure(Exception e) { caughtFailure.set(e); }
            }
        );

        assertNotNull(caughtFailure.get());
        assertEquals("upload failed", caughtFailure.get().getMessage());

        // Write block must be removed
        verify(clusterService).submitStateUpdateTask(
            contains("recover-stuck-pre-warm-tier-write-block[" + indexName + "]"),
            any(ClusterStateUpdateTask.class)
        );
    }


    public void testPreSyncWithFailedShardsRemovesWriteBlock() throws Exception {
        String indexName = "composite-index";
        ClusterState state = buildClusterStateWithIndex(indexName, true, 1);
        when(clusterService.state()).thenReturn(state);

        doAnswer(invocation -> {
            String source = invocation.getArgument(0);
            ClusterStateUpdateTask task = invocation.getArgument(1);
            ClusterState newState = task.execute(state);
            task.clusterStateProcessed(source, state, newState);
            return null;
        }).when(clusterService).submitStateUpdateTask(
            contains("pre-warm-tier-write-block"),
            any(ClusterStateUpdateTask.class)
        );

        // BroadcastResponse with 1 failed shard
        BroadcastResponse failedResponse = mock(BroadcastResponse.class);
        when(failedResponse.getFailedShards()).thenReturn(1);
        doAnswer(invocation -> {
            ActionListener<BroadcastResponse> listener = invocation.getArgument(2);
            listener.onResponse(failedResponse);
            return null;
        }).when(client).execute(eq(WarmTierPreSyncAction.INSTANCE), any(), any());

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        action.clusterManagerOperation(
            new IndexTieringRequest("warm", indexName),
            state,
            new ActionListener<AcknowledgedResponse>() {
                @Override public void onResponse(AcknowledgedResponse r) { fail("Should fail"); }
                @Override public void onFailure(Exception e) { caughtFailure.set(e); }
            }
        );

        assertNotNull(caughtFailure.get());
        verify(clusterService).submitStateUpdateTask(
            contains("recover-stuck-pre-warm-tier-write-block"),
            any(ClusterStateUpdateTask.class)
        );
    }


    /**
     * Builds a cluster state with one shard on "node1" in STARTED state.
     *
     * @param pluggableDataFormat if true adds index.pluggable.dataformat.enabled=true
     */
    private ClusterState buildClusterStateWithIndex(String indexName, boolean pluggableDataFormat, int numShards) {
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(INDEX_TIERING_STATE.getKey(), "HOT");
        if (pluggableDataFormat) {
            indexSettingsBuilder.put("index.pluggable.dataformat.enabled", true);
        }

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettingsBuilder)
            .numberOfShards(numShards)
            .numberOfReplicas(0)
            .build();

        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);
        IndexShardRoutingTable shardTable = new IndexShardRoutingTable.Builder(shardId)
            .addShard(TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED))
            .build();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addIndexShard(shardTable).build();

        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    /** Builds cluster state with a primary shard in INITIALIZING (not STARTED). */
    private ClusterState buildClusterStateWithIndexInitializingPrimary(String indexName) {
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(INDEX_TIERING_STATE.getKey(), "HOT")
            .put("index.pluggable.dataformat.enabled", true);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettingsBuilder)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Index index = indexMetadata.getIndex();
        ShardId shardId = new ShardId(index, 0);
        IndexShardRoutingTable shardTable = new IndexShardRoutingTable.Builder(shardId)
            .addShard(TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.INITIALIZING))
            .build();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addIndexShard(shardTable).build();

        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }
}
