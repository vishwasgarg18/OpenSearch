/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for pre-warm-tier shard sync.
 *
 * <p>Dispatched via {@link TransportBroadcastByNodeAction} to all <em>primary</em> shards of the
 * index being migrated. For each primary it:
 * <ol>
 *   <li>Calls {@code flush(force=true, waitIfOngoing=true)} — drains the translog and commits
 *       a new {@code segments_N} (with serialised {@code CatalogSnapshot}) to local disk.
 *       The flush also triggers async upload of segment data files to remote store.</li>
 *   <li>Calls {@code refresh("force_remote_upload_pre_warm_tier")} — immediately fires
 *       {@code RemoteStoreRefreshListener} which starts the async upload of {@code segments_N}
 *       to remote store ({@code isRefreshAfterCommitSafe()} = true).</li>
 *   <li>Calls {@link IndexShard#waitForRemoteStoreSync()} — blocks until remote store
 *       has the same {@code segments_N} generation as local disk.</li>
 * </ol>
 *
 * <p>The framework handles node routing, parallel dispatch, and response aggregation
 * automatically. {@link ClusterBlockLevel#METADATA_WRITE} blocks are checked but
 * {@code INDEX_READ_ONLY_ALLOW_DELETE_BLOCK} (applied by the caller before dispatching)
 * is intentionally bypassed — the shard operation runs directly against the engine.
 */
public class TransportWarmTierPreSyncAction extends TransportBroadcastByNodeAction<
    TransportWarmTierPreSyncAction.WarmTierPreSyncRequest,
    BroadcastResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private static final Logger logger = LogManager.getLogger(TransportWarmTierPreSyncAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportWarmTierPreSyncAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            WarmTierPreSyncAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            WarmTierPreSyncRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Required TransportBroadcastByNodeAction overrides
    // ──────────────────────────────────────────────────────────────────────────

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected BroadcastResponse newResponse(
        WarmTierPreSyncRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new BroadcastResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected WarmTierPreSyncRequest readRequestFrom(StreamInput in) throws IOException {
        return new WarmTierPreSyncRequest(in);
    }

    /**
     * The pre-sync runs on primary shards only — replicas are rebuilt from remote store
     * during warm migration and do not need to be synced independently.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, WarmTierPreSyncRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShardsSatisfyingPredicate(concreteIndices, ShardRouting::primary);
    }

    /**
     * Per-shard sync operation: flush → refresh → waitForRemoteStoreSync.
     *
     * <p>This method runs on the data node holding the primary shard, on the
     * {@code ThreadPool.Names.MANAGEMENT} executor. It is intentionally blocking
     * (for up to {@code RecoverySettings.INTERNAL_REMOTE_UPLOAD_TIMEOUT}) because
     * we must not proceed with warm migration until remote store is confirmed in sync.
     */
    @Override
    protected EmptyResult shardOperation(WarmTierPreSyncRequest request, ShardRouting shardRouting) throws IOException {
        final IndexShard shard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex())
            .getShard(shardRouting.shardId().id());

        logger.info(
            "Pre-warm-tier sync: flush + remote-store sync for primary shard [{}]",
            shardRouting.shardId()
        );

        // Step 1: Commit segments_N with CatalogSnapshot to local disk.
        //         Internally calls refresh("flush") which uploads segment data files to remote store.
        //         Also syncs and truncates the translog.
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));

        // Step 2: Immediately fire RemoteStoreRefreshListener to upload segments_N.
        //         After flush(), segments_N is on local disk but NOT yet in remote store.
        //         isRefreshAfterCommitSafe()=true starts async upload of segments_N.
        //         Without this, upload only happens at next scheduled index.refresh_interval.
        shard.refresh("force_remote_upload_pre_warm_tier");

        // Step 3: Block until remote store has the same segments_N generation as local disk.
        //         Throws IOException after RecoverySettings.INTERNAL_REMOTE_UPLOAD_TIMEOUT.
        shard.waitForRemoteStoreSync();

        logger.info(
            "Pre-warm-tier sync: completed for shard [{}]",
            shardRouting.shardId()
        );
        return EmptyResult.INSTANCE;
    }

    /**
     * Only the INDEX_READ_ONLY_ALLOW_DELETE_BLOCK we applied (block ID 8) and
     * METADATA_WRITE blocks are present during migration. We skip the write-level
     * block check here — the shard operation runs directly against the engine,
     * bypassing the transport-level write block that protects the index from
     * user-initiated writes.
     *
     * <p>Specifically: we applied INDEX_READ_ONLY_ALLOW_DELETE_BLOCK before dispatching
     * this action. That block would cause the default
     * {@code indicesBlockedException(WRITE)} check to reject our own flush.
     * We intentionally skip the WRITE-level block and only check METADATA_WRITE.
     */
    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, WarmTierPreSyncRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState state,
        WarmTierPreSyncRequest request,
        String[] concreteIndices
    ) {
        // Skip write-block check: we applied INDEX_READ_ONLY_ALLOW_DELETE_BLOCK ourselves
        // and the flush must be allowed to proceed despite it.
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Request class
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * Request for the pre-warm-tier shard sync broadcast action.
     * Carries only the index name(s) — no additional parameters needed.
     */
    public static class WarmTierPreSyncRequest extends BroadcastRequest<WarmTierPreSyncRequest> {

        public WarmTierPreSyncRequest(String... indices) {
            super(indices);
        }

        public WarmTierPreSyncRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
