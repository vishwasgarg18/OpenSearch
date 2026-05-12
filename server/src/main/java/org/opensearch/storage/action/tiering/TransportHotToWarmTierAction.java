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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Transport action for hot-to-warm index tiering.
 *
 * <p>For pluggable-dataformat indexes, performs a pre-migration force-upload step before
 * the standard tiering cluster state update:
 * <ol>
 *   <li>Applies {@link IndexMetadata#INDEX_READ_ONLY_ALLOW_DELETE_BLOCK} — prevents new
 *       writes from entering the engine after the point of flush.</li>
 *   <li>Validates all primary shards are in {@code STARTED} state.</li>
 *   <li>Executes {@link TransportWarmTierPreSyncAction} on all primary shards to force
 *       {@code flush → refresh → waitForRemoteStoreSync}.</li>
 *   <li>Delegates to the parent to call {@link HotToWarmTieringService#tier}.</li>
 * </ol>
 *
 * <p>Non-pluggable indexes skip steps 1–3 and delegate to the parent directly.
 */
public class TransportHotToWarmTierAction extends TransportTierAction {

    private static final Logger logger = LogManager.getLogger(TransportHotToWarmTierAction.class);

    private final Client client;
    private final HotToWarmTieringService hotToWarmTieringService;

    @Inject
    public TransportHotToWarmTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        HotToWarmTieringService hotToWarmTieringService,
        Client client
    ) {
        super(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            HotToWarmTierAction.NAME,
            hotToWarmTieringService
        );
        this.client = client;
        this.hotToWarmTieringService = hotToWarmTieringService;
    }

    @Override
    protected void clusterManagerOperation(
        IndexTieringRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {

        final IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("Index [" + request.getIndex() + "] not found"));
            return;
        }

        // Only pluggable-dataformat indexes need the pre-upload step.
        if (Mapper.isPluggableDataFormatEnabled(indexMetadata.getSettings()) == false) {
            logger.debug("Index [{}] is not pluggable-dataformat; skipping pre-tier sync", request.getIndex());
            super.clusterManagerOperation(request, state, listener);
            return;
        }

        logger.info("Index [{}] is pluggable-dataformat; applying write block and running pre-tier sync", request.getIndex());

        clusterService.submitStateUpdateTask(
            "pre-warm-tier-write-block[" + request.getIndex() + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    blocks.addIndexBlock(request.getIndex(), IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
                    return ClusterState.builder(currentState).blocks(blocks).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("Failed to apply write block for index [{}]", request.getIndex(), e);
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Write block applied for index [{}]; validating primaries and dispatching sync", request.getIndex());

                    final IndexMetadata newMeta = newState.metadata().index(request.getIndex());
                    if (newMeta == null) {
                        hotToWarmTieringService.removeWriteBlock(request.getIndex());
                        listener.onFailure(new IllegalStateException("Index [" + request.getIndex() + "] not found after block"));
                        return;
                    }

                    final List<ShardRouting> nonStarted = newState.routingTable()
                        .allShards(request.getIndex())
                        .stream()
                        .filter(s -> s.primary() && s.state() != ShardRoutingState.STARTED)
                        .collect(Collectors.toList());
                    if (nonStarted.isEmpty() == false) {
                        hotToWarmTieringService.removeWriteBlock(request.getIndex());
                        listener.onFailure(new IllegalStateException(
                            String.format(Locale.ROOT,
                                "Pre-tier sync rejected for index [%s]: primary shards %s are not in STARTED state",
                                request.getIndex(), nonStarted)
                        ));
                        return;
                    }

                    client.execute(
                        WarmTierPreSyncAction.INSTANCE,
                        new TransportWarmTierPreSyncAction.WarmTierPreSyncRequest(request.getIndex()),
                        ActionListener.wrap(
                            broadcastResponse -> {
                                if (broadcastResponse.getFailedShards() > 0) {
                                    hotToWarmTieringService.removeWriteBlock(request.getIndex());
                                    listener.onFailure(new RuntimeException(
                                        "Pre-tier sync failed for " + broadcastResponse.getFailedShards()
                                            + " shards of index [" + request.getIndex() + "]: "
                                            + broadcastResponse.getShardFailures()
                                    ));
                                    return;
                                }
                                logger.info("Pre-tier sync completed for index [{}]; proceeding with warm migration", request.getIndex());
                                hotToWarmTieringService.tier(
                                    request,
                                    new ActionListener<ClusterStateUpdateResponse>() {
                                        @Override
                                        public void onResponse(ClusterStateUpdateResponse r) {
                                            listener.onResponse(new AcknowledgedResponse(r.isAcknowledged()));
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            listener.onFailure(e);
                                        }
                                    },
                                    clusterService.state()
                                );
                            },
                            e -> {
                                logger.warn("Pre-tier sync failed for index [{}]", request.getIndex(), e);
                                hotToWarmTieringService.removeWriteBlock(request.getIndex());
                                listener.onFailure(e);
                            }
                        )
                    );
                }
            }
        );
    }
}
