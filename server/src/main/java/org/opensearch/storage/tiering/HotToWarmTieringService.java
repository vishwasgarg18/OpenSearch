/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdEvaluator;
import org.opensearch.cluster.routing.allocation.WarmNodeDiskThresholdEvaluator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.ShardLimitValidator;

import java.util.Set;

import static org.opensearch.index.IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.IndexModule.TieringState.HOT;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM;
import static org.opensearch.storage.common.tiering.TieringServiceValidator.validateHotToWarmTiering;
import static org.opensearch.storage.common.tiering.TieringUtils.FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.opensearch.storage.common.tiering.TieringUtils.H2W_MAX_CONCURRENT_TIERING_REQUESTS;
import static org.opensearch.storage.common.tiering.TieringUtils.H2W_TIERING_START_TIME_KEY;
import static org.opensearch.storage.common.tiering.TieringUtils.TIERED_COMPOSITE_INDEX_TYPE;

/**
 * Service responsible for tiering indices from the hot tier to the warm tier.
 *
 * <p>In addition to the standard tiering lifecycle inherited from {@link TieringService},
 * this service applies warm-specific index settings at migration start and recovers
 * any write blocks that were left orphaned by a cluster-manager failure during a
 * pre-migration segment upload.
 */
public class HotToWarmTieringService extends TieringService {

    private static final Logger logger = LogManager.getLogger(HotToWarmTieringService.class);

    /**
     * Disk Threshold Evaluator
     */
    private final DiskThresholdEvaluator diskThresholdEvaluator;

    /**
     * The threshold percentage for file cache active usage when tiering from hot to warm.
     */
    private Integer fileCacheActiveUsageThresholdPercent;

    /**
     * Constructs a new HotToWarmTieringService.
     * @param settings the settings
     * @param clusterService the cluster service
     * @param clusterInfoService the cluster info service
     * @param indexNameExpressionResolver the index name expression resolver
     * @param allocationService the allocation service
     * @param nodeEnvironment the node environment
     * @param shardLimitValidator the shard limit validator
     */
    @Inject
    public HotToWarmTieringService(
        final Settings settings,
        final ClusterService clusterService,
        final ClusterInfoService clusterInfoService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AllocationService allocationService,
        final NodeEnvironment nodeEnvironment,
        final ShardLimitValidator shardLimitValidator
    ) {
        super(
            settings,
            clusterService,
            clusterInfoService,
            indexNameExpressionResolver,
            allocationService,
            nodeEnvironment,
            shardLimitValidator
        );
        this.diskThresholdEvaluator = new WarmNodeDiskThresholdEvaluator(
            this.diskThresholdSettings,
            this.fileCacheSettings::getRemoteDataRatio
        );
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT, this::setFileCacheActiveUsageThreshold);
        setFileCacheActiveUsageThreshold(clusterSettings.get(FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT));
    }

    @Override
    protected void validateTieringRequest(
        ClusterState clusterState,
        ClusterInfoService clusterInfoService,
        Set<Index> tieringEntries,
        Integer maxConcurrentTieringRequests,
        Integer jvmActiveUsageThresholdPercent,
        Index index
    ) {
        validateHotToWarmTiering(
            clusterState,
            clusterInfoService.getClusterInfo(),
            tieringEntries,
            maxConcurrentTieringRequests,
            diskThresholdEvaluator,
            fileCacheActiveUsageThresholdPercent,
            jvmActiveUsageThresholdPercent,
            index,
            shardLimitValidator
        );
    }

    @Override
    protected Settings getTieringStartSettingsToAdd() {
        return Settings.builder()
            .put(IS_WARM_INDEX_SETTING.getKey(), true)
            .put(INDEX_TIERING_STATE.getKey(), HOT_TO_WARM)
            .put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), TIERED_COMPOSITE_INDEX_TYPE)
            // Warm indexes have a fixed replica count; prevent auto-expansion matching node count.
            .put(org.opensearch.cluster.metadata.AutoExpandReplicas.SETTING.getKey(), false)
            // Warm indexes are sealed after migration; periodic refresh serves no purpose.
            .put(org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            .build();
    }

    @Override
    protected Settings getIndexTierSettingsToRestoreAfterCancellation() {
        return Settings.builder()
            .put(IS_WARM_INDEX_SETTING.getKey(), false)
            .put(INDEX_TIERING_STATE.getKey(), HOT)
            .put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "default")
            .putNull(org.opensearch.cluster.metadata.AutoExpandReplicas.SETTING.getKey())
            .putNull(org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey())
            .build();
    }

    @Override
    protected Setting<Integer> getMaxConcurrentTieringRequestsSetting() {
        return H2W_MAX_CONCURRENT_TIERING_REQUESTS;
    }

    @Override
    protected String getTieringStartTimeKey() {
        return H2W_TIERING_START_TIME_KEY;
    }

    @Override
    protected IndexModule.TieringState getTargetTieringState() {
        return WARM;
    }

    @Override
    protected IndexModule.TieringState getTieringType() {
        return HOT_TO_WARM;
    }

    /**
     * Sets the file cache active usage threshold for tiering validations.
     *
     * @param fileCacheActiveUsageThreshold the threshold value to set
     */
    private void setFileCacheActiveUsageThreshold(Integer fileCacheActiveUsageThreshold) {
        this.fileCacheActiveUsageThresholdPercent = fileCacheActiveUsageThreshold;
    }

    /**
     * Extends the base cluster-change handler to recover write blocks orphaned by a
     * cluster-manager failure.
     *
     * <p>When the pre-migration shard sync
     * ({@link org.opensearch.storage.action.tiering.TransportWarmTierPreSyncAction})
     * is in flight, an index-level write block is held. If the cluster-manager dies before
     * calling {@link #tier}, the block persists in cluster state with the index still at
     * tiering state {@code HOT}. On the next master election this method detects and
     * removes such orphaned blocks so the index becomes writable again.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        super.clusterChanged(event);

        if (event.localNodeClusterManager()
            && event.previousState().nodes().isLocalNodeElectedClusterManager() == false) {
            recoverStuckPreTierWriteBlocks(event.state());
        }
    }

    /**
     * Scans all indices for orphaned write blocks: an index has
     * {@link IndexMetadata#INDEX_READ_ONLY_ALLOW_DELETE_BLOCK} applied but its
     * {@code INDEX_TIERING_STATE} is still {@link IndexModule.TieringState#HOT}.
     *
     * <p>This condition arises when the cluster-manager failed after applying the block
     * during pre-migration but before the tiering cluster state update ran. No migration
     * state was ever persisted, so the block is safe to remove unconditionally.
     */
    private void recoverStuckPreTierWriteBlocks(ClusterState state) {
        for (IndexMetadata indexMetadata : state.metadata().indices().values()) {
            final String indexName = indexMetadata.getIndex().getName();
            if (state.blocks().hasIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK)) {
                final String tieringStateStr = indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey());
                if (HOT.toString().equals(tieringStateStr)) {
                    logger.warn(
                        "Detected orphaned write block on index [{}] with tiering state HOT; removing.",
                        indexName
                    );
                    removeWriteBlock(indexName);
                }
            }
        }
    }

    /**
     * Submits an urgent cluster state update to remove a write block from the given index.
     * Used both for orphaned-block recovery on master election and for explicit cleanup on
     * pre-migration failure.
     *
     * <p>When called from failure cleanup the tiering state will still be {@code HOT}
     * so the re-check is a safe guard rather than a filter.
     */
    public void removeWriteBlock(String index) {
        clusterService.submitStateUpdateTask(
            "recover-stuck-pre-warm-tier-write-block[" + index + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final IndexMetadata meta = currentState.metadata().index(index);
                    if (meta == null) {
                        return currentState;
                    }
                    final String tieringState = meta.getSettings().get(INDEX_TIERING_STATE.getKey());
                    // Guard: tiering may have started on another node between detection and execution.
                    if (!HOT.toString().equals(tieringState)) {
                        return currentState;
                    }
                    final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    blocks.removeIndexBlockWithId(index, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK.id());
                    return ClusterState.builder(currentState).blocks(blocks).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("Failed to remove orphaned write block from index [{}]", index, e);
                }
            }
        );
    }
}
