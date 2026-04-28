/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.UnifiedCacheService;
import org.opensearch.indices.IndicesService;

import static org.opensearch.monitor.fs.FsProbe.adjustForHugeFilesystems;

/**
 * FileSystem service implementation for warm nodes that calculates disk usage
 * based on total cache SSD reservation (FileCache + block cache) and remote
 * data ratio instead of actual physical disk usage.
 *
 * <p>Uses {@link UnifiedCacheService} so that both the Lucene block-file cache
 * ({@code FileCache}) and the optional Foyer columnar block cache contribute to
 * the reported {@code cache_reserved_in_bytes} and virtual disk capacity.
 *
 * <p>Before this change, only {@code fileCache.capacity()} was used for
 * {@code nodeCacheSize}, so Foyer's SSD allocation was invisible to the
 * cluster's disk-watermark calculations, leading to incorrect routing
 * decisions on warm nodes with a configured block cache.
 *
 * @opensearch.internal
 */
public class WarmFsService extends FsService {

    private static final Logger logger = LogManager.getLogger(WarmFsService.class);

    /**
     * Hardcoded data-to-format-cache ratio used until a dedicated setting is added.
     * Mirrors {@code FileCacheSettings.getRemoteDataRatio()} semantics:
     * for every byte of block-cache SSD, the node can "serve" this many bytes
     * of remote columnar data.
     *
     * <p>TODO: promote to a configurable setting (e.g.
     * {@code format_cache.data_to_cache_ratio}, default 5.0) in a follow-up.
     */
    private static final double DEFAULT_FORMAT_CACHE_DATA_RATIO = 5.0;

    private final FileCacheSettings fileCacheSettings;
    private final IndicesService indicesService;
    private final UnifiedCacheService unifiedCacheService;

    public WarmFsService(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        FileCacheSettings fileCacheSettings,
        IndicesService indicesService,
        UnifiedCacheService unifiedCacheService
    ) {
        super(settings, nodeEnvironment, unifiedCacheService.fileCache());
        this.fileCacheSettings = fileCacheSettings;
        this.indicesService = indicesService;
        this.unifiedCacheService = unifiedCacheService;
    }

    @Override
    public FsInfo stats() {
        // ─── 1. Compute total virtual addressable space ───────────────────────
        //
        // Virtual capacity = (file-cache SSD × dataToFileCacheSizeRatio)
        //                  + (block-cache SSD × dataToFormatCacheSizeRatio)
        //
        // This accounts for both cache tiers. Before this change, Foyer's
        // block-cache capacity was invisible, causing totalBytes to be too low
        // and triggering spurious disk-watermark alerts on warm nodes.

        final double dataToFileCacheRatio   = fileCacheSettings.getRemoteDataRatio();
        final double dataToBlockCacheRatio  = DEFAULT_FORMAT_CACHE_DATA_RATIO;

        final long fileCacheCapacity  = unifiedCacheService.fileCache().capacity();
        final long totalCacheCapacity = unifiedCacheService.totalCacheSSDBytes();     // fileCache + blockCache
        final long blockCacheCapacity = totalCacheCapacity - fileCacheCapacity;       // blockCache only

        final long totalBytes = (long) (dataToFileCacheRatio  * fileCacheCapacity)
                              + (long) (dataToBlockCacheRatio * blockCacheCapacity);

        // ─── 2. Compute used bytes from primary shards ────────────────────────
        long usedBytes = 0;
        if (indicesService != null) {
            for (IndexService indexService : indicesService) {
                for (IndexShard shard : indexService) {
                    if (shard.routingEntry() != null && shard.routingEntry().primary() && shard.routingEntry().active()) {
                        try {
                            usedBytes += shard.store().stats(0).getSizeInBytes();
                        } catch (Exception e) {
                            logger.error("Unable to get store size for shard {} with error: {}", shard.shardId(), e.getMessage());
                        }
                    }
                }
            }
        }

        long freeBytes = Math.max(0, totalBytes - usedBytes);

        // ─── 3. Build the FsInfo.Path for this warm node ─────────────────────
        FsInfo.Path warmPath = new FsInfo.Path();
        warmPath.path      = "/warm";
        warmPath.mount     = "warm";
        warmPath.type      = "warm";
        warmPath.total     = adjustForHugeFilesystems(totalBytes);
        warmPath.free      = adjustForHugeFilesystems(freeBytes);
        warmPath.available = adjustForHugeFilesystems(freeBytes);

        // Aggregated cache reservation = fileCache + blockCache combined.
        // Aggregated cache utilization = fileCache.usage() for now;
        // block-cache usedBytes will be added once BlockCacheContributor is wired.
        // TODO: add blockCache.cacheStats().usedBytes() once BlockCacheContributor is available.
        warmPath.cacheReservedInBytes = adjustForHugeFilesystems(totalCacheCapacity);
        warmPath.cacheUtilized        = adjustForHugeFilesystems(unifiedCacheService.fileCache().usage());

        logger.trace(
            "Warm node disk usage — total: {}, used: {}, free: {}, cacheReserved: {}, cacheUtilized: {}",
            totalBytes, usedBytes, freeBytes, totalCacheCapacity, unifiedCacheService.fileCache().usage()
        );

        FsInfo nodeFsInfo = super.stats();
        return new FsInfo(System.currentTimeMillis(), nodeFsInfo.getIoStats(), new FsInfo.Path[] { warmPath });
    }
}
