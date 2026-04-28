/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.UnifiedCacheService;
import org.opensearch.indices.IndicesService;

/**
 * Factory for creating appropriate FsService implementations based on node type.
 *
 * <p>On warm nodes, creates a {@link WarmFsService} backed by the full
 * {@link UnifiedCacheService} so that both the Lucene block-file cache
 * ({@code FileCache}) and the optional Foyer columnar block cache contribute
 * to disk watermark calculations.
 *
 * <p>On non-warm nodes, creates a standard {@link FsService} backed by
 * {@code UnifiedCacheService.fileCache()} (the raw {@code FileCache}).
 *
 * @opensearch.internal
 */
public class FsServiceProvider {

    private final Settings settings;
    private final NodeEnvironment nodeEnvironment;
    private final UnifiedCacheService unifiedCacheService;
    private final FileCacheSettings fileCacheSettings;
    private final IndicesService indicesService;

    public FsServiceProvider(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        UnifiedCacheService unifiedCacheService,
        ClusterSettings clusterSettings,
        IndicesService indicesService
    ) {
        this.settings = settings;
        this.nodeEnvironment = nodeEnvironment;
        this.unifiedCacheService = unifiedCacheService;
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
        this.indicesService = indicesService;
    }

    /**
     * Creates the appropriate FsService implementation based on node type.
     *
     * @return FsService instance
     */
    public FsService createFsService() {
        if (DiscoveryNode.isWarmNode(settings)) {
            return new WarmFsService(settings, nodeEnvironment, fileCacheSettings, indicesService, unifiedCacheService);
        }
        // Non-warm nodes use FsService directly — pass the raw FileCache extracted from UnifiedCacheService.
        return new FsService(settings, nodeEnvironment, unifiedCacheService.fileCache());
    }
}
