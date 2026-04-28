/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.blockcache.BlockCacheHandle;
import org.opensearch.blockcache.foyer.FoyerBlockCache;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.fs.FsProbe;
import org.opensearch.node.Node;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Single authority for all warm-node SSD caches on this node.
 *
 * <p>Owns the lifecycle, disk budget, validation, and stats aggregation for:
 * <ul>
 *   <li>{@link FileCache} — Java LRU cache for fixed-size (~8 MB) Lucene block files.
 *       Always present on warm nodes.</li>
 *   <li>{@link BlockCacheHandle} (optional) — Rust-backed Foyer LRU cache for
 *       variable-size columnar byte ranges (Parquet column chunks). Present only when
 *       {@code format_cache.disk_bytes > 0} in {@code opensearch.yml}.</li>
 * </ul>
 *
 * <p>Problems solved:
 * <ol>
 *   <li><b>Silent disk over-allocation</b> — {@link #validate} fails fast before either
 *       cache is created if their combined size exceeds available SSD.</li>
 *   <li><b>Wrong disk watermark</b> — {@link #totalCacheSSDBytes()} returns the combined
 *       FileCache + block cache reservation, used by {@code WarmFsService} for correct
 *       cluster disk-pressure decisions.</li>
 *   <li><b>Scattered initialization</b> — a single {@link #create} call replaces
 *       {@code Node.initializeFileCache()} and the ad-hoc block cache init.</li>
 *   <li><b>Unified stats</b> — {@link #aggregateStats()} merges FileCache and block cache
 *       stats via {@link CacheStatsProvider}, producing the cross-cache rollup in
 *       {@code _nodes/stats aggregate_file_cache}.</li>
 *   <li><b>Undefined teardown order</b> — {@link #close()} always closes the block cache
 *       first (it has async I/O in flight) then lets FileCache be GC'd.</li>
 * </ol>
 *
 * <p>Only instantiated on warm nodes. {@code Node.java} creates the instance and passes
 * it to {@code NodeService} for stats exposure via {@code _nodes/stats}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class UnifiedCacheService implements Closeable {

    private static final Logger logger = LogManager.getLogger(UnifiedCacheService.class);

    /**
     * The Java-based LRU cache for Lucene index files (~8 MB fixed-size blocks
     * and small whole files). Always present on warm nodes.
     *
     * <p>Consumed directly by IndicesService (shard ops), FileCacheCleaner (shard deletion),
     * FsServiceProvider (disk watermark), and IndexModule (directory factory wiring).
     */
    private final FileCache fileCache;

    /**
     * The Rust-backed Foyer cache for variable-size columnar byte ranges
     * (Parquet column chunks, 8 KB–64 MB per entry).
     *
     * <p>Nullable — present only when {@code format_cache.disk_bytes > 0} in
     * {@code opensearch.yml}. When {@code null}, Foyer is disabled and all SSD
     * goes to FileCache.
     *
     * <p>Consumed by DataFusionService (attaches native pointer for DataFusion
     * columnar reads) and by {@link #aggregateStats()} for cross-cache rollup.
     * Implements {@link CacheStatsProvider} — stats are read via
     * {@link BlockCacheHandle#cacheStats()} which performs an FFM call to Rust atomics.
     */
    @Nullable
    private final BlockCacheHandle blockCacheHandle;

    // Private constructor — use create().
    private UnifiedCacheService(FileCache fileCache, @Nullable BlockCacheHandle blockCacheHandle) {
        this.fileCache = fileCache;
        this.blockCacheHandle = blockCacheHandle;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Static factory
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a {@code UnifiedCacheService} for a warm node.
     *
     * <p>This is the single place where:
     * <ol>
     *   <li>Total SSD capacity is read from {@code fileCacheNodePath}.</li>
     *   <li>Disk budget is split between FileCache and the block cache.</li>
     *   <li>The combined allocation is validated against available SSD —
     *       fails fast before creating either cache.</li>
     *   <li>FileCache is created and surviving files are restored from disk.</li>
     *   <li>FoyerBlockCache is created if {@code format_cache.disk_bytes > 0}.</li>
     * </ol>
     *
     * <p>Must only be called when {@code DiscoveryNode.isWarmNode(settings) == true}.
     *
     * @param settings        node settings
     * @param nodeEnvironment node environment (provides fileCacheNodePath)
     * @return a fully initialised {@code UnifiedCacheService}
     * @throws IllegalArgumentException if configured capacities exceed available SSD
     * @throws IOException              if FileCache restoration from disk fails
     */
    public static UnifiedCacheService create(Settings settings, NodeEnvironment nodeEnvironment) throws IOException {
        // Step 1: Read total SSD capacity from the fileCacheNodePath.
        NodeEnvironment.NodePath fileCacheNodePath = nodeEnvironment.fileCacheNodePath();
        long totalSSDBytes = ExceptionsHelper.catchAsRuntimeException(() -> FsProbe.getTotalSize(fileCacheNodePath));

        // Step 2: Compute disk split.
        // Block cache gets format_cache.disk_bytes (absolute, default 0 = disabled).
        // FileCache gets the remainder, subject to the node.search.cache.size setting.
        long foyerBytes = BlockCacheSettings.DISK_BYTES_SETTING.get(settings).getBytes();
        String fileCacheCapacityRaw = Node.NODE_SEARCH_CACHE_SIZE_SETTING.get(settings);
        long fileCacheBytes = calculateFileCacheSize(fileCacheCapacityRaw, totalSSDBytes - foyerBytes);

        // Step 3: Validate before creating anything (fails fast, no partial state).
        validate(fileCacheBytes, foyerBytes, totalSSDBytes);

        // Step 4: Create FileCache
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(fileCacheBytes);
        fileCacheNodePath.fileCacheReservedSize = new ByteSizeValue(fileCacheBytes, ByteSizeUnit.BYTES);
        restoreFileCacheFromDisk(settings, fileCacheNodePath, fileCache);

        // Step 5: Create Foyer block cache (optional — only when configured).
        BlockCacheHandle blockCacheHandle = null;
        if (foyerBytes > 0) {
            String diskDir = fileCacheNodePath.fileCachePath.resolve("foyer").toString();
            long blockSizeBytes = BlockCacheSettings.BLOCK_SIZE_SETTING.get(settings).getBytes();
            String ioEngine = BlockCacheSettings.IO_ENGINE_SETTING.get(settings);
            blockCacheHandle = new BlockCacheHandle(new FoyerBlockCache(foyerBytes, diskDir, blockSizeBytes, ioEngine));
            logger.info("Foyer column cache created: {} at {}", new ByteSizeValue(foyerBytes), diskDir);
        }

        return new UnifiedCacheService(fileCache, blockCacheHandle);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Validation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Validates that configured cache sizes are consistent with available SSD capacity.
     *
     * <p>Called before either cache is created — no partial state to clean up on failure.
     * Replaces the current silent over-allocation bug where FileCache and the block cache
     * can together exceed available SSD with no error at startup.
     *
     * @throws IllegalArgumentException if the combined allocation exceeds available SSD,
     *                                  or if FileCache would be allocated zero bytes
     */
    static void validate(long fileCacheBytes, long foyerBytes, long totalSSDBytes) {
        if (fileCacheBytes + foyerBytes > totalSSDBytes) {
            throw new IllegalArgumentException(
                "Warm node SSD allocation exceeds available capacity. "
                    + "file_cache="
                    + new ByteSizeValue(fileCacheBytes)
                    + ", format_cache.disk_bytes="
                    + new ByteSizeValue(foyerBytes)
                    + ", total SSD="
                    + new ByteSizeValue(totalSSDBytes)
                    + ". Reduce format_cache.disk_bytes or increase SSD capacity."
            );
        }
        if (fileCacheBytes <= 0) {
            throw new IllegalArgumentException(
                "After allocating "
                    + new ByteSizeValue(foyerBytes)
                    + " to format_cache, no SSD remains for FileCache. "
                    + "Reduce format_cache.disk_bytes."
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns the {@link FileCache} backing Lucene block-file caching.
     *
     * <p>Used by IndicesService, FileCacheCleaner, FsServiceProvider, and the Guice
     * binding in Node.java.
     */
    public FileCache fileCache() {
        return fileCache;
    }

    /**
     * Returns the block cache handle, or {@code null} if not configured.
     *
     * <p>Used by DataFusionService to attach the native cache pointer.
     * Callers must null-check before use.
     */
    @Nullable
    public BlockCacheHandle blockCacheHandle() {
        return blockCacheHandle;
    }

    /**
     * Returns total SSD bytes reserved by <em>all</em> caches combined.
     *
     * <p>This is the key fix for {@code WarmFsService}:
     * <ul>
     *   <li><b>Before:</b> {@code WarmFsService} used {@code fileCache.capacity()} —
     *       the block cache was invisible, disk watermark calculations were wrong.</li>
     *   <li><b>After:</b> {@code WarmFsService} uses {@code totalCacheSSDBytes()} —
     *       all SSD allocations included; cluster routing and disk pressure alerts
     *       are correct.</li>
     * </ul>
     *
     * @return combined SSD bytes reserved by FileCache + block cache (if configured)
     */
    public long totalCacheSSDBytes() {
        long total = fileCache.capacity();
        if (blockCacheHandle != null && blockCacheHandle.getCache() instanceof FoyerBlockCache foyer) {
            total += foyer.diskCapacityBytes();
        }
        return total;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Stats aggregation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns aggregate stats across all caches for the {@code aggregate_file_cache}
     * section of {@code _nodes/stats}.
     *
     * <ul>
     *   <li>If no block cache is configured: returns {@link FileCache#fileCacheStats()}
     *       unchanged — fully backwards compatible.</li>
     *   <li>If a block cache IS configured: calls {@link BlockCacheHandle#cacheStats()}
     *       (which performs an FFM call to read Rust atomic counters) and merges the result
     *       via {@link AggregateFileCacheStats#merge(AggregateFileCacheStats, CacheStats)}:
     *       <ul>
     *         <li>Top-level fields + {@code over_all_stats} = FileCache + block cache combined</li>
     *         <li>{@code block_file_stats} = FileCache block stats + block cache stats</li>
     *         <li>{@code full_file_stats}, {@code pinned_file_stats} = FileCache only (unchanged)</li>
     *       </ul>
     *   </li>
     * </ul>
     *
     * @return the aggregate stats snapshot; never null
     */
    public AggregateFileCacheStats aggregateStats() {
        AggregateFileCacheStats fileCacheStats = fileCache.fileCacheStats();
        if (blockCacheHandle == null) {
            return fileCacheStats;
        }
        // blockCacheHandle implements CacheStatsProvider; cacheStats() performs the FFM call
        CacheStats blockStats = blockCacheHandle.cacheStats();
        return AggregateFileCacheStats.merge(fileCacheStats, blockStats);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Closes all caches in the correct order for graceful shutdown.
     *
     * <ol>
     *   <li>Block cache FIRST — it has async I/O in flight on the same SSD.
     *       {@link BlockCacheHandle#close()} calls {@link FoyerBlockCache#close()}
     *       which calls {@code FoyerBridge.destroyCache()}, waiting for all pending
     *       writes to complete before releasing file handles.</li>
     *   <li>FileCache has no {@code close()} — it is a pure in-memory structure;
     *       JVM GC handles cleanup. On-disk files are managed by FileCacheCleaner
     *       at shard deletion time, independently of this service.</li>
     * </ol>
     *
     * <p>Called by {@code Node.java} during graceful shutdown, after all plugins
     * have stopped.
     */
    @Override
    public void close() throws IOException {
        if (blockCacheHandle != null) {
            IOUtils.closeWhileHandlingException(Collections.singleton(blockCacheHandle));
            logger.info("Foyer column cache closed");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Restores FileCache entries from surviving files on disk.
     * Mirrors the ForkJoinPool logic currently in {@code Node.java.initializeFileCache()}.
     */
    private static void restoreFileCacheFromDisk(
        Settings settings,
        NodeEnvironment.NodePath fileCacheNodePath,
        FileCache fileCache
    ) throws IOException {
        ForkJoinPool loadFileCacheThreadpool = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            Node.CustomForkJoinWorkerThread::new,
            null,
            false
        );
        SetOnce<UncheckedIOException> exception = new SetOnce<>();
        ForkJoinTask<Void> fileCacheFilesLoadTask = loadFileCacheThreadpool.submit(
            new FileCache.LoadTask(fileCacheNodePath.fileCachePath, fileCache, exception)
        );
        if (org.opensearch.cluster.node.DiscoveryNode.isDedicatedWarmNode(settings)) {
            ForkJoinTask<Void> indicesFilesLoadTask = loadFileCacheThreadpool.submit(
                new FileCache.LoadTask(fileCacheNodePath.indicesPath, fileCache, exception)
            );
            indicesFilesLoadTask.join();
        }
        fileCacheFilesLoadTask.join();
        loadFileCacheThreadpool.shutdown();
        if (exception.get() != null) {
            logger.error("File cache initialization failed.", exception.get());
            throw new OpenSearchException(exception.get());
        }
    }

    /**
     * Parses the raw capacity string (percentage or absolute bytes) into a byte count,
     * applied against the available space after the block cache's allocation.
     */
    private static long calculateFileCacheSize(String capacityRaw, long availableSpace) {
        try {
            RatioValue ratioValue = RatioValue.parseRatioValue(capacityRaw);
            return Math.round(availableSpace * ratioValue.getAsRatio());
        } catch (OpenSearchParseException e) {
            try {
                return ByteSizeValue.parseBytesSizeValue(capacityRaw, Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey()).getBytes();
            } catch (OpenSearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
    }
}
