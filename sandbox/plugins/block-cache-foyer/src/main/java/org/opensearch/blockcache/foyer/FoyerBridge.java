/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.blockcache.stats.BlockCacheStats;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for the Foyer block cache lifecycle.
 *
 * <p>Exposes three operations: {@link #createCache}, {@link #destroyCache},
 * and {@link #snapshotStats}.
 *
 * <p>{@link #createCache} returns an opaque {@code long} handle representing a
 * {@code Box<Arc<dyn BlockCache>>} fat pointer. The handle is passed directly as
 * {@code cache_box_ptr} to {@code ts_create_tiered_object_store} — no additional
 * wrapping is needed.
 *
 * @opensearch.experimental
 */
public final class FoyerBridge {

    private static final Logger logger = LogManager.getLogger(FoyerBridge.class);

    private static final MethodHandle FOYER_CREATE_CACHE;
    private static final MethodHandle FOYER_DESTROY_CACHE;
    private static final MethodHandle FOYER_SNAPSHOT_STATS;
    private static final MethodHandle FOYER_EVICT_PREFIX;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        // i64 foyer_create_cache(u64 disk_bytes, *const u8 dir_ptr, u64 dir_len,
        //                        u64 block_size_bytes, *const u8 io_engine_ptr, u64 io_engine_len)
        // Returns Box<Arc<dyn BlockCache>> fat pointer.
        FOYER_CREATE_CACHE = linker.downcallHandle(
            lib.find("foyer_create_cache").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: opaque i64 fat pointer
                ValueLayout.JAVA_LONG,  // disk_bytes: u64
                ValueLayout.ADDRESS,    // dir_ptr: *const u8
                ValueLayout.JAVA_LONG,  // dir_len: u64
                ValueLayout.JAVA_LONG,  // block_size_bytes: u64
                ValueLayout.ADDRESS,    // io_engine_ptr: *const u8
                ValueLayout.JAVA_LONG   // io_engine_len: u64
            )
        );

        // i64 foyer_destroy_cache(i64 ptr) — 0=success, <0=error pointer
        FOYER_DESTROY_CACHE = linker.downcallHandle(
            lib.find("foyer_destroy_cache").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: 0=ok, <0=error
                ValueLayout.JAVA_LONG   // ptr
            )
        );

        // i64 foyer_snapshot_stats(i64 ptr, i64* out) — 0=success, <0=error
        FOYER_SNAPSHOT_STATS = linker.downcallHandle(
            lib.find("foyer_snapshot_stats").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: 0=ok, <0=error
                ValueLayout.JAVA_LONG,  // ptr: i64 cache handle
                ValueLayout.ADDRESS     // out: *mut i64, output buffer
            )
        );

        // i64 foyer_evict_prefix(i64 ptr, *const u8 prefix_ptr, u64 prefix_len) — 0=success, <0=error
        FOYER_EVICT_PREFIX = linker.downcallHandle(
            lib.find("foyer_evict_prefix").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: 0=ok, <0=error
                ValueLayout.JAVA_LONG,  // ptr: i64 cache handle
                ValueLayout.ADDRESS,    // prefix_ptr: *const u8
                ValueLayout.JAVA_LONG   // prefix_len: u64
            )
        );

        logger.info("FFM downcall handles resolved: foyer_create_cache, foyer_destroy_cache, foyer_snapshot_stats, foyer_evict_prefix");
    }

    /**
     * Create a Foyer block cache.
     *
     * <p>Returns a {@code Box<Arc<dyn BlockCache>>} fat pointer that can be passed
     * directly as {@code cache_box_ptr} to {@code ts_create_tiered_object_store}.
     *
     * @param diskBytes       maximum disk space the cache may use, in bytes
     * @param diskDir         path to the directory where Foyer stores cache data
     * @param blockSizeBytes  Foyer disk block size in bytes
     * @param ioEngine        I/O engine: {@code "auto"}, {@code "io_uring"}, or {@code "psync"}
     * @return an opaque fat pointer representing the cache instance; always positive on success
     * @throws RuntimeException if the native call fails or the directory is invalid
     */
    public static long createCache(long diskBytes, String diskDir, long blockSizeBytes, String ioEngine) {
        try (var call = new NativeCall()) {
            var dir = call.str(diskDir);
            var engine = call.str(ioEngine);
            long ptr = call.invoke(FOYER_CREATE_CACHE, diskBytes, dir.segment(), dir.len(), blockSizeBytes, engine.segment(), engine.len());
            if (ptr <= 0) {
                throw new IllegalStateException("foyer_create_cache returned an invalid handle");
            }
            logger.info(
                "Foyer block cache created: diskBytes={}, blockSizeBytes={}, ioEngine={}, dir={}",
                diskBytes,
                blockSizeBytes,
                ioEngine,
                diskDir
            );
            return ptr;
        }
    }

    /**
     * Destroy a cache previously created by {@link #createCache}.
     *
     * <p>After this call the handle is invalid and must not be used again.
     *
     * @param ptr the handle returned by {@link #createCache}
     * @throws RuntimeException if the native call returns an error (invalid ptr)
     */
    public static void destroyCache(long ptr) {
        try (var call = new NativeCall()) {
            call.invoke(FOYER_DESTROY_CACHE, ptr);
        }
        logger.info("Foyer block cache destroyed");
    }

    /**
     * Snapshot the cache statistics from the native Foyer runtime.
     *
     * <p>Returns a {@code long[]} containing two equal-sized sections:
     * {@code overall} (cross-tier rollup) followed by {@code block_level} (disk tier).
     * Each section contains one value per {@link BlockCacheStats.Field} constant,
     * in ordinal order.
     *
     * <p>The buffer size and per-section field layout are driven by
     * {@link BlockCacheStats.Field} — no separate size constants to maintain.
     */
    public static long[] snapshotStats(long ptr) {
        // Allocate exactly BlockCacheStats.Field.COUNT values per section, two sections total.
        // The native side must write the same number of values; a length mismatch means
        // the Java Field enum and the Rust snapshot() array are out of sync.
        final int bufferSize = BlockCacheStats.Field.COUNT * 2;
        try (Arena arena = Arena.ofConfined()) {
            var out = arena.allocateArray(ValueLayout.JAVA_LONG, bufferSize);
            try (var call = new NativeCall()) {
                call.invoke(FOYER_SNAPSHOT_STATS, ptr, out);
            }
            // toArray copies the entire allocated segment.
            long[] result = out.toArray(ValueLayout.JAVA_LONG);
            if (result.length != bufferSize) {
                // Native/Java field count mismatch — log and return zeros rather than silently
                // mismap fields, which would produce corrupted stats silently.
                logger.error(
                    "foyer_snapshot_stats: expected {} values but got {}; "
                    + "BlockCacheStats.Field and Rust snapshot() are out of sync",
                    bufferSize, result.length
                );
                return new long[bufferSize];
            }
            return result;
        } catch (Exception e) {
            logger.warn("foyer_snapshot_stats failed: {}", e.getMessage());
            return new long[bufferSize];
        }
    }

    /**
     * Evict all cache entries whose key starts with the given prefix.
     *
     * <p>Called by {@code FoyerBlockCache.evictPrefix} during shard/index deletion.
     * Best-effort: if the native call fails, the error is logged but not propagated.
     *
     * @param ptr    the cache handle returned by {@link #createCache}
     * @param prefix absolute path prefix (e.g. shard data path)
     */
    public static void evictPrefix(long ptr, String prefix) {
        try (var call = new NativeCall()) {
            var p = call.str(prefix);
            call.invoke(FOYER_EVICT_PREFIX, ptr, p.segment(), p.len());
        } catch (Exception e) {
            logger.warn("foyer_evict_prefix failed for prefix='{}': {}", prefix, e.getMessage());
        }
    }

    private FoyerBridge() {}
}
