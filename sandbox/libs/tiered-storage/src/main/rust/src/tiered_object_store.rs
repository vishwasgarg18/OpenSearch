/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`TieredObjectStore`] — routes reads between local and remote stores
//! based on [`TieredStorageRegistry`] metadata.
//!
//! On every read, it checks the file registry:
//! - **Remote** → delegates to the store-level remote backend
//! - **Local / Both / not registered** → falls through to the local store
//!
//! # Thread Safety
//!
//! `TieredObjectStore` is `Send + Sync`. All mutable state lives in the
//! registry's atomics and DashMap — no locks are held during I/O.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{
    path::Path, CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OsResult,
};

use opensearch_block_cache::range_cache::range_cache_key;
use opensearch_block_cache::traits::BlockCache;

use crate::registry::traits::FileRegistry;
use crate::registry::TieredStorageRegistry;
use crate::types::{FileLocation, TieredFileEntry};

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// Cache chunk size used for chunk-aligned probe/put.
///
/// All cache writes go under chunk-aligned keys
/// (`path\x1F{chunk_start}-{chunk_end}`) where chunk boundaries are multiples of
/// this value (clamped to `file_size` for the trailing partial chunk). Callers
/// of [`TieredObjectStore::put_metadata`] should align their input ranges to
/// this size so every chunk they touch is fully covered and lands in the
/// metadata tier under the same key shape that [`TieredObjectStore::probe_cache`]
/// will look up at query time.
pub const CACHE_CHUNK_SIZE: u64 = 8 << 20; // 8 MiB

// ---------------------------------------------------------------------------
// MetadataCachingStore — extension trait
// ---------------------------------------------------------------------------

/// An [`ObjectStore`] that supports promoting reads into a sticky metadata tier
/// (the never-evict Foyer SSD instance in production).
///
/// The default implementation is a no-op so callers can invoke `put_metadata`
/// uniformly through any `Arc<dyn MetadataCachingStore>` without checking the
/// concrete type. [`TieredObjectStore`] overrides it to actually route the
/// bytes into the metadata cache; plain stores (e.g. `LocalFileSystem` in
/// tests) just inherit the no-op.
///
/// Used by warmup paths (e.g. `analytics-backend-datafusion`'s
/// `add_files_with_store`) that need to populate the metadata tier after
/// fetching bytes through the store.
pub trait MetadataCachingStore: ObjectStore {
    /// Promote `data` ranges to the never-evict metadata tier.
    ///
    /// Default: no-op.
    fn put_metadata(
        &self,
        _path: &str,
        _ranges: &[std::ops::Range<u64>],
        _data: &[Bytes],
    ) {}
}

// ---------------------------------------------------------------------------
// TieredObjectStore
// ---------------------------------------------------------------------------

/// ObjectStore implementation that routes reads between local and remote
/// stores based on [`TieredStorageRegistry`] metadata.
///
/// Per-shard model: one remote store is set once via [`set_remote()`] and
/// shared across all entries.
pub struct TieredObjectStore {
    registry: Arc<TieredStorageRegistry>,
    local: Arc<dyn ObjectStore>,
    remote: std::sync::OnceLock<Arc<dyn ObjectStore>>,
    /// Optional node-level block cache. `None` on hot nodes or when disabled.
    cache: Option<Arc<dyn BlockCache>>,
}

impl TieredObjectStore {
    /// Create a new tiered store routing between `local` and remote backends.
    #[must_use]
    pub fn new(registry: Arc<TieredStorageRegistry>, local: Arc<dyn ObjectStore>) -> Self {
        native_bridge_common::log_info!("TieredObjectStore: created");
        Self {
            registry,
            local,
            remote: std::sync::OnceLock::new(),
            cache: None,
        }
    }

    /// Reference to the underlying registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<TieredStorageRegistry> {
        &self.registry
    }

    /// Set the remote store (once). Subsequent calls are ignored.
    pub fn set_remote(&self, store: Arc<dyn ObjectStore>) {
        self.remote.set(store).ok(); // ignore if already set
    }

    /// Attach a block cache. Hot nodes skip this; `None` means no caching.
    #[must_use]
    pub fn with_cache(mut self, cache: Arc<dyn BlockCache>) -> Self {
        self.cache = Some(cache);
        self
    }


    /// Evict all cache entries whose key starts with `path`.
    ///
    /// No-op if no cache is attached (hot nodes or cache disabled).
    /// Called from `ts_remove_file` after a file is removed from the registry
    /// so that stale byte-range entries are freed promptly.
    pub fn evict_path(&self, path: &str) {
        if let Some(ref cache) = self.cache {
            cache.evict_prefix(path);
        }
    }

    /// Store byte ranges directly into the metadata cache (never-evict tier).
    ///
    /// Called by warmup after fetching metadata bytes. The warmup code reads
    /// metadata (via this store or any source), then calls this to ensure the
    /// bytes are in the durable metadata_cache — surviving LRU eviction from
    /// data scan pressure and node restarts.
    ///
    /// Each input range is split into chunk-aligned (8 MiB) pieces and only
    /// the chunks **fully contained** inside the range are written. This
    /// keeps the metadata-tier keys aligned with the chunk-aligned probes
    /// performed by [`Self::probe_cache`] so warmup-promoted bytes can
    /// actually serve future queries. Partial edge chunks (where the range
    /// covers only part of a chunk) are skipped — those chunks are typically
    /// already populated in the data tier as a side effect of the upstream
    /// `get_ranges` call that produced `data`.
    ///
    /// No-op if no cache is attached. Falls back to exact-key writes when
    /// `file_size` is unknown (registry has no size for this path).
    pub fn put_metadata(&self, path: &str, ranges: &[std::ops::Range<u64>], data: &[Bytes]) {
        // Normalize path by stripping leading '/' to match `object_store::Path` semantics
        // (read paths through ObjectStore::get_opts/get_ranges arrive with the leading '/'
        // already stripped). Without this, warmup writes under "/Volumes/..." while query
        // reads probe under "Volumes/..." — silent cache miss on every read.
        let path_str = path.strip_prefix('/').unwrap_or(path);
        let total_bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        native_bridge_common::log_debug!(
            "[init::tier-store] put_metadata path='{}' n_ranges={} total_bytes={} has_cache={}",
            path_str, ranges.len(), total_bytes, self.cache.is_some()
        );

        for (r, bytes) in ranges.iter().zip(data.iter()) {
            self.write_chunk_aligned(path_str, r, bytes, /* to_metadata_tier */ true);
        }
    }

    /// Register a file in the registry. For Remote/Both locations, the caller
    /// must provide a `remote_path`.
    pub fn register_file(
        &self,
        path: &str,
        location: FileLocation,
        remote_path: Option<String>,
    ) -> Result<(), crate::types::FileRegistryError> {
        if matches!(location, FileLocation::Remote) && remote_path.is_none() {
            return Err(crate::types::FileRegistryError::InvalidRegistration {
                path: path.to_string(),
                reason: format!("remote_path required for location={}", location),
            });
        }

        let entry = TieredFileEntry::new(location, remote_path.map(Arc::from));
        self.registry.register(path, entry);

        native_bridge_common::log_debug!(
            "TieredObjectStore: register_file path='{}', location={}",
            path,
            location
        );
        Ok(())
    }

    /// Transition a file's location and metadata via `registry.update()`.
    pub fn transition(
        &self,
        path: &str,
        location: FileLocation,
        remote_path: Option<String>,
    ) -> Result<(), crate::types::FileRegistryError> {
        if matches!(location, FileLocation::Remote) && remote_path.is_none() {
            return Err(crate::types::FileRegistryError::InvalidRegistration {
                path: path.to_string(),
                reason: format!("remote_path required for location={}", location),
            });
        }

        let remote_arc: Option<Arc<str>> = remote_path.map(Arc::from);

        self.registry.update(path, move |e| {
            e.location = location;
            e.remote_path = remote_arc;
        });

        native_bridge_common::log_debug!(
            "TieredObjectStore: transition path='{}', location={}",
            path,
            location
        );
        Ok(())
    }

    /// Resolve a GetRange to absolute (start, end) byte offsets.
    ///
    /// - `Bounded(start..end)`: returned directly.
    /// - `Suffix(n)`: resolved to `(file_size - n, file_size)` using registry.
    /// - `Offset(n)`: resolved to `(n, file_size)` using registry.
    ///
    /// Returns `None` if file_size is needed but not available in the registry.
    ///
    /// Note: `GetRange::Offset` is defensive/forward-compatible code. DataFusion's
    /// current parquet reading pipeline only produces `Bounded` (column data) and
    /// `Suffix` (footer metadata). The `Offset` variant is never generated in
    /// practice but is handled for completeness.
    fn resolve_range(&self, path_str: &str, range: &GetRange) -> Option<(u64, u64)> {
        match range {
            GetRange::Bounded(r) => Some((r.start, r.end)),
            GetRange::Suffix(n) => {
                let file_size = self.registry.get(path_str).map(|g| g.size()).filter(|&s| s > 0);
                match file_size {
                    Some(size) => Some((size.saturating_sub(*n), size)),
                    None => {
                        native_bridge_common::log_debug!(
                            "TieredObjectStore: resolve_range Suffix({}) — file_size unavailable for '{}', cache bypassed",
                            n, path_str
                        );
                        None
                    }
                }
            }
            GetRange::Offset(o) => {
                let file_size = self.registry.get(path_str).map(|g| g.size()).filter(|&s| s > 0);
                match file_size {
                    Some(size) => Some((*o, size)),
                    None => {
                        native_bridge_common::log_debug!(
                            "TieredObjectStore: resolve_range Offset({}) — file_size unavailable for '{}', cache bypassed",
                            o, path_str
                        );
                        None
                    }
                }
            }
        }
    }

    // NOTE: The guard is intentionally dropped before I/O. The Arc<dyn ObjectStore>
    // keeps the store alive independently. On writable warm, the guard must be held
    // during I/O to prevent eviction race — resolve_remote should return the guard
    // alongside the resolved path/store to pin the entry for the I/O duration.
    fn resolve_remote(&self, path: &str) -> Option<(Path, Arc<dyn ObjectStore>)> {
        let guard = self.registry.get(path)?;
        if guard.location() != FileLocation::Remote {
            return None;
        }
        let remote_path = guard.remote_path()?;
        let store = Arc::clone(self.remote.get()?); // use store-level remote
        let rp = Path::from(remote_path);
        drop(guard); // release before I/O — Arc keeps store alive
        Some((rp, store))
    }

    /// Checks if a local read error is NotFound and the file has since transitioned
    /// to REMOTE in the registry (e.g., afterSyncToRemote deleted the local copy).
    /// Returns the remote path + store if retry is possible, None otherwise.
    fn should_retry_remote(&self, path_str: &str, err: &object_store::Error) -> Option<(Path, Arc<dyn ObjectStore>)> {
        if matches!(err, object_store::Error::NotFound { .. }) {
            let resolved = self.resolve_remote(path_str);
            if resolved.is_some() {
                native_bridge_common::log_info!(
                    "TieredObjectStore: LOCAL NotFound, file transitioned to REMOTE — retrying path='{}'",
                    path_str
                );
            }
            resolved
        } else {
            None
        }
    }

    /// Fast-path head response from registry or directory existence check.
    /// Returns `Some(GetResult)` if the head can be answered without I/O,
    /// `None` if the caller should fall through to the normal get_opts path.
    fn try_head_from_registry(&self, location: &Path, path_str: &str) -> Option<OsResult<GetResult>> {
        // Check registry for cached file size
        if let Some(guard) = self.registry.get(path_str) {
            let size = guard.size();
            if size > 0 {
                let meta = ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::DateTime::<chrono::Utc>::default(),
                    size,
                    e_tag: None,
                    version: None,
                };
                return Some(Ok(GetResult {
                    payload: object_store::GetResultPayload::Stream(
                        futures::stream::empty().boxed(),
                    ),
                    meta,
                    range: 0..size,
                    attributes: Default::default(),
                }));
            }
        }
        // Directory existence check: if the path is a prefix of any registered file,
        // it's a directory. Return NotFound so DataFusion treats it as a
        // directory and proceeds to list() (which returns all registry files).
        // This matches LocalFileSystem behavior: head() on a directory returns NotFound.
        let prefix_with_slash = if path_str.ends_with('/') {
            path_str.to_string()
        } else {
            format!("{}/", path_str)
        };
        let matches = self.registry.entries_matching(&prefix_with_slash);
        if !matches.is_empty() {
            native_bridge_common::log_info!(
                "TieredObjectStore: try_head_from_registry — path='{}' is a directory ({} files), returning NotFound",
                path_str, matches.len()
            );
            return Some(Err(object_store::Error::NotFound {
                path: path_str.to_string(),
                source: format!("path is a directory with {} files", matches.len()).into(),
            }));
        }

        None
    }

    /// Write bytes covering `range` into the block cache under chunk-aligned keys.
    ///
    /// For every chunk fully contained inside `range`, a slice of `bytes` is written
    /// under that chunk's chunk-aligned key. Partial-edge chunks (where `range` does
    /// not fully cover the chunk) are skipped — the caller must hold the full chunk's
    /// bytes for any chunk to be written.
    ///
    /// When `to_metadata_tier` is true, writes go through `cache.put_metadata` (the
    /// never-evict tier); otherwise through `cache.put` (the LRU data tier).
    ///
    /// File-size-unknown fallback: writes a single exact-key entry covering `range`
    /// (preserves legacy behaviour).
    fn write_chunk_aligned(
        &self,
        path_str: &str,
        range: &Range<u64>,
        bytes: &Bytes,
        to_metadata_tier: bool,
    ) {
        let Some(cache) = self.cache.as_ref() else { return; };

        let put = |key: &opensearch_block_cache::range_cache::CacheKey, b: Bytes| {
            if to_metadata_tier {
                cache.put_metadata(key, b);
            } else {
                cache.put(key, b);
            }
        };

        let file_size = self.registry.get(path_str)
            .map(|g| g.size())
            .filter(|&s| s > 0)
            .unwrap_or(0);

        if file_size == 0 {
            // Legacy: no chunk alignment, exact-key write.
            let key = range_cache_key(path_str, range.start, range.end);
            put(&key, bytes.clone());
            return;
        }

        // Walk every chunk this range touches; write only fully-covered chunks.
        let mut chunk_start = range.start / CACHE_CHUNK_SIZE * CACHE_CHUNK_SIZE;
        while chunk_start < range.end {
            let chunk_end = (chunk_start + CACHE_CHUNK_SIZE).min(file_size);
            if chunk_end <= chunk_start {
                // No progress possible — `range.end` extends past `file_size` and we've
                // already walked past EOF. Defensive bail to avoid an infinite loop.
                break;
            }
            if chunk_start >= range.start && chunk_end <= range.end {
                let off = (chunk_start - range.start) as usize;
                let len = (chunk_end - chunk_start) as usize;
                let key = range_cache_key(path_str, chunk_start, chunk_end);
                put(&key, bytes.slice(off..off + len));
            }
            chunk_start = chunk_end;
        }
    }

    /// Probe the cache for a single byte range with chunk-aligned slicing.
    ///
    /// Returns `Some(bytes)` on cache hit (zero-copy slice from the cached chunk),
    /// `None` on cache miss, cross-boundary range, or when no cache is attached.
    ///
    /// Used as the per-range building block for both [`Self::probe_cache`]
    /// (multi-range get_ranges) and [`Self::try_serve_from_cache`] (single-range
    /// get_opts) so both code paths use identical cache-key shape.
    ///
    /// File-size-unknown fallback: when the registry has no size for the path,
    /// falls back to an exact-key lookup. This preserves legacy behaviour for
    /// tests / paths where size isn't tracked.
    async fn probe_single_range(
        &self,
        path_str: &str,
        start: u64,
        end: u64,
    ) -> Option<Bytes> {
        let cache = self.cache.as_ref()?;

        let file_size = self.registry.get(path_str)
            .map(|g| g.size())
            .filter(|&s| s > 0)
            .unwrap_or(0);

        if file_size == 0 {
            // No file size — fall back to exact-key lookup (legacy).
            let key = range_cache_key(path_str, start, end);
            return cache.get(&key).await;
        }

        let chunk_start = start / CACHE_CHUNK_SIZE * CACHE_CHUNK_SIZE;
        let chunk_end = (chunk_start + CACHE_CHUNK_SIZE).min(file_size);

        if end > chunk_end {
            // Cross-boundary — caller decides how to handle (probe_cache emits a
            // chunk-aligned span as the miss range; try_serve_from_cache returns
            // None and lets the upstream fetch path run).
            return None;
        }

        let key = range_cache_key(path_str, chunk_start, chunk_end);
        let cached = cache.get(&key).await?;
        let off = (start - chunk_start) as usize;
        let len = (end - start) as usize;
        Some(cached.slice(off..off + len))
    }

    /// Try to serve a range read from the cache. Returns `Some(Ok(GetResult))` on hit,
    /// `None` on miss. Does NOT auto-populate the cache on miss.
    ///
    /// Used by `get_opts` to short-circuit range reads when the requested bytes were
    /// previously stored via `put_metadata()` during warmup.
    async fn try_serve_from_cache(
        &self,
        path_str: &str,
        location: &Path,
        range: &GetRange,
    ) -> Option<OsResult<GetResult>> {
        let (start, end) = self.resolve_range(path_str, range)?;
        let cached = self.probe_single_range(path_str, start, end).await?;
        let file_size = self.registry.get(path_str)
            .map(|g| g.size())
            .unwrap_or(end);
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: chrono::DateTime::<chrono::Utc>::default(),
            size: file_size,
            e_tag: None,
            version: None,
        };
        Some(Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(
                futures::stream::once(async { Ok(cached) }).boxed(),
            ),
            meta,
            range: start..end,
            attributes: Default::default(),
        }))
    }

    /// Phase 1 — probe the block cache for each requested range.
    ///
    /// Per-range cache lookup is delegated to [`Self::probe_single_range`] so the
    /// chunk-aligned key shape is shared with [`Self::try_serve_from_cache`].
    ///
    /// Returns:
    /// - `slots`: one entry per input range — `Some(bytes)` for hits, `None` for misses
    /// - `miss_indices`: original indices of the ranges that missed
    /// - `miss_ranges`: ranges to fetch from the backing store. For ranges that fit
    ///   in a single chunk this is the chunk range; for cross-boundary ranges it is
    ///   the chunk-aligned span. Phase 3 splits multi-chunk spans into per-chunk
    ///   cache entries on populate.
    ///
    /// When no cache is attached all ranges are unconditionally treated as misses
    /// with their original (unaligned) bounds.
    async fn probe_cache(
        &self,
        path_str: &str,
        ranges: &[Range<u64>],
    ) -> (Vec<Option<Bytes>>, Vec<usize>, Vec<Range<u64>>) {
        let mut slots: Vec<Option<Bytes>> = Vec::with_capacity(ranges.len());
        let mut miss_indices: Vec<usize> = Vec::new();
        let mut miss_ranges: Vec<Range<u64>> = Vec::new();

        if self.cache.is_none() {
            // No cache — all ranges are misses with original (unaligned) bounds.
            for (i, r) in ranges.iter().enumerate() {
                slots.push(None);
                miss_indices.push(i);
                miss_ranges.push(r.clone());
            }
            return (slots, miss_indices, miss_ranges);
        }

        let file_size = self.registry.get(path_str)
            .map(|g| g.size())
            .filter(|&s| s > 0)
            .unwrap_or(0);

        for (i, r) in ranges.iter().enumerate() {
            if let Some(bytes) = self.probe_single_range(path_str, r.start, r.end).await {
                slots.push(Some(bytes));
                continue;
            }

            // Miss path: figure out what to fetch.
            slots.push(None);
            miss_indices.push(i);

            if file_size == 0 {
                // Legacy: no chunk alignment, push exact range.
                miss_ranges.push(r.clone());
                continue;
            }

            let chunk_start = r.start / CACHE_CHUNK_SIZE * CACHE_CHUNK_SIZE;
            let chunk_end = (chunk_start + CACHE_CHUNK_SIZE).min(file_size);

            let mr = if r.end > chunk_end {
                // Cross-boundary: emit chunk-aligned span (chunk_floor .. chunk_ceil
                // capped at file_size). Phase 3 will fetch this span as one upstream
                // GET and split it into per-chunk cache entries.
                let span_end = r.end.div_ceil(CACHE_CHUNK_SIZE)
                    .saturating_mul(CACHE_CHUNK_SIZE)
                    .min(file_size);
                chunk_start..span_end
            } else {
                // Single-chunk: the chunk range.
                chunk_start..chunk_end
            };

            if !miss_ranges.contains(&mr) {
                miss_ranges.push(mr);
            }
        }

        (slots, miss_indices, miss_ranges)
    }

    /// Phase 2 — fetch missing ranges from the backing store (remote or local).
    ///
    /// Tries the remote store first (registry lookup). Falls back to local,
    /// and retries remote if local returns `NotFound` and the file has since
    /// transitioned to `REMOTE` in the registry.
    async fn fetch_misses(
        &self,
        location: &Path,
        path_str: &str,
        miss_ranges: &[Range<u64>],
    ) -> OsResult<Vec<Bytes>> {
        let n = miss_ranges.len();
        if let Some((rp, store)) = self.resolve_remote(path_str) {
            native_bridge_common::log_debug!(
                "[query::tier-store] →REMOTE path='{}' n={} (file is REMOTE in registry)",
                path_str, n
            );
            return store.get_ranges(&rp, miss_ranges).await;
        }
        native_bridge_common::log_debug!(
            "[query::tier-store] →LOCAL path='{}' n={} (trying local first)",
            path_str, n
        );
        let result = self.local.get_ranges(location, miss_ranges).await;
        match result {
            Ok(bytes) => Ok(bytes),
            Err(ref e) => {
                if let Some((rp, store)) = self.should_retry_remote(path_str, e) {
                    native_bridge_common::log_debug!(
                        "[query::tier-store] →LOCAL_THEN_REMOTE path='{}' n={} (local NotFound, retrying remote)",
                        path_str, n
                    );
                    store.get_ranges(&rp, miss_ranges).await
                } else {
                    result
                }
            }
        }
    }

    /// Phase 3 — populate the block cache with fetched chunks and reassemble results.
    ///
    /// Each fetched miss-range is written via [`Self::write_chunk_aligned`], so:
    /// - Single-chunk miss-range: cached as one entry under its chunk key.
    /// - Multi-chunk span: split into per-chunk slices, each cached under its
    ///   chunk key. Future single-chunk reads of any of those chunks hit the cache.
    ///
    /// Slot reassembly uses containment (`mr.start <= r.start && r.end <= mr.end`)
    /// so both single-chunk slots and cross-boundary span slots resolve uniformly.
    fn populate_cache_and_reassemble(
        &self,
        path_str: &str,
        ranges: &[Range<u64>],
        fetched: &[Bytes],
        miss_indices: &[usize],
        miss_ranges: &[Range<u64>],
        slots: &mut Vec<Option<Bytes>>,
    ) {
        if self.cache.is_some() {
            // Cache each fetched miss-range under chunk-aligned keys (or exact key
            // when file_size is unknown). Multi-chunk spans are split per chunk.
            for (data, mr) in fetched.iter().zip(miss_ranges.iter()) {
                self.write_chunk_aligned(path_str, mr, data, /* to_metadata_tier */ false);
            }

            // Reassemble each missed slot via containment lookup.
            for &slot_i in miss_indices {
                let r = &ranges[slot_i];
                if let Some(pos) = miss_ranges
                    .iter()
                    .position(|mr| mr.start <= r.start && r.end <= mr.end)
                {
                    let off = (r.start - miss_ranges[pos].start) as usize;
                    let len = (r.end - r.start) as usize;
                    slots[slot_i] = Some(fetched[pos].slice(off..off + len));
                }
            }
        } else {
            for (fetched_bytes, &slot_i) in fetched.iter().zip(miss_indices.iter()) {
                slots[slot_i] = Some(fetched_bytes.clone());
            }
        }
    }
}

impl fmt::Debug for TieredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredObjectStore")
            .field("file_count", &self.registry.len())
            .field("cache", &self.cache.is_some())
            .finish()
    }
}

impl fmt::Display for TieredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TieredObjectStore(files={}, cache={})", self.registry.len(), self.cache.is_some())
    }
}

// ---------------------------------------------------------------------------
// MetadataCachingStore impl — delegates to the inherent put_metadata above.
// ---------------------------------------------------------------------------

impl MetadataCachingStore for TieredObjectStore {
    fn put_metadata(&self, path: &str, ranges: &[std::ops::Range<u64>], data: &[Bytes]) {
        TieredObjectStore::put_metadata(self, path, ranges, data);
    }
}

// ---------------------------------------------------------------------------
// ObjectStore impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectStore for TieredObjectStore {
    /// Write to local store and register the file as [`FileLocation::Local`].
    /// On writable warm, caller must pin the file to prevent eviction before
    /// sync completes.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let result = self.local.put_opts(location, payload, opts).await?;

        let path_str = location.as_ref();
        let entry = TieredFileEntry::new(FileLocation::Local, None);
        self.registry.register(path_str, entry);

        native_bridge_common::log_debug!(
            "TieredObjectStore: put_opts registered LOCAL path='{}'",
            path_str,
        );
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support put_multipart_opts".into(),
        })
    }

    /// Primary read path: check registry for remote routing, otherwise local.
    /// If local read fails with NotFound and file transitioned to REMOTE, retries from remote.
    ///
    /// Also handles head requests (options.head == true) by returning cached
    /// size from the registry when available — avoids I/O for the common case.
    /// For directory paths, returns NotFound so DataFusion uses list() instead.
    ///
    /// When a bounded/suffix/offset range is specified AND a cache is attached,
    /// probes the cache first. On hit (entry previously stored via `put_metadata`
    /// during warmup), returns cached bytes immediately — zero S3/local I/O.
    /// On miss, proceeds with normal fetch (no auto-populate — metadata cache is
    /// populated only by explicit `put_metadata()` calls from warmup code).
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let path_str = location.as_ref();

        // (1) Head fast path: answer from the registry/directory without I/O.
        // On Some, return verbatim (including NotFound for directory paths).
        // On None, fall through to the direct path with `options` unmodified.
        if options.head {
            if let Some(result) = self.try_head_from_registry(location, path_str) {
                return result;
            }
        }

        // (2) Ranged read routing.
        if let Some(ref get_range) = options.range {
            // (2a) Hot-hit fast path: single-chunk cache hit served directly,
            // avoiding the get_ranges probe/logging machinery. Returns None for
            // cross-boundary ranges and on any miss, in which case we proceed.
            if let Some(result) = self.try_serve_from_cache(path_str, location, get_range).await {
                return result;
            }

            // (2b) Resolvability: unresolvable Suffix/Offset (unknown file_size)
            // falls through to the direct remote/local path with no cache write.
            if let Some((start, end)) = self.resolve_range(path_str, get_range) {
                // (2c) Streaming guard: oversized ranges stream via the direct
                // path rather than buffering through get_ranges. TieredBlockCache
                // reports the per-entry ceiling; fall back to 32 MiB otherwise.
                let max_size = self
                    .cache
                    .as_ref()
                    .and_then(|c| {
                        c.as_any()
                            .downcast_ref::<opensearch_block_cache::tiered_block_cache::TieredBlockCache>()
                    })
                    .map(|t| t.max_data_entry_size())
                    .unwrap_or(32 * 1024 * 1024);

                if end - start <= max_size {
                    // (2d) Route through the shared chunk-aligned machinery so the
                    // enclosing chunk(s) are fetched and cached symmetrically with
                    // get_ranges. Exactly one range requested ⇒ one Bytes back.
                    let mut parts = self.get_ranges(location, &[start..end]).await?;
                    let bytes = parts.pop().unwrap_or_default();
                    let file_size = self
                        .registry
                        .get(path_str)
                        .map(|g| g.size())
                        .filter(|&s| s > 0)
                        .unwrap_or(end);
                    let meta = ObjectMeta {
                        location: location.clone(),
                        last_modified: chrono::DateTime::<chrono::Utc>::default(),
                        size: file_size,
                        e_tag: None,
                        version: None,
                    };
                    return Ok(GetResult {
                        payload: object_store::GetResultPayload::Stream(
                            futures::stream::once(async { Ok(bytes) }).boxed(),
                        ),
                        meta,
                        range: start..end,
                        attributes: Default::default(),
                    });
                }
                // oversized → fall through to the streaming direct path.
            }
            // unresolvable → fall through to the streaming direct path.
        }

        // (3) Existing direct remote/local fetch path (streaming). Serves head
        // fall-through, non-range reads, unresolvable ranges, and oversized
        // resolvable ranges. Preserves remote-first routing and the
        // local-NotFound→remote retry. No cache write happens on this path.
        let get_result = if let Some((rp, store)) = self.resolve_remote(path_str) {
            native_bridge_common::log_debug!(
                "TieredObjectStore: get_opts REMOTE path='{}'",
                path_str
            );
            store.get_opts(&rp, options.clone()).await
        } else {
            let local_result = self.local.get_opts(location, options.clone()).await;
            match local_result {
                Ok(r) => Ok(r),
                Err(ref e) => {
                    if let Some((rp, store)) = self.should_retry_remote(path_str, e) {
                        store.get_opts(&rp, options.clone()).await
                    } else {
                        local_result
                    }
                }
            }
        }?;

        Ok(get_result)
    }

    /// Multi-range read with cache-first routing.
    /// Probes cache per range, fetches only misses, populates cache on success.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let path_str = location.as_ref();
        let total_bytes: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        native_bridge_common::log_debug!(
            "[query::tier-store] get_ranges path='{}' n={} total_bytes={}",
            path_str, ranges.len(), total_bytes
        );

        let (mut slots, miss_indices, miss_ranges) = self.probe_cache(path_str, ranges).await;

        if miss_ranges.is_empty() {
            // Full cache hit — all ranges served from SSD.
            native_bridge_common::log_debug!(
                "[query::tier-store] FULL_FOYER_HIT path='{}' n={} total_bytes={}",
                path_str, ranges.len(), total_bytes
            );
            return Ok(slots.into_iter().map(|o| o.unwrap()).collect());
        }

        if self.cache.is_some() {
            native_bridge_common::log_debug!(
                "[query::tier-store] FOYER_MISS path='{}' misses={}/{} (will fetch)",
                path_str, miss_ranges.len(), ranges.len()
            );
        } else {
            native_bridge_common::log_debug!(
                "[query::tier-store] NO_CACHE path='{}' fetching={}/{}",
                path_str, miss_ranges.len(), ranges.len()
            );
        }

        let fetched = self.fetch_misses(location, path_str, &miss_ranges).await?;

        self.populate_cache_and_reassemble(
            path_str, ranges, &fetched, &miss_indices, &miss_ranges, &mut slots,
        );

        Ok(slots.into_iter().map(|o| o.unwrap()).collect())
    }

    /// Delete stream: remove each path from registry and evict cache entries.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        let registry = Arc::clone(&self.registry);
        let cache = self.cache.clone();
        let mapped = locations.map(move |result| {
            if let Ok(ref path) = result {
                let path_str = path.as_ref();
                registry.remove(path_str, true);
                if let Some(ref c) = cache {
                    c.evict_prefix(path_str);
                }
            }
            result
        });
        Box::pin(mapped)
    }

    /// List: local entries first, then remote-only entries from registry (deduplicated).
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let registry = Arc::clone(&self.registry);
        let local_stream = self.local.list(prefix);

        let remote_entries: Vec<OsResult<ObjectMeta>> = registry
            .entries_matching(&prefix_str)
            .into_iter()
            .filter(|(_, loc, _)| *loc == FileLocation::Remote)
            .map(|(path, _, size)| {
                Ok(ObjectMeta {
                    location: Path::from(path),
                    last_modified: chrono::DateTime::<chrono::Utc>::default(),
                    size,
                    e_tag: None,
                    version: None,
                })
            })
            .collect();

        let remote_stream = futures::stream::iter(remote_entries);
        Box::pin(local_stream.chain(remote_stream))
    }

    /// List with delimiter: local entries first, then merge remote-only entries (deduplicated).
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let mut result = self.local.list_with_delimiter(prefix).await?;

        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();

        let local_paths: std::collections::HashSet<String> = result
            .objects
            .iter()
            .map(|m| m.location.as_ref().to_string())
            .collect();

        for (path, location, size) in self.registry.entries_matching(&prefix_str) {
            if location == FileLocation::Remote && !local_paths.contains(&path) {
                result.objects.push(ObjectMeta {
                    location: Path::from(path),
                    last_modified: chrono::DateTime::<chrono::Utc>::default(),
                    size,
                    e_tag: None,
                    version: None,
                });
            }
        }

        Ok(result)
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> OsResult<()> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support copy".into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tiered_object_store_tests.rs"]
mod tests;
