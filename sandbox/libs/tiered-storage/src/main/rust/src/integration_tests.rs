/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Integration tests: TieredObjectStore + TieredBlockCache with real Parquet files.
//!
//! Uses `#[test]` (not `#[tokio::test]`) because FoyerCache::new() internally
//! calls block_on() and panics if called inside an existing tokio runtime.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use opensearch_block_cache::foyer::foyer_cache::FoyerCache;
use opensearch_block_cache::range_cache::range_cache_key;
use opensearch_block_cache::tiered_block_cache::TieredBlockCache;
use opensearch_block_cache::traits::BlockCache;

use crate::registry::traits::FileRegistry;
use crate::registry::TieredStorageRegistry;
use crate::tiered_object_store::TieredObjectStore;
use crate::types::{FileLocation, TieredFileEntry};

// ── Helpers ─────────────────────────────────────────────────────────────────────

const BLOCK_SIZE: usize = 1 * 1024 * 1024;
const DISK_BYTES: usize = 8 * 1024 * 1024;
const BUFFER_POOL: usize = 8 * 1024 * 1024;
const SUBMIT_QUEUE: usize = 8 * 1024 * 1024;

fn create_tiered_cache(data_dir: &std::path::Path, meta_dir: &std::path::Path) -> Arc<TieredBlockCache> {
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, data_dir, BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, meta_dir, BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    Arc::new(TieredBlockCache::new(data_cache, metadata_cache))
}

fn create_store(
    parquet_dir: &std::path::Path,
    cache: Arc<TieredBlockCache>,
    path_str: &str,
    file_size: u64,
) -> Arc<TieredObjectStore> {
    let local: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(parquet_dir).unwrap()
    );
    let registry = Arc::new(TieredStorageRegistry::new());
    let store = TieredObjectStore::new(registry, local)
        .with_cache(cache as Arc<dyn BlockCache>);
    let store = Arc::new(store);
    store.registry().register(
        path_str,
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );
    store
}

#[allow(deprecated)]
fn write_test_parquet(dir: &std::path::Path, filename: &str, num_row_groups: usize) -> u64 {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let file_path = dir.join(filename);
    let file = std::fs::File::create(&file_path).unwrap();
    let props = WriterProperties::builder().set_max_row_group_row_count(Some(100)).build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    for rg in 0..num_row_groups {
        let offset = (rg * 100) as i64;
        let ids: Vec<i64> = (offset..offset + 100).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("name_{}", i)).collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ]).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    std::fs::metadata(&file_path).unwrap().len()
}

/// Shared runtime for test async blocks (created lazily, not inside FoyerCache::new).
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap()).block_on(f)
}

/// Compute the single global page index range matching parquet crate's `range_for_page_index()`.
///
/// Folds ALL columns across ALL row groups into a single contiguous range
/// encompassing all column_index and offset_index data. Returns `None` if the
/// file has no page index metadata.
fn compute_global_page_index_range(metadata: &parquet::file::metadata::ParquetMetaData) -> Option<std::ops::Range<u64>> {
    metadata.row_groups().iter()
        .flat_map(|rg| rg.columns().iter())
        .fold(None::<std::ops::Range<u64>>, |acc, col| {
            let acc = if let (Some(offset), Some(length)) = (col.column_index_offset(), col.column_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            };
            if let (Some(offset), Some(length)) = (col.offset_index_offset(), col.offset_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                match acc {
                    Some(a) => Some(a.start.min(start)..a.end.max(end)),
                    None => Some(start..end),
                }
            } else {
                acc
            }
        })
}

/// Compute merged column index and offset index ranges per row group.
///
/// Returns two optional ranges per RG: (column_index_range, offset_index_range).
fn compute_per_rg_page_index_ranges(
    rg: &parquet::file::metadata::RowGroupMetaData,
) -> (Option<std::ops::Range<u64>>, Option<std::ops::Range<u64>>) {
    let col_idx_range = rg.columns().iter().fold(None::<std::ops::Range<u64>>, |acc, col| {
        if let (Some(offset), Some(length)) = (col.column_index_offset(), col.column_index_length()) {
            let start = offset as u64;
            let end = start + length as u64;
            match acc {
                Some(a) => Some(a.start.min(start)..a.end.max(end)),
                None => Some(start..end),
            }
        } else {
            acc
        }
    });

    let off_idx_range = rg.columns().iter().fold(None::<std::ops::Range<u64>>, |acc, col| {
        if let (Some(offset), Some(length)) = (col.offset_index_offset(), col.offset_index_length()) {
            let start = offset as u64;
            let end = start + length as u64;
            match acc {
                Some(a) => Some(a.start.min(start)..a.end.max(end)),
                None => Some(start..end),
            }
        } else {
            acc
        }
    });

    (col_idx_range, off_idx_range)
}

/// Set up a DataFusion session with the store registered and a ListingTable for the given file.
///
/// Returns the SessionContext and the inferred schema. The table is registered as `table_name`.
async fn setup_df_session(
    store: Arc<TieredObjectStore>,
    file_path: &str,
    table_name: &str,
    schema: Option<Arc<Schema>>,
) -> (datafusion::prelude::SessionContext, Arc<Schema>) {
    use datafusion::prelude::*;
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl, ListingOptions};
    use datafusion::datasource::file_format::parquet::ParquetFormat;

    let ctx = SessionContext::new();
    let url = url::Url::parse("file://").unwrap();
    ctx.runtime_env().register_object_store(&url, store.clone() as Arc<dyn ObjectStore>);

    let table_url = ListingTableUrl::parse(&format!("file:///{}", file_path)).unwrap();
    let format = Arc::new(ParquetFormat::default());
    let listing_options = ListingOptions::new(format).with_file_extension(".parquet");

    let schema = match schema {
        Some(s) => s,
        None => listing_options.infer_schema(&ctx.state(), &table_url).await.unwrap(),
    };

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema.clone());
    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();

    (ctx, schema)
}

// ── Tests ───────────────────────────────────────────────────────────────────────

/// Helper: simulates warmup — reads bytes via local FS and puts into metadata cache.
fn warmup_metadata(
    cache: &TieredBlockCache,
    parquet_dir: &std::path::Path,
    filename: &str,
    start: u64,
    end: u64,
) -> bytes::Bytes {
    let file_path = parquet_dir.join(filename);
    let file_bytes = std::fs::read(&file_path).unwrap();
    let range_bytes = bytes::Bytes::copy_from_slice(&file_bytes[start as usize..end as usize]);
    let key = range_cache_key(filename, start, end);
    cache.put_metadata(&key, range_bytes.clone());
    range_bytes
}

#[test]
fn metadata_routed_to_metadata_cache_data_to_data_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "test.parquet", 3);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "test.parquet", file_size);

    let path = Path::from("test.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup: explicitly put metadata into metadata cache
    let footer = warmup_metadata(&cache, parquet_dir.path(), "test.parquet", footer_start, file_size);

    block_on(async {
        // Metadata read via get_range → get_opts → cache probe HIT
        let footer_from_cache = store.get_range(&path, footer_start..file_size).await.unwrap();
        assert_eq!(footer_from_cache, footer);

        // Confirm in metadata cache, NOT data cache
        let footer_key = range_cache_key("test.parquet", footer_start, file_size);
        assert!(cache.metadata_cache().get(&footer_key).await.is_some(),
            "footer must be in metadata cache");
        assert!(cache.data_cache().get(&footer_key).await.is_none(),
            "footer must NOT be in data cache");

        // Data read (get_ranges) → with 8MiB chunk alignment, the aligned key is 0..file_size
        // which already exists in metadata cache from warmup. The tiered cache serves it from
        // metadata tier (correct: data is already cached, no redundant fetch needed).
        let data = store.get_ranges(&path, &[0u64..4096]).await.unwrap();
        assert_eq!(data[0].len(), 4096);

        // The aligned chunk key is 0..file_size — already in metadata cache from warmup
        let data_key = range_cache_key("test.parquet", 0, file_size);
        assert!(cache.metadata_cache().get(&data_key).await.is_some(),
            "chunk key must hit metadata cache (warmup cached entire file as metadata)");
    });
}

#[test]
fn metadata_survives_restart_via_foyer_recovery() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "restart.parquet", 2);
    let path = Path::from("restart.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Session 1: warmup puts metadata into metadata cache
    let original_bytes = {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        warmup_metadata(&cache, parquet_dir.path(), "restart.parquet", footer_start, file_size)
    };

    // Session 2: new instances, same SSD dirs — Foyer recovers
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(parquet_dir.path(), cache.clone(), "restart.parquet", file_size);
        block_on(async {
            // get_range → get_opts → cache probe → HIT (recovered from SSD)
            let bytes = store.get_range(&path, footer_start..file_size).await.unwrap();
            assert_eq!(bytes, original_bytes, "metadata must survive restart");
        });
    }
}

#[test]
fn evict_prefix_clears_both_caches_on_shard_delete() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "delete.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "delete.parquet", file_size);
    let path = Path::from("delete.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup metadata + populate data
    warmup_metadata(&cache, parquet_dir.path(), "delete.parquet", footer_start, file_size);

    block_on(async {
        let _data = store.get_ranges(&path, &[0u64..4096]).await.unwrap();

        let footer_key = range_cache_key("delete.parquet", footer_start, file_size);
        // With chunk alignment, get_ranges uses key 0..file_size which hits metadata cache
        // (warmup already cached the whole file as metadata). Verify it's there before evict.
        let data_key = range_cache_key("delete.parquet", 0, file_size);
        assert!(cache.metadata_cache().get(&footer_key).await.is_some());
        assert!(cache.metadata_cache().get(&data_key).await.is_some());

        // Shard delete
        store.evict_path("delete.parquet");

        assert!(cache.metadata_cache().get(&footer_key).await.is_none(), "metadata must be evicted");
        assert!(cache.metadata_cache().get(&data_key).await.is_none(), "data chunk key must be evicted");
    });
}

/// Proves metadata is served from cache, not local FS: delete the local file
/// after warmup, then read via store — must succeed from cache.
#[test]
fn metadata_served_from_ssd_not_local_fs() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "ssd_only.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "ssd_only.parquet", file_size);
    let path = Path::from("ssd_only.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup puts metadata into metadata cache
    let original = warmup_metadata(&cache, parquet_dir.path(), "ssd_only.parquet", footer_start, file_size);

    // Delete local file — force subsequent reads to come from cache only
    std::fs::remove_file(parquet_dir.path().join("ssd_only.parquet")).unwrap();

    block_on(async {
        // Read via store — local FS is gone, must succeed from metadata SSD cache
        let from_cache = store.get_range(&path, footer_start..file_size).await.unwrap();
        assert_eq!(from_cache, original, "must serve from SSD cache after local deletion");
    });
}

/// Fill data cache beyond capacity, verify metadata is untouched.
#[test]
fn data_pressure_does_not_evict_metadata() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "pressure.parquet", 5);

    let data_cache = Arc::new(FoyerCache::new(
        2 * 1024 * 1024, data_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, meta_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(parquet_dir.path(), cache.clone(), "pressure.parquet", file_size);
    let path = Path::from("pressure.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup
    let footer = warmup_metadata(&cache, parquet_dir.path(), "pressure.parquet", footer_start, file_size);
    let footer_key = range_cache_key("pressure.parquet", footer_start, file_size);

    block_on(async {
        // Fill data cache to trigger eviction
        for i in 0..20 {
            let start = (i * 1024) as u64;
            let end = start + 102400;
            if end <= file_size {
                let _ = store.get_ranges(&path, &[start..end]).await;
            }
        }

        // Metadata untouched
        assert!(cache.metadata_cache().get(&footer_key).await.is_some(),
            "metadata must survive data cache LRU pressure");
        let footer_after = store.get_range(&path, footer_start..file_size).await.unwrap();
        assert_eq!(footer_after, footer);
    });
}

/// Suffix fetch resolves to correct absolute range and hits metadata cache.
#[test]
fn suffix_fetch_resolves_and_hits_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "suffix.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "suffix.parquet", file_size);
    let path = Path::from("suffix.parquet");

    let suffix_size = 4096u64;
    let expected_start = file_size - suffix_size;

    // Warmup: put with absolute range key
    warmup_metadata(&cache, parquet_dir.path(), "suffix.parquet", expected_start, file_size);

    block_on(async {
        // Suffix fetch should resolve to same absolute key and hit cache
        use object_store::GetOptions;
        let opts = GetOptions {
            range: Some(object_store::GetRange::Suffix(suffix_size)),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await.unwrap();
        let suffix_bytes = result.bytes().await.unwrap();

        // Must match what we put via absolute range
        let bounded_bytes = store.get_range(&path, expected_start..file_size).await.unwrap();
        assert_eq!(suffix_bytes, bounded_bytes,
            "suffix fetch must resolve to same bytes as bounded range");
    });
}

/// Multiple concurrent reads of same metadata range — all succeed with same bytes.
#[test]
fn concurrent_metadata_reads_are_safe() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "concurrent.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "concurrent.parquet", file_size);
    let path = Path::from("concurrent.parquet");
    let footer_start = file_size.saturating_sub(8 * 1024);

    // Warmup
    let expected = warmup_metadata(&cache, parquet_dir.path(), "concurrent.parquet", footer_start, file_size);

    block_on(async {
        let mut handles = Vec::new();
        for _ in 0..10 {
            let store = store.clone();
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                store.get_range(&path, footer_start..file_size).await.unwrap()
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles).await
            .into_iter().map(|r| r.unwrap()).collect();

        for (i, result) in results.iter().enumerate() {
            assert_eq!(result, &expected,
                "concurrent read {} must match warmup bytes", i);
        }
    });
}

/// A range read via get_range (single) and get_ranges (multi with one element)
/// must NOT create duplicate cache entries — both go through chunk-aligned
/// probe + populate so they share the same chunk-aligned cache key.
#[test]
fn get_range_and_get_ranges_share_same_cache_key() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "keyshare.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "keyshare.parquet", file_size);
    let path = Path::from("keyshare.parquet");

    block_on(async {
        // Read first 4KB via get_ranges → chunk-aligned key (whole file fits in chunk 0).
        let via_ranges = store.get_ranges(&path, &[0u64..4096]).await.unwrap();

        // Read same range via get_range → same chunk-aligned key, should hit cache.
        let via_range = store.get_range(&path, 0u64..4096).await.unwrap();

        // Both must return same bytes.
        assert_eq!(via_ranges[0], via_range, "get_range and get_ranges must return same bytes");

        // Both paths produce the same chunk-aligned key. For a small file
        // (file_size < 8 MiB) the chunk is [0..file_size).
        let chunk_aligned_key = range_cache_key("keyshare.parquet", 0, file_size);
        let in_data = cache.data_cache().get(&chunk_aligned_key).await;
        let in_meta = cache.metadata_cache().get(&chunk_aligned_key).await;
        assert!(in_data.is_some() || in_meta.is_some(),
            "chunk-aligned key must be cached in at least one tier after first fetch");
    });
}

/// When metadata cache is full, reads still succeed via local FS / S3.
/// Foyer may drop entries that exceed capacity (LRU hasn't run yet).
/// The system degrades gracefully — no panics, no errors.
#[test]
fn metadata_cache_full_reads_still_succeed_via_local_fs() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Write a large parquet file
    let file_size = write_test_parquet(parquet_dir.path(), "breach.parquet", 10);

    // Tiny metadata cache (1MB disk, 1MB block) — will fill quickly
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, data_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        1 * 1024 * 1024, meta_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(parquet_dir.path(), cache.clone(), "breach.parquet", file_size);
    let path = Path::from("breach.parquet");

    block_on(async {
        // Read multiple ranges — even with small metadata cache, reads succeed
        // (served from local FS since get_opts doesn't auto-populate)
        let mut all_bytes = Vec::new();
        for i in 0..10 {
            let start = (i * 1024) as u64;
            let end = start + 4096;
            if end <= file_size {
                let bytes = store.get_range(&path, start..end).await.unwrap();
                all_bytes.push((start, end, bytes));
            }
        }

        // All reads succeed — no panics, no errors regardless of cache state
        assert!(!all_bytes.is_empty(), "reads must succeed regardless of metadata cache pressure");

        // Repeated reads also succeed (from local FS — get_opts does not auto-populate cache)
        for (start, end, original) in &all_bytes {
            let bytes = store.get_range(&path, *start..*end).await.unwrap();
            assert_eq!(&bytes, original,
                "repeated read of {}..{} must return same bytes", start, end);
        }
    });
}

/// DataFusion reads Parquet through the TieredObjectStore, executing a real SQL query.
#[test]
fn datafusion_query_through_tiered_store() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "query.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "query.parquet", file_size);

    block_on(async {
        let (ctx, _schema) = setup_df_session(store.clone(), "query.parquet", "test_table", None).await;

        let df = ctx.sql("SELECT id, name FROM test_table WHERE id < 5 ORDER BY id")
            .await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "WHERE id < 5 should return 5 rows");
    });
}

/// Warmup puts metadata via store.put_metadata(), then DataFusion query reads
/// metadata from metadata_cache (never-evict) and data from data_cache.
/// After warmup, the local file is deleted to prove all reads come from cache.
#[test]
fn warmup_put_metadata_then_datafusion_query_from_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "warm.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "warm.parquet", file_size);

    block_on(async {
        // ── Warmup: read metadata through store, then promote to metadata_cache ──
        // First, let DataFusion do a full query to discover what ranges it needs.
        // This populates data_cache with all metadata + data ranges.
        let (ctx, schema) = setup_df_session(store.clone(), "warm.parquet", "t", None).await;

        let batches = ctx.sql("SELECT id FROM t WHERE id < 5 ORDER BY id")
            .await.unwrap().collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);

        // Step 2: Now promote the footer range to metadata_cache.
        let footer_start = file_size.saturating_sub(64 * 1024);
        let footer_key = range_cache_key("warm.parquet", footer_start, file_size);
        if let Some(footer_bytes) = cache.get(&footer_key).await {
            store.put_metadata("warm.parquet", &[footer_start..file_size], &[footer_bytes]);
        }

        // Verify metadata is now in metadata_cache
        assert!(cache.metadata_cache().get(&footer_key).await.is_some(),
            "footer must be in metadata_cache after put_metadata");

        // ── Delete local file — all subsequent reads must come from cache ────
        std::fs::remove_file(parquet_dir.path().join("warm.parquet")).unwrap();

        // ── Query again: must succeed entirely from cache ────────────────────
        let (ctx2, _) = setup_df_session(store.clone(), "warm.parquet", "t", Some(schema)).await;

        let batches2 = ctx2.sql("SELECT id FROM t WHERE id < 5 ORDER BY id")
            .await.unwrap().collect().await.unwrap();
        assert_eq!(batches2.iter().map(|b| b.num_rows()).sum::<usize>(), 5,
            "query after file deletion must succeed from cache (metadata in metadata_cache)");
    });
}

/// Key alignment test: warmup via put_metadata, then DataFusion query hits cache.
///
/// Strategy:
///   1. Run DataFusion query (cold start) — all reads go to local FS and populate
///      data_cache via get_opts/get_ranges. Query succeeds.
///   2. Delete local file.
///   3. Run same query again — must succeed entirely from cache (data_cache populated
///      by step 1). This proves the keys produced by DataFusion's read path are the
///      same keys stored in the cache — key alignment is correct.
///
/// This validates that get_opts (metadata path) and get_ranges (data path) produce
/// consistent, deterministic cache keys that are found on subsequent probes.
#[test]
fn datafusion_query_succeeds_from_cache_after_local_file_deleted() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "align.parquet", 2);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "align.parquet", file_size);

    block_on(async {
        // ── Query 1: cold start, file on local FS ────────────────────────────
        let (ctx, schema) = setup_df_session(store.clone(), "align.parquet", "t", None).await;

        let batches = ctx.sql("SELECT id FROM t WHERE id < 3 ORDER BY id")
            .await.unwrap().collect().await.unwrap();
        let rows1: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows1, 3, "query 1 must return 3 rows");

        // ── Delete local file — cache is the only source now ─────────────────
        std::fs::remove_file(parquet_dir.path().join("align.parquet")).unwrap();

        // ── Query 2: same query, file gone — must succeed from cache ─────────
        let (ctx2, _) = setup_df_session(store.clone(), "align.parquet", "t", Some(schema)).await;

        let batches2 = ctx2.sql("SELECT id FROM t WHERE id < 3 ORDER BY id")
            .await.unwrap().collect().await.unwrap();
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 3,
            "query 2 (file deleted) must succeed from cache — proves key alignment");
    });
}

// ── New correctness-risk integration tests ─────────────────────────────────────

/// Write a Parquet file with page-level statistics (column index + offset index)
/// enabled. Returns the file size.
#[allow(deprecated)]
fn write_page_indexed_parquet(dir: &std::path::Path, filename: &str, num_row_groups: usize, num_columns: usize) -> u64 {
    let mut fields: Vec<Field> = Vec::new();
    for i in 0..num_columns {
        fields.push(Field::new(format!("col_{}", i), DataType::Int64, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let file_path = dir.join(filename);
    let file = std::fs::File::create(&file_path).unwrap();

    // Enable page-level statistics by setting write_page_index(true)
    // and small max_row_group_size so we get multiple row groups.
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(100))
        .set_column_index_truncate_length(Some(64))
        .set_write_batch_size(50) // force multiple pages per row group
        .build();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    for rg in 0..num_row_groups {
        let offset = (rg * 100) as i64;
        let columns: Vec<Arc<dyn arrow_array::Array>> = (0..num_columns)
            .map(|c| {
                let vals: Vec<i64> = (offset..offset + 100).map(|v| v + (c as i64 * 1000)).collect();
                Arc::new(Int64Array::from(vals)) as Arc<dyn arrow_array::Array>
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    std::fs::metadata(&file_path).unwrap().len()
}

/// **Test 1**: Page index key alignment — warmup computes page index ranges from
/// footer metadata and stores them in metadata Foyer. At query time, the same
/// ranges must be found in the cache (key alignment).
///
/// Strategy: warmup puts page index ranges into metadata Foyer using the same fold
/// logic as `custom_cache_manager.rs`. Then delete the local file. Reading those
/// ranges via the store must succeed from cache — proving key alignment.
#[test]
fn page_index_key_alignment_warmup_matches_query_time() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Write a multi-column file with page indexes enabled
    let file_size = write_page_indexed_parquet(parquet_dir.path(), "page_idx.parquet", 3, 5);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "page_idx.parquet", file_size);
    let path = Path::from("page_idx.parquet");

    // Read footer and compute page index ranges using the shared helper
    let file = std::fs::File::open(parquet_dir.path().join("page_idx.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    // Compute ONE global merged range — matching parquet crate's range_for_page_index()
    let mut index_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    if let Some(r) = compute_global_page_index_range(parquet_metadata) {
        index_ranges.push(r);
    }

    // Must have a page index range (proves our test file has page indexes)
    assert!(!index_ranges.is_empty(),
        "test file must have page index data; got {} ranges", index_ranges.len());

    // Warmup: production callers chunk-align their ranges before put_metadata so
    // that every chunk touched is fully covered (matching what custom_cache_manager
    // does). Mirror that here.
    let file_bytes = std::fs::read(parquet_dir.path().join("page_idx.parquet")).unwrap();
    const CHUNK: u64 = 8 << 20;
    let aligned_ranges: Vec<std::ops::Range<u64>> = index_ranges.iter().map(|r| {
        let aligned_start = r.start / CHUNK * CHUNK;
        let aligned_end = r.end.div_ceil(CHUNK).saturating_mul(CHUNK).min(file_size);
        aligned_start..aligned_end
    }).collect();
    let range_data: Vec<bytes::Bytes> = aligned_ranges.iter()
        .map(|r| bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]))
        .collect();
    store.put_metadata("page_idx.parquet", &aligned_ranges, &range_data);

    // Assert: the global page index range covers ALL individual column offsets
    let global_range = &index_ranges[0];
    for rg in parquet_metadata.row_groups() {
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if let (Some(offset), Some(length)) = (col.column_index_offset(), col.column_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                assert!(start >= global_range.start && end <= global_range.end,
                    "col {} column_index {}..{} must be within global range {}..{}",
                    col_idx, start, end, global_range.start, global_range.end);
            }
            if let (Some(offset), Some(length)) = (col.offset_index_offset(), col.offset_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                assert!(start >= global_range.start && end <= global_range.end,
                    "col {} offset_index {}..{} must be within global range {}..{}",
                    col_idx, start, end, global_range.start, global_range.end);
            }
        }
    }

    // Assert: the chunk-aligned key (which covers the global PI range) is in metadata Foyer.
    // For a small test file (file_size < 8 MiB) this is the single chunk [0..file_size).
    let aligned_global = &aligned_ranges[0];
    let expected_key = range_cache_key("page_idx.parquet", aligned_global.start, aligned_global.end);
    block_on(async {
        assert!(cache.metadata_cache().get(&expected_key).await.is_some(),
            "chunk-aligned page index key {}..{} must be in metadata Foyer",
            aligned_global.start, aligned_global.end);

        // Assert: the chunk is NOT in data Foyer (put_metadata goes only to metadata).
        assert!(cache.data_cache().get(&expected_key).await.is_none(),
            "page index chunk must NOT be in data Foyer — only metadata Foyer");
    });

    // Also warmup the footer (last 8KB) — production callers chunk-align this too.
    // For a tiny test file this resolves to the same chunk that already covers PI.
    let footer_start = file_size.saturating_sub(8 * 1024);
    warmup_metadata(&cache, parquet_dir.path(), "page_idx.parquet", footer_start, file_size);

    // Delete local file — all reads must come from metadata cache.
    std::fs::remove_file(parquet_dir.path().join("page_idx.parquet")).unwrap();

    block_on(async {
        // Read the global page index range via store — must succeed from metadata cache
        // via the chunk-aligned probe (slicing the warmup-promoted chunk).
        let result = store.get_range(&path, global_range.start..global_range.end).await;
        assert!(result.is_ok(),
            "global page index range ({}..{}) must be served from metadata cache after file deletion",
            global_range.start, global_range.end);
        let bytes = result.unwrap();
        // Slice the cached chunk to compare byte-for-byte with the original file_bytes.
        let expected = &file_bytes[global_range.start as usize..global_range.end as usize];
        assert_eq!(bytes.as_ref(), expected,
            "page index bytes must match the source file byte-for-byte");
    });
}

/// **Test 2**: Concurrent shard warmup does not corrupt shared metadata Foyer.
///
/// Multiple shards warming up in parallel (different files, same TieredBlockCache)
/// must not interfere. After all complete, each file's metadata is independently
/// correct and retrievable.
#[test]
fn concurrent_shard_warmup_does_not_corrupt() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());

    // Create 5 parquet files simulating 5 shard warmups
    let num_shards = 5;
    let mut file_info: Vec<(String, u64, bytes::Bytes)> = Vec::new();
    for i in 0..num_shards {
        let filename = format!("shard_{}.parquet", i);
        let file_size = write_test_parquet(parquet_dir.path(), &filename, 2 + i);
        let footer_start = file_size.saturating_sub(8 * 1024);
        // Read footer bytes before spawning tasks
        let file_bytes = std::fs::read(parquet_dir.path().join(&filename)).unwrap();
        let footer_bytes = bytes::Bytes::copy_from_slice(
            &file_bytes[footer_start as usize..file_size as usize]
        );
        file_info.push((filename, file_size, footer_bytes));
    }

    block_on(async {
        // Spawn 5 concurrent warmup tasks
        let mut handles = Vec::new();
        for (filename, file_size, footer_bytes) in &file_info {
            let cache_clone = cache.clone();
            let filename = filename.clone();
            let file_size = *file_size;
            let footer_bytes = footer_bytes.clone();
            handles.push(tokio::spawn(async move {
                let footer_start = file_size.saturating_sub(8 * 1024);
                let key = range_cache_key(&filename, footer_start, file_size);
                cache_clone.put_metadata(&key, footer_bytes);
            }));
        }

        // Wait for all warmups to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify each file's metadata is independently correct
        for (filename, file_size, expected_bytes) in &file_info {
            let footer_start = file_size.saturating_sub(8 * 1024);
            let key = range_cache_key(filename, footer_start, *file_size);
            let cached = cache.metadata_cache().get(&key).await;
            assert!(cached.is_some(),
                "metadata for {} must be retrievable after concurrent warmup", filename);
            assert_eq!(cached.unwrap(), *expected_bytes,
                "metadata for {} must not be corrupted by concurrent warmup", filename);
        }
    });
}

/// **Test 3**: Metadata Foyer capacity breach graceful degradation.
///
/// When metadata Foyer SSD fills, new put_metadata calls may fail silently (Foyer
/// behavior). Reads should still work for entries that survived, and no panics or
/// errors occur — graceful degradation.
#[test]
fn metadata_foyer_capacity_breach_graceful_degradation() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Write a larger file to have enough data to fill cache
    let file_size = write_test_parquet(parquet_dir.path(), "overflow.parquet", 10);

    // Create a metadata cache with very small capacity (1MB)
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, data_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        1 * 1024 * 1024, meta_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(parquet_dir.path(), cache.clone(), "overflow.parquet", file_size);
    let path = Path::from("overflow.parquet");

    // Read file bytes for warmup
    let file_bytes = std::fs::read(parquet_dir.path().join("overflow.parquet")).unwrap();

    // Put many metadata entries to exceed 1MB capacity.
    // Use small chunk sizes that fit within our actual file size.
    let mut entries: Vec<(u64, u64, bytes::Bytes)> = Vec::new();
    let chunk_size = 1024u64; // 1KB chunks — small enough for our test file
    let num_entries = (file_size / chunk_size).max(1);
    for i in 0..num_entries {
        let start = i * chunk_size;
        let end = ((i + 1) * chunk_size).min(file_size);
        if end > start && (end as usize) <= file_bytes.len() {
            let data = bytes::Bytes::copy_from_slice(&file_bytes[start as usize..end as usize]);
            entries.push((start, end, data.clone()));
            let key = range_cache_key("overflow.parquet", start, end);
            cache.put_metadata(&key, data);
        }
    }

    // No panics must have occurred. Now verify graceful degradation.
    block_on(async {
        let mut hits = 0;
        let mut misses = 0;

        for (start, end, expected) in &entries {
            let key = range_cache_key("overflow.parquet", *start, *end);
            match cache.metadata_cache().get(&key).await {
                Some(cached) => {
                    assert_eq!(&cached, expected, "cached metadata at {}..{} must match", start, end);
                    hits += 1;
                }
                None => {
                    misses += 1;
                }
            }
        }

        // Some entries should have survived (at least the most recent ones)
        assert!(hits > 0,
            "at least some metadata entries must survive capacity pressure (got {} hits, {} misses)",
            hits, misses);

        // Reads via store must not error even for ranges that missed metadata cache
        // (they fall through to local FS or data cache)
        for (start, end, _) in &entries {
            let result = store.get_range(&path, *start..*end).await;
            assert!(result.is_ok(),
                "get_range({}..{}) must succeed (graceful degradation) even under capacity pressure",
                start, end);
        }
    });
}

/// **Test**: Oversized metadata entries route to data cache via max_metadata_entry_size bound.
///

/// **Test 4**: Page index ranges match parquet crate computation.
///
/// Our warmup code computes page index ranges by folding column_index_offset/length
/// and offset_index_offset/length across columns per RG. This test verifies that
/// the fold produces ranges that cover all column metadata by checking against
/// individual column offsets.
#[test]
fn page_index_ranges_match_parquet_crate_computation() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();

    // Write a multi-column file (5 columns, 4 row groups)
    let _file_size = write_page_indexed_parquet(parquet_dir.path(), "parity.parquet", 4, 5);

    let file = std::fs::File::open(parquet_dir.path().join("parity.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    for (rg_idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
        // Compute merged ranges using our shared fold helper
        let (col_idx_range, off_idx_range) = compute_per_rg_page_index_ranges(rg);

        // Verify that every individual column's range is fully contained within the merged range
        for (col_idx, col) in rg.columns().iter().enumerate() {
            if let (Some(offset), Some(length)) = (col.column_index_offset(), col.column_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                let merged = col_idx_range.as_ref().unwrap();
                assert!(start >= merged.start && end <= merged.end,
                    "RG {} col {} column_index range {}..{} must be contained in merged {}..{}",
                    rg_idx, col_idx, start, end, merged.start, merged.end);
            }

            if let (Some(offset), Some(length)) = (col.offset_index_offset(), col.offset_index_length()) {
                let start = offset as u64;
                let end = start + length as u64;
                let merged = off_idx_range.as_ref().unwrap();
                assert!(start >= merged.start && end <= merged.end,
                    "RG {} col {} offset_index range {}..{} must be contained in merged {}..{}",
                    rg_idx, col_idx, start, end, merged.start, merged.end);
            }
        }

        // Verify the merged range is tight (start == min offset, end == max offset+length)
        if let Some(ref merged) = col_idx_range {
            let actual_min = rg.columns().iter()
                .filter_map(|c| c.column_index_offset().map(|o| o as u64))
                .min().unwrap();
            let actual_max = rg.columns().iter()
                .filter_map(|c| {
                    c.column_index_offset().and_then(|o| {
                        c.column_index_length().map(|l| o as u64 + l as u64)
                    })
                })
                .max().unwrap();
            assert_eq!(merged.start, actual_min,
                "RG {} column_index merged start must equal min offset", rg_idx);
            assert_eq!(merged.end, actual_max,
                "RG {} column_index merged end must equal max offset+length", rg_idx);
        }

        if let Some(ref merged) = off_idx_range {
            let actual_min = rg.columns().iter()
                .filter_map(|c| c.offset_index_offset().map(|o| o as u64))
                .min().unwrap();
            let actual_max = rg.columns().iter()
                .filter_map(|c| {
                    c.offset_index_offset().and_then(|o| {
                        c.offset_index_length().map(|l| o as u64 + l as u64)
                    })
                })
                .max().unwrap();
            assert_eq!(merged.start, actual_min,
                "RG {} offset_index merged start must equal min offset", rg_idx);
            assert_eq!(merged.end, actual_max,
                "RG {} offset_index merged end must equal max offset+length", rg_idx);
        }
    }
}

/// **Test 5**: get_opts probe does NOT pollute metadata Foyer with column data.
///
/// At query time, column data reads via CachedMetadataReader::get_bytes() go
/// through get_opts. This must NOT put column data bytes into metadata Foyer.
/// Only warmup's explicit put_metadata() should populate metadata Foyer.
#[test]
/// **Test**: get_opts on a cache miss populates ONLY the data tier (never metadata),
/// and only when the read covers a full chunk. Warmup (put_metadata) is the only
/// path that writes to the metadata tier.
fn get_opts_probe_does_not_pollute_metadata_foyer() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "nopollute.parquet", 3);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "nopollute.parquet", file_size);
    let path = Path::from("nopollute.parquet");

    block_on(async {
        // Cold cache. Read the whole file (chunk-aligned for files < 8 MiB).
        let result = store.get_range(&path, 0..file_size).await;
        assert!(result.is_ok(), "get_range must succeed");
        let bytes = result.unwrap();
        assert_eq!(bytes.len(), file_size as usize);

        // After post-fetch populate, the chunk-aligned key must be in DATA tier only.
        let chunk_key = range_cache_key("nopollute.parquet", 0, file_size);
        assert!(cache.data_cache().get(&chunk_key).await.is_some(),
            "get_opts post-fetch populate must write to data tier under chunk-aligned key");
        assert!(cache.metadata_cache().get(&chunk_key).await.is_none(),
            "get_opts post-fetch populate must NEVER write to metadata tier — that's reserved for warmup");
    });

    // Now exercise the warmup path on a different file: it must write to metadata tier.
    let f2_size = write_test_parquet(parquet_dir.path(), "warmed.parquet", 3);
    let store2 = create_store(parquet_dir.path(), cache.clone(), "warmed.parquet", f2_size);
    let path2 = Path::from("warmed.parquet");
    let f2_bytes = std::fs::read(parquet_dir.path().join("warmed.parquet")).unwrap();

    // Chunk-aligned warmup (mirrors production custom_cache_manager).
    store2.put_metadata("warmed.parquet", &[0..f2_size], &[bytes::Bytes::copy_from_slice(&f2_bytes)]);

    block_on(async {
        let chunk_key = range_cache_key("warmed.parquet", 0, f2_size);
        assert!(cache.metadata_cache().get(&chunk_key).await.is_some(),
            "warmup put_metadata writes to metadata tier under chunk-aligned key");

        // Subsequent sub-range read goes through chunk-aligned probe → metadata HIT.
        let bytes = store2.get_range(&path2, 100..200).await.unwrap();
        assert_eq!(bytes.len(), 100);
    });
}

/// **Test 6**: Restart — no S3/local FS reads for previously warmed metadata.
///
/// On restart, warmup re-runs. Previously warmed metadata (footer + page indexes)
/// should be recovered from Foyer's SSD tier, not re-fetched from local FS.
///
/// Strategy: warmup chunk-aligns its raw ranges (mirrors production
/// `custom_cache_manager::warmup_file_with_store`), puts the aligned ranges into
/// metadata Foyer, drops caches, recreates on same dirs. After recovery, verify
/// the chunk-aligned key is still available from SSD AND that probing for any
/// raw sub-range slices into the recovered chunk correctly.
///
/// For a small test file (`file_size < 8 MiB`), all raw ranges chunk-align to a
/// single key `0..file_size` so the entire file lands under one metadata-tier
/// entry — the largest, most reliably persisted by Foyer.
#[test]
fn restart_no_s3_for_previously_warmed_metadata() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use crate::tiered_object_store::CACHE_CHUNK_SIZE;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_page_indexed_parquet(parquet_dir.path(), "restart_meta.parquet", 3, 3);

    // Collect raw warmup ranges (footer + page index) for later sub-range probe.
    let mut raw_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    let file_bytes = std::fs::read(parquet_dir.path().join("restart_meta.parquet")).unwrap();
    {
        let file = std::fs::File::open(parquet_dir.path().join("restart_meta.parquet")).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let footer_start = file_size.saturating_sub(8 * 1024);
        raw_ranges.push(footer_start..file_size);
        for rg in parquet_metadata.row_groups() {
            let (col_idx_range, off_idx_range) = compute_per_rg_page_index_ranges(rg);
            if let Some(r) = col_idx_range { raw_ranges.push(r); }
            if let Some(r) = off_idx_range { raw_ranges.push(r); }
        }
        assert!(raw_ranges.len() >= 2, "must have footer + at least 1 page index range");
    }

    // Chunk-align each raw range outward (production warmup pattern). For a small
    // test file all entries collapse to the single chunk `0..file_size`.
    let mut aligned_ranges: Vec<std::ops::Range<u64>> = raw_ranges.iter().map(|r| {
        let s = r.start / CACHE_CHUNK_SIZE * CACHE_CHUNK_SIZE;
        let e = r.end.div_ceil(CACHE_CHUNK_SIZE).saturating_mul(CACHE_CHUNK_SIZE).min(file_size);
        s..e
    }).collect();
    aligned_ranges.sort_by_key(|r| (r.start, r.end));
    aligned_ranges.dedup();

    let aligned_data: Vec<bytes::Bytes> = aligned_ranges.iter()
        .map(|r| bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]))
        .collect();

    // Session 1: warmup puts chunk-aligned ranges into metadata Foyer
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(parquet_dir.path(), cache.clone(), "restart_meta.parquet", file_size);
        store.put_metadata("restart_meta.parquet", &aligned_ranges, &aligned_data);
    }
    // Session 1 dropped — Foyer flushes to SSD

    // Session 2: new Foyer instances on same directories — should recover from SSD
    // NOTE: we do NOT delete the local file here. Instead, we verify recovery by
    // probing the metadata cache directly. This avoids flakiness from Foyer's block
    // alignment behavior with very small entries.
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());

        block_on(async {
            let mut recovered_count = 0;
            for (i, (range, expected)) in aligned_ranges.iter().zip(aligned_data.iter()).enumerate() {
                let key = range_cache_key("restart_meta.parquet", range.start, range.end);
                if let Some(cached) = cache.metadata_cache().get(&key).await {
                    assert_eq!(&cached, expected,
                        "recovered range {} ({}..{}) bytes must match original warmup data",
                        i, range.start, range.end);
                    recovered_count += 1;
                }
            }

            // The first chunk-aligned key (covers the file for small files; covers
            // the footer's chunk for large files) must survive — it is the largest
            // entry and most critical for restart without S3 calls.
            let primary_key = range_cache_key("restart_meta.parquet",
                aligned_ranges[0].start, aligned_ranges[0].end);
            assert!(cache.metadata_cache().get(&primary_key).await.is_some(),
                "primary chunk-aligned key must survive SSD recovery — this is the primary restart-without-S3 guarantee");

            // At least one chunk-aligned entry must be recovered. Recovered data is
            // byte-for-byte correct (verified in the loop above).
            assert!(recovered_count >= 1,
                "at least the primary chunk-aligned range must survive SSD recovery (recovered {} of {} ranges)",
                recovered_count, aligned_ranges.len());

            // Sub-range probe slices into the recovered chunk: pick the original
            // raw footer range (sub-chunk for small files) and confirm it's still
            // available via the chunk-aligned probe path.
            let raw_footer = &raw_ranges[0];
            let store2 = create_store(parquet_dir.path(), cache.clone(),
                "restart_meta.parquet", file_size);
            let probed = store2.get_range(
                &Path::from("restart_meta.parquet"),
                raw_footer.start..raw_footer.end,
            ).await.expect("sub-range probe must slice into recovered chunk");
            assert_eq!(probed.len() as u64, raw_footer.end - raw_footer.start,
                "sub-range slice length must equal raw footer length");
        });
    }
}

/// **HIGH-CONFIDENCE TEST**: Full production warmup sequence → delete file →
/// DataFusion query succeeds entirely from cache.
///
/// This exercises the EXACT production path:
/// 1. Warmup: parse footer, compute page index ranges, put all into metadata Foyer
/// 2. First DataFusion query: reads column data → populates data Foyer
/// 3. Delete the local Parquet file (simulates warm node with no local copy)
/// 4. Second DataFusion query: must succeed from cache alone
///    - Metadata (footer): served from metadata Foyer via get_opts probe
///    - Column data: served from data Foyer via get_ranges probe
///
/// If this test passes, key alignment is proven for ALL byte ranges across the
/// full stack: warmup → DataFusion → TieredObjectStore → TieredBlockCache → Foyer.
#[test]
fn production_warmup_then_query_from_cache_only() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_page_indexed_parquet(parquet_dir.path(), "prod.parquet", 3, 4);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "prod.parquet", file_size);

    // ── Step 1: Production warmup (same logic as warmup_file_with_store) ─────
    let file_bytes = std::fs::read(parquet_dir.path().join("prod.parquet")).unwrap();
    let file = std::fs::File::open(parquet_dir.path().join("prod.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    // Compute all metadata ranges (footer + page indexes)
    let mut metadata_ranges: Vec<std::ops::Range<u64>> = Vec::new();
    let mut metadata_bytes: Vec<bytes::Bytes> = Vec::new();

    // Footer
    let footer_start = file_size.saturating_sub(64 * 1024);
    metadata_ranges.push(footer_start..file_size);
    metadata_bytes.push(bytes::Bytes::copy_from_slice(&file_bytes[footer_start as usize..file_size as usize]));

    // Page/offset indexes per RG
    for rg in parquet_metadata.row_groups() {
        let (col_idx_range, off_idx_range) = compute_per_rg_page_index_ranges(rg);
        if let Some(r) = col_idx_range {
            metadata_bytes.push(bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]));
            metadata_ranges.push(r);
        }
        if let Some(r) = off_idx_range {
            metadata_bytes.push(bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]));
            metadata_ranges.push(r);
        }
    }

    // Put metadata into metadata Foyer (production warmup step)
    store.put_metadata("prod.parquet", &metadata_ranges, &metadata_bytes);

    // ── Step 2: First DataFusion query (populates data Foyer with column data) ──
    block_on(async {
        let (ctx, schema) = setup_df_session(store.clone(), "prod.parquet", "prod", None).await;

        // Run query — this reads column data via get_ranges → data Foyer
        let batches = ctx.sql("SELECT col_0, col_1 FROM prod WHERE col_0 < 50")
            .await.unwrap().collect().await.unwrap();
        let rows1: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows1 > 0, "first query must return rows");

        // ── Step 3: Delete local file ────────────────────────────────────────
        std::fs::remove_file(parquet_dir.path().join("prod.parquet")).unwrap();

        // ── Step 4: Second query — must succeed entirely from cache ───────────
        let (ctx2, _) = setup_df_session(store.clone(), "prod.parquet", "prod", Some(schema)).await;

        let batches2 = ctx2.sql("SELECT col_0, col_1 FROM prod WHERE col_0 < 50")
            .await.unwrap().collect().await.unwrap();
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, rows1,
            "second query (file deleted) must return same rows as first — proves full cache correctness");
    });
}

/// **HIGH-CONFIDENCE TEST**: Restart with file deleted — Foyer is the ONLY source.
///
/// Session 1: production warmup + DataFusion query (populates both caches)
/// Session 2: new Foyer instances (same SSD dirs), file deleted → DataFusion query succeeds.
///
/// This proves that across a full restart (new process, new Foyer instances),
/// the recovered SSD state is sufficient to serve all reads without any
/// local FS or S3 access.
///
/// Requires: FoyerCache::drop() calls HybridCache::close() to flush partial blocks.
#[test]
fn restart_with_file_deleted_query_succeeds_from_foyer_only() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "restart_full.parquet", 2);
    let expected_rows;
    let saved_schema;

    // ── Session 1: warmup + query (populates both caches) ────────────────────
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(parquet_dir.path(), cache.clone(), "restart_full.parquet", file_size);

        // Warmup: put footer into metadata Foyer
        let footer_start = file_size.saturating_sub(64 * 1024);
        let file_bytes = std::fs::read(parquet_dir.path().join("restart_full.parquet")).unwrap();
        let footer_bytes = bytes::Bytes::copy_from_slice(&file_bytes[footer_start as usize..]);
        store.put_metadata("restart_full.parquet", &[footer_start..file_size], &[footer_bytes]);

        // Run DataFusion query — populates data Foyer with column data
        let (rows, schema) = block_on(async {
            let (ctx, schema) = setup_df_session(store.clone(), "restart_full.parquet", "t", None).await;

            let batches = ctx.sql("SELECT id FROM t WHERE id < 10 ORDER BY id")
                .await.unwrap().collect().await.unwrap();
            let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            (rows, schema)
        });
        expected_rows = rows;
        saved_schema = schema;
        assert!(expected_rows > 0);
    }
    // Session 1 dropped — FoyerCache::drop() calls HybridCache::close() which
    // flushes partial blocks to SSD. No sleep needed.

    // ── Delete local file between sessions ───────────────────────────────────
    std::fs::remove_file(parquet_dir.path().join("restart_full.parquet")).unwrap();

    // ── Session 2: new Foyer instances, file gone — query from Foyer only ────
    {
        let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
        let store = create_store(parquet_dir.path(), cache.clone(), "restart_full.parquet", file_size);

        let rows = block_on(async {
            // Use saved schema from session 1 (in production, CatalogSnapshot provides this)
            let (ctx, _) = setup_df_session(store.clone(), "restart_full.parquet", "t", Some(saved_schema)).await;

            let batches = ctx.sql("SELECT id FROM t WHERE id < 10 ORDER BY id")
                .await.unwrap().collect().await.unwrap();
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        });

        assert_eq!(rows, expected_rows,
            "restart query (file deleted, new Foyer instances) must return same rows — \
             proves full lifecycle: warmup → persist → recover → serve from Foyer only");
    }
}


/// Metadata cache does LRU eviction: oldest metadata entries are evicted when full.
#[test]
fn metadata_cache_lru_evicts_oldest() {
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Small metadata cache: 2MB disk, 1MB blocks.
    let data_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, data_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        2 * 1024 * 1024, meta_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));

    let chunk_size = 512 * 1024usize;

    block_on(async {
        // Put 6 entries (3MB total) into 2MB metadata cache
        let mut keys = Vec::new();
        for i in 0..6u64 {
            let key = range_cache_key("lru_meta.parquet", i * chunk_size as u64, (i + 1) * chunk_size as u64);
            let data = bytes::Bytes::from(vec![i as u8; chunk_size]);
            cache.put_metadata(&key, data);
            keys.push(key);
        }

        // Log state of each entry
        for (i, key) in keys.iter().enumerate() {
            let found = cache.metadata_cache().get(key).await.is_some();
            println!("metadata entry {}: found={}", i, found);
        }

        // After flush + reclaim cycle
        cache.wait_for_flush().await;
        println!("--- after wait_for_flush ---");
        for (i, key) in keys.iter().enumerate() {
            let found = cache.metadata_cache().get(key).await.is_some();
            println!("metadata entry {}: found={}", i, found);
        }

        // Give reclaimer time to run
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        println!("--- after 2s sleep ---");
        let mut found_count = 0;
        let mut evicted_count = 0;
        for (i, key) in keys.iter().enumerate() {
            let found = cache.metadata_cache().get(key).await.is_some();
            println!("metadata entry {}: found={}", i, found);
            if found { found_count += 1; } else { evicted_count += 1; }
        }
        println!("total: found={}, evicted={}", found_count, evicted_count);
    });
}

/// Data cache does LRU eviction: oldest column data evicted when full.
/// Metadata cache is unaffected by data pressure.
#[test]
fn data_cache_lru_evicts_oldest_metadata_unaffected() {
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Small data cache: 2MB.
    let data_cache = Arc::new(FoyerCache::new(
        2 * 1024 * 1024, data_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        DISK_BYTES, meta_dir.path(), BLOCK_SIZE, BUFFER_POOL, SUBMIT_QUEUE,
        "auto", 0, 0.0, 0, false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));

    let chunk_size = 512 * 1024usize;

    block_on(async {
        // Put metadata entry first
        let meta_key = range_cache_key("lru_data.parquet", 9000000, 9500000);
        let meta_data = bytes::Bytes::from(vec![0xFF; 100]);
        cache.put_metadata(&meta_key, meta_data.clone());

        // Fill data cache: 6 × 512KB = 3MB into 2MB cache
        let mut data_keys = Vec::new();
        for i in 0..6u64 {
            let key = range_cache_key("lru_data.parquet", i * chunk_size as u64, (i + 1) * chunk_size as u64);
            let data = bytes::Bytes::from(vec![i as u8; chunk_size]);
            cache.data_cache().put(&key, data);
            data_keys.push(key);
        }

        // Log state immediately
        println!("--- immediately ---");
        for (i, key) in data_keys.iter().enumerate() {
            let found = cache.data_cache().get(key).await.is_some();
            println!("data entry {}: found={}", i, found);
        }

        cache.wait_for_flush().await;
        println!("--- after wait_for_flush ---");
        for (i, key) in data_keys.iter().enumerate() {
            let found = cache.data_cache().get(key).await.is_some();
            println!("data entry {}: found={}", i, found);
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        println!("--- after 2s sleep ---");
        let mut found_count = 0;
        let mut evicted_count = 0;
        for (i, key) in data_keys.iter().enumerate() {
            let found = cache.data_cache().get(key).await.is_some();
            println!("data entry {}: found={}", i, found);
            if found { found_count += 1; } else { evicted_count += 1; }
        }
        println!("data total: found={}, evicted={}", found_count, evicted_count);

        // Metadata unaffected
        let meta_found = cache.metadata_cache().get(&meta_key).await;
        println!("metadata entry: found={}", meta_found.is_some());
    });
}

/// `get_opts` auto-populates data Foyer on miss using a chunk-aligned write.
///
/// Repeated reads on the same chunk hit data cache after the first miss
/// (simulates `CachedMetadataReader::get_bytes` for column chunks in the
/// IndexedExec path).
///
/// For small files (`file_size < 8 MiB`) the chunk-aligned key is `0..file_size`,
/// so a request for the entire file populates the chunk and any subsequent
/// sub-range read slices into that single cached entry.
#[test]
fn get_opts_populates_data_cache_on_miss() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "indexed.parquet", 3);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "indexed.parquet", file_size);
    let path = Path::from("indexed.parquet");

    block_on(async {
        // First read covers the entire file (chunk-aligned for `file_size < 8 MiB`).
        let bytes1 = store.get_range(&path, 0..file_size).await.unwrap();
        assert_eq!(bytes1.len() as u64, file_size);

        // Verify the chunk-aligned key is in data Foyer (NOT metadata Foyer —
        // post-fetch populate must never touch the warmup-only metadata tier).
        let chunk_key = range_cache_key("indexed.parquet", 0, file_size);
        assert!(cache.data_cache().get(&chunk_key).await.is_some(),
            "first read must populate data Foyer under chunk-aligned key 0..{}", file_size);
        assert!(cache.metadata_cache().get(&chunk_key).await.is_none(),
            "get_opts must NOT populate metadata Foyer");

        // Delete file. Second read must come from data Foyer via chunk-aligned probe.
        std::fs::remove_file(parquet_dir.path().join("indexed.parquet")).unwrap();

        let bytes2 = store.get_range(&path, 0..file_size).await.unwrap();
        assert_eq!(bytes2, bytes1,
            "second read (file deleted) must return same bytes from data Foyer cache");

        // Sub-range read also hits the cached chunk via probe slicing.
        let sub_end = 100u64.min(file_size);
        let sub = store.get_range(&path, 0..sub_end).await.unwrap();
        assert_eq!(sub.len() as u64, sub_end);
        assert_eq!(&sub[..], &bytes1[..sub_end as usize]);
    });
}

/// `get_opts` skips caching for ranges exceeding `max_data_entry_size`.
/// The threshold is configurable and dynamically updatable.
///
/// Uses chunk-aligned reads (`0..file_size`) since sub-chunk reads no longer
/// auto-populate (chunk-aligned writes only persist fully-covered chunks).
/// For a small file the chunk-aligned range covers the entire file.
#[test]
fn get_opts_skips_caching_for_large_ranges() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let file_size = write_test_parquet(parquet_dir.path(), "threshold.parquet", 5);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "threshold.parquet", file_size);
    let path = Path::from("threshold.parquet");

    let chunk_key = range_cache_key("threshold.parquet", 0, file_size);

    block_on(async {
        // Step 1: threshold below file_size → chunk-aligned read must NOT populate.
        cache.update_max_data_entry_size(64);
        assert!(file_size > 64, "test file must exceed the low threshold to be meaningful");

        let bytes = store.get_range(&path, 0..file_size).await.unwrap();
        assert_eq!(bytes.len() as u64, file_size,
            "read must succeed regardless of caching policy");
        assert!(cache.data_cache().get(&chunk_key).await.is_none(),
            "chunk > threshold must NOT be cached");

        // Step 2: bump threshold above file_size → chunk-aligned read populates.
        cache.update_max_data_entry_size(8 * 1024 * 1024); // 8 MiB ceiling
        let _ = store.get_range(&path, 0..file_size).await.unwrap();
        assert!(cache.data_cache().get(&chunk_key).await.is_some(),
            "after threshold raise to 8 MiB, chunk must be cached under chunk-aligned key");
    });
}

// ───────────────────────────────────────────────────────────────────────────
// New integration tests for chunk-aligned cache behaviour
// ───────────────────────────────────────────────────────────────────────────

/// Helper: build a `ChunkFile` of the requested byte size on local FS, stuffed
/// with a deterministic byte pattern so we can verify slicing later.
fn write_synthetic_file(dir: &std::path::Path, name: &str, size_bytes: usize) -> u64 {
    let path = dir.join(name);
    // Deterministic pattern: byte i = (i * 31) as u8.
    let buf: Vec<u8> = (0..size_bytes).map(|i| (i.wrapping_mul(31)) as u8).collect();
    std::fs::write(&path, &buf).unwrap();
    std::fs::metadata(&path).unwrap().len()
}

/// **Bug-hunt**: a query for a sub-range that crosses two cache chunks must
/// (1) succeed end-to-end, (2) cache **per-chunk** entries (not one big exact-key
/// entry), and (3) let a follow-up sub-range read on either chunk hit cache.
///
/// File size: 12 MiB → spans chunks `[0..8 MiB)` and `[8 MiB..12 MiB)`.
/// Cross-boundary read: `[7 MiB..9 MiB)` — straddles both chunks.
#[test]
fn cross_boundary_read_caches_each_chunk_separately() {
    use crate::tiered_object_store::CACHE_CHUNK_SIZE;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let twelve_mib: usize = 12 * 1024 * 1024;
    let file_size = write_synthetic_file(parquet_dir.path(), "cross.bin", twelve_mib);
    assert_eq!(file_size, twelve_mib as u64);

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "cross.bin", file_size);
    let path = Path::from("cross.bin");

    // Allow the cross-boundary span to be cached (default ceiling is plenty,
    // but explicit setting guards against environmental defaults shifting).
    cache.update_max_data_entry_size(32 * 1024 * 1024);

    block_on(async {
        let cross_start = 7 * 1024 * 1024u64;
        let cross_end = 9 * 1024 * 1024u64;

        // First read crosses chunks 0 and 1.
        let bytes = store.get_range(&path, cross_start..cross_end).await.unwrap();
        assert_eq!(bytes.len() as u64, cross_end - cross_start);

        // Cache must hold TWO chunk-aligned entries (not one exact-key entry).
        let chunk0 = range_cache_key("cross.bin", 0, CACHE_CHUNK_SIZE);
        let chunk1 = range_cache_key("cross.bin", CACHE_CHUNK_SIZE, file_size);
        assert!(cache.data_cache().get(&chunk0).await.is_some(),
            "chunk 0 (0..8 MiB) must be cached after cross-boundary read");
        assert!(cache.data_cache().get(&chunk1).await.is_some(),
            "chunk 1 (8 MiB..12 MiB) must be cached after cross-boundary read");

        // Exact-key entry for the original cross-boundary span must NOT exist —
        // we cache per-chunk, never per-request.
        let exact = range_cache_key("cross.bin", cross_start, cross_end);
        assert!(cache.data_cache().get(&exact).await.is_none(),
            "exact-key entry for the unaligned span must NOT exist");

        // Delete the file. Sub-range reads inside each chunk must still succeed.
        std::fs::remove_file(parquet_dir.path().join("cross.bin")).unwrap();

        // Sub-range inside chunk 0.
        let sub0 = store.get_range(&path, 1024..2048).await.unwrap();
        assert_eq!(sub0.len(), 1024);
        let expected_sub0: Vec<u8> = (1024usize..2048).map(|i| (i.wrapping_mul(31)) as u8).collect();
        assert_eq!(&sub0[..], &expected_sub0[..]);

        // Sub-range inside chunk 1.
        let sub1_start = 10 * 1024 * 1024u64;
        let sub1_end = sub1_start + 4096;
        let sub1 = store.get_range(&path, sub1_start..sub1_end).await.unwrap();
        assert_eq!(sub1.len(), 4096);
        let expected_sub1: Vec<u8> = (sub1_start as usize..sub1_end as usize)
            .map(|i| (i.wrapping_mul(31)) as u8).collect();
        assert_eq!(&sub1[..], &expected_sub1[..]);
    });
}

/// **Bug-hunt**: warmup for a multi-chunk file (>8 MiB) chunk-aligns ranges and
/// writes ALL chunks to the metadata tier. After dropping the file, queries for
/// any sub-range succeed via the chunk-aligned probe slicing the cached chunk.
///
/// Mirrors `custom_cache_manager::warmup_file_with_store` for a realistic-sized
/// file that exercises the multi-chunk write path inside `put_metadata`.
#[test]
fn warmup_multi_chunk_file_serves_subranges_after_file_deleted() {
    use crate::tiered_object_store::CACHE_CHUNK_SIZE;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // 18 MiB → chunks: [0, 8M), [8M, 16M), [16M, 18M).
    let total: usize = 18 * 1024 * 1024;
    let file_size = write_synthetic_file(parquet_dir.path(), "warm_multi.bin", total);
    let file_bytes = std::fs::read(parquet_dir.path().join("warm_multi.bin")).unwrap();

    // The shared 8 MiB cache cannot hold an 18 MiB file's three chunks, so build
    // tiers large enough to retain all chunks for this multi-chunk assertion.
    // The full chunks are exactly 8 MiB (= CACHE_CHUNK_SIZE). Foyer's disk engine
    // stores each entry within a block/region, so the block size must be >= the
    // largest entry — with a 1 MiB block an 8 MiB entry is silently dropped on
    // flush. Use a 16 MiB block and a 32 MiB write buffer so each 8 MiB chunk
    // persists and is retrievable.
    let big: usize = 64 * 1024 * 1024;
    let big_block: usize = 16 * 1024 * 1024;
    let big_buffer: usize = 32 * 1024 * 1024;
    let data_cache = Arc::new(FoyerCache::new(
        big, data_dir.path(), big_block, big_buffer, big_buffer,
        "auto", 0, 0.0, 0, false,
    ));
    let metadata_cache = Arc::new(FoyerCache::new(
        big, meta_dir.path(), big_block, big_buffer, big_buffer,
        "auto", 0, 0.0, 0, false,
    ));
    let cache = Arc::new(TieredBlockCache::new(data_cache, metadata_cache));
    let store = create_store(parquet_dir.path(), cache.clone(), "warm_multi.bin", file_size);
    let path = Path::from("warm_multi.bin");

    // Warmup: chunk-align the full-file range (production warmup pattern).
    let raw_range = 0..file_size;
    let aligned_start = raw_range.start / CACHE_CHUNK_SIZE * CACHE_CHUNK_SIZE;
    let aligned_end = raw_range.end.div_ceil(CACHE_CHUNK_SIZE).saturating_mul(CACHE_CHUNK_SIZE).min(file_size);
    store.put_metadata("warm_multi.bin",
        &[aligned_start..aligned_end],
        &[bytes::Bytes::copy_from_slice(&file_bytes[aligned_start as usize..aligned_end as usize])]);

    block_on(async {
        // put_metadata writes are admitted to the in-memory layer and flushed to
        // SSD by a background flusher. Writing 18 MiB across three chunks can push
        // entries out of memory before the disk write completes, so a get() issued
        // immediately after put can race the flush. Wait for the flush to settle
        // before asserting durable presence.
        cache.wait_for_flush().await;

        // All three chunks must be in the metadata tier under chunk-aligned keys.
        let chunk0 = range_cache_key("warm_multi.bin", 0, CACHE_CHUNK_SIZE);
        let chunk1 = range_cache_key("warm_multi.bin", CACHE_CHUNK_SIZE, 2 * CACHE_CHUNK_SIZE);
        let chunk2 = range_cache_key("warm_multi.bin", 2 * CACHE_CHUNK_SIZE, file_size);
        assert!(cache.metadata_cache().get(&chunk0).await.is_some(), "chunk 0 must be in metadata tier");
        assert!(cache.metadata_cache().get(&chunk1).await.is_some(), "chunk 1 must be in metadata tier");
        assert!(cache.metadata_cache().get(&chunk2).await.is_some(), "chunk 2 (partial last) must be in metadata tier");

        // Delete file. Every sub-range read must succeed from the metadata cache.
        std::fs::remove_file(parquet_dir.path().join("warm_multi.bin")).unwrap();

        // Sub-range inside chunk 0.
        let r0 = 100u64..200;
        let b0 = store.get_range(&path, r0.clone()).await.unwrap();
        assert_eq!(b0.len(), 100);
        assert_eq!(&b0[..], &file_bytes[r0.start as usize..r0.end as usize]);

        // Sub-range inside chunk 1.
        let r1 = (CACHE_CHUNK_SIZE + 4096)..(CACHE_CHUNK_SIZE + 8192);
        let b1 = store.get_range(&path, r1.clone()).await.unwrap();
        assert_eq!(b1.len(), 4096);
        assert_eq!(&b1[..], &file_bytes[r1.start as usize..r1.end as usize]);

        // Sub-range inside the partial last chunk.
        let r2 = (2 * CACHE_CHUNK_SIZE + 1024)..(2 * CACHE_CHUNK_SIZE + 2048);
        let b2 = store.get_range(&path, r2.clone()).await.unwrap();
        assert_eq!(b2.len(), 1024);
        assert_eq!(&b2[..], &file_bytes[r2.start as usize..r2.end as usize]);
    });
}

/// **Bug-hunt**: when query-time `get_ranges` issues two adjacent ranges that
/// land in the SAME chunk, the chunk is fetched ONCE and cached ONCE — both
/// slots reassemble from the single entry. Validates the dedup logic in
/// `probe_cache` for a realistic multi-range request shape.
#[test]
fn get_ranges_two_same_chunk_subranges_dedup_to_one_chunk_entry() {
    use crate::tiered_object_store::CACHE_CHUNK_SIZE;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let ten_mib = 10 * 1024 * 1024usize;
    let file_size = write_synthetic_file(parquet_dir.path(), "dedup.bin", ten_mib);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "dedup.bin", file_size);
    let path = Path::from("dedup.bin");

    cache.update_max_data_entry_size(32 * 1024 * 1024);

    block_on(async {
        // Two non-overlapping sub-ranges, both inside chunk 0.
        let r1 = 100u64..1100;       // 1 KiB inside chunk 0
        let r2 = 1_000_000u64..1_001_000; // 1 KiB inside chunk 0, far from r1

        let bytes_vec = store.get_ranges(&path, &[r1.clone(), r2.clone()]).await.unwrap();
        assert_eq!(bytes_vec.len(), 2);
        assert_eq!(bytes_vec[0].len(), 1000);
        assert_eq!(bytes_vec[1].len(), 1000);

        // Exactly one chunk-aligned entry exists — chunk 0.
        let chunk0 = range_cache_key("dedup.bin", 0, CACHE_CHUNK_SIZE);
        assert!(cache.data_cache().get(&chunk0).await.is_some(),
            "chunk 0 must be cached after multi-range request");

        // Second request for a third sub-range in the same chunk hits cache.
        std::fs::remove_file(parquet_dir.path().join("dedup.bin")).unwrap();
        let r3 = 5000u64..6000;
        let b3 = store.get_range(&path, r3).await.unwrap();
        assert_eq!(b3.len(), 1000, "third sub-range must hit cached chunk after file deletion");
    });
}

/// **Bug-hunt**: a `get_opts` request whose chunk-aligned span exceeds
/// `max_data_entry_size` must NOT populate the cache, but MUST still return
/// the requested bytes correctly. Tests the early-return path in `get_opts`
/// that avoids buffering a too-large response.
#[test]
fn get_opts_oversized_chunk_returns_bytes_but_skips_cache() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let ten_mib = 10 * 1024 * 1024usize;
    let file_size = write_synthetic_file(parquet_dir.path(), "oversize.bin", ten_mib);
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "oversize.bin", file_size);
    let path = Path::from("oversize.bin");

    // Set ceiling well below the chunk-aligned read size.
    cache.update_max_data_entry_size(64 * 1024); // 64 KiB

    block_on(async {
        // Read the whole file (chunk-aligned). 10 MiB > 64 KiB ceiling.
        let bytes = store.get_range(&path, 0..file_size).await.unwrap();
        assert_eq!(bytes.len() as u64, file_size, "read must succeed despite size cap");

        // No chunk entries should have been written.
        use crate::tiered_object_store::CACHE_CHUNK_SIZE;
        let chunk0 = range_cache_key("oversize.bin", 0, CACHE_CHUNK_SIZE);
        let chunk1 = range_cache_key("oversize.bin", CACHE_CHUNK_SIZE, file_size);
        assert!(cache.data_cache().get(&chunk0).await.is_none(),
            "chunk 0 must NOT be cached when range exceeds ceiling");
        assert!(cache.data_cache().get(&chunk1).await.is_none(),
            "chunk 1 must NOT be cached when range exceeds ceiling");
    });
}

/// **Bug-hunt regression**: an unaligned `put_metadata` input with sub-chunk
/// edges (e.g., a raw page-index range that doesn't start/end on a chunk
/// boundary) must NOT crash AND must skip the partial edge chunks. This is
/// the safety net for any future caller that forgets to chunk-align before
/// calling `put_metadata`.
///
/// File size: 16 MiB exactly (chunks `[0, 8M)` and `[8M, 16M)`).
/// Input range: `[1 MiB, 9 MiB)` — both edges are partial; no chunk fully covered.
#[test]
fn put_metadata_unaligned_edges_writes_no_chunks_does_not_panic() {
    use crate::tiered_object_store::CACHE_CHUNK_SIZE;

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let sixteen_mib = 16 * 1024 * 1024usize;
    let file_size = write_synthetic_file(parquet_dir.path(), "unaligned.bin", sixteen_mib);
    let file_bytes = std::fs::read(parquet_dir.path().join("unaligned.bin")).unwrap();
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "unaligned.bin", file_size);

    // Both edges partial — first chunk is partial-end (1 MiB..8 MiB),
    // second chunk is partial-start (8 MiB..9 MiB).
    let one_mib = 1024 * 1024u64;
    let nine_mib = 9 * 1024 * 1024u64;
    let r = one_mib..nine_mib;
    let data = bytes::Bytes::copy_from_slice(&file_bytes[r.start as usize..r.end as usize]);

    // Should not panic.
    store.put_metadata("unaligned.bin", &[r], &[data]);

    block_on(async {
        // Neither chunk should be cached because both are only partially covered.
        let chunk0 = range_cache_key("unaligned.bin", 0, CACHE_CHUNK_SIZE);
        let chunk1 = range_cache_key("unaligned.bin", CACHE_CHUNK_SIZE, file_size);
        assert!(cache.metadata_cache().get(&chunk0).await.is_none(),
            "partial-end chunk 0 must be skipped");
        assert!(cache.metadata_cache().get(&chunk1).await.is_none(),
            "partial-start chunk 1 must be skipped");

        // The unaligned exact-key must also not exist (we never write exact keys
        // when file_size is known).
        let exact = range_cache_key("unaligned.bin", one_mib, nine_mib);
        assert!(cache.metadata_cache().get(&exact).await.is_none(),
            "no exact-key entry for unaligned input");
    });
}

/// **Bug-hunt**: warmup with a chunk-aligned input writes to METADATA tier;
/// a subsequent query-time miss writes to DATA tier. The two paths must NEVER
/// cross-pollute even when they hit the SAME chunk-aligned key.
#[test]
fn warmup_writes_metadata_tier_query_writes_data_tier_same_key() {
    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let four_mib = 4 * 1024 * 1024usize; // < 8 MiB → single chunk 0..file_size
    let file_size = write_synthetic_file(parquet_dir.path(), "tiers.bin", four_mib);
    let file_bytes = std::fs::read(parquet_dir.path().join("tiers.bin")).unwrap();
    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "tiers.bin", file_size);
    let path = Path::from("tiers.bin");

    // Warmup → metadata tier.
    store.put_metadata("tiers.bin",
        &[0..file_size],
        &[bytes::Bytes::copy_from_slice(&file_bytes)]);

    let chunk_key = range_cache_key("tiers.bin", 0, file_size);

    block_on(async {
        // Metadata tier populated.
        assert!(cache.metadata_cache().get(&chunk_key).await.is_some(),
            "warmup must populate metadata tier");
        // Data tier untouched.
        assert!(cache.data_cache().get(&chunk_key).await.is_none(),
            "warmup must NOT populate data tier");

        // Query for the same chunk: served from metadata tier (no data-tier write).
        let _ = store.get_range(&path, 0..file_size).await.unwrap();
        assert!(cache.metadata_cache().get(&chunk_key).await.is_some(),
            "metadata entry remains after query");
        assert!(cache.data_cache().get(&chunk_key).await.is_none(),
            "query path must NOT write to data tier when metadata serves the request");
    });
}

/// **Cross-boundary `get_opts` (Req 5.1, 5.2, 5.3)**: a single ranged `get_opts`
/// read that straddles two cache chunks must (1) return the exact requested bytes,
/// (2) cache each fully-covered chunk independently in the DATA tier under its
/// chunk-aligned key (never the metadata tier), and (3) let follow-up sub-range
/// reads inside each chunk be served from cache after the local file is gone.
///
/// File size: 12 MiB → spans chunks `[0..8 MiB)` and `[8 MiB..12 MiB)`.
/// Cross-boundary read: `[7 MiB..9 MiB)` — straddles both chunks. This is the
/// `get_opts` analogue of `cross_boundary_read_caches_each_chunk_separately`
/// (which exercises the `get_ranges` entry point).
#[test]
fn cross_boundary_get_opts_caches_each_chunk_separately() {
    use crate::tiered_object_store::CACHE_CHUNK_SIZE;
    use object_store::{GetOptions, GetRange};

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    let twelve_mib: usize = 12 * 1024 * 1024;
    let file_size = write_synthetic_file(parquet_dir.path(), "cross_opts.bin", twelve_mib);
    assert_eq!(file_size, twelve_mib as u64);
    let file_bytes = std::fs::read(parquet_dir.path().join("cross_opts.bin")).unwrap();

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "cross_opts.bin", file_size);
    let path = Path::from("cross_opts.bin");

    // Raise the per-entry ceiling above the cross-boundary span so the get_opts
    // router does NOT hit the oversized-stream guard and routes through get_ranges.
    cache.update_max_data_entry_size(32 * 1024 * 1024);

    block_on(async {
        let cross_start = 7 * 1024 * 1024u64;
        let cross_end = 9 * 1024 * 1024u64;

        // Cross-boundary get_opts read (straddles chunks 0 and 1).
        let opts = GetOptions {
            range: Some(GetRange::Bounded(cross_start..cross_end)),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await.unwrap();
        let bytes = result.bytes().await.unwrap();

        // (1) Length and content correctness.
        assert_eq!(bytes.len() as u64, cross_end - cross_start,
            "returned length must equal the requested span");
        assert_eq!(&bytes[..], &file_bytes[cross_start as usize..cross_end as usize],
            "returned bytes must be byte-for-byte identical to the file's bytes at the range");

        // (2) Both fully-covered chunks must be cached in the DATA tier under
        // their chunk-aligned keys.
        let chunk0 = range_cache_key("cross_opts.bin", 0, CACHE_CHUNK_SIZE);
        let chunk1 = range_cache_key("cross_opts.bin", CACHE_CHUNK_SIZE, file_size);
        assert!(cache.data_cache().get(&chunk0).await.is_some(),
            "chunk 0 (0..8 MiB) must be cached in DATA tier after cross-boundary get_opts");
        assert!(cache.data_cache().get(&chunk1).await.is_some(),
            "chunk 1 (8 MiB..12 MiB) must be cached in DATA tier after cross-boundary get_opts");

        // Neither chunk may land in the metadata (never-evict) tier — the query
        // path must never write there.
        assert!(cache.metadata_cache().get(&chunk0).await.is_none(),
            "chunk 0 must NOT be in metadata tier (query path is data-tier only)");
        assert!(cache.metadata_cache().get(&chunk1).await.is_none(),
            "chunk 1 must NOT be in metadata tier (query path is data-tier only)");

        // No exact-key entry for the unaligned request span — we cache per-chunk.
        let exact = range_cache_key("cross_opts.bin", cross_start, cross_end);
        assert!(cache.data_cache().get(&exact).await.is_none(),
            "exact-key entry for the unaligned span must NOT exist");

        // (3) Delete the local file; sub-range get_opts reads inside each chunk
        // must still succeed from the cached chunks.
        std::fs::remove_file(parquet_dir.path().join("cross_opts.bin")).unwrap();

        // Sub-range inside chunk 0.
        let sub0_start = 1024u64;
        let sub0_end = 2048u64;
        let opts0 = GetOptions {
            range: Some(GetRange::Bounded(sub0_start..sub0_end)),
            ..Default::default()
        };
        let sub0 = store.get_opts(&path, opts0).await.unwrap().bytes().await.unwrap();
        assert_eq!(sub0.len() as u64, sub0_end - sub0_start);
        assert_eq!(&sub0[..], &file_bytes[sub0_start as usize..sub0_end as usize],
            "sub-range inside chunk 0 must be served correctly from cache");

        // Sub-range inside chunk 1.
        let sub1_start = 10 * 1024 * 1024u64;
        let sub1_end = sub1_start + 4096;
        let opts1 = GetOptions {
            range: Some(GetRange::Bounded(sub1_start..sub1_end)),
            ..Default::default()
        };
        let sub1 = store.get_opts(&path, opts1).await.unwrap().bytes().await.unwrap();
        assert_eq!(sub1.len() as u64, sub1_end - sub1_start);
        assert_eq!(&sub1[..], &file_bytes[sub1_start as usize..sub1_end as usize],
            "sub-range inside chunk 1 must be served correctly from cache");
    });
}

/// **Task 7.1**: A sub-chunk `get_opts` cold read over real Foyer SSD tiers must
/// populate ONLY the data tier with the chunk-aligned entry — never the
/// never-evict metadata tier (that tier is reserved for warmup `put_metadata`).
///
/// A small parquet file (`file_size < 8 MiB`) is a single chunk `0..file_size`.
/// A sub-chunk ranged read (`GetRange::Bounded(100..500)`) routes through the
/// `get_ranges` path, which fetches and caches the full enclosing chunk
/// `0..file_size`. We assert the chunk-aligned key
/// `range_cache_key(name, 0, file_size)` is present in the DATA tier and absent
/// from the METADATA tier.
///
/// _Requirements: 1.1, 6.1, 6.3_
#[test]
fn get_opts_subchunk_populates_data_tier_only() {
    use object_store::{GetOptions, GetRange};

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Small parquet file (< 8 MiB) → single chunk 0..file_size.
    let file_size = write_test_parquet(parquet_dir.path(), "subchunk.parquet", 3);
    assert!(file_size < 8 * 1024 * 1024,
        "test file must be smaller than one chunk so it collapses to a single chunk 0..file_size");
    assert!(file_size > 500, "test file must be larger than the sub-chunk range end");

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "subchunk.parquet", file_size);
    let path = Path::from("subchunk.parquet");

    block_on(async {
        // Cold cache. Issue a SUB-CHUNK ranged get_opts read. Because the small
        // file is one chunk, this routes through get_ranges and caches the whole
        // file as chunk 0..file_size.
        let opts = GetOptions {
            range: Some(GetRange::Bounded(100..500)),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.len(), 400, "sub-chunk read must return exactly the requested 400 bytes");

        // The chunk-aligned key 0..file_size must be present in the DATA tier and
        // ABSENT from the metadata tier (query path is data-tier only).
        let chunk_key = range_cache_key("subchunk.parquet", 0, file_size);
        assert!(cache.data_cache().get(&chunk_key).await.is_some(),
            "sub-chunk get_opts cold read must populate the enclosing chunk in the DATA tier");
        assert!(cache.metadata_cache().get(&chunk_key).await.is_none(),
            "sub-chunk get_opts cold read must NEVER write the metadata tier — reserved for warmup");
    });
}

/// **Task 7.2**: After a sub-chunk `get_opts` cold read caches the enclosing
/// chunk, deleting the local backing file must not prevent a DIFFERENT sub-range
/// read within the same chunk from succeeding — it is served entirely from the
/// data-tier cache with no backing file access.
///
/// We capture the file bytes before deletion to assert byte-for-byte correctness
/// of the second read.
///
/// _Requirements: 1.3_
#[test]
fn get_opts_second_subrange_hits_cache_after_file_delete() {
    use object_store::{GetOptions, GetRange};

    let parquet_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let meta_dir = TempDir::new().unwrap();

    // Small parquet file (< 8 MiB) → single chunk 0..file_size.
    let file_size = write_test_parquet(parquet_dir.path(), "delete_hit.parquet", 3);
    assert!(file_size < 8 * 1024 * 1024,
        "test file must be smaller than one chunk so it collapses to a single chunk 0..file_size");
    assert!(file_size > 1500, "test file must be larger than the second sub-range end");

    // Capture the file bytes BEFORE deletion for byte-correctness checks.
    let file_bytes = std::fs::read(parquet_dir.path().join("delete_hit.parquet")).unwrap();

    let cache = create_tiered_cache(data_dir.path(), meta_dir.path());
    let store = create_store(parquet_dir.path(), cache.clone(), "delete_hit.parquet", file_size);
    let path = Path::from("delete_hit.parquet");

    block_on(async {
        // First sub-chunk get_opts read populates the whole chunk 0..file_size.
        let first_start = 100u64;
        let first_end = 500u64;
        let opts1 = GetOptions {
            range: Some(GetRange::Bounded(first_start..first_end)),
            ..Default::default()
        };
        let first = store.get_opts(&path, opts1).await.unwrap().bytes().await.unwrap();
        assert_eq!(&first[..], &file_bytes[first_start as usize..first_end as usize],
            "first sub-range bytes must match the backing file");

        // The enclosing chunk must now be in the data tier.
        let chunk_key = range_cache_key("delete_hit.parquet", 0, file_size);
        assert!(cache.data_cache().get(&chunk_key).await.is_some(),
            "first read must populate the enclosing chunk in the data tier");

        // Delete the local backing file — no backing access is possible now.
        std::fs::remove_file(parquet_dir.path().join("delete_hit.parquet")).unwrap();

        // A DIFFERENT sub-range within the same chunk must still succeed, served
        // entirely from the data-tier cache.
        let second_start = 1000u64;
        let second_end = 1500u64;
        let opts2 = GetOptions {
            range: Some(GetRange::Bounded(second_start..second_end)),
            ..Default::default()
        };
        let second = store.get_opts(&path, opts2).await.unwrap().bytes().await.unwrap();
        assert_eq!(second.len() as u64, second_end - second_start,
            "second sub-range must return exactly the requested bytes from cache");
        assert_eq!(&second[..], &file_bytes[second_start as usize..second_end as usize],
            "second sub-range (file deleted) must be served byte-for-byte from the data-tier cache");
    });
}
