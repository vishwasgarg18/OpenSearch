use super::*;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{CopyOptions, ObjectStoreExt, PutPayload};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

/// Helper: create a registry + tiered store backed by in-memory stores.
fn setup() -> (
    Arc<TieredStorageRegistry>,
    Arc<InMemory>,
    Arc<InMemory>,
    TieredObjectStore,
) {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let remote = Arc::new(InMemory::new());
    let tiered = TieredObjectStore::new(Arc::clone(&registry), Arc::clone(&local) as _);
    tiered.set_remote(Arc::clone(&remote) as _);
    (registry, local, remote, tiered)
}

// -- Routing tests ------------------------------------------------------

#[tokio::test]
async fn test_get_opts_routes_to_remote_for_remote_file() {
    let (_registry, _local, remote, tiered) = setup();

    let remote_path = Path::from("remote/a.parquet");
    remote
        .put(&remote_path, PutPayload::from_static(b"remote-data"))
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let result = tiered
        .get_opts(&Path::from("a.parquet"), GetOptions::default())
        .await
        .unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"remote-data");
}

#[tokio::test]
async fn test_get_opts_routes_to_local_when_not_in_registry() {
    let (_registry, local, _remote, tiered) = setup();

    local
        .put(
            &Path::from("local.parquet"),
            PutPayload::from_static(b"local-data"),
        )
        .await
        .unwrap();

    let result = tiered
        .get_opts(&Path::from("local.parquet"), GetOptions::default())
        .await
        .unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"local-data");
}

#[tokio::test]
async fn test_get_opts_routes_to_local_for_local_file() {
    let (_registry, local, _remote, tiered) = setup();

    local
        .put(
            &Path::from("a.parquet"),
            PutPayload::from_static(b"local-data"),
        )
        .await
        .unwrap();

    tiered
        .register_file("a.parquet", FileLocation::Local, None)
        .unwrap();

    let result = tiered
        .get_opts(&Path::from("a.parquet"), GetOptions::default())
        .await
        .unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"local-data");
}

// -- Ref count balance --------------------------------------------------

#[tokio::test]
async fn test_successful_remote_read_releases_ref_count() {
    let (registry, _local, remote, tiered) = setup();

    remote
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"data"),
        )
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let _ = tiered
        .get_opts(&Path::from("a.parquet"), GetOptions::default())
        .await
        .unwrap();

    // Guard was dropped, ref count should be 0.
    let guard = registry.get("a.parquet").unwrap();
    // This guard adds 1, so underlying should have been 0 before.
    assert_eq!(guard.ref_count(), 1);
}

// -- Head ---------------------------------------------------------------

#[tokio::test]
async fn test_head_returns_local_metadata() {
    let (_registry, local, _remote, tiered) = setup();

    local
        .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
        .await
        .unwrap();

    let meta = tiered.head(&Path::from("a.parquet")).await.unwrap();
    assert_eq!(meta.size, 4);
}

#[tokio::test]
async fn test_head_falls_back_to_remote() {
    let (_registry, _local, remote, tiered) = setup();

    remote
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"remote-data"),
        )
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let meta = tiered.head(&Path::from("a.parquet")).await.unwrap();
    assert_eq!(meta.size, 11);
}

#[tokio::test]
async fn test_head_not_found() {
    let (_registry, _local, _remote, tiered) = setup();
    let result = tiered.head(&Path::from("nonexistent")).await;
    assert!(result.is_err());
}

// -- Put ----------------------------------------------------------------

#[tokio::test]
async fn test_put_writes_local_and_registers() {
    let (registry, local, _remote, tiered) = setup();

    tiered
        .put_opts(
            &Path::from("new.parquet"),
            PutPayload::from_static(b"new-data"),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let result = local.get(&Path::from("new.parquet")).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"new-data");
    assert_eq!(registry.len(), 1);
}

// -- Delete -------------------------------------------------------------

#[tokio::test]
async fn test_delete_removes_registry_entry_only() {
    let (registry, local, _remote, tiered) = setup();

    local
        .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
        .await
        .unwrap();
    tiered
        .register_file("a.parquet", FileLocation::Local, None)
        .unwrap();

    tiered.delete(&Path::from("a.parquet")).await.unwrap();
    assert_eq!(registry.len(), 0, "registry entry should be removed");

    // Local file should still exist (delete only removes registry entry).
    let result = local.get(&Path::from("a.parquet")).await;
    assert!(result.is_ok(), "local file should still exist");
}

// -- put_multipart_opts -------------------------------------------------

#[tokio::test]
async fn test_put_multipart_opts_not_supported() {
    let (_registry, _local, _remote, tiered) = setup();
    let result = tiered
        .put_multipart_opts(&Path::from("a"), PutMultipartOptions::default())
        .await;
    assert!(matches!(
        result,
        Err(object_store::Error::NotSupported { .. })
    ));
}

// -- Range reads --------------------------------------------------------

#[tokio::test]
async fn test_get_range_from_remote() {
    let (_registry, _local, remote, tiered) = setup();

    remote
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"0123456789"),
        )
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let bytes = tiered
        .get_range(&Path::from("a.parquet"), 2..5)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"234");
}

#[tokio::test]
async fn test_get_ranges_empty_returns_empty() {
    let (_registry, local, _remote, tiered) = setup();

    local
        .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
        .await
        .unwrap();

    let result = tiered
        .get_ranges(&Path::from("a.parquet"), &[])
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_get_ranges_multiple_from_remote() {
    let (_registry, _local, remote, tiered) = setup();

    remote
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"0123456789"),
        )
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let results = tiered
        .get_ranges(&Path::from("a.parquet"), &[0..3, 5..8])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref(), b"012");
    assert_eq!(results[1].as_ref(), b"567");
}

// -- Copy/rename not supported ------------------------------------------

#[tokio::test]
async fn test_copy_returns_not_supported() {
    let (_registry, _local, _remote, tiered) = setup();
    let result = tiered.copy(&Path::from("a"), &Path::from("b")).await;
    assert!(matches!(
        result,
        Err(object_store::Error::NotSupported { .. })
    ));
}

#[tokio::test]
async fn test_rename_returns_not_supported() {
    let (_registry, _local, _remote, tiered) = setup();
    let result = tiered
        .rename_if_not_exists(&Path::from("a"), &Path::from("b"))
        .await;
    assert!(matches!(
        result,
        Err(object_store::Error::NotSupported { .. })
    ));
}

// -- List tests ---------------------------------------------------------

#[tokio::test]
async fn test_list_includes_remote_only_files() {
    let (_registry, local, remote, tiered) = setup();

    local
        .put(
            &Path::from("data/local.parquet"),
            PutPayload::from_static(b"local"),
        )
        .await
        .unwrap();
    // Register local file in registry — list() returns registry entries only
    tiered
        .register_file("data/local.parquet", FileLocation::Local, None)
        .unwrap();

    remote
        .put(
            &Path::from("remote/evicted.parquet"),
            PutPayload::from_static(b"remote-data"),
        )
        .await
        .unwrap();
    tiered
        .register_file(
            "data/evicted.parquet",
            FileLocation::Remote,
            Some("remote/evicted.parquet".into()),
        )
        .unwrap();

    let results: Vec<ObjectMeta> = tiered
        .list(Some(&Path::from("data")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let paths: Vec<String> = results.iter().map(|m| m.location.to_string()).collect();
    assert!(paths.contains(&"data/local.parquet".to_string()));
    assert!(paths.contains(&"data/evicted.parquet".to_string()));
}

#[tokio::test]
async fn test_list_no_duplicates_for_local_files() {
    let (_registry, local, _remote, tiered) = setup();

    local
        .put(
            &Path::from("data/a.parquet"),
            PutPayload::from_static(b"data"),
        )
        .await
        .unwrap();
    tiered
        .register_file("data/a.parquet", FileLocation::Local, None)
        .unwrap();

    let results: Vec<ObjectMeta> = tiered
        .list(Some(&Path::from("data")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let count = results
        .iter()
        .filter(|m| m.location.as_ref() == "data/a.parquet")
        .count();
    assert_eq!(count, 1, "local file should appear exactly once");
}

#[tokio::test]
async fn test_list_with_delimiter_includes_remote() {
    let (_registry, local, remote, tiered) = setup();

    local
        .put(
            &Path::from("data/local.parquet"),
            PutPayload::from_static(b"local"),
        )
        .await
        .unwrap();

    remote
        .put(
            &Path::from("remote/evicted.parquet"),
            PutPayload::from_static(b"remote-data"),
        )
        .await
        .unwrap();
    tiered
        .register_file(
            "data/evicted.parquet",
            FileLocation::Remote,
            Some("remote/evicted.parquet".into()),
        )
        .unwrap();

    let result = tiered
        .list_with_delimiter(Some(&Path::from("data")))
        .await
        .unwrap();

    let paths: Vec<String> = result
        .objects
        .iter()
        .map(|m| m.location.to_string())
        .collect();
    assert!(paths.contains(&"data/local.parquet".to_string()));
    assert!(paths.contains(&"data/evicted.parquet".to_string()));
}

// -- Concurrency --------------------------------------------------------

#[tokio::test]
async fn test_concurrent_get_opts_on_same_remote_file() {
    let (registry, _local, remote, tiered) = setup();
    let tiered = Arc::new(tiered);

    remote
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"data"),
        )
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..16 {
        let t = Arc::clone(&tiered);
        handles.push(tokio::spawn(async move {
            t.get_opts(&Path::from("a.parquet"), GetOptions::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        }));
    }

    for h in handles {
        let bytes = h.await.unwrap();
        assert_eq!(bytes.as_ref(), b"data");
    }

    // All reads done, ref count should be 0.
    let guard = registry.get("a.parquet").unwrap();
    assert_eq!(guard.ref_count(), 1); // Only this guard.
}

// -- Mock store for call tracking ---------------------------------------

#[derive(Debug)]
struct CallCountingStore {
    inner: InMemory,
    get_count: AtomicUsize,
}

impl CallCountingStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            get_count: AtomicUsize::new(0),
        }
    }
}

impl fmt::Display for CallCountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CallCountingStore")
    }
}

#[async_trait]
impl ObjectStore for CallCountingStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.get_count.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn test_mock_store_exactly_one_call_per_get_opts() {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let mock_remote = Arc::new(CallCountingStore::new());

    mock_remote
        .inner
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"data"),
        )
        .await
        .unwrap();

    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&mock_remote) as _);
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let _ = tiered
        .get_opts(&Path::from("a.parquet"), GetOptions::default())
        .await
        .unwrap();

    assert_eq!(
        mock_remote.get_count.load(AtomicOrdering::SeqCst),
        1,
        "exactly 1 call to remote get_opts"
    );
}

// -- Error store --------------------------------------------------------

#[derive(Debug)]
struct ErrorStore;

impl fmt::Display for ErrorStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ErrorStore")
    }
}

#[async_trait]
impl ObjectStore for ErrorStore {
    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> OsResult<GetResult> {
        Err(object_store::Error::Generic {
            store: "ErrorStore",
            source: "simulated error".into(),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        Err(object_store::Error::Generic {
            store: "ErrorStore",
            source: "simulated error".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::Generic {
            store: "ErrorStore",
            source: "simulated error".into(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        Box::pin(locations.map(|_| Err(object_store::Error::Generic {
            store: "ErrorStore",
            source: "simulated error".into(),
        })))
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        futures::stream::empty().boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> OsResult<ListResult> {
        Ok(ListResult {
            common_prefixes: vec![],
            objects: vec![],
        })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> OsResult<()> {
        Err(object_store::Error::NotSupported {
            source: "not supported".into(),
        })
    }
}

#[tokio::test]
async fn test_error_store_guard_still_releases() {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let result = tiered
        .get_opts(&Path::from("a.parquet"), GetOptions::default())
        .await;
    assert!(result.is_err());

    // Guard was dropped before the remote call, so ref count should be 0.
    let guard = registry.get("a.parquet").unwrap();
    assert_eq!(guard.ref_count(), 1); // Only this guard.
}

// -- register_file validation -------------------------------------------

#[test]
fn test_register_file_remote_without_remote_path_returns_err() {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let tiered = TieredObjectStore::new(registry, local as _);
    let result = tiered.register_file(
        "/a.parquet",
        FileLocation::Remote,
        None,
    );
    assert!(result.is_err());
}

// -- Error / edge-case tests --------------------------------------------

#[tokio::test]
async fn test_failed_remote_read_not_found_still_completes() {
    let (registry, _local, _remote, tiered) = setup();

    // Register a Remote file pointing to a path that doesn't exist on the remote store.
    tiered
        .register_file(
            "missing.parquet",
            FileLocation::Remote,
            Some("remote/nonexistent.parquet".into()),
        )
        .unwrap();

    let result = tiered
        .get_opts(&Path::from("missing.parquet"), GetOptions::default())
        .await;
    assert!(result.is_err(), "should error for non-existent remote path");

    // Registry entry still exists (guard was dropped before I/O, entry not removed).
    assert_eq!(registry.len(), 1);
    assert!(registry.get("missing.parquet").is_some());
}

#[tokio::test]
async fn test_get_range_error_from_remote_still_completes() {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let result = tiered.get_range(&Path::from("a.parquet"), 0..10).await;
    assert!(result.is_err(), "ErrorStore should return an error");

    // Registry entry still exists.
    assert_eq!(registry.len(), 1);
    assert!(registry.get("a.parquet").is_some());
}

#[tokio::test]
async fn test_get_ranges_error_from_remote_still_completes() {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let result = tiered
        .get_ranges(&Path::from("a.parquet"), &[0..5, 5..10])
        .await;
    assert!(result.is_err(), "ErrorStore should return an error");

    // Registry entry still exists.
    assert_eq!(registry.len(), 1);
    assert!(registry.get("a.parquet").is_some());
}

#[tokio::test]
async fn test_head_remote_fallback_error_still_completes() {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

    // File not found locally. Register as Remote with ErrorStore.
    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    let result = tiered.head(&Path::from("a.parquet")).await;
    assert!(result.is_err(), "head should fail when remote errors");

    // Registry entry still exists.
    assert_eq!(registry.len(), 1);
    assert!(registry.get("a.parquet").is_some());
}

#[tokio::test]
async fn test_concurrent_read_and_delete() {
    let (registry, _local, remote, tiered) = setup();
    let tiered = Arc::new(tiered);

    remote
        .put(
            &Path::from("remote/a.parquet"),
            PutPayload::from_static(b"data"),
        )
        .await
        .unwrap();

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
        )
        .unwrap();

    // Spawn 16 concurrent reads.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let t = Arc::clone(&tiered);
        handles.push(tokio::spawn(async move {
            // Each read may succeed or fail depending on timing with delete;
            // the important thing is no panic.
            let _ = t
                .get_opts(&Path::from("a.parquet"), GetOptions::default())
                .await;
        }));
    }

    // While reads are in flight, delete the file.
    tiered.delete(&Path::from("a.parquet")).await.unwrap();

    // All reads should complete (Arc keeps store alive). No panics.
    for h in handles {
        h.await.expect("task should not panic");
    }

    // After all reads finish, registry should have 0 entries.
    assert_eq!(registry.len(), 0);
}

// -- ReadGuard scope / ref-count tests ----------------------------------

#[test]
fn test_guard_releases_on_scope_exit() {
    let registry = TieredStorageRegistry::new();
    registry.register("a.parquet", local_entry());

    {
        let guard = registry.get("a.parquet").unwrap();
        assert_eq!(guard.ref_count(), 1);
        // guard drops here
    }

    // Get another guard — ref_count should be 1, not 2.
    let guard2 = registry.get("a.parquet").unwrap();
    assert_eq!(guard2.ref_count(), 1, "previous guard should have released");
}

#[test]
fn test_delete_during_active_guard() {
    let registry = TieredStorageRegistry::new();
    registry.register("a.parquet", local_entry());

    // Simulate an active reader via manual acquire (doesn't hold DashMap Ref).
    registry.update("a.parquet", |e| {
        e.acquire();
    });

    // Force-remove while ref_count > 0.
    let removed = registry.remove("a.parquet", true);
    assert!(
        removed,
        "force remove should succeed even with active ref count"
    );

    // Entry is gone.
    assert_eq!(registry.len(), 0);

    // A subsequent get returns None — no crash.
    assert!(registry.get("a.parquet").is_none());
}

// Helper: create a local entry (reused by guard tests above).
fn local_entry() -> TieredFileEntry {
    TieredFileEntry::new(FileLocation::Local, None)
}

// -- head() directory existence check tests ---------------------------------

#[tokio::test]
async fn test_head_directory_path_returns_synthetic_when_registry_has_entries() {
    let (registry, _local, _remote, tiered) = setup();

    // Register a file so registry is non-empty
    let entry = TieredFileEntry::with_size(FileLocation::Remote, Some(Arc::from("remote/a.parquet")), 1024);
    registry.register("data/parquet/a.parquet", entry);

    // head() on a directory path should return NotFound — DataFusion uses list()
    // to discover files in directories, not head(). Returning NotFound tells
    // DataFusion "this is not a file" and it proceeds to list().
    let result = tiered.head(&Path::from("data/parquet")).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), object_store::Error::NotFound { .. }));
}

#[tokio::test]
async fn test_head_directory_path_with_trailing_slash() {
    let (registry, _local, _remote, tiered) = setup();

    let entry = TieredFileEntry::with_size(FileLocation::Remote, Some(Arc::from("remote/b.parquet")), 2048);
    registry.register("data/parquet/b.parquet", entry);

    // Trailing slash also treated as directory — returns NotFound
    let result = tiered.head(&Path::from("data/parquet/")).await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), object_store::Error::NotFound { .. }));
}

#[tokio::test]
async fn test_head_directory_path_returns_not_found_when_registry_empty() {
    let (_registry, _local, _remote, tiered) = setup();

    // Registry is empty — directory doesn't "exist"
    let result = tiered.head(&Path::from("data/parquet")).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_head_file_path_not_treated_as_directory() {
    let (registry, _local, _remote, tiered) = setup();

    // Register a file so registry is non-empty
    let entry = TieredFileEntry::with_size(FileLocation::Remote, Some(Arc::from("remote/c.parquet")), 512);
    registry.register("data/parquet/c.parquet", entry);

    // head() on a file path (has extension) should NOT use directory check
    // — it should try registry lookup, then remote, then local
    let result = tiered.head(&Path::from("data/parquet/nonexistent.parquet")).await;
    // Not in registry, not local → NotFound
    assert!(result.is_err());
}

// -- Chunk-aligned caching tests -------------------------------------------

use opensearch_block_cache::range_cache::{range_cache_key, CacheKey};
use opensearch_block_cache::traits::BlockCache;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

/// Simple in-memory BlockCache for testing chunk caching behavior.
///
/// Tracks which tier each key was written to so tests can assert that
/// `put_metadata` calls actually go to the metadata tier (distinct from `put`).
#[derive(Debug)]
struct MockBlockCache {
    store: Mutex<HashMap<String, Bytes>>,
    /// Subset of `store` keys that were written via `put_metadata` (metadata tier).
    /// A key in `store` not in `metadata_keys` was written via `put` (data tier).
    metadata_keys: Mutex<std::collections::HashSet<String>>,
}

impl MockBlockCache {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            metadata_keys: Mutex::new(std::collections::HashSet::new()),
        }
    }

    fn len(&self) -> usize {
        self.store.lock().unwrap().len()
    }

    fn keys(&self) -> Vec<String> {
        self.store.lock().unwrap().keys().cloned().collect()
    }

    fn metadata_tier_keys(&self) -> Vec<String> {
        self.metadata_keys.lock().unwrap().iter().cloned().collect()
    }

    fn data_tier_keys(&self) -> Vec<String> {
        let store = self.store.lock().unwrap();
        let meta = self.metadata_keys.lock().unwrap();
        store.keys()
            .filter(|k| !meta.contains(k.as_str()))
            .cloned()
            .collect()
    }
}

impl BlockCache for MockBlockCache {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn get<'a>(&'a self, key: &'a CacheKey)
        -> Pin<Box<dyn std::future::Future<Output = Option<Bytes>> + Send + 'a>>
    {
        Box::pin(async move {
            self.store.lock().unwrap().get(key.as_str()).cloned()
        })
    }

    fn put(&self, key: &CacheKey, data: Bytes) {
        let key_str = key.as_str().to_string();
        self.metadata_keys.lock().unwrap().remove(&key_str); // override any prior metadata-tier write
        self.store.lock().unwrap().insert(key_str, data);
    }

    fn put_metadata(&self, key: &CacheKey, data: Bytes) {
        let key_str = key.as_str().to_string();
        self.store.lock().unwrap().insert(key_str.clone(), data);
        self.metadata_keys.lock().unwrap().insert(key_str);
    }

    fn evict_prefix(&self, prefix: &str) {
        self.store.lock().unwrap().retain(|k, _| !k.starts_with(prefix));
        self.metadata_keys.lock().unwrap().retain(|k| !k.starts_with(prefix));
    }

    fn clear(&self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.store.lock().unwrap().clear();
            self.metadata_keys.lock().unwrap().clear();
        })
    }
}

fn setup_with_cache() -> (
    Arc<TieredStorageRegistry>,
    Arc<InMemory>,
    Arc<MockBlockCache>,
    TieredObjectStore,
) {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let cache = Arc::new(MockBlockCache::new());
    let tiered = TieredObjectStore::new(Arc::clone(&registry), Arc::clone(&local) as _)
        .with_cache(Arc::clone(&cache) as _);
    (registry, local, cache, tiered)
}

#[tokio::test]
async fn test_chunk_alignment_subrange_hits_from_cached_chunk() {
    let (registry, local, cache, tiered) = setup_with_cache();

    // 16 MiB file (spans 2 chunks)
    let file_size: u64 = 16 << 20;
    let data = vec![0xABu8; file_size as usize];
    local.put(&Path::from("f.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("f.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // First read: 0..100 — triggers fetch of aligned chunk 0..8MiB
    let r1 = tiered.get_ranges(&Path::from("f.parquet"), &[0..100]).await.unwrap();
    assert_eq!(r1[0].len(), 100);
    assert_eq!(cache.len(), 1);

    // Second read: 500..600 — same chunk, should be a cache hit (no new fetch)
    let r2 = tiered.get_ranges(&Path::from("f.parquet"), &[500..600]).await.unwrap();
    assert_eq!(r2[0].len(), 100);
    assert_eq!(cache.len(), 1, "no new cache entry — served from existing chunk");
}

#[tokio::test]
async fn test_chunk_alignment_multiple_ranges_same_chunk_deduplicates() {
    let (registry, local, cache, tiered) = setup_with_cache();

    let file_size: u64 = 16 << 20;
    let data = vec![0xCDu8; file_size as usize];
    local.put(&Path::from("g.parquet"), PutPayload::from(data)).await.unwrap();
    registry.register("g.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Request 3 ranges all within the same 8MiB chunk — only one chunk fetched
    let results = tiered.get_ranges(&Path::from("g.parquet"), &[0..10, 1000..2000, 5_000_000..5_000_100]).await.unwrap();
    assert_eq!(results[0].len(), 10);
    assert_eq!(results[1].len(), 1000);
    assert_eq!(results[2].len(), 100);
    assert_eq!(cache.len(), 1, "only one chunk should be cached");
}

#[tokio::test]
async fn test_chunk_alignment_file_size_unknown_uses_exact_keys() {
    let (registry, local, cache, tiered) = setup_with_cache();

    // Register file WITHOUT size (size = 0)
    let data = vec![0xEFu8; 10000];
    local.put(&Path::from("h.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("h.parquet", TieredFileEntry::new(FileLocation::Local, None));

    // Read — should use exact key since file_size is unknown
    let r = tiered.get_ranges(&Path::from("h.parquet"), &[0..100]).await.unwrap();
    assert_eq!(r[0].len(), 100);

    // Cache key should be exact: "h.parquet\x1F0-100"
    let exact_key = range_cache_key("h.parquet", 0, 100);
    assert!(cache.store.lock().unwrap().contains_key(exact_key.as_str()),
        "should use exact key when file_size is unknown");
}

#[tokio::test]
async fn test_chunk_alignment_spans_two_chunks_caches_per_chunk() {
    let (registry, local, cache, tiered) = setup_with_cache();

    let file_size: u64 = 16 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    local.put(&Path::from("i.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("i.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Request range spanning chunk boundary (8MiB-100 .. 8MiB+100). The cross-boundary
    // splitter emits a chunk-aligned span (0..16MiB), fetches it, and populate splits
    // it into per-chunk cache entries — chunk 0 and chunk 1.
    let boundary = 8u64 << 20;
    let r = tiered.get_ranges(&Path::from("i.parquet"), &[boundary - 100..boundary + 100]).await.unwrap();
    assert_eq!(r[0].len(), 200, "caller still gets the exact 200 bytes requested");
    // Verify the bytes returned match the source.
    let expected: Vec<u8> = data[(boundary - 100) as usize..(boundary + 100) as usize].to_vec();
    assert_eq!(r[0].as_ref(), expected.as_slice());

    assert_eq!(cache.len(), 2, "cross-boundary range produces TWO chunk-aligned cache entries");
    let keys = cache.keys();
    let chunk_0_key = range_cache_key("i.parquet", 0, 8 << 20);
    let chunk_1_key = range_cache_key("i.parquet", 8 << 20, 16 << 20);
    assert!(keys.contains(&chunk_0_key.as_str().to_string()), "chunk 0 must be cached");
    assert!(keys.contains(&chunk_1_key.as_str().to_string()), "chunk 1 must be cached");
}

#[tokio::test]
async fn test_chunk_alignment_no_cache_passes_original_ranges() {
    let (_registry, local, _remote, tiered) = setup();

    let data = b"hello world 1234567890";
    local.put(&Path::from("j.parquet"), PutPayload::from_static(data)).await.unwrap();

    // No cache attached — should fetch exact original ranges
    let r = tiered.get_ranges(&Path::from("j.parquet"), &[0..5, 6..11]).await.unwrap();
    assert_eq!(r[0].as_ref(), b"hello");
    assert_eq!(r[1].as_ref(), b"world");
}

// =============================================================================
// New tests — chunk-aligned probe + write + put_metadata
// =============================================================================
//
// Covers:
//   1. probe_single_range — cache-hit slicing, file_size==0 fallback,
//      cross-boundary returns None, partial-last-chunk handling.
//   2. write_chunk_aligned — partial-edge skipping, multi-chunk splitting,
//      tier routing (data vs metadata), pathological inputs.
//   3. End-to-end via get_ranges — cross-boundary fetched once then sub-range
//      reads HIT, dedup across input ranges, no-cache passthrough.
//   4. End-to-end via get_opts — chunk-aligned hit serves sub-range,
//      sub-chunk populate is skipped, full-chunk populate makes future hits.
//   5. put_metadata — chunk-aligned warmup writes land in metadata tier,
//      partial edges skipped (or covered when caller pre-aligns).

use crate::tiered_object_store::CACHE_CHUNK_SIZE;

const CHUNK: u64 = 8 << 20;

fn make_data(size: u64) -> Bytes {
    Bytes::from((0..size).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

// -- probe_single_range tests ----------------------------------------------

#[tokio::test]
async fn probe_single_range_returns_none_when_no_cache() {
    let (_registry, _local, _remote, tiered) = setup();
    // No cache attached.
    let result = tiered.probe_single_range("nope.parquet", 0, 100).await;
    assert!(result.is_none(), "no cache → None");
}

#[tokio::test]
async fn probe_single_range_file_size_unknown_uses_exact_key() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    // Register without size — file_size resolves to 0 → exact-key path.
    registry.register("u.parquet", TieredFileEntry::new(FileLocation::Local, None));

    let exact_key = range_cache_key("u.parquet", 100, 200);
    cache.put(&exact_key, Bytes::from_static(b"X").repeat_into(100));

    let result = tiered.probe_single_range("u.parquet", 100, 200).await;
    assert!(result.is_some(), "exact-key hit");
    assert_eq!(result.unwrap().len(), 100);

    // Different range, same path → exact-key MISS (we don't fall back to chunk lookup).
    let result_miss = tiered.probe_single_range("u.parquet", 50, 60).await;
    assert!(result_miss.is_none(), "exact-key miss for unrelated range");
}

#[tokio::test]
async fn probe_single_range_chunk_aligned_hit_slices_correctly() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("h.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Pre-populate chunk 0 with deterministic bytes (first 8 MiB of file).
    let chunk_0 = make_data(CHUNK);
    let chunk_0_key = range_cache_key("h.parquet", 0, CHUNK);
    cache.put(&chunk_0_key, chunk_0.clone());

    // Probe sub-range 100..200 (inside chunk 0).
    let bytes = tiered.probe_single_range("h.parquet", 100, 200).await
        .expect("must hit chunk 0 cache");
    assert_eq!(bytes.len(), 100);
    assert_eq!(bytes.as_ref(), &chunk_0[100..200]);
}

#[tokio::test]
async fn probe_single_range_cross_boundary_returns_none_even_if_chunks_cached() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("xb.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Even if chunks 0 AND 1 are cached, a cross-boundary single-range probe
    // returns None — the helper does NOT attempt multi-chunk reassembly.
    cache.put(&range_cache_key("xb.parquet", 0, CHUNK), make_data(CHUNK));
    cache.put(&range_cache_key("xb.parquet", CHUNK, 2 * CHUNK), make_data(CHUNK));

    let result = tiered.probe_single_range("xb.parquet", CHUNK - 100, CHUNK + 100).await;
    assert!(result.is_none(), "cross-boundary single-range probe is intentionally None");
}

#[tokio::test]
async fn probe_single_range_handles_partial_last_chunk() {
    // file_size NOT a multiple of CHUNK: the last chunk is partial.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 9 << 20; // 9 MiB → chunk 0 [0..8MiB), chunk 1 [8MiB..9MiB) partial
    registry.register("p.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Pre-populate chunk 1 (the partial last chunk) under its chunk-aligned key.
    let chunk_1_size = file_size - CHUNK;
    let chunk_1_data = make_data(chunk_1_size);
    let chunk_1_key = range_cache_key("p.parquet", CHUNK, file_size);
    cache.put(&chunk_1_key, chunk_1_data.clone());

    // Sub-range request at the very end of the file.
    let r_start = file_size - 100;
    let r_end = file_size;
    let bytes = tiered.probe_single_range("p.parquet", r_start, r_end).await
        .expect("must hit partial last chunk");
    assert_eq!(bytes.len(), 100);
    let expected_offset = (r_start - CHUNK) as usize;
    assert_eq!(bytes.as_ref(), &chunk_1_data[expected_offset..expected_offset + 100]);
}

#[tokio::test]
async fn probe_single_range_request_at_chunk_boundary() {
    // r.start == chunk_start, r.end == chunk_end — fully aligned single chunk.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("b.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let chunk_1 = make_data(CHUNK);
    cache.put(&range_cache_key("b.parquet", CHUNK, 2 * CHUNK), chunk_1.clone());

    let bytes = tiered.probe_single_range("b.parquet", CHUNK, 2 * CHUNK).await
        .expect("must hit chunk-aligned exact match");
    assert_eq!(bytes.len(), CHUNK as usize);
    assert_eq!(bytes.as_ref(), chunk_1.as_ref());
}

// -- write_chunk_aligned tests ---------------------------------------------

#[tokio::test]
async fn write_chunk_aligned_single_chunk_writes_one_entry() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("w1.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let bytes = make_data(CHUNK);
    let range = 0..CHUNK;
    tiered.write_chunk_aligned("w1.parquet", &range, &bytes, /* metadata */ false);

    assert_eq!(cache.len(), 1);
    let key = range_cache_key("w1.parquet", 0, CHUNK);
    assert!(cache.keys().contains(&key.as_str().to_string()));
    assert!(cache.data_tier_keys().contains(&key.as_str().to_string()), "must go to data tier");
}

#[tokio::test]
async fn write_chunk_aligned_multi_chunk_writes_per_chunk() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 32 << 20;
    registry.register("w2.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Range covers chunks 0, 1, 2 fully (chunk-aligned span).
    let bytes = make_data(3 * CHUNK);
    let range = 0..(3 * CHUNK);
    tiered.write_chunk_aligned("w2.parquet", &range, &bytes, /* metadata */ false);

    assert_eq!(cache.len(), 3, "3 chunks written");
    for chunk_n in 0..3u64 {
        let start = chunk_n * CHUNK;
        let end = start + CHUNK;
        let key = range_cache_key("w2.parquet", start, end);
        assert!(cache.keys().contains(&key.as_str().to_string()),
            "chunk {} key {} must exist", chunk_n, key.as_str());
    }
}

#[tokio::test]
async fn write_chunk_aligned_skips_partial_first_chunk() {
    // Range starts mid-chunk → first chunk is partially covered → SKIP.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 32 << 20;
    registry.register("w3.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Range = [4 MiB .. 24 MiB). Touches chunks 0 (partial: 4..8 MiB), 1 (full 8..16),
    //                                       2 (full 16..24). Should write only chunks 1 and 2.
    let r_start = 4 << 20;
    let r_end = 24 << 20;
    let bytes = make_data(r_end - r_start);
    tiered.write_chunk_aligned("w3.parquet", &(r_start..r_end), &bytes, false);

    assert_eq!(cache.len(), 2, "chunks 1 and 2 written; chunk 0 skipped (partial)");
    assert!(cache.keys().contains(&range_cache_key("w3.parquet", CHUNK, 2 * CHUNK).as_str().to_string()));
    assert!(cache.keys().contains(&range_cache_key("w3.parquet", 2 * CHUNK, 3 * CHUNK).as_str().to_string()));
    // Verify chunk 1 bytes are sliced correctly: chunk 1 covers bytes [8MiB..16MiB) of the file,
    // which corresponds to bytes [(8-4)MiB..(16-4)MiB) = [4MiB..12MiB) of the input buffer.
    let chunk_1_cached = cache.store.lock().unwrap()
        .get(range_cache_key("w3.parquet", CHUNK, 2 * CHUNK).as_str())
        .unwrap()
        .clone();
    assert_eq!(chunk_1_cached.as_ref(), &bytes[4 << 20..(4 << 20) + CHUNK as usize]);
}

#[tokio::test]
async fn write_chunk_aligned_skips_partial_last_chunk() {
    // Range ends mid-chunk → last chunk partially covered → SKIP.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 32 << 20;
    registry.register("w4.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Range = [0 .. 12 MiB). Chunk 0 (full), chunk 1 (partial 8..12 MiB).
    let r_end = 12 << 20;
    let bytes = make_data(r_end);
    tiered.write_chunk_aligned("w4.parquet", &(0..r_end), &bytes, false);

    assert_eq!(cache.len(), 1, "only chunk 0 written; chunk 1 skipped (partial last)");
    assert!(cache.keys().contains(&range_cache_key("w4.parquet", 0, CHUNK).as_str().to_string()));
}

#[tokio::test]
async fn write_chunk_aligned_sub_chunk_writes_nothing() {
    // Sub-chunk range entirely inside one chunk — neither end is chunk-aligned → NOTHING written.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("w5.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let bytes = make_data(900);
    tiered.write_chunk_aligned("w5.parquet", &(100..1000), &bytes, false);

    assert_eq!(cache.len(), 0, "sub-chunk range writes nothing — partial chunk skipped");
}

#[tokio::test]
async fn write_chunk_aligned_metadata_tier_routing() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("w6.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let bytes = make_data(CHUNK);
    tiered.write_chunk_aligned("w6.parquet", &(0..CHUNK), &bytes, /* metadata */ true);

    let key = range_cache_key("w6.parquet", 0, CHUNK);
    assert!(cache.metadata_tier_keys().contains(&key.as_str().to_string()),
        "to_metadata_tier=true must write to metadata tier");
    assert!(!cache.data_tier_keys().contains(&key.as_str().to_string()),
        "should not appear in data tier");
}

#[tokio::test]
async fn write_chunk_aligned_file_size_unknown_falls_back_to_exact_key() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    registry.register("w7.parquet", TieredFileEntry::new(FileLocation::Local, None));

    let bytes = make_data(500);
    tiered.write_chunk_aligned("w7.parquet", &(100..600), &bytes, false);

    // file_size == 0 → exact-key write of the full range.
    let exact_key = range_cache_key("w7.parquet", 100, 600);
    assert_eq!(cache.len(), 1);
    assert!(cache.keys().contains(&exact_key.as_str().to_string()));
}

#[tokio::test]
async fn write_chunk_aligned_partial_last_chunk_at_eof() {
    // The last chunk of a non-aligned-size file is *partial by definition*.
    // A range that exactly covers it (start == chunk_start, end == file_size)
    // should be considered fully-covered and written.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 9 << 20; // chunk 1 is partial: [8MiB..9MiB)
    registry.register("w8.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let chunk_1_size = file_size - CHUNK;
    let bytes = make_data(chunk_1_size);
    tiered.write_chunk_aligned("w8.parquet", &(CHUNK..file_size), &bytes, false);

    let key = range_cache_key("w8.parquet", CHUNK, file_size);
    assert_eq!(cache.len(), 1, "partial-by-EOF chunk is fully covered by range and gets written");
    assert!(cache.keys().contains(&key.as_str().to_string()));
}

// -- pathological / bug-hunt tests for write_chunk_aligned -----------------

#[tokio::test]
async fn write_chunk_aligned_does_not_infinite_loop_on_oob_range_end() {
    // BUG REGRESSION: range.end > file_size used to cause an infinite loop because
    // chunk_end clamped to file_size while chunk_start kept advancing to file_size,
    // making chunk_start == chunk_end with no progress. The defensive break in
    // write_chunk_aligned now bails when chunk_end <= chunk_start.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("oob.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Bytes long enough to support the OOB range slicing for the in-bounds chunks.
    let bytes = make_data(file_size + (1 << 20));
    let range = 0..(file_size + (1 << 20));

    // write_chunk_aligned is sync — run it on a scoped thread with a deadline.
    // If the loop ever stops progressing again the test will fail with a timeout.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    std::thread::scope(|s| {
        let done_clone = std::sync::Arc::clone(&done);
        s.spawn(move || {
            tiered.write_chunk_aligned("oob.parquet", &range, &bytes, false);
            done_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let start = std::time::Instant::now();
        while !done.load(std::sync::atomic::Ordering::SeqCst) {
            if start.elapsed() > std::time::Duration::from_secs(2) {
                panic!("write_chunk_aligned did not terminate within 2s — likely infinite loop");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    // Chunks 0 and 1 are fully inside [0..file_size); both written. No partial-write
    // for the byte beyond file_size.
    assert!(cache.len() <= 2, "at most 2 chunks should be cached; got {}", cache.len());
    let chunk_0_key = range_cache_key("oob.parquet", 0, CHUNK);
    let chunk_1_key = range_cache_key("oob.parquet", CHUNK, 2 * CHUNK);
    assert!(cache.keys().contains(&chunk_0_key.as_str().to_string()));
    assert!(cache.keys().contains(&chunk_1_key.as_str().to_string()));
}

#[tokio::test]
async fn write_chunk_aligned_empty_range_writes_nothing() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("e.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let bytes = Bytes::new();
    tiered.write_chunk_aligned("e.parquet", &(100..100), &bytes, false);

    assert_eq!(cache.len(), 0, "empty range writes nothing");
}

// -- end-to-end via get_ranges ---------------------------------------------

#[tokio::test]
async fn e2e_cross_boundary_then_subrange_hits_cache() {
    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    local.put(&Path::from("e1.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("e1.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // First read: cross-boundary [7 MiB .. 9 MiB). Pulls a chunk-aligned span,
    // splits into chunk 0 + chunk 1 entries.
    let r1 = tiered.get_ranges(
        &Path::from("e1.parquet"),
        &[(7 << 20)..(9 << 20)],
    ).await.unwrap();
    assert_eq!(r1[0].len(), 2 << 20);
    assert_eq!(cache.len(), 2);

    // Second read: sub-range entirely inside chunk 0. Should HIT cache.
    let key_count_before = cache.len();
    let r2 = tiered.get_ranges(
        &Path::from("e1.parquet"),
        &[100..200],
    ).await.unwrap();
    assert_eq!(r2[0].len(), 100);
    assert_eq!(r2[0].as_ref(), &data[100..200]);
    assert_eq!(cache.len(), key_count_before, "no new cache entry — served from chunk 0");

    // Third read: sub-range entirely inside chunk 1. Also HITs cache.
    let r3 = tiered.get_ranges(
        &Path::from("e1.parquet"),
        &[(8 << 20) + 50..(8 << 20) + 150],
    ).await.unwrap();
    assert_eq!(r3[0].len(), 100);
    let want_start = (8 << 20) + 50;
    assert_eq!(r3[0].as_ref(), &data[want_start..want_start + 100]);
    assert_eq!(cache.len(), key_count_before, "still no new cache entry");
}

#[tokio::test]
async fn e2e_dedup_across_input_ranges_in_same_chunk() {
    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    local.put(&Path::from("e2.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("e2.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // 3 input ranges all in chunk 0 — dedup'd to 1 fetch, all 3 slots filled correctly.
    let r = tiered.get_ranges(
        &Path::from("e2.parquet"),
        &[0..10, 1000..2000, 5_000_000..5_000_100],
    ).await.unwrap();
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].as_ref(), &data[0..10]);
    assert_eq!(r[1].as_ref(), &data[1000..2000]);
    assert_eq!(r[2].as_ref(), &data[5_000_000..5_000_100]);
    assert_eq!(cache.len(), 1, "single chunk fetched and cached");
}

#[tokio::test]
async fn e2e_multi_chunk_input_ranges_dedup_when_same_chunk_span() {
    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 32 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    local.put(&Path::from("e3.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("e3.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Two cross-boundary input ranges that produce the SAME chunk-aligned span (0..16MiB).
    // probe_cache should dedup to one miss_range, populate writes chunks 0+1 once.
    let r = tiered.get_ranges(
        &Path::from("e3.parquet"),
        &[(1 << 20)..(15 << 20), (2 << 20)..(14 << 20)],
    ).await.unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].as_ref(), &data[1 << 20..(15 << 20) as usize]);
    assert_eq!(r[1].as_ref(), &data[2 << 20..(14 << 20) as usize]);
    assert_eq!(cache.len(), 2, "chunks 0 and 1 cached once via dedup'd span");
}

// -- end-to-end via get_opts -----------------------------------------------

#[tokio::test]
async fn e2e_get_opts_warmup_subrange_hit() {
    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    local.put(&Path::from("e4.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("e4.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Warmup: put_metadata for a chunk-aligned range covering chunks 0+1.
    let warmup_bytes = Bytes::from(data[..2 * CHUNK as usize].to_vec());
    tiered.put_metadata("e4.parquet", &[0..(2 * CHUNK)], &[warmup_bytes]);
    assert_eq!(cache.metadata_tier_keys().len(), 2, "warmup writes 2 chunks to metadata tier");

    // Sub-range get_opts inside chunk 0 — should hit metadata tier via probe_single_range.
    use object_store::{GetOptions, GetRange};
    let opts = GetOptions {
        range: Some(GetRange::Bounded(100..500)),
        ..Default::default()
    };
    let result = tiered.get_opts(&Path::from("e4.parquet"), opts).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.len(), 400);
    assert_eq!(bytes.as_ref(), &data[100..500]);
}

#[tokio::test]
async fn e2e_get_opts_subchunk_populate_caches_enclosing_chunk() {
    // A sub-chunk get_opts cold read now routes through get_ranges and caches
    // the full enclosing 8 MiB chunk in the DATA tier (clamped to file_size).
    // A second sub-chunk read on the same chunk is served entirely from cache
    // and issues zero fetches to the backing object store.
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let cache = Arc::new(MockBlockCache::new());
    let remote = Arc::new(CallCountingStore::new());

    let file_size: u64 = 16 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    remote
        .inner
        .put(&Path::from("remote/e5.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();

    let tiered = TieredObjectStore::new(Arc::clone(&registry), Arc::clone(&local) as _)
        .with_cache(Arc::clone(&cache) as _);
    tiered.set_remote(Arc::clone(&remote) as _);
    registry.register(
        "e5.parquet",
        TieredFileEntry::with_size(
            FileLocation::Remote,
            Some("remote/e5.parquet".into()),
            file_size,
        ),
    );

    use object_store::{GetOptions, GetRange};

    // (cold) Sub-chunk read 100..500 — routes through get_ranges, fetches and
    // caches the full enclosing chunk 0..CHUNK in the data tier, then slices.
    let opts = GetOptions {
        range: Some(GetRange::Bounded(100..500)),
        ..Default::default()
    };
    let result = tiered.get_opts(&Path::from("e5.parquet"), opts).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), &data[100..500]);

    // (a) The full enclosing chunk is now cached in the DATA tier (not metadata).
    let chunk_key = range_cache_key("e5.parquet", 0, CHUNK).as_str().to_string();
    assert!(
        cache.data_tier_keys().contains(&chunk_key),
        "sub-chunk get_opts must cache the enclosing chunk in the data tier"
    );
    assert!(
        !cache.metadata_tier_keys().contains(&chunk_key),
        "query-path populate must never write the metadata tier"
    );

    // (b) A second sub-chunk read on the same chunk issues 0 backing fetches.
    let fetches_after_first = remote.get_count.load(AtomicOrdering::SeqCst);
    let opts2 = GetOptions {
        range: Some(GetRange::Bounded(2000..3000)),
        ..Default::default()
    };
    let r2 = tiered.get_opts(&Path::from("e5.parquet"), opts2).await.unwrap();
    let b2 = r2.bytes().await.unwrap();
    assert_eq!(b2.as_ref(), &data[2000..3000]);
    assert_eq!(
        remote.get_count.load(AtomicOrdering::SeqCst),
        fetches_after_first,
        "second sub-chunk read on the cached chunk must issue zero backing fetches"
    );
}

#[tokio::test]
async fn e2e_get_opts_chunk_aligned_populate_writes_chunk() {
    // get_opts post-fetch populate WRITES when request is exactly a chunk.
    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    let data: Vec<u8> = (0..file_size).map(|i| (i % 251) as u8).collect();
    local.put(&Path::from("e6.parquet"), PutPayload::from(data.clone())).await.unwrap();
    registry.register("e6.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    use object_store::{GetOptions, GetRange};
    let opts = GetOptions {
        range: Some(GetRange::Bounded(0..CHUNK)),
        ..Default::default()
    };
    let _ = tiered.get_opts(&Path::from("e6.parquet"), opts).await.unwrap();

    let key = range_cache_key("e6.parquet", 0, CHUNK);
    assert!(cache.keys().contains(&key.as_str().to_string()),
        "exact-chunk get_opts populate writes chunk-aligned entry");

    // Second call should hit cache via probe_single_range.
    let opts2 = GetOptions {
        range: Some(GetRange::Bounded(100..200)),
        ..Default::default()
    };
    let cache_size_before = cache.len();
    let r2 = tiered.get_opts(&Path::from("e6.parquet"), opts2).await.unwrap();
    let b2 = r2.bytes().await.unwrap();
    assert_eq!(b2.as_ref(), &data[100..200]);
    assert_eq!(cache.len(), cache_size_before, "second get_opts hits chunk cache");
}

// -- put_metadata tests ----------------------------------------------------

#[tokio::test]
async fn put_metadata_chunk_aligned_input_writes_all_chunks() {
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 32 << 20;
    registry.register("m1.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Chunk-aligned span 0..24 MiB — covers chunks 0, 1, 2.
    let bytes = make_data(3 * CHUNK);
    tiered.put_metadata("m1.parquet", &[0..(3 * CHUNK)], &[bytes.clone()]);

    assert_eq!(cache.metadata_tier_keys().len(), 3);
    for chunk_n in 0..3u64 {
        let key = range_cache_key("m1.parquet", chunk_n * CHUNK, (chunk_n + 1) * CHUNK);
        assert!(cache.metadata_tier_keys().contains(&key.as_str().to_string()),
            "chunk {} missing in metadata tier", chunk_n);
    }
}

#[tokio::test]
async fn put_metadata_unaligned_input_skips_partial_edges() {
    // Caller hands an UNALIGNED range. Only fully-covered chunks land in metadata tier.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 32 << 20;
    registry.register("m2.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    // Range 4 MiB .. 20 MiB → chunk 0 (partial), chunk 1 (full), chunk 2 (partial). Only chunk 1 written.
    let r_start = 4 << 20;
    let r_end = 20 << 20;
    let bytes = make_data(r_end - r_start);
    tiered.put_metadata("m2.parquet", &[r_start..r_end], &[bytes]);

    assert_eq!(cache.metadata_tier_keys().len(), 1, "only chunk 1 fully covered");
    let chunk_1_key = range_cache_key("m2.parquet", CHUNK, 2 * CHUNK);
    assert!(cache.metadata_tier_keys().contains(&chunk_1_key.as_str().to_string()));
}

#[tokio::test]
async fn put_metadata_path_normalization() {
    // put_metadata strips leading '/' so key shape matches read paths.
    let (registry, _local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20;
    registry.register("normalized.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));

    let bytes = make_data(CHUNK);
    tiered.put_metadata("/normalized.parquet", &[0..CHUNK], &[bytes]);

    // Key written without the leading slash.
    let key = range_cache_key("normalized.parquet", 0, CHUNK);
    assert!(cache.metadata_tier_keys().contains(&key.as_str().to_string()),
        "path was stripped of leading '/' before key construction");
}

#[tokio::test]
async fn put_metadata_constant_matches_module_constant() {
    // Sanity check the public constant.
    assert_eq!(CACHE_CHUNK_SIZE, CHUNK);
}

// -- Bytes helper for the file_size==0 test --------------------------------

trait BytesRepeatHelper {
    fn repeat_into(&self, n: usize) -> Bytes;
}

impl BytesRepeatHelper for Bytes {
    fn repeat_into(&self, n: usize) -> Bytes {
        let mut buf = Vec::with_capacity(self.len() * n);
        for _ in 0..n {
            buf.extend_from_slice(self.as_ref());
        }
        Bytes::from(buf)
    }
}

// =============================================================================
// Property-based tests (proptest) — get-opts-chunk-aligned-caching
// =============================================================================

use proptest::prelude::*;

/// The three cache states a resolvable ranged `get_opts` read can run under.
#[derive(Debug, Clone, Copy)]
enum CacheState {
    /// Cache attached; the range is read once to warm the chunk(s), then re-read.
    Hit,
    /// Cache attached but cold; the range is read once (a miss that fetches+populates).
    Miss,
    /// No cache attached at all.
    NoCache,
}

/// File sizes spanning the meaningful buckets: smaller than a chunk, exactly a
/// chunk, an exact multi-chunk multiple, and arbitrary multi-chunk sizes with a
/// partial last chunk. Kept at or below 20 MiB so each case stays fast while
/// still guaranteeing multi-chunk and cross-boundary coverage. Every size keeps
/// the resolved range size below `max_data_entry_size` (32 MiB fallback), so all
/// generated ranges route through the chunk-aligned `get_ranges` path.
fn prop_file_size() -> impl Strategy<Value = u64> {
    prop_oneof![
        1u64..CHUNK,             // < CHUNK (single, sub-chunk-sized file)
        Just(CHUNK),             // == CHUNK
        Just(2 * CHUNK),         // exact multi-chunk multiple
        (CHUNK + 1)..=(20u64 << 20), // multi-chunk, frequently a partial last chunk
    ]
}

/// Generate `(file_size, start, end)` with `0 <= start <= end <= file_size`.
/// Sampling both bounds uniformly over the whole file yields sub-chunk,
/// chunk-aligned, cross-boundary, and empty (`start == end`) ranges.
fn prop_file_and_range() -> impl Strategy<Value = (u64, u64, u64)> {
    prop_file_size()
        .prop_flat_map(|fs| (Just(fs), 0u64..=fs, 0u64..=fs))
        .prop_map(|(fs, a, b)| {
            let (start, end) = if a <= b { (a, b) } else { (b, a) };
            (fs, start, end)
        })
}

/// Perform a resolvable ranged `get_opts` read under the given cache state and
/// return the payload bytes together with the resolved `GetResult.range`.
async fn prop1_read(
    state: CacheState,
    file_size: u64,
    start: u64,
    end: u64,
    data: &Bytes,
) -> (Bytes, std::ops::Range<u64>) {
    use object_store::{GetOptions, GetRange};

    let path = Path::from("prop1.parquet");
    let make_opts = || GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };

    match state {
        CacheState::NoCache => {
            let (registry, local, _remote, tiered) = setup();
            local
                .put(&path, PutPayload::from(data.clone()))
                .await
                .unwrap();
            registry.register(
                "prop1.parquet",
                TieredFileEntry::with_size(FileLocation::Local, None, file_size),
            );
            let result = tiered.get_opts(&path, make_opts()).await.unwrap();
            let range = result.range.clone();
            (result.bytes().await.unwrap(), range)
        }
        CacheState::Miss => {
            let (registry, local, _cache, tiered) = setup_with_cache();
            local
                .put(&path, PutPayload::from(data.clone()))
                .await
                .unwrap();
            registry.register(
                "prop1.parquet",
                TieredFileEntry::with_size(FileLocation::Local, None, file_size),
            );
            let result = tiered.get_opts(&path, make_opts()).await.unwrap();
            let range = result.range.clone();
            (result.bytes().await.unwrap(), range)
        }
        CacheState::Hit => {
            let (registry, local, _cache, tiered) = setup_with_cache();
            local
                .put(&path, PutPayload::from(data.clone()))
                .await
                .unwrap();
            registry.register(
                "prop1.parquet",
                TieredFileEntry::with_size(FileLocation::Local, None, file_size),
            );
            // Warm read populates the enclosing chunk(s)...
            let warm = tiered.get_opts(&path, make_opts()).await.unwrap();
            let _ = warm.bytes().await.unwrap();
            // ...then re-read, which should be served from cache.
            let result = tiered.get_opts(&path, make_opts()).await.unwrap();
            let range = result.range.clone();
            (result.bytes().await.unwrap(), range)
        }
    }
}

proptest! {
    // Reduced case count (8) to keep the property suite fast.
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    // Feature: get-opts-chunk-aligned-caching, Property 1: Resolvable ranged read returns correct bytes, range, and length
    //
    // For any backing object and any resolvable range whose resolved size does not
    // exceed max_data_entry_size, a get_opts read returns payload bytes byte-for-byte
    // identical to the backing object's bytes over [start, end), with
    // GetResult.range == start..end and payload length == end - start, ordered by
    // ascending offset — identically for a cache hit, a cache miss, and no cache
    // attached. Cross-boundary ranges are included by the range generator.
    //
    // Validates: Requirements 1.2, 5.1, 7.1, 7.2
    #[test]
    fn prop1_resolvable_ranged_read_correct((file_size, start, end) in prop_file_and_range()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let data = make_data(file_size);
        let expected: &[u8] = &data[start as usize..end as usize];

        for state in [CacheState::Hit, CacheState::Miss, CacheState::NoCache] {
            let (bytes, range) = rt.block_on(prop1_read(state, file_size, start, end, &data));

            prop_assert_eq!(
                range.start, start,
                "range.start mismatch (state={:?}, file_size={}, range={}..{})",
                state, file_size, start, end
            );
            prop_assert_eq!(
                range.end, end,
                "range.end mismatch (state={:?}, file_size={}, range={}..{})",
                state, file_size, start, end
            );
            prop_assert_eq!(
                bytes.len() as u64, end - start,
                "payload length mismatch (state={:?}, file_size={}, range={}..{})",
                state, file_size, start, end
            );
            prop_assert_eq!(
                bytes.as_ref(), expected,
                "payload bytes mismatch (state={:?}, file_size={}, range={}..{})",
                state, file_size, start, end
            );
        }
    }
}

// =============================================================================
// Property-based tests (proptest) — Properties 2, 3, 4, 5
//
// These mirror the established prop1 patterns: deterministic file contents via
// `make_data`, generators built on `prop_file_size`, and a per-case current-thread
// tokio runtime with `rt.block_on(...)`. Case count is reduced to 24 (from the
// design's 100) to keep the property suite fast.
// =============================================================================

/// Backing store + cache wiring that counts every backing-store fetch.
///
/// The file is registered as `FileLocation::Remote` so reads route through
/// `fetch_misses` → the remote `CallCountingStore`, whose `get_count` reflects
/// the number of backing fetches issued.
fn setup_counting_remote_with_cache() -> (
    Arc<TieredStorageRegistry>,
    Arc<CallCountingStore>,
    Arc<MockBlockCache>,
    TieredObjectStore,
) {
    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let remote = Arc::new(CallCountingStore::new());
    let cache = Arc::new(MockBlockCache::new());
    let tiered = TieredObjectStore::new(Arc::clone(&registry), Arc::clone(&local) as _)
        .with_cache(Arc::clone(&cache) as _);
    tiered.set_remote(Arc::clone(&remote) as _);
    (registry, remote, cache, tiered)
}

/// Generate `(file_size, chunk_start, chunk_end, a_start, a_end, b_start, b_end)`
/// where both `[a_start, a_end)` and `[b_start, b_end)` are NON-EMPTY ranges that
/// lie entirely within the SAME 8 MiB chunk `[chunk_start, chunk_end)`.
fn prop_file_chunk_two_subranges() -> impl Strategy<Value = (u64, u64, u64, u64, u64, u64, u64)> {
    prop_file_size()
        .prop_flat_map(|fs| {
            let num_chunks = fs.div_ceil(CHUNK).max(1);
            (Just(fs), 0u64..num_chunks)
        })
        .prop_flat_map(|(fs, idx)| {
            let chunk_start = idx * CHUNK;
            let chunk_end = (chunk_start + CHUNK).min(fs);
            let chunk_len = chunk_end - chunk_start; // >= 1
            (
                Just(fs),
                Just(chunk_start),
                Just(chunk_end),
                0u64..chunk_len,
                0u64..chunk_len,
            )
                .prop_flat_map(move |(fs, cs, ce, a_off, b_off)| {
                    let cl = ce - cs;
                    (
                        Just(fs),
                        Just(cs),
                        Just(ce),
                        Just(a_off),
                        1u64..=(cl - a_off),
                        Just(b_off),
                        1u64..=(cl - b_off),
                    )
                })
                .prop_map(|(fs, cs, ce, a_off, a_len, b_off, b_len)| {
                    (
                        fs,
                        cs,
                        ce,
                        cs + a_off,
                        cs + a_off + a_len,
                        cs + b_off,
                        cs + b_off + b_len,
                    )
                })
        })
}

/// Generate `(file_size, chunk_start, chunk_end, start, end)` for a single
/// NON-EMPTY range `[start, end)` lying entirely within one chunk.
fn prop_file_single_chunk_range() -> impl Strategy<Value = (u64, u64, u64, u64, u64)> {
    prop_file_size()
        .prop_flat_map(|fs| {
            let num_chunks = fs.div_ceil(CHUNK).max(1);
            (Just(fs), 0u64..num_chunks)
        })
        .prop_flat_map(|(fs, idx)| {
            let chunk_start = idx * CHUNK;
            let chunk_end = (chunk_start + CHUNK).min(fs);
            let chunk_len = chunk_end - chunk_start; // >= 1
            (Just(fs), Just(chunk_start), Just(chunk_end), 0u64..chunk_len)
                .prop_flat_map(move |(fs, cs, ce, off)| {
                    let cl = ce - cs;
                    (Just(fs), Just(cs), Just(ce), Just(off), 1u64..=(cl - off))
                })
                .prop_map(|(fs, cs, ce, off, len)| (fs, cs, ce, cs + off, cs + off + len))
        })
}

/// Generate `(file_size, start, end)` for a cross-boundary range that straddles
/// at least two chunk boundaries (the lines at 8 MiB and 16 MiB), with partial
/// coverage at both edge chunks. `file_size` is constrained to span three chunks
/// (chunk 2 is a partial last chunk). All three chunks are overlapped by the
/// requested range, so the chunk-aligned fetch caches all three in full.
fn prop_cross_boundary_two_plus() -> impl Strategy<Value = (u64, u64, u64)> {
    ((17u64 << 20)..=(20u64 << 20)) // file_size in (2*CHUNK, ~2.5*CHUNK]
        .prop_flat_map(|fs| {
            (
                Just(fs),
                1u64..CHUNK,                 // start inside chunk 0 (partial: start > 0)
                (2 * CHUNK + 1)..fs,         // end inside chunk 2 (partial: end < fs), crosses 2nd boundary
            )
        })
}

// -- Property 2 ------------------------------------------------------------

/// Observations gathered for a single Property 2 case.
struct Prop2Obs {
    bytes_a: Bytes,
    cached_chunk: Option<Bytes>,
    data_keys_after_a: Vec<String>,
    fetches_after_a: usize,
    bytes_b_get_opts: Bytes,
    fetches_after_get_opts: usize,
    bytes_b_get_range: Bytes,
    fetches_after_get_range: usize,
    bytes_b_get_ranges: Bytes,
    fetches_after_get_ranges: usize,
}

#[allow(clippy::too_many_arguments)]
async fn prop2_run(
    chunk_start: u64,
    chunk_end: u64,
    file_size: u64,
    a_start: u64,
    a_end: u64,
    b_start: u64,
    b_end: u64,
    data: &Bytes,
) -> Prop2Obs {
    use object_store::{GetOptions, GetRange};

    let name = "p2.parquet";
    let remote_path = "remote/p2.parquet";
    let path = Path::from(name);

    let (registry, remote, cache, tiered) = setup_counting_remote_with_cache();
    remote
        .inner
        .put(&Path::from(remote_path), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        name,
        TieredFileEntry::with_size(FileLocation::Remote, Some(remote_path.into()), file_size),
    );

    // Cold read of range A — populates the full enclosing chunk.
    let opts_a = GetOptions {
        range: Some(GetRange::Bounded(a_start..a_end)),
        ..Default::default()
    };
    let ra = tiered.get_opts(&path, opts_a).await.unwrap();
    let bytes_a = ra.bytes().await.unwrap();

    let chunk_key = range_cache_key(name, chunk_start, chunk_end);
    let cached_chunk = cache.store.lock().unwrap().get(chunk_key.as_str()).cloned();
    let data_keys_after_a = cache.data_tier_keys();
    let fetches_after_a = remote.get_count.load(AtomicOrdering::SeqCst);

    // Second read of range B (same chunk) via get_opts.
    let opts_b = GetOptions {
        range: Some(GetRange::Bounded(b_start..b_end)),
        ..Default::default()
    };
    let bytes_b_get_opts = tiered
        .get_opts(&path, opts_b)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let fetches_after_get_opts = remote.get_count.load(AtomicOrdering::SeqCst);

    // Second read of range B via get_range.
    let bytes_b_get_range = tiered.get_range(&path, b_start..b_end).await.unwrap();
    let fetches_after_get_range = remote.get_count.load(AtomicOrdering::SeqCst);

    // Second read of range B via get_ranges.
    let bytes_b_get_ranges = tiered
        .get_ranges(&path, &[b_start..b_end])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let fetches_after_get_ranges = remote.get_count.load(AtomicOrdering::SeqCst);

    Prop2Obs {
        bytes_a,
        cached_chunk,
        data_keys_after_a,
        fetches_after_a,
        bytes_b_get_opts,
        fetches_after_get_opts,
        bytes_b_get_range,
        fetches_after_get_range,
        bytes_b_get_ranges,
        fetches_after_get_ranges,
    }
}

proptest! {
    // Reduced case count (8) to keep the property suite fast.
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    // Feature: get-opts-chunk-aligned-caching, Property 2: Sub-chunk cold read caches the enclosing chunk; later overlapping reads do not re-fetch
    //
    // For any file with a known size and any two ranges within the same 8 MiB chunk,
    // a cold get_opts of range A populates the data tier with the full enclosing
    // chunk under key path\x1F{chunk_start}-{chunk_end}, and a subsequent read of
    // range B (via get_opts, get_range, or get_ranges) is served entirely from cache
    // with zero backing fetches.
    //
    // Validates: Requirements 1.1, 1.3, 1.5
    #[test]
    fn prop2_subchunk_caches_chunk_no_refetch(
        (file_size, chunk_start, chunk_end, a_start, a_end, b_start, b_end)
            in prop_file_chunk_two_subranges()
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let data = make_data(file_size);
        let obs = rt.block_on(prop2_run(
            chunk_start, chunk_end, file_size, a_start, a_end, b_start, b_end, &data,
        ));

        let chunk_key = range_cache_key("p2.parquet", chunk_start, chunk_end);
        let chunk_key_str = chunk_key.as_str().to_string();

        // Range A bytes correct.
        prop_assert_eq!(
            obs.bytes_a.as_ref(),
            &data[a_start as usize..a_end as usize],
            "range A bytes mismatch (fs={}, chunk={}..{}, A={}..{})",
            file_size, chunk_start, chunk_end, a_start, a_end
        );

        // The full enclosing chunk is present in the data tier under the chunk key,
        // and its bytes equal the backing chunk bytes.
        prop_assert!(
            obs.data_keys_after_a.contains(&chunk_key_str),
            "enclosing chunk key {} not in data tier (keys={:?})",
            chunk_key_str, obs.data_keys_after_a
        );
        prop_assert!(obs.cached_chunk.is_some(), "enclosing chunk not cached");
        prop_assert_eq!(
            obs.cached_chunk.as_ref().unwrap().as_ref(),
            &data[chunk_start as usize..chunk_end as usize],
            "cached chunk bytes mismatch (chunk={}..{})",
            chunk_start, chunk_end
        );

        // Subsequent reads of range B are served from cache: zero new fetches.
        prop_assert_eq!(
            obs.bytes_b_get_opts.as_ref(),
            &data[b_start as usize..b_end as usize],
            "range B (get_opts) bytes mismatch"
        );
        prop_assert_eq!(
            obs.fetches_after_get_opts, obs.fetches_after_a,
            "get_opts re-fetched (before={}, after={})",
            obs.fetches_after_a, obs.fetches_after_get_opts
        );

        prop_assert_eq!(
            obs.bytes_b_get_range.as_ref(),
            &data[b_start as usize..b_end as usize],
            "range B (get_range) bytes mismatch"
        );
        prop_assert_eq!(
            obs.fetches_after_get_range, obs.fetches_after_a,
            "get_range re-fetched (before={}, after={})",
            obs.fetches_after_a, obs.fetches_after_get_range
        );

        prop_assert_eq!(
            obs.bytes_b_get_ranges.as_ref(),
            &data[b_start as usize..b_end as usize],
            "range B (get_ranges) bytes mismatch"
        );
        prop_assert_eq!(
            obs.fetches_after_get_ranges, obs.fetches_after_a,
            "get_ranges re-fetched (before={}, after={})",
            obs.fetches_after_a, obs.fetches_after_get_ranges
        );
    }
}

// -- Property 3 ------------------------------------------------------------

/// Cold get_opts of a single-chunk range; return the data-tier key set populated.
async fn prop3_run(file_size: u64, start: u64, end: u64, data: &Bytes) -> Vec<String> {
    use object_store::{GetOptions, GetRange};

    let name = "p3.parquet";
    let path = Path::from(name);
    let (registry, local, cache, tiered) = setup_with_cache();
    local
        .put(&path, PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        name,
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };
    let _ = tiered
        .get_opts(&path, opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    cache.data_tier_keys()
}

proptest! {
    // Reduced case count (8) to keep the property suite fast.
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    // Feature: get-opts-chunk-aligned-caching, Property 3: Cache key identity between the get_opts and get_ranges paths
    //
    // For any file with a known size and any resolvable single-chunk range, the
    // cache key under which the routed get_opts path populates the enclosing chunk
    // is byte-for-byte equal to range_cache_key(path, chunk_start, chunk_end) — the
    // exact key the get_ranges path produces and probes for any range in that chunk.
    //
    // Validates: Requirements 1.4
    #[test]
    fn prop3_cache_key_identity(
        (file_size, chunk_start, chunk_end, start, end) in prop_file_single_chunk_range()
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let data = make_data(file_size);
        let data_keys = rt.block_on(prop3_run(file_size, start, end, &data));

        let expected_key = range_cache_key("p3.parquet", chunk_start, chunk_end)
            .as_str()
            .to_string();

        // Exactly the enclosing chunk's key is populated in the data tier.
        prop_assert_eq!(
            data_keys.len(), 1,
            "expected exactly one data-tier key, got {:?} (fs={}, range={}..{})",
            data_keys, file_size, start, end
        );
        prop_assert_eq!(
            &data_keys[0], &expected_key,
            "data-tier key mismatch (fs={}, range={}..{})",
            file_size, start, end
        );
    }
}

// -- Property 4 ------------------------------------------------------------

/// Cold get_opts of a cross-boundary range; return the data-tier entries
/// (key + cached bytes).
async fn prop4_run(file_size: u64, start: u64, end: u64, data: &Bytes) -> Vec<(String, Bytes)> {
    use object_store::{GetOptions, GetRange};

    let name = "p4.parquet";
    let path = Path::from(name);
    let (registry, local, cache, tiered) = setup_with_cache();
    local
        .put(&path, PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        name,
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };
    let _ = tiered
        .get_opts(&path, opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let keys = cache.data_tier_keys();
    let store = cache.store.lock().unwrap();
    keys.into_iter()
        .map(|k| {
            let b = store.get(&k).cloned().unwrap();
            (k, b)
        })
        .collect()
}

proptest! {
    // Reduced case count (8) to keep the property suite fast.
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    // Feature: get-opts-chunk-aligned-caching, Property 4: Cross-boundary cold read caches every overlapped chunk, each fully populated
    //
    // For any resolvable range that straddles two or more 8 MiB chunk boundaries and
    // misses the cache, the read populates exactly one independent data-tier entry
    // for each chunk the requested range overlaps. Because probe_cache expands the
    // miss range outward to chunk boundaries (floor(start)..ceil(end)) before
    // fetching, every overlapped chunk — including partially-requested edge chunks —
    // is fetched and cached in full.
    //
    // Validates: Requirements 5.2, 5.3
    #[test]
    fn prop4_cross_boundary_caches_fully_covered_chunks(
        (file_size, start, end) in prop_cross_boundary_two_plus()
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let data = make_data(file_size);
        let entries = rt.block_on(prop4_run(file_size, start, end, &data));

        // Compute the chunks the requested range [start, end) overlaps. The
        // miss range is expanded outward to chunk boundaries before fetching, so
        // every overlapped chunk is fetched and cached in full.
        let mut expected: Vec<(String, std::ops::Range<usize>)> = Vec::new();
        let mut chunk_start = 0u64;
        while chunk_start < file_size {
            let chunk_end = (chunk_start + CHUNK).min(file_size);
            let overlaps = start < chunk_end && chunk_start < end;
            if overlaps {
                let key = range_cache_key("p4.parquet", chunk_start, chunk_end)
                    .as_str()
                    .to_string();
                expected.push((key, chunk_start as usize..chunk_end as usize));
            }
            chunk_start = chunk_end;
        }

        // Exactly one data-tier entry per overlapped chunk, no more.
        prop_assert_eq!(
            entries.len(), expected.len(),
            "data-tier entry count mismatch (fs={}, range={}..{}): expected {} overlapped chunk(s), got keys {:?}",
            file_size, start, end, expected.len(),
            entries.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>()
        );

        // Each overlapped chunk is present with byte-correct contents (full chunk).
        for (key, slice) in &expected {
            let found = entries.iter().find(|(k, _)| k == key);
            prop_assert!(
                found.is_some(),
                "fully-covered chunk key {} missing from data tier (keys={:?})",
                key,
                entries.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>()
            );
            prop_assert_eq!(
                found.unwrap().1.as_ref(),
                &data[slice.clone()],
                "cached bytes mismatch for chunk key {}",
                key
            );
        }
    }
}

// -- Property 5 ------------------------------------------------------------

/// Cold get_opts of a resolvable range that misses; return (metadata-tier keys,
/// data-tier keys) after the read.
async fn prop5_run(file_size: u64, start: u64, end: u64, data: &Bytes) -> (Vec<String>, Vec<String>) {
    use object_store::{GetOptions, GetRange};

    let name = "p5.parquet";
    let path = Path::from(name);
    let (registry, local, cache, tiered) = setup_with_cache();
    local
        .put(&path, PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        name,
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };
    let _ = tiered
        .get_opts(&path, opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    (cache.metadata_tier_keys(), cache.data_tier_keys())
}

proptest! {
    // Reduced case count (8) to keep the property suite fast.
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    // Feature: get-opts-chunk-aligned-caching, Property 5: Query-path population never writes the metadata tier
    //
    // For any resolvable get_opts read that misses and populates the cache, every
    // resulting write goes to the data tier via put and zero writes go to the
    // metadata tier via put_metadata.
    //
    // Validates: Requirements 6.1, 6.2, 6.3
    #[test]
    fn prop5_query_path_never_writes_metadata_tier(
        (file_size, start, end) in prop_file_and_range()
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let data = make_data(file_size);
        let (metadata_keys, data_keys) = rt.block_on(prop5_run(file_size, start, end, &data));

        // The metadata tier is never written by the query path.
        prop_assert!(
            metadata_keys.is_empty(),
            "query path wrote to metadata tier (fs={}, range={}..{}): {:?}",
            file_size, start, end, metadata_keys
        );

        // Whenever at least one chunk is fully covered by the requested range, the
        // data tier is populated.
        let mut any_fully_covered = false;
        let mut chunk_start = 0u64;
        while chunk_start < file_size {
            let chunk_end = (chunk_start + CHUNK).min(file_size);
            if start <= chunk_start && chunk_end <= end {
                any_fully_covered = true;
                break;
            }
            chunk_start = chunk_end;
        }
        if any_fully_covered {
            prop_assert!(
                !data_keys.is_empty(),
                "data tier empty despite a fully-covered chunk (fs={}, range={}..{})",
                file_size, start, end
            );
        }
    }
}

// =============================================================================
// Unit / example tests — get-opts-chunk-aligned-caching routing branches
//
// Tasks 4.1 (head fast path), 4.2 (unresolvable fallthrough), 4.3 (oversized
// streaming guard + boundary), 4.4 (returned-result correctness), 5.1
// (cross-boundary), 6.1 (remote/local routing + retry).
//
// All test fns share the `unit_getopts_` prefix so they can be run together via
//   cargo test --lib tiered_object_store::tests::unit_getopts_
// =============================================================================

/// A local `ObjectStore` that, on its first read, transitions the registry entry
/// to `Remote` and then returns `NotFound` — simulating a local copy that was
/// synced to remote and deleted between routing decisions. Used to exercise the
/// local-NotFound-then-remote retry inside `fetch_misses`.
#[derive(Debug)]
struct TransitioningLocalStore {
    registry: Arc<TieredStorageRegistry>,
    path: String,
    remote_path: String,
}

impl fmt::Display for TransitioningLocalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransitioningLocalStore")
    }
}

#[async_trait]
impl ObjectStore for TransitioningLocalStore {
    async fn get_opts(&self, location: &Path, _options: GetOptions) -> OsResult<GetResult> {
        // Flip the registry entry to Remote so the tiered store's
        // should_retry_remote check finds the file and retries remotely.
        let rp = self.remote_path.clone();
        self.registry.update(&self.path, move |e| {
            e.location = FileLocation::Remote;
            e.remote_path = Some(Arc::from(rp.as_str()));
        });
        Err(object_store::Error::NotFound {
            path: location.to_string(),
            source: "simulated local miss after sync-to-remote".into(),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        Err(object_store::Error::NotSupported { source: "not supported".into() })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported { source: "not supported".into() })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        Box::pin(locations.map(|_| {
            Err(object_store::Error::NotSupported { source: "not supported".into() })
        }))
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        futures::stream::empty().boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> OsResult<ListResult> {
        Ok(ListResult { common_prefixes: vec![], objects: vec![] })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> OsResult<()> {
        Err(object_store::Error::NotSupported { source: "not supported".into() })
    }
}

// -- Task 4.1: head fast path (Req 2.1-2.4) --------------------------------

#[tokio::test]
async fn unit_getopts_head_returns_registry_meta_and_no_chunk_cache() {
    // A head get_opts answers from try_head_from_registry, produces NO chunk
    // cache entry, and issues ZERO backing get_opts fetches. (Req 2.1, 2.2, 2.3)
    use object_store::GetOptions;

    let (registry, remote, cache, tiered) = setup_counting_remote_with_cache();
    registry.register(
        "h.parquet",
        TieredFileEntry::with_size(FileLocation::Remote, Some("remote/h.parquet".into()), 4096),
    );

    let opts = GetOptions { head: true, ..Default::default() };
    let result = tiered.get_opts(&Path::from("h.parquet"), opts).await.unwrap();

    assert_eq!(result.meta.size, 4096, "head meta.size comes from the registry");
    assert_eq!(result.range, 0..4096, "head range spans the whole file");
    assert_eq!(cache.len(), 0, "head must not populate any chunk cache entry");
    assert!(cache.data_tier_keys().is_empty());
    assert_eq!(
        remote.get_count.load(AtomicOrdering::SeqCst),
        0,
        "head answered from registry issues zero backing fetches"
    );
}

#[tokio::test]
async fn unit_getopts_head_directory_path_returns_not_found() {
    // A head on a directory path returns NotFound unchanged. (Req 2.3)
    use object_store::GetOptions;

    let (registry, _local, cache, tiered) = setup_with_cache();
    registry.register(
        "data/parquet/a.parquet",
        TieredFileEntry::with_size(FileLocation::Remote, Some("remote/a.parquet".into()), 1024),
    );

    let opts = GetOptions { head: true, ..Default::default() };
    let result = tiered.get_opts(&Path::from("data/parquet"), opts).await;

    assert!(matches!(result.unwrap_err(), object_store::Error::NotFound { .. }));
    assert_eq!(cache.len(), 0, "directory head must not populate the cache");
}

#[tokio::test]
async fn unit_getopts_head_absent_from_registry_falls_through_to_backing() {
    // A head for a file absent from the registry falls through to the backing
    // head (here the local store). (Req 2.4)
    use object_store::GetOptions;

    let (_registry, local, _cache, tiered) = setup_with_cache();
    local
        .put(&Path::from("solo.parquet"), PutPayload::from_static(b"hello"))
        .await
        .unwrap();

    let opts = GetOptions { head: true, ..Default::default() };
    let result = tiered.get_opts(&Path::from("solo.parquet"), opts).await.unwrap();

    assert_eq!(result.meta.size, 5, "head falls through to the backing local head");
}

// -- Task 4.2: unresolvable range fallthrough (Req 3.1-3.5) ----------------

#[tokio::test]
async fn unit_getopts_suffix_unresolvable_falls_through_no_cache() {
    // Suffix with unknown file_size -> resolve_range None -> direct path returns
    // correct bytes and writes NOTHING to the cache. (Req 3.1, 3.2, 3.3, 3.4)
    use object_store::{GetOptions, GetRange};

    let (registry, local, cache, tiered) = setup_with_cache();
    let content: &[u8] = b"0123456789ABCDEF"; // 16 bytes
    local
        .put(&Path::from("s.parquet"), PutPayload::from_static(content))
        .await
        .unwrap();
    // Registered WITHOUT a size -> resolve_range(Suffix) yields None.
    registry.register("s.parquet", TieredFileEntry::new(FileLocation::Local, None));

    let opts = GetOptions { range: Some(GetRange::Suffix(5)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("s.parquet"), opts).await.unwrap();
    let bytes = result.bytes().await.unwrap();

    assert_eq!(bytes.as_ref(), &content[content.len() - 5..], "suffix bytes correct");
    assert_eq!(cache.len(), 0, "unresolvable range must not write to the cache");
}

#[tokio::test]
async fn unit_getopts_unresolvable_range_propagates_backing_error() {
    // ErrorStore backing + unresolvable range -> error propagated unchanged. (Req 3.5)
    use object_store::{GetOptions, GetRange};

    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);
    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&error_remote));
    // Remote location but NO size -> Suffix is unresolvable, so the direct
    // remote/local path (which hits ErrorStore) services the read.
    registry.register(
        "e.parquet",
        TieredFileEntry::new(FileLocation::Remote, Some("remote/e.parquet".into())),
    );

    let opts = GetOptions { range: Some(GetRange::Suffix(5)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("e.parquet"), opts).await;

    assert!(result.is_err(), "backing error must propagate for unresolvable range");
}

// -- Task 4.3: oversized streaming guard + boundary (Req 4.1-4.4) ----------
//
// NOTE: MockBlockCache is not a TieredBlockCache, so the router cannot read a
// custom per-entry ceiling from it and falls back to the 32 MiB default
// (Req 4.4 fallback clause). Driving the boundary via `update_max_data_entry_size`
// requires a real TieredBlockCache, which (via FoyerCache) cannot be constructed
// inside a tokio runtime; that movable-boundary case is covered by the
// integration tests (`get_opts_skips_caching_for_large_ranges`). Here we exercise
// the 32 MiB fallback on both sides of the threshold.

#[tokio::test]
async fn unit_getopts_under_threshold_routes_and_caches() {
    // Resolved size <= threshold -> routed through get_ranges -> enclosing chunk
    // is cached. (Req 4.3)
    use object_store::{GetOptions, GetRange};

    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 1 << 20; // 1 MiB single-chunk file
    let data = make_data(file_size);
    local
        .put(&Path::from("u.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "u.parquet",
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("u.parquet"), opts).await.unwrap();
    let bytes = result.bytes().await.unwrap();

    assert_eq!(bytes.as_ref(), &data[100..500], "bytes correct");
    let chunk_key = range_cache_key("u.parquet", 0, file_size);
    assert!(
        cache.data_tier_keys().contains(&chunk_key.as_str().to_string()),
        "within-threshold range routes through get_ranges and caches the enclosing chunk"
    );
}

#[tokio::test]
async fn unit_getopts_oversized_streams_and_skips_cache() {
    // Resolved size strictly > threshold (32 MiB fallback) -> streamed via the
    // direct path, NOT routed through the buffering get_ranges path, and NOT
    // cached. The single backing get_opts confirms the streaming (non-chunked)
    // path. (Req 4.1, 4.2)
    use object_store::{GetOptions, GetRange};

    let (registry, remote, cache, tiered) = setup_counting_remote_with_cache();
    // Just over the 32 MiB fallback ceiling so the whole-file range is oversized.
    let file_size: u64 = 32 * 1024 * 1024 + 1024;
    let data = make_data(file_size);
    remote
        .inner
        .put(&Path::from("remote/o.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "o.parquet",
        TieredFileEntry::with_size(FileLocation::Remote, Some("remote/o.parquet".into()), file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded(0..file_size)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("o.parquet"), opts).await.unwrap();
    let bytes = result.bytes().await.unwrap();

    assert_eq!(bytes.len() as u64, file_size, "full requested length returned");
    assert_eq!(bytes.as_ref(), data.as_ref(), "streamed bytes are correct");
    assert_eq!(cache.len(), 0, "oversized range must not populate the cache");
    assert!(cache.data_tier_keys().is_empty());
    assert_eq!(
        remote.get_count.load(AtomicOrdering::SeqCst),
        1,
        "oversized read streams via a single direct backing get_opts"
    );
}

// -- Task 4.4: returned-result correctness (Req 1.4, 7.2, 7.3, 7.4) --------

#[tokio::test]
async fn unit_getopts_meta_size_equals_registry_size_when_recorded() {
    // meta.size == registry size when a positive size is recorded. (Req 7.3)
    use object_store::{GetOptions, GetRange};

    let (registry, local, _cache, tiered) = setup_with_cache();
    let file_size: u64 = 1 << 20;
    let data = make_data(file_size);
    local
        .put(&Path::from("ms.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "ms.parquet",
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("ms.parquet"), opts).await.unwrap();

    assert_eq!(result.meta.size, file_size, "meta.size reflects the recorded registry size");
}

#[tokio::test]
async fn unit_getopts_meta_size_falls_back_to_end_when_no_size() {
    // meta.size == end when no positive size is recorded for a resolvable Bounded
    // range. (Req 7.4)
    use object_store::{GetOptions, GetRange};

    let (registry, local, _cache, tiered) = setup_with_cache();
    let content = make_data(10_000);
    local
        .put(&Path::from("ns.parquet"), PutPayload::from(content.clone()))
        .await
        .unwrap();
    // Registered WITHOUT a size; Bounded still resolves from its explicit bounds.
    registry.register("ns.parquet", TieredFileEntry::new(FileLocation::Local, None));

    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("ns.parquet"), opts).await.unwrap();

    assert_eq!(result.meta.size, 500, "meta.size falls back to the resolved range end");
    let bytes = result.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), &content[100..500], "bytes still correct on the fallback path");
}

#[tokio::test]
async fn unit_getopts_range_and_payload_length_correct() {
    // result.range == start..end and payload length == end - start. (Req 7.2)
    use object_store::{GetOptions, GetRange};

    let (registry, local, _cache, tiered) = setup_with_cache();
    let file_size: u64 = 1 << 20;
    let data = make_data(file_size);
    local
        .put(&Path::from("rl.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "rl.parquet",
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded(1000..4000)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("rl.parquet"), opts).await.unwrap();
    let range = result.range.clone();
    let bytes = result.bytes().await.unwrap();

    assert_eq!(range, 1000..4000, "result.range equals the resolved absolute offsets");
    assert_eq!(bytes.len() as u64, 4000 - 1000, "payload length equals end - start");
    assert_eq!(bytes.as_ref(), &data[1000..4000], "payload bytes correct");
}

#[tokio::test]
async fn unit_getopts_same_chunk_key_as_get_ranges() {
    // get_opts and get_ranges produce the IDENTICAL chunk key for the same chunk
    // (compared on fresh caches). (Req 1.4)
    use object_store::{GetOptions, GetRange};

    let file_size: u64 = 1 << 20; // single-chunk file: enclosing chunk is 0..file_size
    let data = make_data(file_size);

    // get_opts path on a fresh cache.
    let (reg_a, local_a, cache_a, tiered_a) = setup_with_cache();
    local_a
        .put(&Path::from("k.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    reg_a.register("k.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));
    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let _ = tiered_a
        .get_opts(&Path::from("k.parquet"), opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let keys_opts = cache_a.data_tier_keys();

    // get_ranges path on a fresh cache.
    let (reg_b, local_b, cache_b, tiered_b) = setup_with_cache();
    local_b
        .put(&Path::from("k.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    reg_b.register("k.parquet", TieredFileEntry::with_size(FileLocation::Local, None, file_size));
    let _ = tiered_b.get_ranges(&Path::from("k.parquet"), &[100..500]).await.unwrap();
    let keys_ranges = cache_b.data_tier_keys();

    let expected = range_cache_key("k.parquet", 0, file_size).as_str().to_string();
    assert_eq!(keys_opts, vec![expected.clone()], "get_opts caches under the chunk key");
    assert_eq!(keys_ranges, vec![expected], "get_ranges caches under the same chunk key");
    assert_eq!(keys_opts, keys_ranges, "both paths use the identical chunk key");
}

// -- Task 5.1: cross-boundary unit tests (Req 5.1-5.4, 6.4) ----------------

#[tokio::test]
async fn unit_getopts_cross_boundary_returns_full_bytes_and_caches_each_chunk() {
    // A cross-boundary get_opts read returns the full requested bytes and caches
    // each chunk the range overlaps. (Req 5.1, 5.2, 5.3)
    use object_store::{GetOptions, GetRange};

    let (registry, local, cache, tiered) = setup_with_cache();
    let file_size: u64 = 16 << 20; // two full chunks
    let data = make_data(file_size);
    local
        .put(&Path::from("cb.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "cb.parquet",
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    // [7 MiB .. 9 MiB) straddles the 8 MiB boundary.
    let start = 7 << 20;
    let end = 9 << 20;
    let opts = GetOptions { range: Some(GetRange::Bounded(start..end)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("cb.parquet"), opts).await.unwrap();
    let bytes = result.bytes().await.unwrap();

    assert_eq!(bytes.len() as u64, end - start, "full requested length returned");
    assert_eq!(bytes.as_ref(), &data[start as usize..end as usize], "cross-boundary bytes correct");

    // Both overlapped chunks are cached in full in the data tier.
    let chunk_0 = range_cache_key("cb.parquet", 0, CHUNK).as_str().to_string();
    let chunk_1 = range_cache_key("cb.parquet", CHUNK, 2 * CHUNK).as_str().to_string();
    let keys = cache.data_tier_keys();
    assert_eq!(keys.len(), 2, "exactly two chunk entries cached");
    assert!(keys.contains(&chunk_0), "chunk 0 cached");
    assert!(keys.contains(&chunk_1), "chunk 1 cached");
}

#[tokio::test]
async fn unit_getopts_cross_boundary_error_no_partial_bytes_no_cache_writes() {
    // An ErrorStore mid cross-boundary yields an error, no partial bytes, and
    // both tiers unchanged. (Req 5.4, 6.4)
    use object_store::{GetOptions, GetRange};

    let registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(InMemory::new());
    let cache = Arc::new(MockBlockCache::new());
    let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);
    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _)
        .with_cache(Arc::clone(&cache) as _);
    tiered.set_remote(Arc::clone(&error_remote));

    let file_size: u64 = 16 << 20;
    registry.register(
        "cbe.parquet",
        TieredFileEntry::with_size(FileLocation::Remote, Some("remote/cbe.parquet".into()), file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded((7 << 20)..(9 << 20))), ..Default::default() };
    let result = tiered.get_opts(&Path::from("cbe.parquet"), opts).await;

    assert!(result.is_err(), "cross-boundary fetch failure propagates an error");
    assert_eq!(cache.len(), 0, "no cache entry written on fetch failure");
    assert!(cache.data_tier_keys().is_empty(), "data tier unchanged");
    assert!(cache.metadata_tier_keys().is_empty(), "metadata tier unchanged");
}

// -- Task 6.1: remote/local routing + retry (Req 8.1-8.5) ------------------

#[tokio::test]
async fn unit_getopts_remote_file_returns_remote_bytes() {
    // Remote file -> bytes from the remote store. (Req 8.1)
    use object_store::{GetOptions, GetRange};

    let (registry, remote, _cache, tiered) = setup_counting_remote_with_cache();
    let file_size: u64 = 1 << 20;
    let data = make_data(file_size);
    remote
        .inner
        .put(&Path::from("remote/r.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "r.parquet",
        TieredFileEntry::with_size(FileLocation::Remote, Some("remote/r.parquet".into()), file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let bytes = tiered
        .get_opts(&Path::from("r.parquet"), opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(bytes.as_ref(), &data[100..500], "remote bytes returned");
}

#[tokio::test]
async fn unit_getopts_local_file_returns_local_bytes() {
    // Local file -> bytes from the local store. (Req 8.2)
    use object_store::{GetOptions, GetRange};

    let (registry, local, _cache, tiered) = setup_with_cache();
    let file_size: u64 = 1 << 20;
    let data = make_data(file_size);
    local
        .put(&Path::from("l.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    registry.register(
        "l.parquet",
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );

    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let bytes = tiered
        .get_opts(&Path::from("l.parquet"), opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(bytes.as_ref(), &data[100..500], "local bytes returned");
}

#[tokio::test]
async fn unit_getopts_local_notfound_retries_remote_once() {
    // Local NotFound then the file is Remote in the registry -> retried against
    // remote exactly once, returns remote bytes, through the
    // get_opts -> get_ranges -> fetch_misses path. (Req 8.3, 8.5)
    use object_store::{GetOptions, GetRange};

    let registry = Arc::new(TieredStorageRegistry::new());
    let file_size: u64 = 1 << 20;
    let data = make_data(file_size);

    let remote = Arc::new(CallCountingStore::new());
    remote
        .inner
        .put(&Path::from("remote/rt.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();

    let local = Arc::new(TransitioningLocalStore {
        registry: Arc::clone(&registry),
        path: "rt.parquet".to_string(),
        remote_path: "remote/rt.parquet".to_string(),
    });

    let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
    tiered.set_remote(Arc::clone(&remote) as _);
    // Starts LOCAL so fetch_misses tries local first, gets NotFound, then retries
    // remote after the store flips the registry entry to Remote.
    registry.register("rt.parquet", TieredFileEntry::new(FileLocation::Local, None));

    let opts = GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };
    let bytes = tiered
        .get_opts(&Path::from("rt.parquet"), opts)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(bytes.as_ref(), &data[100..500], "remote bytes returned after retry");
    assert_eq!(
        remote.get_count.load(AtomicOrdering::SeqCst),
        1,
        "remote is retried exactly once"
    );
}

#[tokio::test]
async fn unit_getopts_local_notfound_not_remote_propagates() {
    // Local NotFound and not Remote -> NotFound propagated, registry unchanged. (Req 8.4)
    use object_store::{GetOptions, GetRange};

    let (registry, _local, _cache, tiered) = setup_with_cache();
    // Registered LOCAL with no backing bytes -> local read is NotFound, and the
    // file never transitions to Remote, so no retry occurs.
    registry.register("nf.parquet", TieredFileEntry::new(FileLocation::Local, None));

    let opts = GetOptions { range: Some(GetRange::Bounded(0..100)), ..Default::default() };
    let result = tiered.get_opts(&Path::from("nf.parquet"), opts).await;

    assert!(
        matches!(result.unwrap_err(), object_store::Error::NotFound { .. }),
        "NotFound propagated without remote retry"
    );
    // Registry entry unchanged: still present and still Local.
    let guard = registry.get("nf.parquet").expect("registry entry still present");
    assert!(matches!(guard.location(), FileLocation::Local), "location remains Local");
    assert_eq!(registry.len(), 1, "registry has exactly one (unchanged) entry");
}

#[tokio::test]
async fn unit_getopts_routed_matches_direct_for_remote_and_local() {
    // A routed range read returns the same bytes as a direct get_opts of the same
    // range, for both Remote and Local locations. (Req 8.5)
    use object_store::{GetOptions, GetRange};

    let file_size: u64 = 1 << 20;
    let data = make_data(file_size);
    let mk_opts = || GetOptions { range: Some(GetRange::Bounded(100..500)), ..Default::default() };

    // Remote: routed read vs direct backing read.
    let (reg_r, remote, _cache_r, tiered_r) = setup_counting_remote_with_cache();
    remote
        .inner
        .put(&Path::from("remote/x.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    reg_r.register(
        "x.parquet",
        TieredFileEntry::with_size(FileLocation::Remote, Some("remote/x.parquet".into()), file_size),
    );
    let routed_r = tiered_r
        .get_opts(&Path::from("x.parquet"), mk_opts())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let direct_r = remote
        .inner
        .get_opts(&Path::from("remote/x.parquet"), mk_opts())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(routed_r, direct_r, "routed remote read matches a direct remote get_opts");

    // Local: routed read vs direct backing read.
    let (reg_l, local, _cache_l, tiered_l) = setup_with_cache();
    local
        .put(&Path::from("y.parquet"), PutPayload::from(data.clone()))
        .await
        .unwrap();
    reg_l.register(
        "y.parquet",
        TieredFileEntry::with_size(FileLocation::Local, None, file_size),
    );
    let routed_l = tiered_l
        .get_opts(&Path::from("y.parquet"), mk_opts())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let direct_l = local
        .get_opts(&Path::from("y.parquet"), mk_opts())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(routed_l, direct_l, "routed local read matches a direct local get_opts");
}
