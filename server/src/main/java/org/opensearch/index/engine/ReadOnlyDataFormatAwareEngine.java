/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A read-only {@link Indexer} implementation for warm Mustang/Parquet composite indexes.
 *
 * <p>Warm indexes are immutable after migration from hot to warm tier. This engine:
 * <ul>
 *   <li>Reads the committed {@link CatalogSnapshot} from the last commit at startup</li>
 *   <li>Builds {@link EngineReaderManager}s for all registered data formats (Lucene + DataFusion/Parquet)</li>
 *   <li>Rejects all write operations with {@link UnsupportedOperationException}</li>
 *   <li>Treats lifecycle operations (refresh, flush, forceMerge) as no-ops</li>
 *   <li>Returns frozen seq-no stats from the committed state — never changes</li>
 *   <li>Uses {@link NoOpTranslogManager} — no translog activity on warm</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ReadOnlyDataFormatAwareEngine implements Indexer {

    private final Logger logger;
    private final EngineConfig engineConfig;
    private final ShardId shardId;
    private final Store store;

    // Per-format reader managers — one per DataFormat (Lucene, Parquet, etc.)
    private final Map<DataFormat, EngineReaderManager<?>> readerManagers;

    // Manages the lifecycle and reference counting of the committed CatalogSnapshot.
    // Initialized with no-op deleters: warm shards never delete segment files from remote store.
    private final CatalogSnapshotManager catalogSnapshotManager;

    // Used read-only at startup to deserialize the committed CatalogSnapshot and seq-no state.
    // commit() is never called on warm shards.
    private final Committer committer;

    // No-op translog: warm shards have no translog activity and skip recovery replay on startup.
    private final TranslogManager translogManager;

    // Seq-no state frozen from the last committed userData. Never advances on warm.
    private final SeqNoStats seqNoStats;

    @Nullable
    private final String historyUUID;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final SetOnce<Exception> failedEngine = new SetOnce<>();
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private final ReentrantLock failEngineLock = new ReentrantLock();

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final ReleasableLock readLock = new ReleasableLock(rwl.readLock());
    private final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());

    /**
     * Opens a read-only engine for a warm composite shard.
     *
     * <p>Reads the committed {@link CatalogSnapshot} from the last Lucene commit,
     * builds per-format {@link EngineReaderManager}s, and opens readers immediately.
     * The shard must have been flushed at least once before migration to warm — an empty
     * committed snapshot is a fatal error.
     *
     * <p>Validates that {@code maxSeqNo == globalCheckpoint}: a warm shard must have all
     * operations globally acknowledged before it is sealed. A mismatch indicates the shard
     * was migrated before being fully flushed and replicated.
     *
     * @param engineConfig the engine configuration; must have a {@link DataFormatRegistry} set
     * @throws EngineCreationFailureException if the committed state cannot be read
     * @throws IllegalStateException if maxSeqNo does not equal globalCheckpoint at open time
     */
    public ReadOnlyDataFormatAwareEngine(EngineConfig engineConfig) {
        this.logger = Loggers.getLogger(ReadOnlyDataFormatAwareEngine.class, engineConfig.getShardId());
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();

        boolean success = false;
        try {
            store.incRef();

            this.committer = engineConfig.getCommitterFactory().getCommitter(new CommitterConfig(engineConfig));

            final Map<String, String> userData = committer.getLastCommittedData();
            this.historyUUID = userData.get(Engine.HISTORY_UUID_KEY);
            final String translogUUID = userData.getOrDefault(Translog.TRANSLOG_UUID_KEY, "");

            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
            this.seqNoStats = new SeqNoStats(
                commitInfo.maxSeqNo,
                commitInfo.localCheckpoint,
                engineConfig.getGlobalCheckpointSupplier().getAsLong()
            );
            ensureMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats);

            // skipRecovery=true: the committed CatalogSnapshot is the complete state;
            // translog replay would be incorrect on a sealed warm shard.
            this.translogManager = new NoOpTranslogManager(
                shardId,
                readLock,
                this::ensureOpen,
                new TranslogStats(),
                Translog.EMPTY_TRANSLOG_SNAPSHOT,
                translogUUID,
                true
            );

            // Build EngineReaderManagers via the DataFormatRegistry. Each registered
            // SearchBackEndPlugin contributes a reader manager for its format.
            DataFormatRegistry registry = engineConfig.getDataFormatRegistry();
            this.readerManagers = registry.getReaderManager(
                new ReaderManagerConfig(
                    Optional.empty(),
                    registry.format(engineConfig.getIndexSettings().pluggableDataFormat()),
                    registry,
                    store.shardPath()
                )
            );

            List<CatalogSnapshot> committedSnapshots = committer.listCommittedSnapshots();
            if (committedSnapshots.isEmpty()) {
                throw new EngineCreationFailureException(
                    shardId,
                    "warm shard has no committed CatalogSnapshot — was it flushed before migration to warm?",
                    null
                );
            }

            Map<String, FilesListener> filesListeners = new HashMap<>();
            List<CatalogSnapshotLifecycleListener> snapshotListeners = new ArrayList<>();
            for (Map.Entry<DataFormat, EngineReaderManager<?>> entry : readerManagers.entrySet()) {
                filesListeners.put(entry.getKey().name(), entry.getValue());
                snapshotListeners.add(entry.getValue());
            }
            // Null shardPath: no orphan scanning (files live in remote store, not local disk).
            // Null commitFileManager: warm engine never writes new commits.
            // No-op file deleter: segment files in remote store must never be deleted.
            this.catalogSnapshotManager = new CatalogSnapshotManager(
                committedSnapshots,
                null,
                (filesToDelete) -> Map.of(),
                filesListeners,
                snapshotListeners,
                null,
                null
            );

            // Notify reader managers with the committed snapshot so they open readers immediately.
            CatalogSnapshot committedSnapshot = committedSnapshots.getLast();
            for (EngineReaderManager<?> rm : readerManagers.values()) {
                rm.afterRefresh(true, committedSnapshot);
            }

            success = true;
            logger.debug(
                "ReadOnlyDataFormatAwareEngine opened for warm shard [{}], maxSeqNo=[{}], localCheckpoint=[{}]",
                shardId,
                seqNoStats.getMaxSeqNo(),
                seqNoStats.getLocalCheckpoint()
            );
        } catch (IOException e) {
            throw new EngineCreationFailureException(shardId, "failed to create ReadOnlyDataFormatAwareEngine", e);
        } finally {
            if (success == false) {
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
    }

    /**
     * Verifies that maxSeqNo equals globalCheckpoint when opening a warm shard.
     * A warm shard must be fully sealed — all operations must be globally acknowledged
     * before migration. If this invariant is violated, the shard was migrated prematurely.
     */
    private void ensureMaxSeqNoEqualsToGlobalCheckpoint(SeqNoStats seqNoStats) {
        if (seqNoStats.getMaxSeqNo() != seqNoStats.getGlobalCheckpoint()) {
            throw new IllegalStateException(
                "Maximum sequence number ["
                    + seqNoStats.getMaxSeqNo()
                    + "] from last commit does not match global checkpoint ["
                    + seqNoStats.getGlobalCheckpoint()
                    + "] on warm shard ["
                    + shardId
                    + "] — shard may not have been fully flushed and replicated before warm migration"
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // READ PATH
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        ensureOpen();
        return catalogSnapshotManager.acquireSnapshot();
    }

    /**
     * Acquires a per-format reader backed by the current catalog snapshot.
     * Returns a {@link DataFormatAwareEngine.DataFormatAwareReader} containing
     * handles for all registered formats (Lucene DirectoryReaders, DataFusion readers, etc.).
     */
    public GatedCloseable<IndexReaderProvider.Reader> acquireReader() throws IOException {
        ensureOpen();
        GatedCloseable<CatalogSnapshot> snapshotRef = catalogSnapshotManager.acquireSnapshot();
        try {
            CatalogSnapshot catalogSnapshot = snapshotRef.get();
            Map<DataFormat, Object> readers = new HashMap<>();
            for (Map.Entry<DataFormat, EngineReaderManager<?>> entry : readerManagers.entrySet()) {
                Object reader = entry.getValue().getReader(catalogSnapshot);
                if (reader != null) {
                    readers.put(entry.getKey(), reader);
                }
            }
            DataFormatAwareEngine.DataFormatAwareReader reader = new DataFormatAwareEngine.DataFormatAwareReader(snapshotRef, readers);
            return new GatedCloseable<>(reader, reader::close);
        } catch (Exception e) {
            snapshotRef.close();
            throw e;
        }
    }

    /**
     * Not supported on warm shards — replicas recover directly from remote store,
     * not from a local index commit on the primary.
     */
    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        throw new UnsupportedOperationException(
            "acquireSafeIndexCommit is not supported on warm read-only engine for shard [" + shardId + "]"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // WRITE OPERATIONS — rejected: warm indexes are permanently immutable
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        throw new UnsupportedOperationException(
            "indexing is not supported on warm read-only engine for shard [" + shardId + "]"
        );
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        throw new UnsupportedOperationException(
            "deletes are not supported on warm read-only engine for shard [" + shardId + "]"
        );
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        throw new UnsupportedOperationException(
            "no-ops are not supported on warm read-only engine for shard [" + shardId + "]"
        );
    }

    @Override
    public Engine.Index prepareIndex(
        DocumentMapperForType docMapper,
        SourceToParse source,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long autoGeneratedIdTimestamp,
        boolean isRetry,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        throw new UnsupportedOperationException(
            "prepareIndex is not supported on warm read-only engine for shard [" + shardId + "]"
        );
    }

    @Override
    public Engine.Delete prepareDelete(
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        throw new UnsupportedOperationException(
            "prepareDelete is not supported on warm read-only engine for shard [" + shardId + "]"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // LIFECYCLE OPERATIONS — no-ops: infrastructure calls these unconditionally
    // on all shards; throwing would crash the shard unnecessarily.
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public void refresh(String source) throws EngineException {}

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {}

    @Override
    public void flush() {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {}

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {}

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public void onSettingsChanged(
        org.opensearch.common.unit.TimeValue translogRetentionAge,
        ByteSizeValue translogRetentionSize,
        long softDeletesRetentionOps
    ) {}

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public void maybePruneDeletes() {}

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {}

    // ─────────────────────────────────────────────────────────────────────────
    // SEQ-NO STATE — frozen from the last committed userData, never advances
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public long getPersistedLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public long lastRefreshedCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(seqNoStats.getMaxSeqNo(), seqNoStats.getLocalCheckpoint(), globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return seqNoStats.getGlobalCheckpoint();
    }

    @Override
    public long getMinRetainedSeqNo() {
        return 0L;
    }

    @Override
    public long getMaxSeenAutoIdTimestamp() {
        return Long.MIN_VALUE;
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {}

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return seqNoStats.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {}

    @Override
    public long getLastWriteNanos() {
        return 0L;
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        // No translog on warm; operation-based recovery is only possible
        // if the caller is asking for ops beyond the max seq no we have.
        return startingSeqNo > seqNoStats.getMaxSeqNo();
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // STATISTICS
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public CommitStats commitStats() {
        return committer.getCommitStats();
    }

    @Override
    public DocsStats docStats() {
        try (GatedCloseable<CatalogSnapshot> snapshot = acquireSnapshot()) {
            long count = snapshot.get()
                .getSegments()
                .stream()
                .flatMap(seg -> seg.dfGroupedSearchableFiles().values().stream())
                .mapToLong(wfs -> wfs.numRows())
                .sum();
            long totalSize = snapshot.get()
                .getSegments()
                .stream()
                .flatMap(seg -> seg.dfGroupedSearchableFiles().values().stream())
                .mapToLong(wfs -> wfs.getTotalSize())
                .sum();
            return new DocsStats.Builder().deleted(0L).count(count).totalSizeInBytes(totalSize).build();
        } catch (IOException ex) {
            throw new org.opensearch.OpenSearchException(ex);
        }
    }

    @Override
    public SegmentsStats segmentsStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        return new SegmentsStats();
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStats();
    }

    @Override
    public PollingIngestStats pollingIngestStats() {
        return null;
    }

    @Override
    public MergeStats getMergeStats() {
        return new MergeStats();
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0L;
    }

    @Override
    public long getWritingBytes() {
        return 0L;
    }

    @Override
    public long unreferencedFileCleanUpsPerformed() {
        return 0L;
    }

    @Override
    public long getNativeBytesUsed() {
        return 0L;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0L;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INFRASTRUCTURE
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public EngineConfig config() {
        return engineConfig;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return committer.getSafeCommitInfo();
    }

    @Override
    public TranslogManager translogManager() {
        return translogManager;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return () -> {};
    }

    /** Returns an empty snapshot — warm shards have no translog history. */
    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        return Translog.EMPTY_TRANSLOG_SNAPSHOT;
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    /** Warm shards are already sealed; close without flushing. */
    @Override
    public void flushAndClose() throws IOException {
        close();
    }

    @Override
    public void failEngine(String reason, @Nullable Exception failure) {
        if (failEngineLock.tryLock()) {
            try {
                if (failedEngine.get() != null) {
                    logger.warn(
                        () -> new ParameterizedMessage("tried to fail engine but already failed, ignoring. [{}]", reason),
                        failure
                    );
                    return;
                }
                failedEngine.set(failure != null ? failure : new IllegalStateException(reason));
                try {
                    closeNoLock("engine failed on: [" + reason + "]");
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed engine [{}]", reason), failure);
                    engineConfig.getEventListener().onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null) {
                    inner.addSuppressed(failure);
                }
                logger.warn("failEngine threw exception", inner);
            }
        } else {
            logger.debug(() -> new ParameterizedMessage("tried to fail engine but could not acquire lock [{}]", reason), failure);
        }
    }

    @Override
    public void ensureOpen() {
        if (isClosed.get()) {
            throw new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) {
            try (ReleasableLock lock = writeLock.acquire()) {
                closeNoLock("api");
            }
        }
        awaitPendingClose();
    }

    private void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock or fail lock must be held";
            try {
                IOUtils.close(committer, translogManager);
                closeReaders();
            } catch (Exception e) {
                logger.warn("failed to close ReadOnlyDataFormatAwareEngine resources", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("ReadOnlyDataFormatAwareEngine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    private void closeReaders() throws IOException {
        List<Exception> exceptions = new ArrayList<>();
        for (EngineReaderManager<?> rm : readerManagers.values()) {
            try {
                rm.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        if (exceptions.isEmpty() == false) {
            IOException ioException = new IOException("Failed to close ReadOnlyDataFormatAwareEngine reader managers");
            for (Exception e : exceptions) {
                ioException.addSuppressed(e);
            }
            throw ioException;
        }
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
