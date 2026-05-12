/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ReadOnlyDataFormatAwareEngine}.
 */
public class ReadOnlyDataFormatAwareEngineTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Store store;
    private ShardId shardId;
    private AtomicLong primaryTerm;
    private MockDataFormat mockDataFormat;
    private MockDataFormatPlugin mockPlugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        primaryTerm = new AtomicLong(1L);
        mockDataFormat = new MockDataFormat("composite", 100L, Set.of());
        mockPlugin = MockDataFormatPlugin.of(mockDataFormat);
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            store.close();
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    /**
     * A warm shard with no committed CatalogSnapshot must fail at engine creation.
     * This guards against migrating to warm before the hot flush that serialises the
     * CatalogSnapshot into segments_N.userData.
     */
    public void testConstructionFailsWithEmptyCatalogSnapshot() throws IOException {
        Path translogPath = createTempDir();
        bootstrapStore(store);
        CommitterFactory emptyFactory = config -> new InMemoryCommitter(store);

        expectThrows(EngineCreationFailureException.class, () -> {
            try (ReadOnlyDataFormatAwareEngine engine = createEngine(translogPath, emptyFactory, 0L, 0L)) {
                // should not reach here
            }
        });
    }

    /**
     * When the committed CatalogSnapshot is present and maxSeqNo equals globalCheckpoint
     * the engine opens successfully. Basic seq-no invariants must hold — they are frozen
     * from the committed state and must never advance on a sealed warm shard.
     */
    public void testConstructionSucceedsAndSeqNoIsFrozen() throws IOException {
        Path translogPath = createTempDir();
        bootstrapStore(store);
        long maxSeqNo = 42L;

        try (ReadOnlyDataFormatAwareEngine engine = createEngine(translogPath, buildSnapshotFactory(maxSeqNo), maxSeqNo, maxSeqNo)) {
            assertNotNull(engine.translogManager());
            assertEquals(maxSeqNo, engine.getPersistedLocalCheckpoint());
            assertEquals(maxSeqNo, engine.getProcessedLocalCheckpoint());
            assertEquals(maxSeqNo, engine.getSeqNoStats(maxSeqNo).getMaxSeqNo());
        }
    }

    /**
     * maxSeqNo ≠ globalCheckpoint at engine open means the shard was not fully
     * replicated before warm migration. The engine must refuse to open so that
     * the warm shard is never served with missing documents.
     */
    public void testConstructionFailsWhenShardNotFullySealed() throws IOException {
        Path translogPath = createTempDir();
        bootstrapStore(store);
        long maxSeqNo = 10L;
        long globalCheckpoint = 5L; // shard is not caught up

        expectThrows(IllegalStateException.class, () -> {
            try (ReadOnlyDataFormatAwareEngine engine = createEngine(translogPath, buildSnapshotFactory(maxSeqNo), maxSeqNo, globalCheckpoint)) {
                // should not reach here
            }
        });
    }

    /**
     * Any write operation on a warm engine must throw UnsupportedOperationException.
     * Warm indexes are permanently sealed after migration.
     */
    public void testWriteOperationThrowsUnsupportedOperation() throws IOException {
        Path translogPath = createTempDir();
        bootstrapStore(store);

        try (ReadOnlyDataFormatAwareEngine engine = createEngine(translogPath, buildSnapshotFactory(0L), 0L, 0L)) {
            expectThrows(UnsupportedOperationException.class, () -> engine.index(mock(Engine.Index.class)));
            expectThrows(UnsupportedOperationException.class, () -> engine.delete(mock(Engine.Delete.class)));
        }
    }

    /**
     * After close, ensureOpen must throw AlreadyClosedException so that any
     * in-flight operation can detect the closed state.
     */
    public void testOperationsAfterCloseThrowAlreadyClosed() throws IOException {
        Path translogPath = createTempDir();
        bootstrapStore(store);

        ReadOnlyDataFormatAwareEngine engine = createEngine(translogPath, buildSnapshotFactory(0L), 0L, 0L);
        engine.close();

        expectThrows(AlreadyClosedException.class, engine::ensureOpen);
    }

    private ReadOnlyDataFormatAwareEngine createEngine(
        Path translogPath,
        CommitterFactory committerFactory,
        long maxSeqNo,
        long globalCheckpoint
    ) throws IOException {
        IndexSettings indexSettings = warmIndexSettings();
        String uuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, uuid, false);
        EngineConfig config = new EngineConfig.Builder()
            .shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(List.of())
            .internalRefreshListener(List.of())
            .globalCheckpointSupplier(() -> globalCheckpoint)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(EngineTestCase.tombstoneDocSupplier())
            .dataFormatRegistry(createMockRegistry())
            .committerFactory(committerFactory)
            .build();
        return new ReadOnlyDataFormatAwareEngine(config);
    }

    private CommitterFactory buildSnapshotFactory(long seqNo) throws IOException {
        return config -> new WarmShardCommitter(store, seqNo);
    }

    private void bootstrapStore(Store store) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                store.directory(),
                new IndexWriterConfig(Lucene.STANDARD_ANALYZER)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            )
        ) {
            Map<String, String> commitData = new HashMap<>();
            commitData.put(Translog.TRANSLOG_UUID_KEY, UUID.randomUUID().toString());
            commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            commitData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            commitData.put(Engine.HISTORY_UUID_KEY, UUID.randomUUID().toString());
            writer.setLiveCommitData(commitData.entrySet());
            writer.commit();
        }
    }

    private Store createStore() throws IOException {
        Directory dir = newDirectory();
        IndexSettings indexSettings = warmIndexSettings();
        Path path = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        ShardPath shardPath = new ShardPath(false, path, path, shardId);
        return new Store(shardId, indexSettings, dir, new DummyShardLock(shardId), Store.OnClose.EMPTY, shardPath, new FsDirectoryFactory());
    }

    private IndexSettings warmIndexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), mockDataFormat.name())
                .put(org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                .build()
        );
    }

    private DataFormatRegistry createMockRegistry() {
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(mockPlugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(
            List.of(new MockSearchBackEndPlugin(List.of(mockDataFormat.name())))
        );
        return new DataFormatRegistry(pluginsService);
    }

    private static class WarmShardCommitter extends InMemoryCommitter {
        private final long seqNo;

        WarmShardCommitter(Store store, long seqNo) throws IOException {
            super(store);
            this.seqNo = seqNo;
        }

        @Override
        public java.util.List<CatalogSnapshot> listCommittedSnapshots() {
            Map<String, String> userData = new HashMap<>();
            userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(seqNo));
            userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(seqNo));
            userData.put(Translog.TRANSLOG_UUID_KEY, UUID.randomUUID().toString());
            userData.put(Engine.HISTORY_UUID_KEY, UUID.randomUUID().toString());
            return List.of(CatalogSnapshotManager.createInitialSnapshot(1L, 1L, 1L, List.of(), 1L, userData));
        }

        @Override
        public Map<String, String> getLastCommittedData() {
            Map<String, String> data = new HashMap<>();
            data.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(seqNo));
            data.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(seqNo));
            data.put(Translog.TRANSLOG_UUID_KEY, UUID.randomUUID().toString());
            data.put(Engine.HISTORY_UUID_KEY, UUID.randomUUID().toString());
            return data;
        }
    }
}
