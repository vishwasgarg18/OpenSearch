/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.ReadOnlyDataFormatAwareEngine;

/**
 * {@link IndexerFactory} that creates the correct {@link Indexer} for pluggable data format shards,
 * routing based on the shard role and warm/hot tiering state.
 *
 * <p>Routing logic:
 * <ul>
 *   <li>Warm composite primary ({@code index.warm=true} + {@code DataFormatRegistry} present + not replica):
 *       → {@link ReadOnlyDataFormatAwareEngine} — sealed, read-only, no translog/writers/merges</li>
 *   <li>Warm composite replica ({@code index.warm=true} + {@code DataFormatRegistry} present + replica):
 *       → {@link UnsupportedOperationException} until {@code DataFormatAwareNRTReplicationEngine} is built</li>
 *   <li>Hot composite primary ({@code DataFormatRegistry} present, not warm):
 *       → {@link DataFormatAwareEngine} — full read-write engine</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Override
    public Indexer createIndexer(EngineConfig config) {
        boolean isWarmIndex = config.getIndexSettings().isWarmIndex();
        // DataFormatRegistry being non-null is the definitive signal that the pluggable
        // data format feature is enabled. It is only set when the PLUGGABLE_DATAFORMAT
        // feature flag is on AND index.pluggable.dataformat.enabled = true.
        boolean isPluggableDataFormat = config.getDataFormatRegistry() != null;

        if (isWarmIndex && isPluggableDataFormat) {
            if (config.isReadOnlyReplica() == false) {
                // Warm composite PRIMARY: sealed, read-only, serves queries from committed CatalogSnapshot.
                // No translog, no writers, no merge scheduler — just CatalogSnapshotManager + EngineReaderManagers.
                return new ReadOnlyDataFormatAwareEngine(config);
            }
            // Warm composite REPLICA: requires DataFormatAwareNRTReplicationEngine (future PR, tracked separately).
            // The replica needs the NRT replication protocol for initial sync from the primary at startup.
            throw new UnsupportedOperationException(
                "NRT DataFormat replica engine not yet implemented for warm composite shards. "
                    + "Shard ["
                    + config.getShardId()
                    + "]: isWarmIndex=true with isReadOnlyReplica=true is not yet supported."
            );
        }

        // Hot composite primary (and any non-warm case): full read-write engine
        return new DataFormatAwareEngine(config);
    }
}
