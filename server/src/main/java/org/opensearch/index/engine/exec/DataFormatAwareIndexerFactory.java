/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.DataFormatAwareNRTReplicationEngine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.ReadOnlyDataFormatAwareEngine;

/**
 * {@link IndexerFactory} that selects the correct {@link Indexer} for pluggable-dataformat shards.
 *
 * <ul>
 *   <li>Segment-replication replica → {@link DataFormatAwareNRTReplicationEngine}</li>
 *   <li>Warm primary (isWarmIndex + dataFormatRegistry present) → {@link ReadOnlyDataFormatAwareEngine}</li>
 *   <li>Hot primary → {@link DataFormatAwareEngine}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFormatAwareIndexerFactory implements IndexerFactory {

    @Override
    public Indexer createIndexer(EngineConfig config) {
        if (config.isReadOnlyReplica()) {
            return new DataFormatAwareNRTReplicationEngine(config);
        }

        if (config.getIndexSettings().isWarmIndex() && config.getDataFormatRegistry() != null) {
            return new ReadOnlyDataFormatAwareEngine(config);
        }

        return new DataFormatAwareEngine(config);
    }
}
