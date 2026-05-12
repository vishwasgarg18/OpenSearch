/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.broadcast.BroadcastResponse;

/**
 * Action type for pre-warm-tier shard sync: forces flush + remote store upload
 * on all primary shards of an index before warm migration.
 */
public class WarmTierPreSyncAction extends ActionType<BroadcastResponse> {

    public static final WarmTierPreSyncAction INSTANCE = new WarmTierPreSyncAction();
    public static final String NAME = "indices:admin/tiering/pre_warm_sync";

    private WarmTierPreSyncAction() {
        super(NAME, BroadcastResponse::new);
    }
}
