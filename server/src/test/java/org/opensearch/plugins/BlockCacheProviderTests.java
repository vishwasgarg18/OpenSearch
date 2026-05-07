/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

/**
 * Unit tests for the {@link BlockCacheProvider} SPI interface defaults.
 *
 * <p>Verifies the default method contracts that every implementation inherits:
 * <ul>
 *   <li>{@link BlockCacheProvider#requestedCapacityBytes} returns 0 by default.</li>
 *   <li>{@link BlockCacheProvider#dataToCapacityRatio} returns 1.0 by default.</li>
 * </ul>
 * These defaults are semantically important: a plugin that does not override
 * them must not accidentally claim SSD budget or inflate virtual capacity.
 */
public class BlockCacheProviderTests extends OpenSearchTestCase {

    // ── Default implementation via anonymous class ─────────────────────────────

    private static final BlockCacheProvider MINIMAL_PROVIDER = new BlockCacheProvider() {
        @Override
        public Optional<BlockCache> getBlockCache() {
            return Optional.empty();
        }
    };

    // ── requestedCapacityBytes default ────────────────────────────────────────

    public void testRequestedCapacityBytesDefaultIsZero() {
        assertEquals(0L, MINIMAL_PROVIDER.requestedCapacityBytes(Settings.EMPTY, 1_000_000L));
    }

    public void testRequestedCapacityBytesDefaultIsZeroForAnyBudget() {
        assertEquals(0L, MINIMAL_PROVIDER.requestedCapacityBytes(Settings.EMPTY, 0L));
        assertEquals(0L, MINIMAL_PROVIDER.requestedCapacityBytes(Settings.EMPTY, Long.MAX_VALUE));
    }

    public void testRequestedCapacityBytesDefaultDoesNotDependOnSettings() {
        Settings custom = Settings.builder().put("block_cache.size", "50%").build();
        // Default impl ignores settings and always returns 0
        assertEquals(0L, MINIMAL_PROVIDER.requestedCapacityBytes(custom, 1_000_000L));
    }

    // ── dataToCapacityRatio default ────────────────────────────────────────────

    public void testDataToCapacityRatioDefaultIsOne() {
        assertEquals(1.0, MINIMAL_PROVIDER.dataToCapacityRatio(Settings.EMPTY), 0.0);
    }

    public void testDataToCapacityRatioDefaultDoesNotDependOnSettings() {
        Settings custom = Settings.builder().put("block_cache.data_to_cache_ratio", "10.0").build();
        // Default impl ignores settings and always returns 1.0
        assertEquals(1.0, MINIMAL_PROVIDER.dataToCapacityRatio(custom), 0.0);
    }

    public void testDataToCapacityRatioDefaultSatisfiesSpiMinimum() {
        assertTrue(MINIMAL_PROVIDER.dataToCapacityRatio(Settings.EMPTY) >= 1.0);
    }

    // ── getBlockCache default contract ────────────────────────────────────────

    public void testGetBlockCacheReturnsNonNullOptional() {
        assertNotNull(MINIMAL_PROVIDER.getBlockCache());
    }

    public void testGetBlockCacheReturnsEmptyOptionalWhenNotInitialised() {
        assertTrue(MINIMAL_PROVIDER.getBlockCache().isEmpty());
    }

    // ── Override semantics: overriding requestedCapacityBytes works ───────────

    public void testOverriddenRequestedCapacityBytesIsUsed() {
        BlockCacheProvider custom = new BlockCacheProvider() {
            @Override
            public long requestedCapacityBytes(Settings settings, long totalBudgetBytes) {
                return totalBudgetBytes / 4; // always 25%
            }

            @Override
            public Optional<BlockCache> getBlockCache() {
                return Optional.empty();
            }
        };
        assertEquals(250L, custom.requestedCapacityBytes(Settings.EMPTY, 1000L));
        // Default ratio still 1.0
        assertEquals(1.0, custom.dataToCapacityRatio(Settings.EMPTY), 0.0);
    }

    public void testOverriddenDataToCapacityRatioIsUsed() {
        BlockCacheProvider custom = new BlockCacheProvider() {
            @Override
            public double dataToCapacityRatio(Settings settings) {
                return 5.0;
            }

            @Override
            public Optional<BlockCache> getBlockCache() {
                return Optional.empty();
            }
        };
        assertEquals(5.0, custom.dataToCapacityRatio(Settings.EMPTY), 0.0);
        // Default requestedCapacityBytes still 0
        assertEquals(0L, custom.requestedCapacityBytes(Settings.EMPTY, 1_000_000L));
    }
}
