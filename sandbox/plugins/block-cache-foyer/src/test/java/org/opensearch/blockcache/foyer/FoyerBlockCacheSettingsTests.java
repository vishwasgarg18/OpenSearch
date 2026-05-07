/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FoyerBlockCacheSettings} setting validation and defaults.
 */
public class FoyerBlockCacheSettingsTests extends OpenSearchTestCase {

    // ── CACHE_SIZE_SETTING ────────────────────────────────────────────────────

    public void testCacheSizeDefaultIs25Percent() {
        assertEquals("25%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.EMPTY));
    }

    public void testCacheSizeAcceptsPercentage() {
        Settings s = Settings.builder().put("block_cache.size", "50%").build();
        assertEquals("50%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(s));
    }

    public void testCacheSizeAcceptsDecimalRatioForm() {
        // The validator accepts both "25%" and "0.25" — both represent 25%
        Settings s = Settings.builder().put("block_cache.size", "0.25").build();
        assertEquals("0.25", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(s));
    }

    public void testCacheSizeAcceptsZeroToDisableCache() {
        Settings s = Settings.builder().put("block_cache.size", "0%").build();
        assertEquals("0%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(s));
    }

    public void testCacheSizeAcceptsNearMaximum() {
        Settings s = Settings.builder().put("block_cache.size", "99%").build();
        assertEquals("99%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(s));
    }

    public void testCacheSizeRejectsHundredPercent() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "100%").build()
            )
        );
        assertTrue("error message should mention setting name", ex.getMessage().contains("block_cache.size"));
    }

    public void testCacheSizeRejectsNegative() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "-1%").build()
            )
        );
    }

    public void testCacheSizeRejectsGarbage() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "notanumber").build()
            )
        );
    }

    public void testCacheSizeRejectsRatioGreaterThanOrEqualToOne() {
        // 1.0 in ratio form == 100% — should be rejected
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "1.0").build()
            )
        );
    }

    // ── IO_ENGINE_SETTING ─────────────────────────────────────────────────────

    public void testIoEngineDefaultIsAuto() {
        assertEquals("auto", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.EMPTY));
    }

    public void testIoEngineAcceptsPsync() {
        Settings s = Settings.builder().put("block_cache.io_engine", "psync").build();
        assertEquals("psync", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(s));
    }

    public void testIoEngineAcceptsIoUring() {
        Settings s = Settings.builder().put("block_cache.io_engine", "io_uring").build();
        assertEquals("io_uring", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(s));
    }

    public void testIoEngineRejectsUnknown() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
                Settings.builder().put("block_cache.io_engine", "libaio").build()
            )
        );
        assertTrue("error should list valid options",
            ex.getMessage().contains("auto") && ex.getMessage().contains("io_uring") && ex.getMessage().contains("psync"));
    }

    public void testIoEngineIsCaseSensitive() {
        // "PSYNC" (uppercase) should be rejected — only lowercase is valid
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
                Settings.builder().put("block_cache.io_engine", "PSYNC").build()
            )
        );
    }

    public void testIoEngineRejectsEmptyString() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
                Settings.builder().put("block_cache.io_engine", "").build()
            )
        );
    }

    // ── BLOCK_SIZE_SETTING ────────────────────────────────────────────────────

    public void testBlockSizeDefaultIs64MB() {
        ByteSizeValue def = FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.EMPTY);
        assertEquals(new ByteSizeValue(64, ByteSizeUnit.MB), def);
    }

    public void testBlockSizeAcceptsMinimum() {
        Settings s = Settings.builder().put("block_cache.block_size", "1mb").build();
        assertEquals(new ByteSizeValue(1, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(s));
    }

    public void testBlockSizeAcceptsMaximum() {
        Settings s = Settings.builder().put("block_cache.block_size", "256mb").build();
        assertEquals(new ByteSizeValue(256, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(s));
    }

    public void testBlockSizeAcceptsMiddleValue() {
        Settings s = Settings.builder().put("block_cache.block_size", "128mb").build();
        assertEquals(new ByteSizeValue(128, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(s));
    }

    public void testBlockSizeRejectsBelowMinimum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.block_size", "512kb").build()
            )
        );
    }

    public void testBlockSizeRejectsAboveMaximum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.block_size", "257mb").build()
            )
        );
    }

    // ── DATA_TO_CACHE_RATIO_SETTING ───────────────────────────────────────────

    public void testDataToCacheRatioDefaultIs5() {
        assertEquals(5.0, FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(Settings.EMPTY), 0.0);
    }

    public void testDataToCacheRatioAccepts1AsMinimum() {
        Settings s = Settings.builder().put("block_cache.data_to_cache_ratio", "1.0").build();
        assertEquals(1.0, FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(s), 0.0);
    }

    public void testDataToCacheRatioAcceptsLargeValue() {
        Settings s = Settings.builder().put("block_cache.data_to_cache_ratio", "100.0").build();
        assertEquals(100.0, FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(s), 0.0);
    }

    public void testDataToCacheRatioRejectsBelowMinimum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.data_to_cache_ratio", "0.5").build()
            )
        );
    }

    public void testDataToCacheRatioRejectsZero() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.data_to_cache_ratio", "0.0").build()
            )
        );
    }
}
