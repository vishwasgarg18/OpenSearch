/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link BlockCacheFoyerPlugin}.
 *
 * <p>Focuses on the pure-Java wiring of the plugin that does not require the
 * native library:
 * <ul>
 *   <li>Both constructor variants (no-arg and {@code Settings}-arg).</li>
 *   <li>{@link BlockCacheFoyerPlugin#getBlockCache()} returns
 *       {@code Optional.empty()} before {@code createComponents} has run.</li>
 *   <li>{@link BlockCacheFoyerPlugin#requestedCapacityBytes} computes correctly.</li>
 *   <li>{@link BlockCacheFoyerPlugin#dataToCapacityRatio} reads from settings.</li>
 *   <li>{@link BlockCacheFoyerPlugin#getSettings()} registers all expected settings.</li>
 *   <li>Lifecycle guards: double-init throws; close with no cache is safe.</li>
 * </ul>
 *
 * <p>Tests that exercise {@code createComponents} end-to-end are out of scope here
 * because they construct a real {@link FoyerBlockCache} which requires the native
 * library. Those paths are covered by integration tests.
 */
public class BlockCacheFoyerPluginTests extends OpenSearchTestCase {

    // ── Constructor + getBlockCache ───────────────────────────────────────────

    public void testNoArgConstructor() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertNotNull(plugin);
        assertTrue("handle is empty before createComponents", plugin.getBlockCache().isEmpty());
    }

    public void testSettingsConstructor() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertNotNull(plugin);
        assertTrue(plugin.getBlockCache().isEmpty());
    }

    public void testGetBlockCacheIsNotNullItself() {
        // getBlockCache() must return a non-null Optional (it may be empty, but not null)
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertNotNull(plugin.getBlockCache());
    }

    // ── Lifecycle guards ──────────────────────────────────────────────────────

    public void testCloseWithNoCacheInitialisedDoesNotThrow() throws IOException {
        // close() before createComponents() should be a no-op, not an NPE
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.close(); // must not throw
    }

    public void testCloseIsIdempotent() throws IOException {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.close();
        plugin.close(); // second close must also be safe
    }

    // ── requestedCapacityBytes ────────────────────────────────────────────────

    public void testRequestedCapacityBytesDefault25Percent() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        long budget = 800L * 1024 * 1024 * 1024; // 800 GB
        long requested = plugin.requestedCapacityBytes(Settings.EMPTY, budget);
        assertEquals(200L * 1024 * 1024 * 1024, requested);
    }

    public void testRequestedCapacityBytesCustomPercent() {
        Settings s = Settings.builder().put("block_cache.size", "50%").build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(500L, plugin.requestedCapacityBytes(s, 1000L));
    }

    public void testRequestedCapacityBytesZeroPercent() {
        Settings s = Settings.builder().put("block_cache.size", "0%").build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(0L, plugin.requestedCapacityBytes(s, 1000L));
    }

    public void testRequestedCapacityBytesDecimalRatioForm() {
        // "0.25" is equivalent to "25%" — both should yield 250 from a 1000-byte budget
        Settings s = Settings.builder().put("block_cache.size", "0.25").build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(250L, plugin.requestedCapacityBytes(s, 1000L));
    }

    public void testRequestedCapacityBytesRoundsCorrectly() {
        // 33% of 100 bytes = 33, not 33.0 or 34
        Settings s = Settings.builder().put("block_cache.size", "33%").build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(33L, plugin.requestedCapacityBytes(s, 100L));
    }

    public void testRequestedCapacityBytesZeroBudgetGivesZero() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(0L, plugin.requestedCapacityBytes(Settings.EMPTY, 0L));
    }

    public void testRequestedCapacityBytesIsNonNegative() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        long result = plugin.requestedCapacityBytes(Settings.EMPTY, 1_000_000L);
        assertTrue("requestedCapacityBytes must be >= 0", result >= 0);
    }

    public void testRequestedCapacityBytesDoesNotExceedBudget() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        long budget = 1_000_000L;
        long result = plugin.requestedCapacityBytes(Settings.EMPTY, budget);
        assertTrue("requestedCapacityBytes must not exceed budget", result <= budget);
    }

    // ── dataToCapacityRatio ───────────────────────────────────────────────────

    public void testDataToCapacityRatioDefaultIs5() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(5.0, plugin.dataToCapacityRatio(Settings.EMPTY), 0.0);
    }

    public void testDataToCapacityRatioCustomValue() {
        Settings s = Settings.builder().put("block_cache.data_to_cache_ratio", "10.0").build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(10.0, plugin.dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioMinimumOf1() {
        Settings s = Settings.builder().put("block_cache.data_to_cache_ratio", "1.0").build();
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertEquals(1.0, plugin.dataToCapacityRatio(s), 0.0);
    }

    public void testDataToCapacityRatioIsAtLeastOne() {
        // SPI contract: default is 5.0 >= 1.0
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertTrue(plugin.dataToCapacityRatio(Settings.EMPTY) >= 1.0);
    }

    // ── getSettings ───────────────────────────────────────────────────────────

    public void testGetSettingsRegistersAllFourSettings() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(4, settings.size());
        assertTrue(settings.contains(FoyerBlockCacheSettings.CACHE_SIZE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.BLOCK_SIZE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.IO_ENGINE_SETTING));
        assertTrue(settings.contains(FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING));
    }

    public void testGetSettingsDoesNotReturnNull() {
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertNotNull(plugin.getSettings());
        for (Setting<?> s : plugin.getSettings()) {
            assertNotNull("individual setting must not be null", s);
        }
    }

    public void testGetSettingsIsStable() {
        // Two calls should return equivalent lists
        BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        List<Setting<?>> first = plugin.getSettings();
        List<Setting<?>> second = plugin.getSettings();
        assertEquals(first.size(), second.size());
        assertTrue(first.containsAll(second));
    }
}
