package com.tidesdb.kafka.store;

import com.tidesdb.CfStats;
import com.tidesdb.ColumnFamily;
import com.tidesdb.ColumnFamilyConfig;
import com.tidesdb.CommitOp;
import com.tidesdb.CompressionAlgorithm;
import com.tidesdb.Config;
import com.tidesdb.DbStats;
import com.tidesdb.IsolationLevel;
import com.tidesdb.LogLevel;
import com.tidesdb.RangeStats;
import com.tidesdb.SyncMode;
import com.tidesdb.TidesDBException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TidesDBStore}, driven through a mocked
 * {@link StateStoreContext}. {@link TidesDBStoreTopologyTest} covers the store
 * inside a real Kafka Streams topology.
 */
class TidesDBStoreTest {

    @TempDir
    File tempDir;

    private TidesDBStore store;
    private StateStoreContext context;

    @BeforeEach
    void setUp() {
        store = new TidesDBStore("test-store", TidesDBStoreConfig.builder()
            .logLevel(LogLevel.NONE)
            .build());
        context = contextAt(tempDir);
        store.init(context, store);
    }

    @AfterEach
    void tearDown() {
        if (store != null && store.isOpen()) {
            store.close();
        }
    }

    private static StateStoreContext contextAt(File dir) {
        StateStoreContext ctx = mock(StateStoreContext.class);
        when(ctx.stateDir()).thenReturn(dir);
        return ctx;
    }

    private TidesDBStore openStore(String name, TidesDBStoreConfig config) {
        File dir = new File(tempDir, name);
        dir.mkdirs();
        TidesDBStore s = new TidesDBStore(name, config);
        s.init(contextAt(dir), s);
        return s;
    }

    private static Bytes k(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] v(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    private static List<String> keys(KeyValueIterator<Bytes, byte[]> it) {
        List<String> out = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> iter = it) {
            while (iter.hasNext()) {
                out.add(str(iter.next().key.get()));
            }
        }
        return out;
    }

    private void putAllKeys(String... ks) {
        for (String key : ks) {
            store.put(k(key), v("value_" + key));
        }
    }

    /* ===== lifecycle ===== */

    @Test
    @DisplayName("Should initialize store successfully")
    void testInitialization() {
        assertThat(store.isOpen()).isTrue();
        assertThat(store.name()).isEqualTo("test-store");
        assertThat(store.persistent()).isTrue();
        assertThat(new File(tempDir, "test-store")).isDirectory();
    }

    @Test
    @DisplayName("Should register a batching restore callback with the context")
    void testRegistersRestoreCallback() {
        ArgumentCaptor<StateRestoreCallback> captor = ArgumentCaptor.forClass(StateRestoreCallback.class);
        verify(context).register(eq(store), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(BatchingStateRestoreCallback.class);
    }

    @Test
    @DisplayName("Should apply changelog records through the restore callback")
    void testRestoreCallbackAppliesRecords() {
        ArgumentCaptor<StateRestoreCallback> captor = ArgumentCaptor.forClass(StateRestoreCallback.class);
        verify(context).register(eq(store), captor.capture());
        BatchingStateRestoreCallback restore = (BatchingStateRestoreCallback) captor.getValue();

        store.put(k("stale"), v("x"));
        restore.restoreAll(Arrays.asList(
            KeyValue.pair(v("a"), v("1")),
            KeyValue.pair(v("b"), v("2")),
            KeyValue.pair(v("stale"), null)));

        assertThat(str(store.get(k("a")))).isEqualTo("1");
        assertThat(str(store.get(k("b")))).isEqualTo("2");
        assertThat(store.get(k("stale"))).as("a null record is a delete").isNull();
    }

    @Test
    @DisplayName("Should keep data across a close and reopen")
    void testDataSurvivesReopen() {
        store.put(k("durable"), v("yes"));
        store.flush();
        store.close();

        TidesDBStore reopened = new TidesDBStore("test-store",
            TidesDBStoreConfig.builder().logLevel(LogLevel.NONE).build());
        reopened.init(contextAt(tempDir), reopened);
        try {
            assertThat(str(reopened.get(k("durable")))).isEqualTo("yes");
        } finally {
            reopened.close();
        }
    }

    @Test
    @DisplayName("Should throw InvalidStateStoreException when accessing a closed store")
    void testAccessClosedStore() {
        store.close();
        assertThat(store.isOpen()).isFalse();

        assertThatThrownBy(() -> store.put(k("key"), v("value")))
            .isInstanceOf(InvalidStateStoreException.class)
            .hasMessageContaining("closed");
        assertThatThrownBy(() -> store.get(k("key")))
            .isInstanceOf(InvalidStateStoreException.class);
        assertThatThrownBy(() -> store.all())
            .isInstanceOf(InvalidStateStoreException.class);
        assertThatThrownBy(() -> store.flush())
            .isInstanceOf(InvalidStateStoreException.class);
    }

    @Test
    @DisplayName("Should make close idempotent")
    void testCloseIsIdempotent() {
        store.close();
        assertThatCode(() -> store.close()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should close iterators left open when the store closes")
    void testCloseClosesOpenIterators() {
        putAllKeys("a", "b", "c");
        KeyValueIterator<Bytes, byte[]> leaked = store.all();
        assertThat(leaked.hasNext()).isTrue();

        assertThatCode(() -> store.close()).doesNotThrowAnyException();
        assertThat(leaked.hasNext()).isFalse();
        assertThatCode(leaked::close).doesNotThrowAnyException();
    }

    /* ===== point operations ===== */

    @Test
    @DisplayName("Should put and get a value")
    void testPutAndGet() {
        store.put(k("key1"), v("value1"));
        assertThat(store.get(k("key1"))).isEqualTo(v("value1"));
    }

    @Test
    @DisplayName("Should return null for non-existent key")
    void testGetNonExistent() {
        assertThat(store.get(k("nonexistent"))).isNull();
    }

    @Test
    @DisplayName("Should store an empty value distinctly from an absence")
    void testEmptyValue() {
        store.put(k("empty"), new byte[0]);
        assertThat(store.get(k("empty"))).isNotNull().isEmpty();
    }

    @Test
    @DisplayName("Should delete a key and return the old value")
    void testDelete() {
        store.put(k("key1"), v("value1"));
        assertThat(store.delete(k("key1"))).isEqualTo(v("value1"));
        assertThat(store.get(k("key1"))).isNull();
    }

    @Test
    @DisplayName("Should return null when deleting a non-existent key")
    void testDeleteNonExistent() {
        assertThat(store.delete(k("never-written"))).isNull();
    }

    @Test
    @DisplayName("Should delete with null value in put")
    void testPutNullValue() {
        store.put(k("key1"), v("value1"));
        store.put(k("key1"), null);
        assertThat(store.get(k("key1"))).isNull();
    }

    @Test
    @DisplayName("Should handle putIfAbsent correctly")
    void testPutIfAbsent() {
        assertThat(store.putIfAbsent(k("key1"), v("value1"))).isNull();
        assertThat(store.get(k("key1"))).isEqualTo(v("value1"));

        assertThat(store.putIfAbsent(k("key1"), v("value2"))).isEqualTo(v("value1"));
        assertThat(store.get(k("key1"))).isEqualTo(v("value1"));
    }

    @Test
    @DisplayName("Should handle putAll correctly, including deletes")
    void testPutAll() {
        store.put(k("gone"), v("x"));
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(KeyValue.pair(k("key1"), v("value1")));
        entries.add(KeyValue.pair(k("key2"), v("value2")));
        entries.add(KeyValue.pair(k("gone"), null));

        store.putAll(entries);

        assertThat(str(store.get(k("key1")))).isEqualTo("value1");
        assertThat(str(store.get(k("key2")))).isEqualTo("value2");
        assertThat(store.get(k("gone"))).isNull();
    }

    @Test
    @DisplayName("Should reject a putAll batch with a null key without writing any of it")
    void testPutAllNullKey() {
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(KeyValue.pair(k("first"), v("v")));
        entries.add(KeyValue.pair(null, v("v")));
        assertThatThrownBy(() -> store.putAll(entries)).isInstanceOf(NullPointerException.class);
        assertThat(store.get(k("first"))).isNull();
    }

    @Test
    @DisplayName("Should update existing key")
    void testUpdateExistingKey() {
        store.put(k("key1"), v("value1"));
        store.put(k("key1"), v("value2"));
        assertThat(store.get(k("key1"))).isEqualTo(v("value2"));
    }

    @Test
    @DisplayName("Should handle large values")
    void testLargeValues() {
        byte[] largeValue = new byte[1024 * 1024];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }
        store.put(k("large"), largeValue);
        assertThat(store.get(k("large"))).isEqualTo(largeValue);
    }

    @Test
    @DisplayName("Should handle many keys")
    void testManyKeys() {
        int numKeys = 10000;
        for (int i = 0; i < numKeys; i++) {
            store.put(k(String.format("key_%06d", i)), v(String.format("value_%06d", i)));
        }
        for (int i = 0; i < numKeys; i += 97) {
            assertThat(str(store.get(k(String.format("key_%06d", i)))))
                .isEqualTo(String.format("value_%06d", i));
        }
        assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(numKeys);
    }

    @Test
    @DisplayName("Should reject null keys on writes and return null on reads")
    void testNullKey() {
        assertThatThrownBy(() -> store.put(null, v("value"))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> store.delete(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> store.putIfAbsent(null, v("value"))).isInstanceOf(NullPointerException.class);
        assertThat(store.get(null)).isNull();
    }

    @Test
    @DisplayName("Should single-delete a key")
    void testSingleDelete() {
        store.put(k("sd-key"), v("sd-value"));
        store.singleDelete(k("sd-key"));
        assertThat(store.get(k("sd-key"))).isNull();
        assertThatThrownBy(() -> store.singleDelete(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should return approximate number of entries, memtable included")
    void testApproximateNumEntries() {
        assertThat(store.approximateNumEntries()).isEqualTo(0);
        putAllKeys("key1", "key2", "key3");
        assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @DisplayName("Should flush without losing data")
    void testFlush() {
        putAllKeys("key1", "key2");
        assertThatCode(() -> store.flush()).doesNotThrowAnyException();
        assertThat(store.get(k("key1"))).isNotNull();
        assertThat(store.get(k("key2"))).isNotNull();
    }

    @Test
    @DisplayName("Should serve concurrent readers")
    void testConcurrentReads() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            store.put(k("key" + i), v("value" + i));
        }
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        int idx = (int) (Math.random() * 1000);
                        assertThat(str(store.get(k("key" + idx)))).isEqualTo("value" + idx);
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(failures).isEmpty();
    }

    /* ===== TTL ===== */

    @Test
    @DisplayName("Should store an explicit TTL as seconds from now")
    void testPutWithTtl() {
        long before = System.currentTimeMillis() / 1000;
        store.putWithTtl(k("ttl-key"), v("ttl-value"), 3600);
        long after = System.currentTimeMillis() / 1000;

        assertThat(store.get(k("ttl-key"))).isEqualTo(v("ttl-value"));
        Long expiry = store.getTtl(k("ttl-key"));
        assertThat(expiry).isNotNull().isBetween(before + 3600, after + 3601);
    }

    @Test
    @DisplayName("Should expire an entry once its TTL passes")
    void testTtlExpires() throws InterruptedException {
        store.putWithTtl(k("brief"), v("v"), 1);
        assertThat(store.get(k("brief"))).isNotNull();
        Thread.sleep(2500);
        assertThat(store.get(k("brief"))).isNull();
        assertThat(store.getTtl(k("brief"))).isNull();
    }

    @Test
    @DisplayName("Should report -1 for an entry without a TTL")
    void testNoTtl() {
        store.put(k("forever"), v("v"));
        assertThat(store.getTtl(k("forever"))).isEqualTo(-1L);
        assertThat(store.getTtl(k("missing"))).isNull();
    }

    @Test
    @DisplayName("Should reject null arguments to putWithTtl")
    void testPutWithTtlNulls() {
        assertThatThrownBy(() -> store.putWithTtl(k("key"), null, 60))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> store.putWithTtl(null, v("value"), 60))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should apply the configured default TTL to put, putIfAbsent and putAll")
    void testDefaultTtlConfig() {
        TidesDBStore ttlStore = openStore("ttl-store", TidesDBStoreConfig.builder()
            .logLevel(LogLevel.NONE)
            .defaultTtlSeconds(7200)
            .build());
        try {
            long now = System.currentTimeMillis() / 1000;
            ttlStore.put(k("put"), v("v"));
            ttlStore.putIfAbsent(k("pia"), v("v"));
            ttlStore.putAll(Arrays.asList(KeyValue.pair(k("all"), v("v"))));

            for (String key : new String[] {"put", "pia", "all"}) {
                assertThat(ttlStore.getTtl(k(key)))
                    .as("TTL of %s", key)
                    .isBetween(now + 7200 - 1, now + 7200 + 5);
            }
        } finally {
            ttlStore.close();
        }
    }

    /* ===== iteration ===== */

    @Test
    @DisplayName("Should iterate over all entries in byte order")
    void testAllIterator() {
        putAllKeys("c", "a", "b");
        List<KeyValue<String, String>> results = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
            while (it.hasNext()) {
                KeyValue<Bytes, byte[]> kv = it.next();
                results.add(KeyValue.pair(str(kv.key.get()), str(kv.value)));
            }
        }
        assertThat(results).extracting(kv -> kv.key).containsExactly("a", "b", "c");
        assertThat(results).extracting(kv -> kv.value).containsExactly("value_a", "value_b", "value_c");
    }

    @Test
    @DisplayName("Should iterate over all entries in reverse")
    void testReverseAll() {
        putAllKeys("a", "b", "c");
        assertThat(keys(store.reverseAll())).containsExactly("c", "b", "a");
    }

    @Test
    @DisplayName("Should iterate an inclusive range")
    void testRangeIterator() {
        putAllKeys("a", "b", "c", "d", "e");
        assertThat(keys(store.range(k("b"), k("d")))).containsExactly("b", "c", "d");
    }

    @Test
    @DisplayName("Should iterate an inclusive range in reverse")
    void testReverseRange() {
        putAllKeys("a", "b", "c", "d", "e");
        assertThat(keys(store.reverseRange(k("b"), k("d")))).containsExactly("d", "c", "b");
    }

    @Test
    @DisplayName("Should treat null range bounds as open")
    void testOpenRangeBounds() {
        putAllKeys("a", "b", "c", "d");
        assertThat(keys(store.range(null, k("b")))).containsExactly("a", "b");
        assertThat(keys(store.range(k("c"), null))).containsExactly("c", "d");
        assertThat(keys(store.reverseRange(null, k("b")))).containsExactly("b", "a");
        assertThat(keys(store.reverseRange(k("c"), null))).containsExactly("d", "c");
    }

    @Test
    @DisplayName("Should return nothing for an inverted range")
    void testInvertedRange() {
        putAllKeys("a", "b", "c");
        assertThat(keys(store.range(k("c"), k("a")))).isEmpty();
        assertThat(keys(store.reverseRange(k("c"), k("a")))).isEmpty();
    }

    @Test
    @DisplayName("Should handle bounds between and beyond stored keys")
    void testRangeBoundsBetweenKeys() {
        putAllKeys("b", "d", "f");
        assertThat(keys(store.range(k("a"), k("e")))).containsExactly("b", "d");
        assertThat(keys(store.range(k("g"), k("z")))).isEmpty();
        assertThat(keys(store.reverseRange(k("c"), k("g")))).containsExactly("f", "d");
    }

    @Test
    @DisplayName("Should order keys unsigned byte-wise")
    void testUnsignedOrder() {
        store.put(Bytes.wrap(new byte[] {(byte) 0x80}), v("high"));
        store.put(Bytes.wrap(new byte[] {0x01}), v("low"));
        List<byte[]> seen = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
            it.forEachRemaining(kv -> seen.add(kv.key.get()));
        }
        assertThat(seen).containsExactly(new byte[] {0x01}, new byte[] {(byte) 0x80});
    }

    @Test
    @DisplayName("Should scan by prefix")
    void testPrefixScan() {
        putAllKeys("user:1", "user:2", "user:10", "users", "order:1", "use");
        assertThat(keys(store.prefixScan("user:", Serdes.String().serializer())))
            .containsExactly("user:1", "user:10", "user:2");
        assertThat(keys(store.prefixScan("nope", Serdes.String().serializer()))).isEmpty();
    }

    @Test
    @DisplayName("Should scan a prefix that ends in 0xFF")
    void testPrefixScanAllOnes() {
        byte[] prefix = {0x01, (byte) 0xFF};
        store.put(Bytes.wrap(new byte[] {0x01, (byte) 0xFF, 0x00}), v("in"));
        store.put(Bytes.wrap(new byte[] {0x01, (byte) 0xFF, (byte) 0xFF}), v("in"));
        store.put(Bytes.wrap(new byte[] {0x02}), v("out"));
        List<byte[]> seen = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> it =
                 store.prefixScan(prefix, Serdes.ByteArray().serializer())) {
            it.forEachRemaining(kv -> seen.add(kv.key.get()));
        }
        assertThat(seen).hasSize(2);
    }

    @Test
    @DisplayName("Should compute prefix successors")
    void testSuccessor() {
        assertThat(TidesDBRangeIterator.successor(new byte[] {0x61, 0x62})).containsExactly(0x61, 0x63);
        assertThat(TidesDBRangeIterator.successor(new byte[] {0x61, (byte) 0xFF})).containsExactly(0x62);
        assertThat(TidesDBRangeIterator.successor(new byte[] {(byte) 0xFF, (byte) 0xFF})).isNull();
    }

    @Test
    @DisplayName("Should handle an empty iterator")
    void testEmptyIterator() {
        try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
            assertThat(it.hasNext()).isFalse();
            assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
            assertThatThrownBy(it::peekNextKey).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    @DisplayName("Should peek without advancing")
    void testPeekNextKey() {
        putAllKeys("a", "b");
        try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
            assertThat(str(it.peekNextKey().get())).isEqualTo("a");
            assertThat(str(it.peekNextKey().get())).isEqualTo("a");
            assertThat(str(it.next().key.get())).isEqualTo("a");
            assertThat(str(it.peekNextKey().get())).isEqualTo("b");
        }
    }

    @Test
    @DisplayName("Should read a consistent snapshot while writes continue")
    void testIteratorSnapshot() {
        putAllKeys("a", "b");
        try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
            store.put(k("c"), v("late"));
            store.delete(k("b"));
            List<String> seen = new ArrayList<>();
            it.forEachRemaining(kv -> seen.add(str(kv.key.get())));
            assertThat(seen).containsExactly("a", "b");
        }
    }

    @Test
    @DisplayName("Should keep the legacy iterator types working")
    void testLegacyIteratorTypes() throws Exception {
        putAllKeys("a", "b", "c");
        assertThat(keys(new TidesDBIteratorWrapper(store.getDb(), store.getColumnFamily())))
            .containsExactly("a", "b", "c");
        assertThat(keys(new TidesDBRangeIterator(store.getDb(), store.getColumnFamily(), k("b"), k("c"))))
            .containsExactly("b", "c");
    }

    /* ===== configuration ===== */

    @Test
    @DisplayName("Should default to the native configuration plus LZ4")
    void testConfigDefaults() {
        TidesDBStoreConfig config = TidesDBStoreConfig.defaultConfig();
        Config nativeDb = Config.defaultConfig("x");
        ColumnFamilyConfig nativeCf = ColumnFamilyConfig.defaultConfig();

        assertThat(config.getDatabaseConfig().getBlockCacheSize()).isEqualTo(nativeDb.getBlockCacheSize());
        assertThat(config.getDatabaseConfig().getMemtableWriteBufferSize())
            .isEqualTo(nativeDb.getMemtableWriteBufferSize());
        assertThat(config.getDatabaseConfig().getMemtableSyncMode()).isEqualTo(nativeDb.getMemtableSyncMode());
        assertThat(config.getColumnFamilyConfig().getMinLevels()).isEqualTo(nativeCf.getMinLevels());
        assertThat(config.getColumnFamilyConfig().getEncodingPipeline())
            .containsExactly(CompressionAlgorithm.LZ4.getValue());
        assertThat(config.getColumnFamilyName()).isEqualTo("default");
        assertThat(config.getDefaultTtlSeconds()).isEqualTo(0);
        assertThat(config.getRaiseOpenFileLimit()).isEqualTo(0);
        assertThat(config.getDefaultIsolationLevel()).isEqualTo(IsolationLevel.READ_COMMITTED);
    }

    @Test
    @DisplayName("Should carry builder settings through to the TidesDB configs")
    void testConfigSetters() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .numFlushThreads(3)
            .numCompactionThreads(4)
            .blockCacheSize(32L << 20)
            .maxOpenSSTables(512)
            .memtableWriteBufferSize(16L << 20)
            .memtableSyncMode(SyncMode.SYNC_INTERVAL)
            .memtableSyncIntervalUs(5000)
            .valueSeparationThreshold(2048)
            .compression(CompressionAlgorithm.ZSTD)
            .bloomFpr(0.001)
            .defaultIsolationLevel(IsolationLevel.SNAPSHOT)
            .levelSizeRatio(8)
            .l1FileCountTrigger(6)
            .tombstoneDensityTrigger(0.4)
            .tombstoneDensityMinEntries(2048)
            .columnFamilyName("events")
            .build();

        Config db = config.toDatabaseConfig("/data/store");
        assertThat(db.getDbPath()).isEqualTo("/data/store");
        assertThat(db.getNumFlushThreads()).isEqualTo(3);
        assertThat(db.getNumCompactionThreads()).isEqualTo(4);
        assertThat(db.getBlockCacheSize()).isEqualTo(32L << 20);
        assertThat(db.getMaxOpenSSTables()).isEqualTo(512);
        assertThat(db.getMemtableWriteBufferSize()).isEqualTo(16L << 20);
        assertThat(db.getMemtableSyncMode()).isEqualTo(SyncMode.SYNC_INTERVAL);
        assertThat(db.getMemtableSyncIntervalUs()).isEqualTo(5000);
        assertThat(db.getValueSeparationThreshold()).isEqualTo(2048);

        ColumnFamilyConfig cf = config.getColumnFamilyConfig();
        assertThat(cf.getEncodingPipeline()).containsExactly(CompressionAlgorithm.ZSTD.getValue());
        assertThat(cf.getBloomFpr()).isEqualTo(0.001);
        assertThat(cf.getDefaultIsolationLevel()).isEqualTo(IsolationLevel.SNAPSHOT);
        assertThat(cf.getLevelSizeRatio()).isEqualTo(8);
        assertThat(cf.getL1FileCountTrigger()).isEqualTo(6);
        assertThat(cf.getTombstoneDensityTrigger()).isEqualTo(0.4);
        assertThat(cf.getTombstoneDensityMinEntries()).isEqualTo(2048);
        assertThat(config.getColumnFamilyName()).isEqualTo("events");
    }

    @Test
    @DisplayName("Should accept whole TidesDB configs and ignore their path")
    void testWholeConfigs() {
        TidesDBStoreConfig config = TidesDBStoreConfig.builder()
            .databaseConfig(Config.defaultConfig("/ignored").toBuilder().numFlushThreads(5).build())
            .columnFamilyConfig(ColumnFamilyConfig.builder().compression(CompressionAlgorithm.NONE).build())
            .blockCacheSize(1L << 20)
            .build();

        assertThat(config.toDatabaseConfig("/real").getDbPath()).isEqualTo("/real");
        assertThat(config.getDatabaseConfig().getNumFlushThreads()).isEqualTo(5);
        assertThat(config.getDatabaseConfig().getBlockCacheSize()).isEqualTo(1L << 20);
        assertThat(config.getColumnFamilyConfig().getEncodingPipeline()).isEmpty();
    }

    @Test
    @DisplayName("Should reject an empty column family name")
    void testRejectsEmptyCfName() {
        assertThatThrownBy(() -> TidesDBStoreConfig.builder().columnFamilyName(""))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TidesDBStoreConfig.builder().databaseConfig(null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should open a store with a custom configuration")
    void testCustomConfigStore() throws Exception {
        TidesDBStore custom = openStore("custom-store", TidesDBStoreConfig.builder()
            .logLevel(LogLevel.NONE)
            .compression(CompressionAlgorithm.ZSTD)
            .bloomFpr(0.001)
            .memtableWriteBufferSize(8L << 20)
            .columnFamilyName("custom_cf")
            .build());
        try {
            custom.put(k("key"), v("value"));
            assertThat(custom.get(k("key"))).isEqualTo(v("value"));
            assertThat(custom.getColumnFamily().getName()).isEqualTo("custom_cf");
            assertThat(custom.getStats().getConfig().getEncodingPipeline())
                .containsExactly(CompressionAlgorithm.ZSTD.getValue());
        } finally {
            custom.close();
        }
    }

    @Test
    @DisplayName("Should open store with raiseOpenFileLimit config applied")
    void testRaiseOpenFileLimitConfig() {
        assertThat(TidesDBStore.raiseOpenFileLimit(0)).isGreaterThan(0);
        TidesDBStore rol = openStore("rol-store", TidesDBStoreConfig.builder()
            .logLevel(LogLevel.NONE)
            .raiseOpenFileLimit(4096)
            .build());
        try {
            rol.put(k("rk1"), v("rv1"));
            assertThat(rol.get(k("rk1"))).isEqualTo(v("rv1"));
        } finally {
            rol.close();
        }
    }

    @Test
    @DisplayName("Should build stores from the supplier and builder")
    void testSupplierAndBuilder() {
        TidesDBStoreSupplier supplier = new TidesDBStoreSupplier("supplied");
        assertThat(supplier.name()).isEqualTo("supplied");
        assertThat(supplier.metricsScope()).isEqualTo("tidesdb");
        assertThat(supplier.get()).isInstanceOf(TidesDBStore.class);

        TidesDBStoreBuilder builder = TidesDBStoreBuilder.create("built");
        assertThat(builder.name()).isEqualTo("built");
        assertThat(builder.loggingEnabled()).isFalse();
        TidesDBStore built = builder.build();
        assertThat(built.name()).isEqualTo("built");
        assertThat(built.isOpen()).isFalse();

        assertThatThrownBy(() -> new TidesDBStoreSupplier("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TidesDBStoreBuilder(null)).isInstanceOf(IllegalArgumentException.class);
    }

    /* ===== TidesDB extensions ===== */

    @Test
    @DisplayName("Should expose statistics")
    void testStatistics() throws Exception {
        putAllKeys("a", "b");
        store.delete(k("a"));

        CfStats cf = store.getStats();
        assertThat(cf).isNotNull();
        assertThat(cf.getTotalKeys() + cf.getUnflushedKeyCount()).isGreaterThan(0);
        assertThat(cf.getTombstoneRatio()).isBetween(0.0, 1.0);

        DbStats db = store.getDbStats();
        assertThat(db.getNumColumnFamilies()).isEqualTo(1);
        assertThat(db.getTxnCommits()).isGreaterThanOrEqualTo(3);

        assertThat(store.getCacheStats()).isNotNull();
        assertThat(store.getStallStats()).isNotNull();
        assertThat(store.getIoStats()).isNotNull();
        assertThat(store.estimateCardinality()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("Should report range statistics")
    void testRangeStats() throws Exception {
        for (int i = 0; i < 100; i++) {
            store.put(k(String.format("rk%03d", i)), v("v" + i));
        }
        store.flushMemtable();
        RangeStats stats = store.rangeStats(v("rk000"), v("rk050"));
        assertThat(stats).isNotNull();
    }

    @Test
    @DisplayName("Should compact and compact a range")
    void testCompaction() throws Exception {
        for (int i = 0; i < 100; i++) {
            store.put(k("rk" + i), v("v" + i));
        }
        store.flushMemtable();
        assertThatCode(() -> store.compact()).doesNotThrowAnyException();
        assertThatCode(() -> store.compactRange(v("rk0"), v("rk5"))).doesNotThrowAnyException();
        assertThatThrownBy(() -> store.compactRange(null, null)).isInstanceOf(TidesDBException.class);
        assertThat(str(store.get(k("rk7")))).isEqualTo("v7");
        assertThat(store.isCompacting()).isIn(true, false);
        assertThat(store.isFlushing()).isIn(true, false);
    }

    @Test
    @DisplayName("Should sync the WAL and checkpoint")
    void testDurability() {
        putAllKeys("a");
        assertThatCode(() -> store.syncWal()).doesNotThrowAnyException();
        assertThatCode(() -> store.checkpoint()).doesNotThrowAnyException();
        assertThat(store.get(k("a"))).isNotNull();
    }

    @Test
    @DisplayName("Should write an openable backup into an absent directory")
    void testBackup() throws Exception {
        putAllKeys("backed-up");
        File backupDir = new File(tempDir, "backup-out");
        store.backup(backupDir.getAbsolutePath());
        assertThat(backupDir).isDirectory();

        // the backup opens as a store of its own: <state dir>/<store name>
        File restoredState = new File(tempDir, "restored-state");
        assertThat(restoredState.mkdirs()).isTrue();
        Files.move(backupDir.toPath(), new File(restoredState, "test-store").toPath());
        TidesDBStore restored = new TidesDBStore("test-store",
            TidesDBStoreConfig.builder().logLevel(LogLevel.NONE).build());
        restored.init(contextAt(restoredState), restored);
        try {
            assertThat(str(restored.get(k("backed-up")))).isEqualTo("value_backed-up");
        } finally {
            restored.close();
        }
    }

    @Test
    @DisplayName("Should refuse a backup into a non-empty directory")
    void testBackupNonEmptyDir() throws Exception {
        File occupied = new File(tempDir, "occupied");
        assertThat(occupied.mkdirs()).isTrue();
        Files.write(new File(occupied, "stray").toPath(), v("x"));
        assertThatThrownBy(() -> store.backup(occupied.getAbsolutePath()))
            .isInstanceOf(TidesDBException.class);
    }

    @Test
    @DisplayName("Should deliver committed writes to a commit hook")
    void testCommitHook() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> lastSeq = new AtomicReference<>();
        store.setCommitHook((ops, commitSeq) -> {
            for (CommitOp op : ops) {
                seen.add(str(op.getKey()) + (op.isDelete() ? ":del" : ":put"));
            }
            lastSeq.set(commitSeq);
            return 0;
        });
        store.put(k("h1"), v("v"));
        store.delete(k("h1"));
        assertThat(seen).containsExactly("h1:put", "h1:del");
        assertThat(lastSeq.get()).isNotNull();

        store.clearCommitHook();
        store.put(k("h2"), v("v"));
        assertThat(seen).hasSize(2);
    }

    @Test
    @DisplayName("Should update the column family configuration at runtime")
    void testUpdateRuntimeConfig() throws Exception {
        ColumnFamilyConfig updated = store.getStats().getConfig().toBuilder()
            .bloomFpr(0.05)
            .build();
        store.updateRuntimeConfig(updated, false);
        assertThat(store.getStats().getConfig().getBloomFpr()).isEqualTo(0.05);
    }

    /* ===== column family management ===== */

    @Test
    @DisplayName("Should list the store's column family")
    void testListColumnFamilies() throws Exception {
        assertThat(store.listColumnFamilies()).contains("default");
    }

    @Test
    @DisplayName("Should create, fetch and drop an extra column family")
    void testCreateAndDropColumnFamily() throws Exception {
        store.createColumnFamily("extra_cf", ColumnFamilyConfig.builder().build());
        ColumnFamily cf = store.getColumnFamily("extra_cf");
        assertThat(cf.getName()).isEqualTo("extra_cf");
        store.dropColumnFamily("extra_cf");
        assertThat(store.listColumnFamilies()).doesNotContain("extra_cf");
    }

    @Test
    @DisplayName("Should rename and clone column families")
    void testRenameAndClone() throws Exception {
        store.createColumnFamily("src_cf", ColumnFamilyConfig.builder().build());
        store.cloneColumnFamily("src_cf", "clone_cf");
        store.renameColumnFamily("src_cf", "renamed_cf");
        assertThat(store.listColumnFamilies())
            .contains("clone_cf", "renamed_cf")
            .doesNotContain("src_cf");
    }

    @Test
    @DisplayName("Should give direct access to the database and column family")
    void testDirectAccess() {
        assertThat(store.getDb()).isNotNull();
        assertThat(store.getColumnFamily()).isNotNull();
        assertThat(store.getStoreConfig()).isNotNull();
    }

    @Test
    @DisplayName("Should surface ERR_ROOT_EXECUTION as a constant")
    void testRootExecutionCode() {
        // a root-run open is refused by the engine and wrapped with guidance; that is not
        // exercisable without root, so only the code the store matches on is checked
        assertThat(TidesDBException.ERR_ROOT_EXECUTION).isEqualTo(-18);
    }
}
