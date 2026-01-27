package com.tidesdb.kafka.store;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for TidesDBStore
 */
class TidesDBStoreTest {

    @TempDir
    File tempDir;

    private TidesDBStore store;
    private StateStoreContext context;

    @BeforeEach
    void setUp() {
        store = new TidesDBStore("test-store");
        context = mock(StateStoreContext.class);
        when(context.stateDir()).thenReturn(tempDir);
        
        store.init(context, store);
    }

    @AfterEach
    void tearDown() {
        if (store != null && store.isOpen()) {
            store.close();
        }
    }

    @Test
    @DisplayName("Should initialize store successfully")
    void testInitialization() {
        assertThat(store.isOpen()).isTrue();
        assertThat(store.name()).isEqualTo("test-store");
        assertThat(store.persistent()).isTrue();
    }

    @Test
    @DisplayName("Should put and get a value")
    void testPutAndGet() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

        store.put(key, value);
        byte[] retrieved = store.get(key);

        assertThat(retrieved).isEqualTo(value);
    }

    @Test
    @DisplayName("Should return null for non-existent key")
    void testGetNonExistent() {
        Bytes key = Bytes.wrap("nonexistent".getBytes(StandardCharsets.UTF_8));
        byte[] value = store.get(key);

        assertThat(value).isNull();
    }

    @Test
    @DisplayName("Should delete a key")
    void testDelete() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

        store.put(key, value);
        assertThat(store.get(key)).isNotNull();

        byte[] deleted = store.delete(key);
        assertThat(deleted).isEqualTo(value);
        assertThat(store.get(key)).isNull();
    }

    @Test
    @DisplayName("Should delete with null value in put")
    void testPutNullValue() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

        store.put(key, value);
        assertThat(store.get(key)).isNotNull();

        store.put(key, null);
        assertThat(store.get(key)).isNull();
    }

    @Test
    @DisplayName("Should handle putIfAbsent correctly")
    void testPutIfAbsent() {
        Bytes key = Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8));
        byte[] value1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value2".getBytes(StandardCharsets.UTF_8);

        // First put should succeed
        byte[] existing = store.putIfAbsent(key, value1);
        assertThat(existing).isNull();
        assertThat(store.get(key)).isEqualTo(value1);

        // Second put should fail and return existing value
        existing = store.putIfAbsent(key, value2);
        assertThat(existing).isEqualTo(value1);
        assertThat(store.get(key)).isEqualTo(value1);
    }

    @Test
    @DisplayName("Should handle putAll correctly")
    void testPutAll() {
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(KeyValue.pair(
            Bytes.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            "value1".getBytes(StandardCharsets.UTF_8)
        ));
        entries.add(KeyValue.pair(
            Bytes.wrap("key2".getBytes(StandardCharsets.UTF_8)),
            "value2".getBytes(StandardCharsets.UTF_8)
        ));
        entries.add(KeyValue.pair(
            Bytes.wrap("key3".getBytes(StandardCharsets.UTF_8)),
            "value3".getBytes(StandardCharsets.UTF_8)
        ));

        store.putAll(entries);

        assertThat(new String(store.get(Bytes.wrap("key1".getBytes())), StandardCharsets.UTF_8))
            .isEqualTo("value1");
        assertThat(new String(store.get(Bytes.wrap("key2".getBytes())), StandardCharsets.UTF_8))
            .isEqualTo("value2");
        assertThat(new String(store.get(Bytes.wrap("key3".getBytes())), StandardCharsets.UTF_8))
            .isEqualTo("value3");
    }

    @Test
    @DisplayName("Should iterate over all entries")
    void testAllIterator() {
        // Insert test data
        store.put(Bytes.wrap("a".getBytes()), "value_a".getBytes());
        store.put(Bytes.wrap("b".getBytes()), "value_b".getBytes());
        store.put(Bytes.wrap("c".getBytes()), "value_c".getBytes());

        List<KeyValue<String, String>> results = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<Bytes, byte[]> kv = iterator.next();
                results.add(KeyValue.pair(
                    new String(kv.key.get(), StandardCharsets.UTF_8),
                    new String(kv.value, StandardCharsets.UTF_8)
                ));
            }
        }

        assertThat(results).hasSize(3);
        assertThat(results).extracting(kv -> kv.key)
            .containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    @DisplayName("Should iterate over range of entries")
    void testRangeIterator() {
        // Insert test data
        store.put(Bytes.wrap("a".getBytes()), "value_a".getBytes());
        store.put(Bytes.wrap("b".getBytes()), "value_b".getBytes());
        store.put(Bytes.wrap("c".getBytes()), "value_c".getBytes());
        store.put(Bytes.wrap("d".getBytes()), "value_d".getBytes());
        store.put(Bytes.wrap("e".getBytes()), "value_e".getBytes());

        List<String> keys = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> iterator = store.range(
                Bytes.wrap("b".getBytes()),
                Bytes.wrap("d".getBytes())
        )) {
            while (iterator.hasNext()) {
                KeyValue<Bytes, byte[]> kv = iterator.next();
                keys.add(new String(kv.key.get(), StandardCharsets.UTF_8));
            }
        }

        assertThat(keys).containsExactly("b", "c", "d");
    }

    @Test
    @DisplayName("Should return approximate number of entries")
    void testApproximateNumEntries() {
        assertThat(store.approximateNumEntries()).isEqualTo(0);

        store.put(Bytes.wrap("key1".getBytes()), "value1".getBytes());
        store.put(Bytes.wrap("key2".getBytes()), "value2".getBytes());
        store.put(Bytes.wrap("key3".getBytes()), "value3".getBytes());

        // After flush, stats should reflect the entries
        store.flush();
        assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @DisplayName("Should handle large values")
    void testLargeValues() {
        Bytes key = Bytes.wrap("large".getBytes());
        byte[] largeValue = new byte[1024 * 1024]; // 1MB
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        store.put(key, largeValue);
        byte[] retrieved = store.get(key);

        assertThat(retrieved).isEqualTo(largeValue);
    }

    @Test
    @DisplayName("Should handle many keys")
    void testManyKeys() {
        int numKeys = 10000;

        // Insert many keys
        for (int i = 0; i < numKeys; i++) {
            String keyStr = String.format("key_%06d", i);
            String valueStr = String.format("value_%06d", i);
            store.put(
                Bytes.wrap(keyStr.getBytes()),
                valueStr.getBytes()
            );
        }

        // Verify random keys
        for (int i = 0; i < 100; i++) {
            int idx = (int) (Math.random() * numKeys);
            String keyStr = String.format("key_%06d", idx);
            String expectedValue = String.format("value_%06d", idx);
            
            byte[] value = store.get(Bytes.wrap(keyStr.getBytes()));
            assertThat(new String(value)).isEqualTo(expectedValue);
        }

        store.flush();
        assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(numKeys);
    }

    @Test
    @DisplayName("Should throw exception when accessing closed store")
    void testAccessClosedStore() {
        store.close();

        assertThatThrownBy(() -> store.put(
            Bytes.wrap("key".getBytes()),
            "value".getBytes()
        )).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("not open");
    }

    @Test
    @DisplayName("Should throw exception for null key")
    void testNullKey() {
        assertThatThrownBy(() -> store.put(null, "value".getBytes()))
            .isInstanceOf(NullPointerException.class);

        assertThatCode(() -> store.get(null))
            .doesNotThrowAnyException(); // get returns null for null key

        assertThatThrownBy(() -> store.delete(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should handle flush operation")
    void testFlush() {
        store.put(Bytes.wrap("key1".getBytes()), "value1".getBytes());
        store.put(Bytes.wrap("key2".getBytes()), "value2".getBytes());

        assertThatCode(() -> store.flush()).doesNotThrowAnyException();

        // Data should still be accessible after flush
        assertThat(store.get(Bytes.wrap("key1".getBytes()))).isNotNull();
        assertThat(store.get(Bytes.wrap("key2".getBytes()))).isNotNull();
    }

    @Test
    @DisplayName("Should update existing key")
    void testUpdateExistingKey() {
        Bytes key = Bytes.wrap("key1".getBytes());
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key, value1);
        assertThat(store.get(key)).isEqualTo(value1);

        store.put(key, value2);
        assertThat(store.get(key)).isEqualTo(value2);
    }

    @Test
    @DisplayName("Should handle empty iterator")
    void testEmptyIterator() {
        try (KeyValueIterator<Bytes, byte[]> iterator = store.all()) {
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    @DisplayName("Should handle concurrent reads")
    void testConcurrentReads() throws InterruptedException {
        // Populate store
        for (int i = 0; i < 1000; i++) {
            store.put(
                Bytes.wrap(("key" + i).getBytes()),
                ("value" + i).getBytes()
            );
        }

        // Start multiple reader threads
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    int idx = (int) (Math.random() * 1000);
                    byte[] value = store.get(Bytes.wrap(("key" + idx).getBytes()));
                    assertThat(value).isNotNull();
                }
            });
            threads[i].start();
        }

        // Wait for all threads
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
