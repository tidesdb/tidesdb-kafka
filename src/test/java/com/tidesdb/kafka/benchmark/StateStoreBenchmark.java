package com.tidesdb.kafka.benchmark;

import com.tidesdb.kafka.store.TidesDBStore;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive benchmark suite comparing TidesDB vs RocksDB.
 * Generates CSV files for graphing performance metrics.
 */
public class StateStoreBenchmark {

    @TempDir
    File tempDir;

    private StateStoreContext context;
    private final Random random = new Random(42);
    private static final String TIMESTAMP = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));

    @BeforeEach
    void setUp() {
        context = mock(StateStoreContext.class);
        when(context.stateDir()).thenReturn(tempDir);
    }

    @AfterEach
    void tearDown() {
        // Cleanup is handled by @TempDir
    }

    @Test
    public void runAllBenchmarks() throws IOException {
        System.out.println("Starting comprehensive benchmarks...\n");

        // Run all benchmark scenarios
        benchmarkSequentialWrites();
        benchmarkRandomWrites();
        benchmarkSequentialReads();
        benchmarkRandomReads();
        benchmarkMixedWorkload();
        benchmarkRangeScans();
        benchmarkBulkWrites();
        benchmarkUpdateWorkload();
        benchmarkLargeValues();
        benchmarkIterationPerformance();

        System.out.println("\nAll benchmarks completed!");
        System.out.println("CSV files generated in: " + new File("benchmarks").getAbsolutePath());
    }

    private void benchmarkSequentialWrites() throws IOException {
        System.out.println("Benchmarking Sequential Writes...");
        
        int[] sizes = {1000, 5000, 10000, 50000, 100000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("seq-write-tides");
            long tidesTime = measureSequentialWrites(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("seq-write-rocks");
            long rocksTime = measureSequentialWrites(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Sequential Writes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/sequential_writes_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkRandomWrites() throws IOException {
        System.out.println("\nBenchmarking Random Writes...");
        
        int[] sizes = {1000, 5000, 10000, 50000, 100000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("rand-write-tides");
            long tidesTime = measureRandomWrites(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("rand-write-rocks");
            long rocksTime = measureRandomWrites(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Random Writes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/random_writes_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkSequentialReads() throws IOException {
        System.out.println("\nBenchmarking Sequential Reads...");
        
        int[] sizes = {1000, 5000, 10000, 50000, 100000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("seq-read-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureSequentialReads(tidesStore, size);
            tidesStore.close();

            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("seq-read-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureSequentialReads(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Sequential Reads", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/sequential_reads_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkRandomReads() throws IOException {
        System.out.println("\nBenchmarking Random Reads...");
        
        int[] sizes = {1000, 5000, 10000, 50000, 100000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("rand-read-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureRandomReads(tidesStore, size, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("rand-read-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureRandomReads(rocksStore, size, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Random Reads", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/random_reads_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkMixedWorkload() throws IOException {
        System.out.println("\nBenchmarking Mixed Workload (50% read, 50% write)...");
        
        int[] sizes = {1000, 5000, 10000, 50000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("mixed-tides");
            populateSequential(tidesStore, size / 2);
            long tidesTime = measureMixedWorkload(tidesStore, size, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("mixed-rocks");
            populateSequential(rocksStore, size / 2);
            long rocksTime = measureMixedWorkload(rocksStore, size, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Mixed Workload", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d ops: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/mixed_workload_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkRangeScans() throws IOException {
        System.out.println("\nBenchmarking Range Scans...");
        
        int dataSize = 50000;
        int[] rangeSizes = {10, 100, 1000, 5000, 10000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int rangeSize : rangeSizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("range-tides");
            populateSequential(tidesStore, dataSize);
            long tidesTime = measureRangeScan(tidesStore, rangeSize);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("range-rocks");
            populateSequential(rocksStore, dataSize);
            long rocksTime = measureRangeScan(rocksStore, rangeSize);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Range Scan", rangeSize, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                rangeSize, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/range_scans_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkBulkWrites() throws IOException {
        System.out.println("\nBenchmarking Bulk Writes (putAll)...");
        
        int[] sizes = {1000, 5000, 10000, 50000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("bulk-tides");
            long tidesTime = measureBulkWrites(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("bulk-rocks");
            long rocksTime = measureBulkWrites(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Bulk Writes", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/bulk_writes_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkUpdateWorkload() throws IOException {
        System.out.println("\nBenchmarking Update Workload...");
        
        int[] sizes = {1000, 5000, 10000, 50000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("update-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureUpdates(tidesStore, size);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("update-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureUpdates(rocksStore, size);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Updates", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/updates_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkLargeValues() throws IOException {
        System.out.println("\nBenchmarking Large Values (10KB each)...");
        
        int[] sizes = {100, 500, 1000, 5000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("large-tides");
            long tidesTime = measureLargeValueWrites(tidesStore, size, 10 * 1024);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("large-rocks");
            long rocksTime = measureLargeValueWrites(rocksStore, size, 10 * 1024);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Large Values", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/large_values_" + TIMESTAMP + ".csv", results);
    }

    private void benchmarkIterationPerformance() throws IOException {
        System.out.println("\nBenchmarking Full Iteration...");
        
        int[] sizes = {1000, 5000, 10000, 50000};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            // TidesDB
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("iter-tides");
            populateSequential(tidesStore, size);
            long tidesTime = measureFullIteration(tidesStore);
            tidesStore.close();

            // RocksDB
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("iter-rocks");
            populateSequential(rocksStore, size);
            long rocksTime = measureFullIteration(rocksStore);
            rocksStore.close();

            results.add(new BenchmarkResult(
                "Full Iteration", size, tidesTime, rocksTime
            ));

            System.out.printf("  %d keys: TidesDB=%dms, RocksDB=%dms (%.2fx)%n",
                size, tidesTime, rocksTime, (double) rocksTime / tidesTime);
        }

        writeCsv("benchmarks/iteration_" + TIMESTAMP + ".csv", results);
    }

    // Helper methods for measurements

    private long measureSequentialWrites(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("value_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureRandomWrites(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            int idx = random.nextInt(count * 2);
            String key = String.format("key_%08d", idx);
            String value = String.format("value_%08d", idx);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureSequentialReads(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
        }
        return System.currentTimeMillis() - start;
    }

    private long measureRandomReads(KeyValueStore<Bytes, byte[]> store, int dataSize, int readCount) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < readCount; i++) {
            int idx = random.nextInt(dataSize);
            String key = String.format("key_%08d", idx);
            store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
        }
        return System.currentTimeMillis() - start;
    }

    private long measureMixedWorkload(KeyValueStore<Bytes, byte[]> store, int dataSize, int opCount) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < opCount; i++) {
            int idx = random.nextInt(dataSize);
            String key = String.format("key_%08d", idx);
            
            if (random.nextBoolean()) {
                // Read
                store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
            } else {
                // Write
                String value = String.format("value_%08d", idx);
                store.put(
                    Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                    value.getBytes(StandardCharsets.UTF_8)
                );
            }
        }
        return System.currentTimeMillis() - start;
    }

    private long measureRangeScan(KeyValueStore<Bytes, byte[]> store, int rangeSize) {
        int startIdx = random.nextInt(50000 - rangeSize);
        String fromKey = String.format("key_%08d", startIdx);
        String toKey = String.format("key_%08d", startIdx + rangeSize);

        long start = System.currentTimeMillis();
        try (KeyValueIterator<Bytes, byte[]> iter = store.range(
                Bytes.wrap(fromKey.getBytes(StandardCharsets.UTF_8)),
                Bytes.wrap(toKey.getBytes(StandardCharsets.UTF_8))
        )) {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        }
        return System.currentTimeMillis() - start;
    }

    private long measureBulkWrites(KeyValueStore<Bytes, byte[]> store, int count) {
        List<KeyValue<Bytes, byte[]>> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("value_%08d", i);
            batch.add(KeyValue.pair(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            ));
        }

        long start = System.currentTimeMillis();
        store.putAll(batch);
        return System.currentTimeMillis() - start;
    }

    private long measureUpdates(KeyValueStore<Bytes, byte[]> store, int count) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("updated_value_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureLargeValueWrites(KeyValueStore<Bytes, byte[]> store, int count, int valueSize) {
        byte[] largeValue = new byte[valueSize];
        random.nextBytes(largeValue);

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                largeValue
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long measureFullIteration(KeyValueStore<Bytes, byte[]> store) {
        long start = System.currentTimeMillis();
        try (KeyValueIterator<Bytes, byte[]> iter = store.all()) {
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        }
        return System.currentTimeMillis() - start;
    }

    private void populateSequential(KeyValueStore<Bytes, byte[]> store, int count) {
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", i);
            String value = String.format("value_%08d", i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    private KeyValueStore<Bytes, byte[]> createTidesDBStore(String name) {
        TidesDBStore store = new TidesDBStore(name);
        store.init(context, store);
        return store;
    }

    private RocksDBWrapper createRocksDBStore(String name) {
        try {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            String dbPath = new File(tempDir, name).getAbsolutePath();
            RocksDB db = RocksDB.open(options, dbPath);
            return new RocksDBWrapper(db, options);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to create RocksDB store", e);
        }
    }

    /**
     * Simple wrapper around RocksDB to match the KeyValueStore interface for benchmarking
     */
    private static class RocksDBWrapper implements KeyValueStore<Bytes, byte[]> {
        private final RocksDB db;
        private final Options options;
        private boolean open = true;

        RocksDBWrapper(RocksDB db, Options options) {
            this.db = db;
            this.options = options;
        }

        @Override
        public void put(Bytes key, byte[] value) {
            try {
                if (value == null) {
                    db.delete(key.get());
                } else {
                    db.put(key.get(), value);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] putIfAbsent(Bytes key, byte[] value) {
            byte[] existing = get(key);
            if (existing == null) {
                put(key, value);
                return null;
            }
            return existing;
        }

        @Override
        public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
            for (KeyValue<Bytes, byte[]> entry : entries) {
                put(entry.key, entry.value);
            }
        }

        @Override
        public byte[] delete(Bytes key) {
            byte[] old = get(key);
            try {
                db.delete(key.get());
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return old;
        }

        @Override
        public byte[] get(Bytes key) {
            try {
                return db.get(key.get());
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
            return new RocksDBRangeIterator(db, from, to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            return new RocksDBRangeIterator(db, null, null);
        }

        @Override
        public long approximateNumEntries() {
            try {
                return Long.parseLong(db.getProperty("rocksdb.estimate-num-keys"));
            } catch (RocksDBException e) {
                return 0;
            }
        }

        @Override
        public String name() {
            return "rocksdb-benchmark";
        }

        @Override
        public void init(org.apache.kafka.streams.processor.ProcessorContext context,
                         org.apache.kafka.streams.processor.StateStore root) {
        }

        @Override
        public void init(org.apache.kafka.streams.processor.StateStoreContext context,
                         org.apache.kafka.streams.processor.StateStore root) {
        }

        @Override
        public void flush() {
            try {
                db.flush(new org.rocksdb.FlushOptions());
            } catch (RocksDBException e) {
                // ignore
            }
        }

        @Override
        public void close() {
            if (open) {
                db.close();
                options.close();
                open = false;
            }
        }

        @Override
        public boolean persistent() {
            return true;
        }

        @Override
        public boolean isOpen() {
            return open;
        }
    }

    /**
     * Simple RocksDB range iterator for benchmarking
     */
    private static class RocksDBRangeIterator implements KeyValueIterator<Bytes, byte[]> {
        private final org.rocksdb.RocksIterator iterator;
        private final byte[] toKey;
        private boolean hasNext;
        private KeyValue<Bytes, byte[]> next;

        RocksDBRangeIterator(RocksDB db, Bytes from, Bytes to) {
            this.iterator = db.newIterator();
            this.toKey = to != null ? to.get() : null;
            
            if (from != null) {
                iterator.seek(from.get());
            } else {
                iterator.seekToFirst();
            }
            advance();
        }

        private void advance() {
            if (iterator.isValid()) {
                byte[] key = iterator.key();
                if (toKey != null && compareBytes(key, toKey) >= 0) {
                    hasNext = false;
                    next = null;
                } else {
                    next = KeyValue.pair(Bytes.wrap(key), iterator.value());
                    hasNext = true;
                    iterator.next();
                }
            } else {
                hasNext = false;
                next = null;
            }
        }

        private int compareBytes(byte[] a, byte[] b) {
            int minLen = Math.min(a.length, b.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return a.length - b.length;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext) throw new java.util.NoSuchElementException();
            return next.key;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext) throw new java.util.NoSuchElementException();
            KeyValue<Bytes, byte[]> current = next;
            advance();
            return current;
        }
    }

    private void writeCsv(String filename, List<BenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Size,TidesDB_ms,RocksDB_ms,Speedup");
            for (BenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%.2f%n",
                    result.name,
                    result.size,
                    result.tidesTime,
                    result.rocksTime,
                    (double) result.rocksTime / result.tidesTime
                );
            }
        }
    }

    private static class BenchmarkResult {
        final String name;
        final int size;
        final long tidesTime;
        final long rocksTime;

        BenchmarkResult(String name, int size, long tidesTime, long rocksTime) {
            this.name = name;
            this.size = size;
            this.tidesTime = tidesTime;
            this.rocksTime = rocksTime;
        }
    }
}
