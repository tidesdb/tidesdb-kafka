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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    
    // Extended benchmark configuration
    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASUREMENT_ITERATIONS = 5;
    private static final int[] LARGE_SIZES = {100000, 500000, 1000000, 5000000, 10000000, 25000000};
    private static final int[] CONCURRENT_THREAD_COUNTS = {1, 2, 4, 8, 16};
    
    // Memory and CPU monitoring
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

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
    public void runAllBenchmarks() throws IOException, InterruptedException {
        System.out.println("Starting comprehensive benchmarks...\n");
        System.out.println("Configuration:");
        System.out.println("  Warmup iterations: " + WARMUP_ITERATIONS);
        System.out.println("  Measurement iterations: " + MEASUREMENT_ITERATIONS);
        System.out.println("  Large dataset sizes: up to 1M keys");
        System.out.println();

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
        
        // Extended benchmarks
        benchmarkLargeDatasets();
        benchmarkConcurrentAccess();
        benchmarkCompactionPressure();
        benchmarkWithMetrics();

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

    // ==================== EXTENDED BENCHMARKS ====================

    /**
     * Benchmark with large datasets (up to 1M keys) with warmup
     */
    private void benchmarkLargeDatasets() throws IOException {
        System.out.println("\n=== EXTENDED: Large Dataset Benchmark (with warmup) ===");
        
        List<ExtendedBenchmarkResult> results = new ArrayList<>();

        for (int size : LARGE_SIZES) {
            System.out.printf("\nTesting %,d keys...%n", size);
            
            // Warmup phase
            System.out.println("  Warming up...");
            for (int w = 0; w < WARMUP_ITERATIONS; w++) {
                KeyValueStore<Bytes, byte[]> warmupStore = createTidesDBStore("warmup-tides-" + w);
                measureSequentialWrites(warmupStore, Math.min(size / 10, 10000));
                warmupStore.close();
            }
            
            // Measurement phase - multiple iterations
            long[] tidesTimes = new long[MEASUREMENT_ITERATIONS];
            long[] rocksTimes = new long[MEASUREMENT_ITERATIONS];
            
            for (int iter = 0; iter < MEASUREMENT_ITERATIONS; iter++) {
                // TidesDB
                KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("large-tides-" + iter);
                tidesTimes[iter] = measureSequentialWrites(tidesStore, size);
                tidesStore.close();

                // RocksDB
                KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("large-rocks-" + iter);
                rocksTimes[iter] = measureSequentialWrites(rocksStore, size);
                rocksStore.close();
            }
            
            // Calculate statistics
            long tidesAvg = average(tidesTimes);
            long rocksAvg = average(rocksTimes);
            long tidesStdDev = stdDev(tidesTimes);
            long rocksStdDev = stdDev(rocksTimes);
            
            results.add(new ExtendedBenchmarkResult(
                "Large Dataset Writes", size, tidesAvg, rocksAvg, tidesStdDev, rocksStdDev
            ));

            System.out.printf("  %,d keys: TidesDB=%dms (±%d), RocksDB=%dms (±%d), Speedup=%.2fx%n",
                size, tidesAvg, tidesStdDev, rocksAvg, rocksStdDev, (double) rocksAvg / tidesAvg);
        }

        writeExtendedCsv("benchmarks/large_datasets_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark concurrent/multi-threaded access
     */
    private void benchmarkConcurrentAccess() throws IOException, InterruptedException {
        System.out.println("\n=== EXTENDED: Concurrent Access Benchmark ===");
        
        int dataSize = 100000;
        int opsPerThread = 10000;
        List<ConcurrentBenchmarkResult> results = new ArrayList<>();

        for (int threadCount : CONCURRENT_THREAD_COUNTS) {
            System.out.printf("\nTesting with %d threads...%n", threadCount);
            
            // TidesDB concurrent test
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("concurrent-tides");
            populateSequential(tidesStore, dataSize);
            long tidesTime = measureConcurrentMixedWorkload(tidesStore, dataSize, opsPerThread, threadCount);
            long tidesThroughput = (long) ((threadCount * opsPerThread) / (tidesTime / 1000.0));
            tidesStore.close();

            // RocksDB concurrent test
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("concurrent-rocks");
            populateSequential(rocksStore, dataSize);
            long rocksTime = measureConcurrentMixedWorkload(rocksStore, dataSize, opsPerThread, threadCount);
            long rocksThroughput = (long) ((threadCount * opsPerThread) / (rocksTime / 1000.0));
            rocksStore.close();

            results.add(new ConcurrentBenchmarkResult(
                "Concurrent Mixed", threadCount, threadCount * opsPerThread,
                tidesTime, rocksTime, tidesThroughput, rocksThroughput
            ));

            System.out.printf("  %d threads: TidesDB=%dms (%,d ops/s), RocksDB=%dms (%,d ops/s)%n",
                threadCount, tidesTime, tidesThroughput, rocksTime, rocksThroughput);
        }

        writeConcurrentCsv("benchmarks/concurrent_access_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark with compaction pressure (accumulated data over time)
     */
    private void benchmarkCompactionPressure() throws IOException {
        System.out.println("\n=== EXTENDED: Compaction Pressure Benchmark ===");
        
        int batchSize = 50000;
        int numBatches = 5;
        List<CompactionBenchmarkResult> results = new ArrayList<>();

        // TidesDB - accumulate data over multiple batches
        System.out.println("\nTidesDB compaction pressure test...");
        KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("compaction-tides");
        for (int batch = 1; batch <= numBatches; batch++) {
            int startKey = (batch - 1) * batchSize;
            long writeTime = measureSequentialWritesWithOffset(tidesStore, batchSize, startKey);
            long readTime = measureRandomReads(tidesStore, batch * batchSize, batchSize);
            
            results.add(new CompactionBenchmarkResult(
                "TidesDB", batch, batch * batchSize, writeTime, readTime
            ));
            
            System.out.printf("  Batch %d (%,d total keys): write=%dms, read=%dms%n",
                batch, batch * batchSize, writeTime, readTime);
        }
        tidesStore.close();

        // RocksDB - accumulate data over multiple batches
        System.out.println("\nRocksDB compaction pressure test...");
        KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("compaction-rocks");
        for (int batch = 1; batch <= numBatches; batch++) {
            int startKey = (batch - 1) * batchSize;
            long writeTime = measureSequentialWritesWithOffset(rocksStore, batchSize, startKey);
            long readTime = measureRandomReads(rocksStore, batch * batchSize, batchSize);
            
            results.add(new CompactionBenchmarkResult(
                "RocksDB", batch, batch * batchSize, writeTime, readTime
            ));
            
            System.out.printf("  Batch %d (%,d total keys): write=%dms, read=%dms%n",
                batch, batch * batchSize, writeTime, readTime);
        }
        rocksStore.close();

        writeCompactionCsv("benchmarks/compaction_pressure_" + TIMESTAMP + ".csv", results);
    }

    /**
     * Benchmark with memory and CPU metrics
     */
    private void benchmarkWithMetrics() throws IOException {
        System.out.println("\n=== EXTENDED: Benchmark with Memory/CPU Metrics ===");
        
        int[] sizes = {10000, 50000, 100000, 250000};
        List<MetricsBenchmarkResult> results = new ArrayList<>();

        for (int size : sizes) {
            System.out.printf("\nTesting %,d keys with metrics...%n", size);
            
            // Force GC before measurement
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            
            // TidesDB with metrics
            long tidesMemBefore = getUsedMemory();
            double tidesCpuBefore = getProcessCpuLoad();
            
            KeyValueStore<Bytes, byte[]> tidesStore = createTidesDBStore("metrics-tides");
            long tidesWriteTime = measureSequentialWrites(tidesStore, size);
            long tidesReadTime = measureRandomReads(tidesStore, size, size);
            
            long tidesMemAfter = getUsedMemory();
            double tidesCpuAfter = getProcessCpuLoad();
            long tidesMemUsed = tidesMemAfter - tidesMemBefore;
            tidesStore.close();
            
            // Force GC before RocksDB
            System.gc();
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            
            // RocksDB with metrics
            long rocksMemBefore = getUsedMemory();
            double rocksCpuBefore = getProcessCpuLoad();
            
            KeyValueStore<Bytes, byte[]> rocksStore = createRocksDBStore("metrics-rocks");
            long rocksWriteTime = measureSequentialWrites(rocksStore, size);
            long rocksReadTime = measureRandomReads(rocksStore, size, size);
            
            long rocksMemAfter = getUsedMemory();
            double rocksCpuAfter = getProcessCpuLoad();
            long rocksMemUsed = rocksMemAfter - rocksMemBefore;
            rocksStore.close();

            results.add(new MetricsBenchmarkResult(
                "With Metrics", size,
                tidesWriteTime, tidesReadTime, tidesMemUsed, (tidesCpuBefore + tidesCpuAfter) / 2,
                rocksWriteTime, rocksReadTime, rocksMemUsed, (rocksCpuBefore + rocksCpuAfter) / 2
            ));

            System.out.printf("  TidesDB: write=%dms, read=%dms, mem=%,dKB%n",
                tidesWriteTime, tidesReadTime, tidesMemUsed / 1024);
            System.out.printf("  RocksDB: write=%dms, read=%dms, mem=%,dKB%n",
                rocksWriteTime, rocksReadTime, rocksMemUsed / 1024);
        }

        writeMetricsCsv("benchmarks/metrics_" + TIMESTAMP + ".csv", results);
    }

    // ==================== EXTENDED HELPER METHODS ====================

    private long measureConcurrentMixedWorkload(KeyValueStore<Bytes, byte[]> store, int dataSize, 
                                                 int opsPerThread, int threadCount) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong totalOps = new AtomicLong(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    Random threadRandom = new Random(42 + threadId);
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int i = 0; i < opsPerThread; i++) {
                        int idx = threadRandom.nextInt(dataSize);
                        String key = String.format("key_%08d", idx);
                        
                        if (threadRandom.nextBoolean()) {
                            store.get(Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)));
                        } else {
                            String value = String.format("value_%08d_%d", idx, threadId);
                            store.put(
                                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                                value.getBytes(StandardCharsets.UTF_8)
                            );
                        }
                        totalOps.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long start = System.currentTimeMillis();
        startLatch.countDown(); // Start all threads
        doneLatch.await(5, TimeUnit.MINUTES); // Wait for completion
        long elapsed = System.currentTimeMillis() - start;
        
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        return elapsed;
    }

    private long measureSequentialWritesWithOffset(KeyValueStore<Bytes, byte[]> store, int count, int offset) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String key = String.format("key_%08d", offset + i);
            String value = String.format("value_%08d", offset + i);
            store.put(
                Bytes.wrap(key.getBytes(StandardCharsets.UTF_8)),
                value.getBytes(StandardCharsets.UTF_8)
            );
        }
        return System.currentTimeMillis() - start;
    }

    private long getUsedMemory() {
        return memoryBean.getHeapMemoryUsage().getUsed() + 
               memoryBean.getNonHeapMemoryUsage().getUsed();
    }

    private double getProcessCpuLoad() {
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuLoad() * 100;
        }
        return -1;
    }

    private long average(long[] values) {
        long sum = 0;
        for (long v : values) sum += v;
        return sum / values.length;
    }

    private long stdDev(long[] values) {
        long avg = average(values);
        long sumSquares = 0;
        for (long v : values) {
            sumSquares += (v - avg) * (v - avg);
        }
        return (long) Math.sqrt(sumSquares / values.length);
    }

    // ==================== EXTENDED RESULT CLASSES ====================

    private static class ExtendedBenchmarkResult {
        final String name;
        final int size;
        final long tidesAvg;
        final long rocksAvg;
        final long tidesStdDev;
        final long rocksStdDev;

        ExtendedBenchmarkResult(String name, int size, long tidesAvg, long rocksAvg, 
                                long tidesStdDev, long rocksStdDev) {
            this.name = name;
            this.size = size;
            this.tidesAvg = tidesAvg;
            this.rocksAvg = rocksAvg;
            this.tidesStdDev = tidesStdDev;
            this.rocksStdDev = rocksStdDev;
        }
    }

    private static class ConcurrentBenchmarkResult {
        final String name;
        final int threads;
        final int totalOps;
        final long tidesTime;
        final long rocksTime;
        final long tidesThroughput;
        final long rocksThroughput;

        ConcurrentBenchmarkResult(String name, int threads, int totalOps,
                                   long tidesTime, long rocksTime,
                                   long tidesThroughput, long rocksThroughput) {
            this.name = name;
            this.threads = threads;
            this.totalOps = totalOps;
            this.tidesTime = tidesTime;
            this.rocksTime = rocksTime;
            this.tidesThroughput = tidesThroughput;
            this.rocksThroughput = rocksThroughput;
        }
    }

    private static class CompactionBenchmarkResult {
        final String store;
        final int batch;
        final int totalKeys;
        final long writeTime;
        final long readTime;

        CompactionBenchmarkResult(String store, int batch, int totalKeys, 
                                   long writeTime, long readTime) {
            this.store = store;
            this.batch = batch;
            this.totalKeys = totalKeys;
            this.writeTime = writeTime;
            this.readTime = readTime;
        }
    }

    private static class MetricsBenchmarkResult {
        final String name;
        final int size;
        final long tidesWriteTime;
        final long tidesReadTime;
        final long tidesMemUsed;
        final double tidesCpuAvg;
        final long rocksWriteTime;
        final long rocksReadTime;
        final long rocksMemUsed;
        final double rocksCpuAvg;

        MetricsBenchmarkResult(String name, int size,
                               long tidesWriteTime, long tidesReadTime, long tidesMemUsed, double tidesCpuAvg,
                               long rocksWriteTime, long rocksReadTime, long rocksMemUsed, double rocksCpuAvg) {
            this.name = name;
            this.size = size;
            this.tidesWriteTime = tidesWriteTime;
            this.tidesReadTime = tidesReadTime;
            this.tidesMemUsed = tidesMemUsed;
            this.tidesCpuAvg = tidesCpuAvg;
            this.rocksWriteTime = rocksWriteTime;
            this.rocksReadTime = rocksReadTime;
            this.rocksMemUsed = rocksMemUsed;
            this.rocksCpuAvg = rocksCpuAvg;
        }
    }

    // ==================== EXTENDED CSV WRITERS ====================

    private void writeExtendedCsv(String filename, List<ExtendedBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Size,TidesDB_avg_ms,RocksDB_avg_ms,TidesDB_stddev,RocksDB_stddev,Speedup");
            for (ExtendedBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d,%d,%.2f%n",
                    result.name,
                    result.size,
                    result.tidesAvg,
                    result.rocksAvg,
                    result.tidesStdDev,
                    result.rocksStdDev,
                    (double) result.rocksAvg / result.tidesAvg
                );
            }
        }
    }

    private void writeConcurrentCsv(String filename, List<ConcurrentBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Threads,TotalOps,TidesDB_ms,RocksDB_ms,TidesDB_ops_sec,RocksDB_ops_sec,Speedup");
            for (ConcurrentBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d,%d,%d,%.2f%n",
                    result.name,
                    result.threads,
                    result.totalOps,
                    result.tidesTime,
                    result.rocksTime,
                    result.tidesThroughput,
                    result.rocksThroughput,
                    (double) result.rocksTime / result.tidesTime
                );
            }
        }
    }

    private void writeCompactionCsv(String filename, List<CompactionBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Store,Batch,TotalKeys,WriteTime_ms,ReadTime_ms");
            for (CompactionBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d%n",
                    result.store,
                    result.batch,
                    result.totalKeys,
                    result.writeTime,
                    result.readTime
                );
            }
        }
    }

    private void writeMetricsCsv(String filename, List<MetricsBenchmarkResult> results) throws IOException {
        File file = new File(filename);
        file.getParentFile().mkdirs();

        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Benchmark,Size,TidesDB_write_ms,TidesDB_read_ms,TidesDB_mem_bytes,TidesDB_cpu_pct,RocksDB_write_ms,RocksDB_read_ms,RocksDB_mem_bytes,RocksDB_cpu_pct");
            for (MetricsBenchmarkResult result : results) {
                writer.printf("%s,%d,%d,%d,%d,%.2f,%d,%d,%d,%.2f%n",
                    result.name,
                    result.size,
                    result.tidesWriteTime,
                    result.tidesReadTime,
                    result.tidesMemUsed,
                    result.tidesCpuAvg,
                    result.rocksWriteTime,
                    result.rocksReadTime,
                    result.rocksMemUsed,
                    result.rocksCpuAvg
                );
            }
        }
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
            Options options = new Options()
                .setCreateIfMissing(true)
                // Disable sync for fair comparison with TidesDB (SYNC_NONE)
                // By default RocksDB doesn't sync, but we explicitly set WriteOptions in put()
                .setParanoidChecks(false);
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
        private final org.rocksdb.WriteOptions writeOptions;
        private boolean open = true;

        RocksDBWrapper(RocksDB db, Options options) {
            this.db = db;
            this.options = options;
            // Explicitly disable sync for fair comparison with TidesDB SYNC_NONE
            this.writeOptions = new org.rocksdb.WriteOptions().setSync(false).setDisableWAL(false);
        }

        @Override
        public void put(Bytes key, byte[] value) {
            try {
                if (value == null) {
                    db.delete(writeOptions, key.get());
                } else {
                    db.put(writeOptions, key.get(), value);
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
                writeOptions.close();
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
