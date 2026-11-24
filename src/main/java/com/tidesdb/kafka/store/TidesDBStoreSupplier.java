package com.tidesdb.kafka.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Supplies TidesDB-backed key-value stores to Kafka Streams. Kafka wraps each
 * store it gets from here with change-logging, caching and metrics, as it does
 * RocksDB's.
 *
 * <p>With the DSL:
 * <pre>{@code
 * builder.stream("input")
 *     .groupByKey()
 *     .count(Materialized.as(new TidesDBStoreSupplier("my-store")));
 * }</pre>
 *
 * <p>With a custom configuration:
 * <pre>{@code
 * TidesDBStoreConfig config = TidesDBStoreConfig.builder()
 *     .compression(CompressionAlgorithm.ZSTD)
 *     .blockCacheSize(256L * 1024 * 1024)
 *     .build();
 * builder.stream("input")
 *     .groupByKey()
 *     .count(Materialized.as(new TidesDBStoreSupplier("my-store", config)));
 * }</pre>
 *
 * <p>With the Processor API:
 * <pre>{@code
 * topology.addStateStore(
 *     Stores.keyValueStoreBuilder(new TidesDBStoreSupplier("my-store"),
 *                                 Serdes.String(), Serdes.Long()),
 *     "my-processor");
 * }</pre>
 */
public class TidesDBStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private final TidesDBStoreConfig storeConfig;

    public TidesDBStoreSupplier(String name) {
        this(name, TidesDBStoreConfig.defaultConfig());
    }

    public TidesDBStoreSupplier(String name, TidesDBStoreConfig config) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be null or empty");
        }
        this.name = name;
        this.storeConfig = config != null ? config : TidesDBStoreConfig.defaultConfig();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new TidesDBStore(name, storeConfig);
    }

    @Override
    public String metricsScope() {
        return "tidesdb";
    }

    /**
     * Create a new TidesDB store supplier with the given name
     */
    public static TidesDBStoreSupplier create(String name) {
        return new TidesDBStoreSupplier(name);
    }

    /**
     * Create a new TidesDB store supplier with the given name and config
     */
    public static TidesDBStoreSupplier create(String name, TidesDBStoreConfig config) {
        return new TidesDBStoreSupplier(name, config);
    }
}
