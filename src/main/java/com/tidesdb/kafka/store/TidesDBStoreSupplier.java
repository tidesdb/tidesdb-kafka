package com.tidesdb.kafka.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Supplier for TidesDB-backed KeyValue stores in Kafka Streams.
 * 
 * Usage with Materialized:
 * <pre>
 * builder.stream("input")
 *     .groupByKey()
 *     .count(Materialized.as(new TidesDBStoreSupplier("my-store")));
 * </pre>
 */
public class TidesDBStoreSupplier implements KeyValueBytesStoreSupplier {

    private final String name;

    public TidesDBStoreSupplier(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be null or empty");
        }
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new TidesDBStore(name);
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
}
