package com.tidesdb.kafka.store;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Builder for creating TidesDB-backed state stores in Kafka Streams.
 * 
 * Usage:
 * <pre>
 * StoreBuilder<KeyValueStore<String, Long>> storeBuilder = 
 *     new TidesDBStoreBuilder("my-store")
 *         .withCachingEnabled()
 *         .withLoggingEnabled(Collections.emptyMap());
 * </pre>
 */
public class TidesDBStoreBuilder implements StoreBuilder<TidesDBStore> {
    private static final Logger log = LoggerFactory.getLogger(TidesDBStoreBuilder.class);

    private final String name;
    private boolean enableCaching = false;
    private boolean enableLogging = false;
    private Map<String, String> logConfig;

    public TidesDBStoreBuilder(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be null or empty");
        }
        this.name = name;
    }

    @Override
    public StoreBuilder<TidesDBStore> withCachingEnabled() {
        this.enableCaching = true;
        return this;
    }

    @Override
    public StoreBuilder<TidesDBStore> withCachingDisabled() {
        this.enableCaching = false;
        return this;
    }

    @Override
    public StoreBuilder<TidesDBStore> withLoggingEnabled(Map<String, String> config) {
        this.enableLogging = true;
        this.logConfig = config;
        return this;
    }

    @Override
    public StoreBuilder<TidesDBStore> withLoggingDisabled() {
        this.enableLogging = false;
        this.logConfig = null;
        return this;
    }

    @Override
    public TidesDBStore build() {
        log.info("Building TidesDB store '{}' (caching={}, logging={})", 
                 name, enableCaching, enableLogging);
        return new TidesDBStore(name);
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return enableLogging;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Create a new TidesDB store builder with the given name
     */
    public static TidesDBStoreBuilder create(String name) {
        return new TidesDBStoreBuilder(name);
    }
}
