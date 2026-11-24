package com.tidesdb.kafka.store;

import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Builds a bare {@link TidesDBStore}, a {@code Bytes}/{@code byte[]} store
 * with no Kafka Streams wrapping.
 *
 * <p><strong>This builder does not add change-logging or caching.</strong>
 * {@link #withLoggingEnabled(Map)} and {@link #withCachingEnabled()} only record
 * the request, so a changelog topic would be created that nothing writes to.
 * For a store Kafka Streams can restore after a failover, let Kafka build the
 * wrappers around {@link TidesDBStoreSupplier}:
 * <pre>{@code
 * StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
 *     Stores.keyValueStoreBuilder(
 *             new TidesDBStoreSupplier("my-store", TidesDBStoreConfig.builder()
 *                 .compression(CompressionAlgorithm.ZSTD)
 *                 .build()),
 *             Serdes.String(), Serdes.Long())
 *         .withCachingEnabled()
 *         .withLoggingEnabled(Collections.emptyMap());
 * }</pre>
 * or pass the supplier to {@code Materialized.as(...)} in the DSL.
 */
public class TidesDBStoreBuilder implements StoreBuilder<TidesDBStore> {
    private static final Logger log = LoggerFactory.getLogger(TidesDBStoreBuilder.class);

    private final String name;
    private TidesDBStoreConfig storeConfig;
    private boolean enableCaching = false;
    private boolean enableLogging = false;
    private Map<String, String> logConfig;

    public TidesDBStoreBuilder(String name) {
        this(name, TidesDBStoreConfig.defaultConfig());
    }

    public TidesDBStoreBuilder(String name, TidesDBStoreConfig config) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Store name cannot be null or empty");
        }
        this.name = name;
        this.storeConfig = config != null ? config : TidesDBStoreConfig.defaultConfig();
    }

    /**
     * Set the TidesDB store configuration.
     */
    public TidesDBStoreBuilder withConfig(TidesDBStoreConfig config) {
        this.storeConfig = config != null ? config : TidesDBStoreConfig.defaultConfig();
        return this;
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
        if (enableLogging || enableCaching) {
            log.warn("TidesDBStoreBuilder '{}' builds a bare store: change-logging and caching are "
                + "not applied. Use Stores.keyValueStoreBuilder(new TidesDBStoreSupplier(...), "
                + "keySerde, valueSerde) or Materialized.as(supplier) instead.", name);
        }
        log.debug("Building TidesDB store '{}' with {}", name, storeConfig);
        return new TidesDBStore(name, storeConfig);
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

    /**
     * Create a new TidesDB store builder with the given name and config
     */
    public static TidesDBStoreBuilder create(String name, TidesDBStoreConfig config) {
        return new TidesDBStoreBuilder(name, config);
    }
}
