package com.tidesdb.kafka.store;

import com.tidesdb.ColumnFamilyConfig;
import com.tidesdb.CompressionAlgorithm;
import com.tidesdb.Config;
import com.tidesdb.IsolationLevel;
import com.tidesdb.LogLevel;
import com.tidesdb.SyncMode;

/**
 * Configuration for TidesDB-backed Kafka Streams state stores.
 *
 * <p>A store configuration is three things:
 * <ul>
 *   <li>a TidesDB database {@link Config} (threads, block cache, memtable,
 *       write-ahead log, value log), applied when the store opens its database;</li>
 *   <li>a {@link ColumnFamilyConfig} (compression, bloom filter, level shape,
 *       isolation), applied when the store creates its column family;</li>
 *   <li>store behaviour: the column family name, a default TTL, and an
 *       optional open-file limit raise.</li>
 * </ul>
 *
 * <p>Both TidesDB configurations start from the native library's own defaults
 * ({@code tidesdb_default_config()} and
 * {@code tidesdb_default_column_family_config()}). The one store-specific
 * default is LZ4 compression on the column family. The common knobs have
 * setters on {@link Builder}; anything else can be set by passing a full
 * {@link Config} or {@link ColumnFamilyConfig} through
 * {@link Builder#databaseConfig(Config)} and
 * {@link Builder#columnFamilyConfig(ColumnFamilyConfig)}.
 *
 * <p>The database path is never taken from the {@link Config}; the store
 * always opens its database under the Kafka Streams task state directory.
 *
 * <p>Instances are immutable.
 */
public class TidesDBStoreConfig {

    /** The column family the store creates when none is named. */
    public static final String DEFAULT_COLUMN_FAMILY_NAME = "default";

    /**
     * The placeholder path the database configuration carries until the store
     * opens. The real path is the task state directory joined with the store
     * name.
     */
    static final String UNSET_PATH = "<state-dir>";

    private final Config databaseConfig;
    private final ColumnFamilyConfig columnFamilyConfig;
    private final String columnFamilyName;
    private final long defaultTtlSeconds;
    private final long raiseOpenFileLimit;

    private TidesDBStoreConfig(Builder builder) {
        this.databaseConfig = builder.db.build();
        this.columnFamilyConfig = builder.cf.build();
        this.columnFamilyName = builder.columnFamilyName;
        this.defaultTtlSeconds = builder.defaultTtlSeconds;
        this.raiseOpenFileLimit = builder.raiseOpenFileLimit;
    }

    /**
     * Returns a builder starting from the native defaults.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a configuration carrying the native defaults plus LZ4
     * compression.
     *
     * @return the default configuration
     */
    public static TidesDBStoreConfig defaultConfig() {
        return builder().build();
    }

    /**
     * Returns the database configuration with {@code dbPath} applied, ready to
     * pass to {@link com.tidesdb.TidesDB#open(Config)}.
     *
     * @param dbPath the directory the database lives in
     * @return the database configuration for that path
     */
    public Config toDatabaseConfig(String dbPath) {
        return databaseConfig.toBuilder().dbPath(dbPath).build();
    }

    /**
     * Returns the database configuration. Its path is a placeholder; the store
     * substitutes the task state directory when it opens.
     *
     * @return the database configuration
     */
    public Config getDatabaseConfig() {
        return databaseConfig;
    }

    /**
     * Returns the configuration the store's column family is created with.
     *
     * @return the column family configuration
     */
    public ColumnFamilyConfig getColumnFamilyConfig() {
        return columnFamilyConfig;
    }

    /**
     * Returns the name of the column family the store reads and writes.
     *
     * @return the column family name
     */
    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    /**
     * Returns the TTL applied to every {@code put}, in seconds from the write,
     * or a value {@code <= 0} for no expiry.
     *
     * @return the default TTL in seconds
     */
    public long getDefaultTtlSeconds() {
        return defaultTtlSeconds;
    }

    /**
     * Returns the open-file ceiling the store asks for before opening, or
     * {@code 0} to leave the process limit alone.
     *
     * @return the requested open-file ceiling
     */
    public long getRaiseOpenFileLimit() {
        return raiseOpenFileLimit;
    }

    /**
     * Returns the isolation level the store's own transactions run at, which is
     * the column family's default isolation level.
     *
     * @return the isolation level
     */
    public IsolationLevel getDefaultIsolationLevel() {
        return columnFamilyConfig.getDefaultIsolationLevel();
    }

    @Override
    public String toString() {
        return "TidesDBStoreConfig{" +
            "columnFamilyName='" + columnFamilyName + '\'' +
            ", defaultTtlSeconds=" + defaultTtlSeconds +
            ", raiseOpenFileLimit=" + raiseOpenFileLimit +
            ", databaseConfig=" + databaseConfig +
            ", columnFamilyConfig=" + columnFamilyConfig +
            '}';
    }

    /**
     * Builder for {@link TidesDBStoreConfig}. Every setter overrides one field
     * of the native defaults; anything not set keeps the engine's value.
     */
    public static class Builder {

        private Config.Builder db;
        private ColumnFamilyConfig.Builder cf;
        private String columnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
        private long defaultTtlSeconds = 0;
        private long raiseOpenFileLimit = 0;

        Builder() {
            this.db = Config.defaultConfig(UNSET_PATH).toBuilder();
            this.cf = ColumnFamilyConfig.builder().compression(CompressionAlgorithm.LZ4);
        }

        /* ===== whole configurations ===== */

        /**
         * Replaces the database configuration with {@code config}. Its path is
         * ignored. Setters called after this one adjust the given configuration.
         *
         * @param config the database configuration; must not be {@code null}
         * @return this builder
         */
        public Builder databaseConfig(Config config) {
            if (config == null) {
                throw new IllegalArgumentException("Database config cannot be null");
            }
            this.db = config.toBuilder().dbPath(UNSET_PATH);
            return this;
        }

        /**
         * Replaces the column family configuration with {@code config}. Its name
         * is ignored; use {@link #columnFamilyName(String)}. Setters called after
         * this one adjust the given configuration.
         *
         * @param config the column family configuration; must not be {@code null}
         * @return this builder
         */
        public Builder columnFamilyConfig(ColumnFamilyConfig config) {
            if (config == null) {
                throw new IllegalArgumentException("Column family config cannot be null");
            }
            this.cf = config.toBuilder();
            return this;
        }

        /* ===== database settings ===== */

        /** @see Config.Builder#numFlushThreads(int) */
        public Builder numFlushThreads(int n) { db.numFlushThreads(n); return this; }

        /** @see Config.Builder#numCompactionThreads(int) */
        public Builder numCompactionThreads(int n) { db.numCompactionThreads(n); return this; }

        /** @see Config.Builder#logLevel(LogLevel) */
        public Builder logLevel(LogLevel level) { db.logLevel(level); return this; }

        /** @see Config.Builder#logToFile(boolean) */
        public Builder logToFile(boolean enable) { db.logToFile(enable); return this; }

        /** @see Config.Builder#logTruncationAt(long) */
        public Builder logTruncationAt(long bytes) { db.logTruncationAt(bytes); return this; }

        /** @see Config.Builder#blockCacheSize(long) */
        public Builder blockCacheSize(long bytes) { db.blockCacheSize(bytes); return this; }

        /** @see Config.Builder#maxOpenSSTables(long) */
        public Builder maxOpenSSTables(long n) { db.maxOpenSSTables(n); return this; }

        /**
         * Sets the size the shared memtable grows to before it is flushed.
         * Replaces the per-column-family {@code writeBufferSize} of 0.4.x, since
         * TidesDB 10 has one memtable for the whole database.
         *
         * @see Config.Builder#memtableWriteBufferSize(long)
         */
        public Builder memtableWriteBufferSize(long bytes) { db.memtableWriteBufferSize(bytes); return this; }

        /** @see Config.Builder#memtableSkipListMaxLevel(int) */
        public Builder memtableSkipListMaxLevel(int n) { db.memtableSkipListMaxLevel(n); return this; }

        /** @see Config.Builder#memtableSkipListProbability(float) */
        public Builder memtableSkipListProbability(float p) { db.memtableSkipListProbability(p); return this; }

        /**
         * Sets how the write-ahead log is synced. Kafka Streams can rebuild a
         * store from its changelog, so the default {@link SyncMode#SYNC_NONE} is
         * usually right; {@link TidesDBStore#flush()} forces a sync at every
         * Kafka commit regardless.
         *
         * @see Config.Builder#memtableSyncMode(SyncMode)
         */
        public Builder memtableSyncMode(SyncMode mode) { db.memtableSyncMode(mode); return this; }

        /** @see Config.Builder#memtableSyncIntervalUs(long) */
        public Builder memtableSyncIntervalUs(long us) { db.memtableSyncIntervalUs(us); return this; }

        /** @see Config.Builder#memtableL0QueueStallThreshold(int) */
        public Builder memtableL0QueueStallThreshold(int n) { db.memtableL0QueueStallThreshold(n); return this; }

        /** @see Config.Builder#memtableIdleFlushSeconds(int) */
        public Builder memtableIdleFlushSeconds(int seconds) { db.memtableIdleFlushSeconds(seconds); return this; }

        /** @see Config.Builder#valueSeparationThreshold(long) */
        public Builder valueSeparationThreshold(long bytes) { db.valueSeparationThreshold(bytes); return this; }

        /** @see Config.Builder#vlogSegmentSize(long) */
        public Builder vlogSegmentSize(long bytes) { db.vlogSegmentSize(bytes); return this; }

        /** @see Config.Builder#txnTimeoutSeconds(long) */
        public Builder txnTimeoutSeconds(long seconds) { db.txnTimeoutSeconds(seconds); return this; }

        /* ===== column family settings ===== */

        /**
         * Sets a single-codec encoding pipeline. The store default is
         * {@link CompressionAlgorithm#LZ4}; {@link CompressionAlgorithm#NONE}
         * stores data verbatim.
         *
         * @see ColumnFamilyConfig.Builder#compression(CompressionAlgorithm)
         */
        public Builder compression(CompressionAlgorithm algorithm) { cf.compression(algorithm); return this; }

        /** @see ColumnFamilyConfig.Builder#encodingPipeline(CompressionAlgorithm...) */
        public Builder encodingPipeline(CompressionAlgorithm... algorithms) { cf.encodingPipeline(algorithms); return this; }

        /** @see ColumnFamilyConfig.Builder#enableBloomFilter(boolean) */
        public Builder enableBloomFilter(boolean enable) { cf.enableBloomFilter(enable); return this; }

        /** @see ColumnFamilyConfig.Builder#bloomFpr(double) */
        public Builder bloomFpr(double fpr) { cf.bloomFpr(fpr); return this; }

        /**
         * Sets the isolation level the store's transactions run at.
         *
         * @see ColumnFamilyConfig.Builder#defaultIsolationLevel(IsolationLevel)
         */
        public Builder defaultIsolationLevel(IsolationLevel level) { cf.defaultIsolationLevel(level); return this; }

        /** @see ColumnFamilyConfig.Builder#levelSizeRatio(long) */
        public Builder levelSizeRatio(long ratio) { cf.levelSizeRatio(ratio); return this; }

        /** @see ColumnFamilyConfig.Builder#minLevels(int) */
        public Builder minLevels(int n) { cf.minLevels(n); return this; }

        /** @see ColumnFamilyConfig.Builder#dividingLevelOffset(int) */
        public Builder dividingLevelOffset(int n) { cf.dividingLevelOffset(n); return this; }

        /** @see ColumnFamilyConfig.Builder#l1FileCountTrigger(int) */
        public Builder l1FileCountTrigger(int n) { cf.l1FileCountTrigger(n); return this; }

        /** @see ColumnFamilyConfig.Builder#keepValuesInline(boolean) */
        public Builder keepValuesInline(boolean enable) { cf.keepValuesInline(enable); return this; }

        /** @see ColumnFamilyConfig.Builder#btreeKlogBlockSize(long) */
        public Builder btreeKlogBlockSize(long bytes) { cf.btreeKlogBlockSize(bytes); return this; }

        /** @see ColumnFamilyConfig.Builder#tombstoneDensityTrigger(double) */
        public Builder tombstoneDensityTrigger(double trigger) { cf.tombstoneDensityTrigger(trigger); return this; }

        /** @see ColumnFamilyConfig.Builder#tombstoneDensityMinEntries(long) */
        public Builder tombstoneDensityMinEntries(long entries) { cf.tombstoneDensityMinEntries(entries); return this; }

        /* ===== store behaviour ===== */

        /**
         * Sets the column family the store reads and writes. It is created on
         * first open with the configured column family settings, and reused as
         * it is on later opens.
         *
         * @param name the column family name; must not be {@code null} or empty
         * @return this builder
         */
        public Builder columnFamilyName(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Column family name cannot be null or empty");
            }
            this.columnFamilyName = name;
            return this;
        }

        /**
         * Sets a TTL applied to every {@code put}, {@code putIfAbsent} and
         * {@code putAll}, in seconds from the write. Zero or negative (the
         * default) never expires.
         *
         * @param seconds how long each entry lives
         * @return this builder
         */
        public Builder defaultTtlSeconds(long seconds) {
            this.defaultTtlSeconds = seconds;
            return this;
        }

        /**
         * Raises this process's open-file ceiling toward {@code desired}
         * descriptors before the database opens, so the engine can keep more
         * SSTables open. The engine sizes its open-table cap to the ceiling at
         * open time, which is why this is applied in {@code init}. Process-wide.
         * {@code 0} (the default) leaves the limit untouched.
         *
         * @param desired the target descriptor count
         * @return this builder
         */
        public Builder raiseOpenFileLimit(long desired) {
            this.raiseOpenFileLimit = desired;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return a new immutable configuration
         */
        public TidesDBStoreConfig build() {
            return new TidesDBStoreConfig(this);
        }
    }
}
