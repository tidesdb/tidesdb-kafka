package com.tidesdb.kafka.store;

import com.tidesdb.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * TidesDB-backed implementation of Kafka Streams KeyValueStore.
 * Provides a drop-in replacement for RocksDB state stores.
 */
public class TidesDBStore implements KeyValueStore<Bytes, byte[]> {
    private static final Logger log = LoggerFactory.getLogger(TidesDBStore.class);

    private final String name;
    private TidesDB db;
    private ColumnFamily columnFamily;
    private boolean open = false;

    public TidesDBStore(String name) {
        this.name = name;
    }

    @Override
    public void put(Bytes key, byte[] value) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        try {
            if (value == null) {
                // Null value means delete
                delete(key);
                return;
            }

            try (Transaction txn = db.beginTransaction()) {
                txn.put(columnFamily, key.get(), value);
                txn.commit();
            }
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to put key-value pair", e);
        }
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        try (Transaction txn = db.beginTransaction()) {
            byte[] existing = null;
            try {
                existing = txn.get(columnFamily, key.get());
            } catch (TidesDBException e) {
                // Key not found -- treat as null
                if (!e.getMessage().contains("not found")) {
                    throw e;
                }
            }
            if (existing == null) {
                txn.put(columnFamily, key.get(), value);
                txn.commit();
                return null;
            }
            return existing;
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to putIfAbsent", e);
        }
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        validateStoreOpen();
        
        try (Transaction txn = db.beginTransaction()) {
            for (KeyValue<Bytes, byte[]> entry : entries) {
                if (entry.key == null) {
                    throw new NullPointerException("Key cannot be null");
                }
                if (entry.value == null) {
                    txn.delete(columnFamily, entry.key.get());
                } else {
                    txn.put(columnFamily, entry.key.get(), entry.value);
                }
            }
            txn.commit();
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to putAll", e);
        }
    }

    @Override
    public byte[] delete(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }

        try (Transaction txn = db.beginTransaction()) {
            byte[] oldValue = txn.get(columnFamily, key.get());
            if (oldValue != null) {
                txn.delete(columnFamily, key.get());
                txn.commit();
            }
            return oldValue;
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to delete key", e);
        }
    }

    @Override
    public byte[] get(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            return null;
        }

        try (Transaction txn = db.beginTransaction()) {
            return txn.get(columnFamily, key.get());
        } catch (TidesDBException e) {
            // Key not found returns null
            if (e.getMessage().contains("not found")) {
                return null;
            }
            throw new RuntimeException("Failed to get value", e);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        validateStoreOpen();
        return new TidesDBRangeIterator(db, columnFamily, from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        validateStoreOpen();
        return new TidesDBIteratorWrapper(db, columnFamily);
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        try {
            Stats stats = columnFamily.getStats();
            return stats.getTotalKeys();
        } catch (TidesDBException e) {
            log.warn("Failed to get approximate entry count", e);
            return 0L;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(org.apache.kafka.streams.processor.ProcessorContext context,
                     org.apache.kafka.streams.processor.StateStore root) {
        // Legacy init -- delegate to new init
    }

    @Override
    public void init(org.apache.kafka.streams.processor.StateStoreContext context,
                     org.apache.kafka.streams.processor.StateStore root) {
        try {
            String stateDir = context.stateDir().getAbsolutePath();
            String dbPath = stateDir + "/" + name;

            log.info("Initializing TidesDB store '{}' at path: {}", name, dbPath);

            Config config = Config.builder(dbPath)
                .numFlushThreads(2)
                .numCompactionThreads(2)
                .logLevel(LogLevel.INFO)
                .blockCacheSize(64 * 1024 * 1024) // 64MB cache
                .maxOpenSSTables(256)
                .build();

            this.db = TidesDB.open(config);

            // Create or get default column family
            ColumnFamilyConfig cfConfig = ColumnFamilyConfig.builder()
                .writeBufferSize(128 * 1024 * 1024) // 128MB
                .compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)
                .enableBloomFilter(true)
                .bloomFPR(0.01)
                .syncMode(SyncMode.SYNC_INTERVAL)
                .syncIntervalUs(100000) // 100ms
                .build();

            try {
                this.columnFamily = db.getColumnFamily("default");
            } catch (TidesDBException e) {
                // Column family doesn't exist, create it
                db.createColumnFamily("default", cfConfig);
                this.columnFamily = db.getColumnFamily("default");
            }

            this.open = true;
            log.info("TidesDB store '{}' initialized successfully", name);

        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to initialize TidesDB store", e);
        }
    }

    @Override
    public void flush() {
        if (open) {
            try {
                columnFamily.flushMemtable();
            } catch (TidesDBException e) {
                log.warn("Failed to flush memtable", e);
            }
        }
    }

    @Override
    public void close() {
        if (open) {
            try {
                if (db != null) {
                    db.close();
                }
                open = false;
                log.info("TidesDB store '{}' closed", name);
            } catch (Exception e) {
                log.error("Error closing TidesDB store", e);
            }
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

    private void validateStoreOpen() {
        if (!open) {
            throw new IllegalStateException("Store is not open");
        }
    }

    /**
     * Get column family statistics
     */
    public Stats getStats() throws TidesDBException {
        validateStoreOpen();
        return columnFamily.getStats();
    }

    /**
     * Manually trigger compaction
     */
    public void compact() throws TidesDBException {
        validateStoreOpen();
        columnFamily.compact();
    }
}
