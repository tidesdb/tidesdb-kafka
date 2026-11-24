package com.tidesdb.kafka.store;

import com.tidesdb.CacheStats;
import com.tidesdb.CfStats;
import com.tidesdb.ColumnFamily;
import com.tidesdb.ColumnFamilyConfig;
import com.tidesdb.CommitHook;
import com.tidesdb.DbStats;
import com.tidesdb.IoStats;
import com.tidesdb.RangeStats;
import com.tidesdb.StallStats;
import com.tidesdb.TidesDB;
import com.tidesdb.TidesDBException;
import com.tidesdb.Transaction;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * TidesDB-backed implementation of a Kafka Streams {@link KeyValueStore}, a
 * drop-in replacement for the RocksDB store.
 *
 * <p>The store opens one TidesDB database under the task state directory
 * ({@code <state.dir>/<application.id>/<task>/<store name>}) and keeps its data
 * in one column family. It registers itself with Kafka Streams for changelog
 * restoration. Every operation runs in its own TidesDB transaction at the column
 * family's default isolation level, and is retried while the engine reports
 * transient contention ({@link TidesDBException#ERR_LOCKED}).
 *
 * <p>Errors surface as {@link ProcessorStateException}, and calls on a closed
 * store as {@link InvalidStateStoreException}, as with Kafka's own stores.
 */
public class TidesDBStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger log = LoggerFactory.getLogger(TidesDBStore.class);

    private final String name;
    private final TidesDBStoreConfig storeConfig;
    private final Set<TidesDBRangeIterator> openIterators =
        Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    private TidesDB db;
    private ColumnFamily columnFamily;
    private volatile boolean open = false;

    /** Where the store has read up to in its input, for interactive-query consistency. */
    private final Position position = Position.emptyPosition();

    /** Set by the {@link StateStoreContext} init; supplies record metadata for {@link #position}. */
    private StateStoreContext context;

    /** One transaction kept for single-key reads and writes, the hot path. */
    private Transaction reusableTxn;

    public TidesDBStore(String name) {
        this(name, TidesDBStoreConfig.defaultConfig());
    }

    public TidesDBStore(String name, TidesDBStoreConfig config) {
        this.name = name;
        this.storeConfig = config != null ? config : TidesDBStoreConfig.defaultConfig();
    }

    /* ===== lifecycle ===== */

    @Override
    public String name() {
        return name;
    }

    /**
     * @deprecated Kafka Streams calls
     * {@link #init(StateStoreContext, StateStore)}; this overload is only here
     * because the interface still declares it.
     */
    @Deprecated
    @Override
    public void init(org.apache.kafka.streams.processor.ProcessorContext context, StateStore root) {
        openDatabase(context.stateDir());
        context.register(root, (BatchingStateRestoreCallback) this::restoreAll);
        open = true;
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        openDatabase(context.stateDir());
        context.register(root, (BatchingStateRestoreCallback) this::restoreAll);
        open = true;
    }

    private void openDatabase(File stateDir) {
        String dbPath = new File(stateDir, name).getAbsolutePath();
        log.info("Opening TidesDB store '{}' at {}", name, dbPath);

        if (storeConfig.getRaiseOpenFileLimit() > 0) {
            // the engine sizes its open-table cap to the ceiling at open, so this precedes open
            long ceiling = TidesDB.raiseOpenFileLimit(storeConfig.getRaiseOpenFileLimit());
            log.info("Open-file ceiling for store '{}' is now {}", name, ceiling);
        }

        try {
            this.db = TidesDB.open(storeConfig.toDatabaseConfig(dbPath));
        } catch (TidesDBException e) {
            if (e.getErrorCode() == TidesDBException.ERR_ROOT_EXECUTION) {
                throw new ProcessorStateException("TidesDB refuses to open store '" + name
                    + "' as root, since the files would be owned by root. Run Kafka Streams as an "
                    + "unprivileged user (e.g. set USER in the container image); if " + dbPath
                    + " is already owned by root, chown it to that user first.", e);
            }
            throw new ProcessorStateException("Failed to open TidesDB store '" + name + "' at "
                + dbPath, e);
        }

        try {
            String cfName = storeConfig.getColumnFamilyName();
            try {
                this.columnFamily = db.getColumnFamily(cfName);
            } catch (TidesDBException e) {
                if (e.getErrorCode() != TidesDBException.ERR_NOT_FOUND) {
                    throw e;
                }
                db.createColumnFamily(cfName, storeConfig.getColumnFamilyConfig());
                this.columnFamily = db.getColumnFamily(cfName);
            }
            this.reusableTxn = db.beginTransaction(storeConfig.getDefaultIsolationLevel());
        } catch (TidesDBException e) {
            db.close();
            db = null;
            throw new ProcessorStateException("Failed to open column family '"
                + storeConfig.getColumnFamilyName() + "' in TidesDB store '" + name + "'", e);
        }

        log.info("TidesDB store '{}' open (column family '{}')", name,
                 storeConfig.getColumnFamilyName());
    }

    /**
     * Makes everything written so far durable by syncing the write-ahead log.
     * Kafka Streams calls this before it checkpoints changelog offsets.
     *
     * <p>This deliberately does not flush the memtable to an SSTable: the
     * memtable is shared by the whole database in TidesDB 10, and flushing it at
     * every commit interval would litter level 1 with tiny tables. The synced
     * log is what recovery replays.
     */
    @Override
    public void flush() {
        validateStoreOpen();
        try {
            Retry.run(() -> {
                db.syncWal();
                return null;
            });
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to sync TidesDB store '" + name + "'", e);
        }
    }

    @Override
    public void close() {
        if (!open) {
            return;
        }
        open = false;

        // close what callers left open: the binding needs every transaction freed before the db
        TidesDBRangeIterator[] leftOpen;
        synchronized (openIterators) {
            leftOpen = openIterators.toArray(new TidesDBRangeIterator[0]);
        }
        if (leftOpen.length > 0) {
            log.warn("Closing {} iterator(s) left open on TidesDB store '{}'", leftOpen.length, name);
            for (TidesDBRangeIterator it : leftOpen) {
                it.close();
            }
        }

        synchronized (this) {
            if (reusableTxn != null) {
                reusableTxn.free();
                reusableTxn = null;
            }
        }
        if (db != null) {
            db.close();
            db = null;
        }
        columnFamily = null;
        log.info("TidesDB store '{}' closed", name);
    }

    @Override
    public boolean persistent() {
        return true;
    }

    /**
     * Returns the input offsets this store has applied writes from, which Kafka
     * Streams' wrapping stores and interactive queries read. Tracked in memory
     * for the life of the open store.
     */
    @Override
    public Position getPosition() {
        return position;
    }

    /** Records the offset of the record being processed, if there is one. */
    private void updatePosition() {
        if (context == null) {
            return;
        }
        context.recordMetadata().ifPresent((RecordMetadata meta) -> {
            if (meta.topic() != null) {
                position.withComponent(meta.topic(), meta.partition(), meta.offset());
            }
        });
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /* ===== writes ===== */

    @Override
    public void put(Bytes key, byte[] value) {
        validateStoreOpen();
        requireKey(key);
        if (value == null) {
            delete(key);
            return;
        }
        writeOne(key, value, storeConfig.getDefaultTtlSeconds(), "put");
    }

    /**
     * Puts a key-value pair that expires {@code ttlSeconds} after the write.
     * Zero or negative never expires. Overrides the configured default TTL.
     *
     * @param key the key; must not be {@code null}
     * @param value the value; must not be {@code null}
     * @param ttlSeconds how long the entry lives, in seconds from now
     */
    public void putWithTtl(Bytes key, byte[] value, long ttlSeconds) {
        validateStoreOpen();
        requireKey(key);
        if (value == null) {
            throw new NullPointerException("Value cannot be null for TTL put");
        }
        writeOne(key, value, ttlSeconds, "put with TTL");
    }

    private void writeOne(Bytes key, byte[] value, long ttlSeconds, String what) {
        try {
            withReusableTxn(txn -> {
                txn.put(columnFamily, key.get(), value, ttlSeconds);
                txn.commit();
                return null;
            });
            updatePosition();
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to " + what + " in TidesDB store '" + name + "'", e);
        }
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        validateStoreOpen();
        requireKey(key);
        try {
            byte[] existing = inTxn(txn -> {
                byte[] current = txn.get(columnFamily, key.get());
                if (current != null) {
                    return current;
                }
                if (value != null) {
                    txn.put(columnFamily, key.get(), value, storeConfig.getDefaultTtlSeconds());
                    txn.commit();
                }
                return null;
            });
            updatePosition();
            return existing;
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to putIfAbsent in TidesDB store '" + name + "'", e);
        }
    }

    /**
     * Writes every entry in one transaction, so the batch lands atomically. A
     * {@code null} value deletes its key.
     */
    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        validateStoreOpen();
        for (KeyValue<Bytes, byte[]> entry : entries) {
            requireKey(entry.key);
        }
        try {
            inTxn(txn -> {
                long ttl = storeConfig.getDefaultTtlSeconds();
                for (KeyValue<Bytes, byte[]> entry : entries) {
                    if (entry.value == null) {
                        txn.delete(columnFamily, entry.key.get());
                    } else {
                        txn.put(columnFamily, entry.key.get(), entry.value, ttl);
                    }
                }
                txn.commit();
                return null;
            });
            updatePosition();
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to putAll in TidesDB store '" + name + "'", e);
        }
    }

    @Override
    public byte[] delete(Bytes key) {
        validateStoreOpen();
        requireKey(key);
        try {
            byte[] oldValue = inTxn(txn -> {
                byte[] current = txn.get(columnFamily, key.get());
                if (current != null) {
                    txn.delete(columnFamily, key.get());
                    txn.commit();
                }
                return current;
            });
            updatePosition();
            return oldValue;
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to delete from TidesDB store '" + name + "'", e);
        }
    }

    /**
     * Writes a single-delete tombstone for a key. Reads behave as after
     * {@link #delete(Bytes)}, but compaction can drop the put and the tombstone
     * together as soon as both meet in one merge. Only correct when the key was
     * put at most once since its last single-delete; when in doubt use
     * {@link #delete(Bytes)}.
     *
     * @param key the key; must not be {@code null}
     */
    public void singleDelete(Bytes key) {
        validateStoreOpen();
        requireKey(key);
        try {
            inTxn(txn -> {
                txn.singleDelete(columnFamily, key.get());
                txn.commit();
                return null;
            });
            updatePosition();
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to single-delete in TidesDB store '" + name + "'", e);
        }
    }

    /* ===== reads ===== */

    @Override
    public byte[] get(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            return null;
        }
        try {
            return withReusableTxn(txn -> txn.get(columnFamily, key.get()));
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to read from TidesDB store '" + name + "'", e);
        }
    }

    /**
     * Reads a key's absolute expiry.
     *
     * @param key the key
     * @return the expiry in unix seconds, {@code -1} if the key never expires,
     *         or {@code null} if it is missing, deleted or already expired
     */
    public Long getTtl(Bytes key) {
        validateStoreOpen();
        if (key == null) {
            return null;
        }
        try {
            return withReusableTxn(txn -> txn.getTtl(columnFamily, key.get()));
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to read TTL from TidesDB store '" + name + "'", e);
        }
    }

    /**
     * Iterates {@code [from, to]}, both inclusive, in ascending byte order. A
     * {@code null} bound is open. An inverted range ({@code from > to}) is empty.
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        return openIterator(bytesOf(from), bytesOf(to), false, null);
    }

    /**
     * Iterates {@code [from, to]}, both inclusive, in descending byte order.
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        return openIterator(bytesOf(from), bytesOf(to), true, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return openIterator(null, null, false, null);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return openIterator(null, null, true, null);
    }

    /**
     * Iterates the keys that start with the serialized {@code prefix}, in
     * ascending byte order.
     */
    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(
            P prefix, PS prefixKeySerializer) {
        validateStoreOpen();
        byte[] serialized = prefixKeySerializer.serialize(null, prefix);
        if (serialized == null || serialized.length == 0) {
            return all();
        }
        return openIterator(null, null, false, serialized);
    }

    private KeyValueIterator<Bytes, byte[]> openIterator(byte[] from, byte[] to, boolean reverse,
                                                         byte[] prefix) {
        validateStoreOpen();
        TidesDBRangeIterator it = new TidesDBRangeIterator(db, columnFamily,
            storeConfig.getDefaultIsolationLevel(), from, to, reverse, prefix, openIterators::remove);
        openIterators.add(it);
        return it;
    }

    /**
     * Returns the approximate number of live keys: those in SSTables plus those
     * still in the memtable. Overwrites and deletes not yet compacted away are
     * counted, as Kafka allows for an approximation.
     */
    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        try {
            CfStats stats = columnFamily.getStats();
            long total = stats.getTotalKeys() + stats.getUnflushedKeyCount();
            return total < 0 ? Long.MAX_VALUE : total;
        } catch (TidesDBException e) {
            log.warn("Failed to read entry count of TidesDB store '{}'", name, e);
            return 0L;
        }
    }

    /* ===== restoration ===== */

    /**
     * Applies a batch of changelog records in one transaction. A {@code null}
     * value is a delete. The configured default TTL is applied again from the
     * moment of restoration.
     */
    private void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            inTxn(txn -> {
                long ttl = storeConfig.getDefaultTtlSeconds();
                for (KeyValue<byte[], byte[]> record : records) {
                    if (record.value == null) {
                        txn.delete(columnFamily, record.key);
                    } else {
                        txn.put(columnFamily, record.key, record.value, ttl);
                    }
                }
                txn.commit();
                return null;
            });
        } catch (TidesDBException e) {
            throw new ProcessorStateException("Failed to restore TidesDB store '" + name + "'", e);
        }
    }

    /* ===== transactions ===== */

    /** Runs {@code op} in a fresh transaction, retrying the whole of it on contention. */
    private <T> T inTxn(TxnOp<T> op) throws TidesDBException {
        return Retry.run(() -> {
            try (Transaction txn = db.beginTransaction(storeConfig.getDefaultIsolationLevel())) {
                return op.callIn(txn);
            }
        });
    }

    /**
     * Runs {@code op} on the reusable transaction, or on a fresh one when another
     * thread holds it, and resets it for the next caller afterwards.
     */
    private <T> T withReusableTxn(TxnOp<T> op) throws TidesDBException {
        return Retry.run(() -> {
            Transaction txn = acquireReusableTxn();
            try {
                return op.callIn(txn);
            } finally {
                releaseReusableTxn(txn);
            }
        });
    }

    @FunctionalInterface
    private interface TxnOp<T> {
        T callIn(Transaction txn) throws TidesDBException;
    }

    private synchronized Transaction acquireReusableTxn() throws TidesDBException {
        if (reusableTxn != null) {
            Transaction txn = reusableTxn;
            reusableTxn = null;
            return txn;
        }
        return db.beginTransaction(storeConfig.getDefaultIsolationLevel());
    }

    /** Resets {@code txn} and keeps it, keeping its buffers; frees it if one is already kept. */
    private synchronized void releaseReusableTxn(Transaction txn) {
        if (!open || reusableTxn != null) {
            txn.free();
            return;
        }
        try {
            txn.reset(storeConfig.getDefaultIsolationLevel());
            reusableTxn = txn;
        } catch (TidesDBException e) {
            txn.free();
        }
    }

    /* ===== helpers ===== */

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    private static void requireKey(Bytes key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
    }

    private static byte[] bytesOf(Bytes b) {
        return b == null ? null : b.get();
    }

    /* ===== TidesDB extensions ===== */

    /**
     * Returns column family statistics.
     *
     * @return the statistics of the store's column family
     */
    public CfStats getStats() throws TidesDBException {
        validateStoreOpen();
        return columnFamily.getStats();
    }

    /** Returns database-wide statistics. */
    public DbStats getDbStats() throws TidesDBException {
        validateStoreOpen();
        return db.getDbStats();
    }

    /** Returns block cache statistics. */
    public CacheStats getCacheStats() throws TidesDBException {
        validateStoreOpen();
        return db.getCacheStats();
    }

    /** Returns write-stall statistics. */
    public StallStats getStallStats() throws TidesDBException {
        validateStoreOpen();
        return db.getStallStats();
    }

    /** Returns device I/O statistics. */
    public IoStats getIoStats() throws TidesDBException {
        validateStoreOpen();
        return db.getIoStats();
    }

    /**
     * Reports what a scan of {@code [fromKey, toKey]} would touch: the SSTables
     * that overlap it, their bytes, and an estimated key count. Replaces
     * {@code rangeCost} of 0.4.x.
     */
    public RangeStats rangeStats(byte[] fromKey, byte[] toKey) throws TidesDBException {
        validateStoreOpen();
        return columnFamily.rangeStats(fromKey, toKey);
    }

    /** Estimates the number of distinct keys in the store's column family. */
    public long estimateCardinality() throws TidesDBException {
        validateStoreOpen();
        return columnFamily.estimateCardinality();
    }

    /** Queues a compaction of the store's column family. */
    public void compact() throws TidesDBException {
        validateStoreOpen();
        columnFamily.compact();
    }

    /**
     * Compacts only the SSTables overlapping {@code [startKey, endKey)}. A
     * {@code null} or empty endpoint is unbounded on that side.
     */
    public void compactRange(byte[] startKey, byte[] endKey) throws TidesDBException {
        validateStoreOpen();
        columnFamily.compactRange(startKey, endKey);
    }

    /**
     * Flushes the shared memtable to an SSTable. Kafka's {@link #flush()} does not
     * do this; call it before a maintenance step that wants everything on disk
     * as tables.
     */
    public void flushMemtable() throws TidesDBException {
        validateStoreOpen();
        db.flushMemtable();
    }

    /** Forces an fsync of the write-ahead log. */
    public void syncWal() throws TidesDBException {
        validateStoreOpen();
        db.syncWal();
    }

    /**
     * Writes a directly-openable copy of the store's database into
     * {@code backupPath}, which must not exist or must be empty. Writers wait
     * only while the backup is admitted, not while files are copied.
     */
    public void backup(String backupPath) throws TidesDBException {
        validateStoreOpen();
        db.backup(backupPath);
    }

    /**
     * Establishes a durability barrier in the live database: flushes the
     * memtable and forces the value log, write-ahead log and manifest to disk.
     * Unlike 0.4.x this takes no directory; use {@link #backup(String)} for a
     * copy.
     */
    public void checkpoint() throws TidesDBException {
        validateStoreOpen();
        db.checkpoint();
    }

    /** Registers a commit hook on the store's column family for change data capture. */
    public void setCommitHook(CommitHook hook) throws TidesDBException {
        validateStoreOpen();
        columnFamily.setCommitHook(hook);
    }

    /** Removes the commit hook. */
    public void clearCommitHook() throws TidesDBException {
        validateStoreOpen();
        columnFamily.clearCommitHook();
    }

    /**
     * Applies a new configuration to the store's column family without a
     * restart.
     *
     * @param newConfig the configuration to apply; its name is ignored
     * @param persistToDisk whether to record it in the manifest so it survives
     *        a reopen
     */
    public void updateRuntimeConfig(ColumnFamilyConfig newConfig, boolean persistToDisk)
            throws TidesDBException {
        validateStoreOpen();
        columnFamily.updateRuntimeConfig(newConfig, persistToDisk);
    }

    /**
     * Raises this process's open-file ceiling toward {@code desired}
     * descriptors. Must run before a store opens to take effect; prefer
     * {@link TidesDBStoreConfig.Builder#raiseOpenFileLimit(long)}.
     *
     * @param desired target descriptor count; {@code <= 0} only reports the
     *        current ceiling
     * @return the ceiling in effect afterwards
     */
    public static long raiseOpenFileLimit(long desired) {
        return TidesDB.raiseOpenFileLimit(desired);
    }

    /** Reports whether the shared memtable is being flushed. */
    public boolean isFlushing() {
        validateStoreOpen();
        return db.isFlushing();
    }

    /** Reports whether the store's column family is being compacted. */
    public boolean isCompacting() {
        validateStoreOpen();
        return columnFamily.isCompacting();
    }

    /* ===== column family management ===== */

    /**
     * Creates another column family in the store's database. The store's own
     * column family is created by {@code init}.
     */
    public void createColumnFamily(String cfName, ColumnFamilyConfig cfConfig) throws TidesDBException {
        validateStoreOpen();
        db.createColumnFamily(cfName, cfConfig);
    }

    /** Returns a column family handle by name. */
    public ColumnFamily getColumnFamily(String cfName) throws TidesDBException {
        validateStoreOpen();
        return db.getColumnFamily(cfName);
    }

    /** Lists the column families in the store's database. */
    public String[] listColumnFamilies() throws TidesDBException {
        validateStoreOpen();
        return db.listColumnFamilies();
    }

    /** Renames a column family. */
    public void renameColumnFamily(String oldName, String newName) throws TidesDBException {
        validateStoreOpen();
        db.renameColumnFamily(oldName, newName);
    }

    /** Clones a column family to a new, independent one. */
    public void cloneColumnFamily(String srcName, String dstName) throws TidesDBException {
        validateStoreOpen();
        db.cloneColumnFamily(srcName, dstName);
    }

    /** Drops a column family by name. */
    public void dropColumnFamily(String cfName) throws TidesDBException {
        validateStoreOpen();
        db.dropColumnFamily(cfName);
    }

    /* ===== accessors ===== */

    /** Returns the store configuration. */
    public TidesDBStoreConfig getStoreConfig() {
        return storeConfig;
    }

    /**
     * Returns the underlying database. Close any transaction or iterator taken
     * from it before the store closes.
     */
    public TidesDB getDb() {
        validateStoreOpen();
        return db;
    }

    /** Returns the store's column family. */
    public ColumnFamily getColumnFamily() {
        validateStoreOpen();
        return columnFamily;
    }
}
