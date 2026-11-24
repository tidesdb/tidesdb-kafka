package com.tidesdb.kafka.store;

import com.tidesdb.ColumnFamily;
import com.tidesdb.IsolationLevel;
import com.tidesdb.TidesDB;
import com.tidesdb.TidesDBException;
import com.tidesdb.TidesDBIterator;
import com.tidesdb.Transaction;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Iterates a TidesDB column family between two inclusive key bounds, forward or
 * in reverse. Either bound may be {@code null} for an open end.
 *
 * <p>Each iterator runs in its own read-only transaction, so it reads one
 * consistent snapshot of the store whatever is written meanwhile. Keys are
 * ordered byte-wise, as Kafka Streams expects of a {@code Bytes} store.
 *
 * <p>Close every iterator. An iterator the caller leaves open is closed when
 * the store closes, after which it reports no further entries.
 *
 * <p>Not thread-safe.
 */
public class TidesDBRangeIterator implements KeyValueIterator<Bytes, byte[]> {

    private final byte[] from;
    private final byte[] to;
    private final boolean reverse;
    private final byte[] prefix;
    private final Consumer<TidesDBRangeIterator> onClose;

    private Transaction transaction;
    private TidesDBIterator iterator;
    private KeyValue<Bytes, byte[]> next;
    private boolean closed;

    /**
     * Creates a forward iterator over {@code [from, to]}.
     *
     * @param db the database
     * @param columnFamily the column family to iterate
     * @param from the inclusive lower bound, or {@code null} for the first key
     * @param to the inclusive upper bound, or {@code null} for the last key
     */
    public TidesDBRangeIterator(TidesDB db, ColumnFamily columnFamily, Bytes from, Bytes to) {
        this(db, columnFamily, IsolationLevel.READ_COMMITTED,
             from == null ? null : from.get(), to == null ? null : to.get(), false, null, null);
    }

    /**
     * Creates an iterator over {@code [from, to]}, in ascending order or, with
     * {@code reverse}, descending from {@code to}. A non-null {@code prefix}
     * replaces both bounds: the iterator walks forward over the keys that start
     * with it.
     */
    TidesDBRangeIterator(TidesDB db, ColumnFamily columnFamily, IsolationLevel isolation,
                         byte[] from, byte[] to, boolean reverse, byte[] prefix,
                         Consumer<TidesDBRangeIterator> onClose) {
        this.prefix = emptyToNull(prefix);
        this.from = this.prefix != null ? this.prefix : emptyToNull(from);
        this.to = this.prefix != null ? null : emptyToNull(to);
        this.reverse = this.prefix == null && reverse;
        this.onClose = onClose;

        if (this.from != null && this.to != null && compare(this.from, this.to) > 0) {
            // an inverted range is empty, which is how Kafka's own stores treat it
            this.closed = true;
            return;
        }

        try {
            this.transaction = db.beginTransaction(isolation);
            // a range iterator skips the SSTables that cannot meet the range; the bounds are a
            // promise rather than a fence, which is why advance() still checks them
            byte[] pruneUpper = this.prefix != null ? successor(this.prefix) : this.to;
            this.iterator = (this.from != null && pruneUpper != null)
                ? transaction.newRangeIterator(columnFamily, this.from, pruneUpper)
                : transaction.newIterator(columnFamily);

            Retry.run(() -> {
                if (reverse) {
                    if (this.to != null) {
                        iterator.seekForPrev(this.to);
                    } else {
                        iterator.seekToLast();
                    }
                } else if (this.from != null) {
                    iterator.seek(this.from);
                } else {
                    iterator.seekToFirst();
                }
                return null;
            });
            advance();
        } catch (TidesDBException e) {
            releaseNative();
            this.closed = true;
            throw new ProcessorStateException("Failed to create TidesDB iterator", e);
        } catch (RuntimeException e) {
            releaseNative();
            this.closed = true;
            throw e;
        }
    }

    /**
     * Reads the entry under the cursor into {@code next} and steps past it, or
     * clears {@code next} at the end of the range.
     */
    private void advance() throws TidesDBException {
        if (!iterator.isValid()) {
            next = null;
            return;
        }
        com.tidesdb.KeyValue kv = Retry.run(iterator::keyValue);
        byte[] key = kv.getKey();
        boolean outside = prefix != null ? !startsWith(key, prefix)
            : reverse ? (from != null && compare(key, from) < 0)
                      : (to != null && compare(key, to) > 0);
        if (outside) {
            next = null;
            return;
        }
        next = KeyValue.pair(Bytes.wrap(key), kv.getValue());
        Retry.run(() -> {
            if (reverse) {
                iterator.prev();
            } else {
                iterator.next();
            }
            return null;
        });
    }

    @Override
    public boolean hasNext() {
        return !closed && next != null;
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        KeyValue<Bytes, byte[]> current = next;
        try {
            advance();
        } catch (TidesDBException e) {
            close();
            throw new ProcessorStateException("Failed to advance TidesDB iterator", e);
        }
        return current;
    }

    @Override
    public Bytes peekNextKey() {
        if (closed) {
            throw new InvalidStateStoreException("Iterator has been closed");
        }
        if (next == null) {
            throw new NoSuchElementException();
        }
        return next.key;
    }

    @Override
    public void close() {
        if (closed && transaction == null) {
            return;
        }
        closed = true;
        next = null;
        releaseNative();
        if (onClose != null) {
            onClose.accept(this);
        }
    }

    /** Frees the iterator before its transaction, the order the binding asks for. */
    private void releaseNative() {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
        if (transaction != null) {
            transaction.close();
            transaction = null;
        }
    }

    private static byte[] emptyToNull(byte[] bound) {
        return bound == null || bound.length == 0 ? null : bound;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * The shortest key greater than every key starting with {@code prefix}, or
     * {@code null} when the prefix is all {@code 0xFF} and no such key exists.
     */
    static byte[] successor(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            if (prefix[i] != (byte) 0xFF) {
                byte[] out = java.util.Arrays.copyOf(prefix, i + 1);
                out[i]++;
                return out;
            }
        }
        return null;
    }

    /** Unsigned byte-wise comparison, the order TidesDB keeps keys in. */
    static int compare(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int cmp = Byte.compareUnsigned(a[i], b[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.length, b.length);
    }
}
