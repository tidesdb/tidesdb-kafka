package com.tidesdb.kafka.store;

import com.tidesdb.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * Range iterator for TidesDB that iterates between two key bounds.
 */
public class TidesDBRangeIterator implements KeyValueIterator<Bytes, byte[]> {
    private final TidesDB db;
    private final ColumnFamily columnFamily;
    private final Bytes to;
    private Transaction transaction;
    private TidesDBIterator iterator;
    private boolean hasNext;
    private KeyValue<Bytes, byte[]> next;
    private final Comparator<byte[]> comparator;

    public TidesDBRangeIterator(TidesDB db, ColumnFamily columnFamily, Bytes from, Bytes to) {
        this.db = db;
        this.columnFamily = columnFamily;
        this.to = to;
        this.comparator = new LexicographicComparator();
        
        try {
            this.transaction = db.beginTransaction();
            this.iterator = transaction.newIterator(columnFamily);
            
            if (from != null) {
                iterator.seek(from.get());
            } else {
                iterator.seekToFirst();
            }
            
            advance();
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to create range iterator", e);
        }
    }

    private void advance() {
        try {
            if (iterator.isValid()) {
                byte[] key = iterator.key();
                
                // Check if we've passed the upper bound
                if (to != null && comparator.compare(key, to.get()) > 0) {
                    this.next = null;
                    this.hasNext = false;
                    return;
                }
                
                byte[] value = iterator.value();
                this.next = KeyValue.pair(Bytes.wrap(key), value);
                this.hasNext = true;
                iterator.next();
            } else {
                this.next = null;
                this.hasNext = false;
            }
        } catch (TidesDBException e) {
            this.next = null;
            this.hasNext = false;
        }
    }

    @Override
    public void close() {
        try {
            if (iterator != null) {
                iterator.close();
            }
            if (transaction != null) {
                transaction.close();
            }
        } catch (Exception e) {
            // Ignore close errors
        }
    }

    @Override
    public Bytes peekNextKey() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        return next.key;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public KeyValue<Bytes, byte[]> next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        KeyValue<Bytes, byte[]> current = next;
        advance();
        return current;
    }

    /**
     * Lexicographic byte array comparator
     */
    private static class LexicographicComparator implements Comparator<byte[]> {
        @Override
        public int compare(byte[] a, byte[] b) {
            int minLength = Math.min(a.length, b.length);
            for (int i = 0; i < minLength; i++) {
                int cmp = Byte.compareUnsigned(a[i], b[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(a.length, b.length);
        }
    }
}
