package com.tidesdb.kafka.store;

import com.tidesdb.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.NoSuchElementException;

/**
 * Iterator wrapper for TidesDB that provides full iteration over all keys.
 */
public class TidesDBIteratorWrapper implements KeyValueIterator<Bytes, byte[]> {
    private final TidesDB db;
    private final ColumnFamily columnFamily;
    private Transaction transaction;
    private TidesDBIterator iterator;
    private boolean hasNext;
    private KeyValue<Bytes, byte[]> next;

    public TidesDBIteratorWrapper(TidesDB db, ColumnFamily columnFamily) {
        this.db = db;
        this.columnFamily = columnFamily;
        try {
            this.transaction = db.beginTransaction();
            this.iterator = transaction.newIterator(columnFamily);
            try {
                this.iterator.seekToFirst();
            } catch (TidesDBException e) {
                // Empty store -- no entries to iterate
                if (e.getMessage().contains("not found")) {
                    this.hasNext = false;
                    this.next = null;
                    return;
                }
                throw e;
            }
            advance();
        } catch (TidesDBException e) {
            throw new RuntimeException("Failed to create iterator", e);
        }
    }

    private void advance() {
        try {
            if (iterator.isValid()) {
                byte[] key = iterator.key();
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
}
