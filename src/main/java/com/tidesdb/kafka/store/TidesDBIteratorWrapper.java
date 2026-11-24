package com.tidesdb.kafka.store;

import com.tidesdb.ColumnFamily;
import com.tidesdb.TidesDB;

/**
 * Iterates every key in a TidesDB column family in ascending byte-wise order.
 * Equivalent to a {@link TidesDBRangeIterator} with both bounds open; kept as
 * its own type for code written against earlier releases.
 */
public class TidesDBIteratorWrapper extends TidesDBRangeIterator {

    /**
     * Creates an iterator over the whole column family.
     *
     * @param db the database
     * @param columnFamily the column family to iterate
     */
    public TidesDBIteratorWrapper(TidesDB db, ColumnFamily columnFamily) {
        super(db, columnFamily, null, null);
    }
}
