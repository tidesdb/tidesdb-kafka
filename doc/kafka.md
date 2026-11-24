---
title: TidesDB Kafka Streams State Store Reference
description: Using TidesDB as the state store for Kafka Streams applications
---

## Getting Started

`tidesdb-kafka-streams` is a Kafka Streams `KeyValueStore` backed by TidesDB.
It replaces the RocksDB store wherever Kafka Streams accepts a
`KeyValueBytesStoreSupplier`. Kafka still wraps the store with change-logging,
caching and metrics, as it does RocksDB's.

### Requirements

| Component | Version |
|---|---|
| Java | 11 or higher |
| Kafka Streams | 3.7.x |
| tidesdb-java | 1.1.x (the version in `pom.xml`) |
| TidesDB native library | 10.2.0 or newer |

Run the application as a **non-root user**. TidesDB refuses to open a database
from a process whose effective uid is 0, because the files it creates would be
owned by root. Container images run as root by default, so add a `USER` to the
image or start it with `--user`. If a state directory is already owned by root,
`chown -R` it to that user first. The store reports this case with a
`ProcessorStateException` that says what to do. The refusal ships in the TidesDB
release after 10.2.1.

### Installing

```bash
./install.sh
```

The script builds and installs the latest TidesDB release, then the tidesdb-java
version this plugin's `pom.xml` names (JNI library included), then this plugin.
It uses `sudo` to install into `/usr/local`.

To build by hand, install TidesDB and tidesdb-java first (see the
[Java reference](/reference/java/)), then:

```bash
mvn package
```

### TidesDB built with jemalloc or tcmalloc

The JVM loads the JNI library with `dlopen`, and distribution builds of jemalloc
and tcmalloc cannot be loaded that late. The store then fails to open with
`cannot allocate memory in static TLS block`. Preload the allocator:

```bash
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2   # path varies by distro
java -Djava.library.path=/usr/local/lib -jar my-streams-app.jar
```

## Using the Store

### DSL

Pass a `TidesDBStoreSupplier` to `Materialized.as`:

```java
import com.tidesdb.kafka.store.TidesDBStoreSupplier;

KTable<String, Long> counts = builder.stream("words", Consumed.with(Serdes.String(), Serdes.String()))
    .groupBy((key, word) -> word)
    .count(Materialized.<String, Long>as(new TidesDBStoreSupplier("word-counts"))
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Long()));
```

### Processor API

Let Kafka build the store around the supplier, so change-logging and caching are
applied:

```java
topology.addStateStore(
    Stores.keyValueStoreBuilder(new TidesDBStoreSupplier("latest-store"),
                                Serdes.String(), Serdes.String())
        .withLoggingEnabled(Collections.emptyMap()),
    "my-processor");
```

`TidesDBStoreBuilder` builds a **bare** store and does not apply change-logging
or caching, even when they are requested, so use it only when you want an
unwrapped `Bytes`/`byte[]` store. It logs a warning if logging or caching is
asked of it.

### Where Data Lives

Each task opens one TidesDB database at
`<state.dir>/<application.id>/<task id>/<store name>` and keeps the store's data
in one column family, `default` unless configured otherwise. The store registers
itself with Kafka Streams for changelog restoration. Restored records are written
in batches, one transaction per batch.

### Behaviour

| Operation | Behaviour |
|---|---|
| `put(k, null)` | Deletes `k`. |
| `putAll` | One transaction: the batch lands atomically. A `null` value deletes. |
| `putIfAbsent`, `delete` | Read and write in one transaction. `delete` returns the old value. |
| `range(from, to)` | Inclusive on both ends, ascending byte order. A `null` bound is open; `from > to` is empty. |
| `reverseRange`, `reverseAll` | Descending byte order, with the same bounds. |
| `prefixScan(prefix, serializer)` | Keys starting with the serialized prefix, ascending. |
| `approximateNumEntries()` | Keys in SSTables plus keys still in the memtable; overwrites and deletes not yet compacted are counted. |
| `flush()` | Syncs the write-ahead log. Kafka calls this before checkpointing offsets. |
| `getPosition()` | The input offsets the store has applied, for interactive-query consistency. |

Each iterator reads one consistent snapshot, whatever is written meanwhile.
Close every iterator; any left open are closed when the store closes.

Keys are ordered unsigned byte-wise, the order Kafka Streams expects of a
`Bytes` store.

The engine can report transient contention (`ERR_LOCKED`) on any read, iterator
step or commit, for example while a compaction moves files or a backup admits
writers. The store retries those calls with a short backoff for about a second,
then gives up. Other errors are thrown as `ProcessorStateException`, and calls on
a closed store throw `InvalidStateStoreException`, as with Kafka's own stores.

`flush()` does not flush the memtable to an SSTable. TidesDB 10 has one memtable
for the whole database, and flushing it every commit interval would fill level 1
with tiny tables. The synced log is what recovery replays. Call
`flushMemtable()` when you want the data written out as tables.

Interactive Queries v1 (`store.get`, `range`, ...) is supported. Interactive
Queries v2 (`StateStore.query`) is not implemented yet.

## Configuration

`TidesDBStoreConfig` holds a TidesDB database `Config`, a `ColumnFamilyConfig`,
and a few store settings. Both TidesDB configurations start from the native
library's own defaults. The one store-specific default is LZ4 compression.

```java
TidesDBStoreConfig config = TidesDBStoreConfig.builder()
    .blockCacheSize(256L * 1024 * 1024)
    .memtableWriteBufferSize(64L * 1024 * 1024)
    .compression(CompressionAlgorithm.ZSTD)
    .bloomFpr(0.01)
    .defaultTtlSeconds(3600)
    .build();

builder.stream("events")
    .groupByKey()
    .count(Materialized.as(new TidesDBStoreSupplier("event-counts", config)));
```

For a setting without a builder method, pass a whole configuration. Its database
path and column family name are ignored, because the store decides those:

```java
TidesDBStoreConfig config = TidesDBStoreConfig.builder()
    .databaseConfig(Config.defaultConfig("unused").toBuilder()
        .memtableIdleFlushSeconds(10)
        .build())
    .columnFamilyConfig(ColumnFamilyConfig.builder()
        .keepValuesInline(true)
        .build())
    .build();
```

### Database Settings

| Builder method | Description |
|---|---|
| `numFlushThreads(int)` | Flush worker threads. |
| `numCompactionThreads(int)` | Compaction worker threads. |
| `logLevel(LogLevel)` | Engine log level. |
| `logToFile(boolean)`, `logTruncationAt(long)` | Log to a file in the database directory, truncated at a size. |
| `blockCacheSize(long)` | Shared block cache, in bytes. |
| `maxOpenSSTables(long)` | Cap on open SSTable file handles. |
| `memtableWriteBufferSize(long)` | Size the shared memtable grows to before it is flushed. |
| `memtableSkipListMaxLevel(int)`, `memtableSkipListProbability(float)` | Memtable skip-list shape. |
| `memtableSyncMode(SyncMode)`, `memtableSyncIntervalUs(long)` | How the write-ahead log is synced. `SYNC_NONE` is the default and usually right, since `flush()` syncs at every Kafka commit and the changelog can rebuild the store. |
| `memtableL0QueueStallThreshold(int)` | Immutable memtables queued before writes are throttled. |
| `memtableIdleFlushSeconds(int)` | Flush a memtable that has been idle this long. |
| `valueSeparationThreshold(long)` | Values at least this large go to the value log. |
| `vlogSegmentSize(long)` | Value-log segment size. |
| `txnTimeoutSeconds(long)` | Default transaction timeout. |

### Column Family Settings

| Builder method | Description |
|---|---|
| `compression(CompressionAlgorithm)` | Single-codec encoding: `NONE`, `LZ4` (default), `LZ4_FAST`, `ZSTD`, `SNAPPY`. |
| `encodingPipeline(CompressionAlgorithm...)` | Stacked encodings, applied in order. |
| `enableBloomFilter(boolean)`, `bloomFpr(double)` | Partitioned bloom filter for point-get pruning. |
| `defaultIsolationLevel(IsolationLevel)` | Isolation the store's transactions run at. Default `READ_COMMITTED`. |
| `levelSizeRatio(long)`, `minLevels(int)`, `dividingLevelOffset(int)` | LSM level shape. |
| `l1FileCountTrigger(int)` | L1 file count that triggers compaction. |
| `tombstoneDensityTrigger(double)`, `tombstoneDensityMinEntries(long)` | Compact SSTables dense with tombstones. |
| `keepValuesInline(boolean)` | Keep values in the key log instead of the value log. |
| `btreeKlogBlockSize(long)` | Key-log block size. |

### Store Settings

| Builder method | Default | Description |
|---|---|---|
| `columnFamilyName(String)` | `default` | The column family the store uses. Created with the column family settings on first open, and reused as it is afterwards. |
| `defaultTtlSeconds(long)` | `0` (never) | TTL applied to `put`, `putIfAbsent`, `putAll` and restored records, in seconds from the write. |
| `raiseOpenFileLimit(long)` | `0` (leave alone) | Raises the process open-file ceiling before the database opens. |

Column family settings apply when the column family is created. To change an
existing store, use `updateRuntimeConfig`.

## TTL

A store-wide TTL is set with `defaultTtlSeconds`. A per-key TTL needs the
`TidesDBStore` itself. `context.getStateStore(...)` returns Kafka's wrapper around
it, so this works on a store you hold directly, such as one from
`TidesDBStoreBuilder`:

```java
store.putWithTtl(key, value, 1800);   // expires 30 minutes from now
Long expiry = store.getTtl(key);      // absolute unix seconds, -1 for never, null if absent
```

TTLs are seconds from the write; `getTtl` reports the absolute deadline the
engine stored. An expired key reads as absent. The engine drops it during
compaction, and no delete reaches the changelog, so a key restored from the
changelog gets the default TTL again, counted from the restore.

## TidesDB Extensions

`TidesDBStore` exposes TidesDB operations beyond the `KeyValueStore` interface.
They are on the `TidesDBStore` object, not on the wrapper Kafka returns from
`context.getStateStore(...)`, so reach them on a store you hold directly.

| Method | Description |
|---|---|
| `getStats()` | Column family statistics (`CfStats`). |
| `getDbStats()`, `getCacheStats()`, `getStallStats()`, `getIoStats()` | Database, block cache, write-stall and device I/O statistics. |
| `rangeStats(from, to)` | What a scan of a range would touch. |
| `estimateCardinality()` | Estimated distinct keys. |
| `compact()`, `compactRange(from, to)` | Queue a compaction, or compact one key range. |
| `flushMemtable()`, `syncWal()`, `checkpoint()` | Flush the memtable; fsync the log; durability barrier. |
| `backup(dir)` | Directly-openable copy. `dir` must not exist or must be empty. |
| `singleDelete(key)` | Tombstone that compacts away with a key put exactly once. |
| `setCommitHook(hook)`, `clearCommitHook()` | Change data capture on every commit. |
| `updateRuntimeConfig(config, persist)` | Change column family settings without a restart. |
| `createColumnFamily`, `getColumnFamily`, `listColumnFamilies`, `renameColumnFamily`, `cloneColumnFamily`, `dropColumnFamily` | Manage other column families in the store's database. |

## Migrating from 0.4.x

0.5.0 moves to tidesdb-java 1.1 and TidesDB 10.2. The on-disk format changed in
TidesDB 10: **a store written by 0.4.x cannot be opened**. Delete the local state
directories and let Kafka Streams restore the stores from their changelogs (for
example, run `kafka-streams-application-reset` or start with a clean
`state.dir`).

### Configuration

The configuration now starts from the engine's defaults rather than a set tuned
for TidesDB 9, so a store built with no settings behaves like a plain TidesDB 10
database, plus LZ4.

| 0.4.x | 0.5.0 |
|---|---|
| `writeBufferSize(n)` | `memtableWriteBufferSize(n)`. The memtable is now shared by the whole database. |
| `syncMode(m)`, `syncIntervalUs(n)` | `memtableSyncMode(m)`, `memtableSyncIntervalUs(n)` |
| `skipListMaxLevel`, `skipListProbability` | `memtableSkipListMaxLevel`, `memtableSkipListProbability` |
| `klogValueThreshold(n)` | `valueSeparationThreshold(n)` |
| `l0QueueStallThreshold(n)` | `memtableL0QueueStallThreshold(n)` |
| `compressionAlgorithm(CompressionAlgorithm.LZ4_COMPRESSION)` | `compression(CompressionAlgorithm.LZ4)` |
| `bloomFPR(x)` | `bloomFpr(x)` |
| `unifiedMemtable*` | Removed. The memtable is always shared. |
| `objectStoreFsPath`, `objectStoreConfig`, `objectStoreS3Config`, `objectLazyCompaction`, `objectPrefetchCompaction` | Removed. TidesDB 10 has no object store mode. |
| `useBtree`, `enableBlockIndexes`, `indexSampleRatio`, `blockIndexPrefixLen`, `minDiskSpace`, `maxMemoryUsage`, `maxConcurrentFlushes` | Removed from the engine. |
| `comparatorName` | Removed. Keys are always ordered byte-wise. |
| `cancelBackgroundWorkOnClose`, `finishCompactionsOnClose` | Removed. `close()` handles shutdown. |
| `defaultTtlSeconds(-1)` for no TTL | `0` or any negative value. |

### Store Methods

| 0.4.x | 0.5.0 |
|---|---|
| `getStats()` returning `Stats` | Returns `CfStats`. |
| `rangeCost(from, to)` | `rangeStats(from, to)` |
| `checkpoint(dir)` | `backup(dir)` for a copy; `checkpoint()` is now a durability barrier. |
| `purge()`, `purgeAll()` | `flushMemtable()` and `compact()` |
| `cancelBackgroundWork()`, `registerComparator`, `promoteToPrimary`, `isS3Available`, `deleteColumnFamily(ColumnFamily)` | Removed. Use `dropColumnFamily(String)` to drop a column family. |
| — | Added: `getTtl`, `reverseRange`, `reverseAll`, `prefixScan`, `getPosition`, `getStallStats`, `getIoStats`, `estimateCardinality`, `flushMemtable` |

### Behaviour

- **TTL is fixed.** 0.4.x added the current time to the TTL and the engine then
  treated the sum as a duration, so entries lived decades too long. TTLs are now
  seconds from the write, as documented.
- **The store registers with Kafka Streams.** 0.4.x never called
  `context.register`, so Kafka could not restore it from its changelog. Kafka
  Streams 3.7 also could not run it inside a topology at all, because it lacked
  `getPosition()`.
- **Errors propagate.** 0.4.x ended an iteration silently on any error and
  matched "not found" in exception messages. Errors are now thrown as
  `ProcessorStateException`, after retrying transient contention.
- **Closed-store calls** throw `InvalidStateStoreException` (it was
  `IllegalStateException`).
- **`flush()`** syncs the write-ahead log instead of flushing the memtable.
- **`TidesDBStoreBuilder`** warns that it does not apply logging or caching.

## Testing

```bash
./run.sh -t          # unit tests and topology tests
./run.sh -b          # benchmarks against RocksDB

# by hand
mvn test -Dtest='TidesDBStoreTest,TidesDBStoreTopologyTest' \
    -DargLine="-Djava.library.path=/usr/local/lib"

# TidesDB built with jemalloc
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./run.sh -t
```

The tests must not run as root.
