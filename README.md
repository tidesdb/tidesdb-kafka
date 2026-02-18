# tidesdb-kafka
TidesDB + Kafka Streams

A drop-in replacement for RocksDB state stores in Kafka Streams applications, powered by [TidesDB](https://tidesdb.com).

## Installation

```bash
./install.sh
```

This will:
1. Build and install TidesDB native library
2. Build and install TidesDB Java bindings 
3. Install the Kafka Streams plugin

## Usage

### Running Tests and Benchmarks

```bash
./run.sh [options]

Options:
  -t, --tests            Run unit tests
  -b, --benchmarks       Run benchmarks
  -c, --charts           Generate charts from benchmark data
  -a, --all              Run everything
  -d, --data-dir <path>  Set data directory for benchmark databases
  -h, --help             Show this help message
```

### Examples

```bash
# Run unit tests
./run.sh -t

# Run benchmarks with default temp directory
./run.sh -b

# Run benchmarks on a fast SSD for better performance
./run.sh -b -d /mnt/nvme/bench

# Run benchmarks and generate charts
./run.sh -b -c

# Run everything
./run.sh -a
```

### Configuration

Both TidesDB and RocksDB run with sync disabled for fair comparison:
- TidesDB: `SyncMode.SYNC_NONE`
- RocksDB: `WriteOptions.setSync(false)`

## License

**Apache Kafka is a trademark of the Apache Software Foundation.**

**This project/product is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.**

Licensed under Mozilla Public License 2.0
