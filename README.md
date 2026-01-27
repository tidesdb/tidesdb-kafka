# tidesdb-kafka
TidesDB + Kafka Streams

Simply run `./install.sh` to install the plugin.

You can use the `run.sh`
```bash
:~/tidesdb-kafka$ ./run.sh
░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
░░       TidesDB Kafka Streams -- Test Runner       ░░
░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

Usage: ./run.sh [options]

Options:
  -t, --tests        Run unit tests
  -b, --benchmarks   Run benchmarks
  -c, --charts       Generate charts from benchmark data
  -a, --all          Run everything
  -h, --help         Show this help message
```

## Benchmark Results

Intel Core i7-11700K (8 cores, 16 threads) @ 4.9GHz
48GB DDR4
Western Digital 500GB WD Blue 3D NAND Internal PC SSD (SATA)
Ubuntu 23.04 x86_64 6.2.0-39-generic

### Summary
![Summary Table](benchmarks/charts/summary_table.png)

### Sequential Writes
![Sequential Writes Comparison](benchmarks/charts/sequential_writes_comparison.png)
![Sequential Writes Speedup](benchmarks/charts/sequential_writes_speedup.png)
![Sequential Writes Throughput](benchmarks/charts/sequential_writes_throughput.png)

### Random Writes
![Random Writes Comparison](benchmarks/charts/random_writes_comparison.png)
![Random Writes Speedup](benchmarks/charts/random_writes_speedup.png)
![Random Writes Throughput](benchmarks/charts/random_writes_throughput.png)

### Sequential Reads
![Sequential Reads Comparison](benchmarks/charts/sequential_reads_comparison.png)
![Sequential Reads Speedup](benchmarks/charts/sequential_reads_speedup.png)
![Sequential Reads Throughput](benchmarks/charts/sequential_reads_throughput.png)

### Random Reads
![Random Reads Comparison](benchmarks/charts/random_reads_comparison.png)
![Random Reads Speedup](benchmarks/charts/random_reads_speedup.png)
![Random Reads Throughput](benchmarks/charts/random_reads_throughput.png)

### Mixed Workload
![Mixed Workload Comparison](benchmarks/charts/mixed_workload_comparison.png)
![Mixed Workload Speedup](benchmarks/charts/mixed_workload_speedup.png)
![Mixed Workload Throughput](benchmarks/charts/mixed_workload_throughput.png)

### Range Scans
![Range Scans Comparison](benchmarks/charts/range_scans_comparison.png)
![Range Scans Speedup](benchmarks/charts/range_scans_speedup.png)
![Range Scans Throughput](benchmarks/charts/range_scans_throughput.png)

### Bulk Writes
![Bulk Writes Comparison](benchmarks/charts/bulk_writes_comparison.png)
![Bulk Writes Speedup](benchmarks/charts/bulk_writes_speedup.png)
![Bulk Writes Throughput](benchmarks/charts/bulk_writes_throughput.png)

### Updates
![Updates Comparison](benchmarks/charts/updates_comparison.png)
![Updates Speedup](benchmarks/charts/updates_speedup.png)
![Updates Throughput](benchmarks/charts/updates_throughput.png)

### Large Values (10KB)
![Large Values Comparison](benchmarks/charts/large_values_comparison.png)
![Large Values Speedup](benchmarks/charts/large_values_speedup.png)
![Large Values Throughput](benchmarks/charts/large_values_throughput.png)

### Full Iteration
![Iteration Comparison](benchmarks/charts/iteration_comparison.png)
![Iteration Speedup](benchmarks/charts/iteration_speedup.png)
![Iteration Throughput](benchmarks/charts/iteration_throughput.png)

