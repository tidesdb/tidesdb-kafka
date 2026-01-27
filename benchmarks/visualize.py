#!/usr/bin/env python3
"""
Benchmark Visualization Script
Generates charts from TidesDB vs RocksDB benchmark CSV files
"""

import warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

def load_benchmark_data(csv_file):
    """Load benchmark data from CSV file"""
    if not os.path.exists(csv_file):
        print(f"Warning: {csv_file} not found")
        return None
    return pd.read_csv(csv_file)

def plot_performance_comparison(df, title, output_file):
    """Create a bar chart comparing TidesDB vs RocksDB"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = range(len(df))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], df['TidesDB_ms'], width, 
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], df['RocksDB_ms'], width,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Number of Operations', fontsize=12, fontweight='bold')
    ax.set_ylabel('Time (milliseconds)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df['Size'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(height)}',
                   ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_speedup_chart(df, title, output_file):
    """Create a line chart showing TidesDB speedup over RocksDB"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.plot(df['Size'], df['Speedup'], marker='o', linewidth=2.5, 
            markersize=8, color='#F18F01', label='Speedup Factor')
    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5, 
               alpha=0.7, label='Break-even (1.0x)')
    
    ax.set_xlabel('Number of Operations', fontsize=12, fontweight='bold')
    ax.set_ylabel('Speedup (RocksDB time / TidesDB time)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    # Add value labels
    for i, (size, speedup) in enumerate(zip(df['Size'], df['Speedup'])):
        ax.text(size, speedup, f'{speedup:.2f}x', 
               ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def plot_throughput_comparison(df, title, output_file):
    """Create throughput comparison (ops/sec)"""
    df['TidesDB_ops_sec'] = (df['Size'] / df['TidesDB_ms']) * 1000
    df['RocksDB_ops_sec'] = (df['Size'] / df['RocksDB_ms']) * 1000
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = range(len(df))
    width = 0.35
    
    bars1 = ax.bar([i - width/2 for i in x], df['TidesDB_ops_sec'], width,
                   label='TidesDB', color='#2E86AB', alpha=0.8)
    bars2 = ax.bar([i + width/2 for i in x], df['RocksDB_ops_sec'], width,
                   label='RocksDB', color='#A23B72', alpha=0.8)
    
    ax.set_xlabel('Number of Operations', fontsize=12, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df['Size'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels (skip infinite or NaN values)
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0 and height != float('inf') and not pd.isna(height):
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}',
                       ha='center', va='bottom', fontsize=8, rotation=0)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()

def create_summary_table(benchmarks_dir, output_file):
    """Create a summary table of all benchmarks"""
    summaries = []
    
    for csv_file in Path(benchmarks_dir).glob('*.csv'):
        df = pd.read_csv(csv_file)
        if len(df) > 0:
            benchmark_name = df['Benchmark'].iloc[0]
            avg_speedup = df['Speedup'].mean()
            max_speedup = df['Speedup'].max()
            min_speedup = df['Speedup'].min()
            
            summaries.append({
                'Benchmark': benchmark_name,
                'Avg Speedup': f'{avg_speedup:.2f}x',
                'Min Speedup': f'{min_speedup:.2f}x',
                'Max Speedup': f'{max_speedup:.2f}x',
                'Winner': 'TidesDB' if avg_speedup > 1.0 else 'RocksDB'
            })
    
    summary_df = pd.DataFrame(summaries)
    
    # Create table visualization
    fig, ax = plt.subplots(figsize=(12, len(summaries) * 0.5 + 1))
    ax.axis('tight')
    ax.axis('off')
    
    table = ax.table(cellText=summary_df.values,
                     colLabels=summary_df.columns,
                     cellLoc='center',
                     loc='center',
                     colColours=['#2E86AB'] * len(summary_df.columns))
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Style header
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Color winner column
    for i in range(len(summaries)):
        if summaries[i]['Winner'] == 'TidesDB':
            table[(i+1, 4)].set_facecolor('#C8E6C9')
        else:
            table[(i+1, 4)].set_facecolor('#FFCDD2')
    
    plt.title('Benchmark Summary: TidesDB vs RocksDB', 
             fontsize=14, fontweight='bold', pad=20)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Created: {output_file}")
    plt.close()
    
    return summary_df

def find_latest_csv(benchmarks_dir, base_name):
    """Find the latest timestamped CSV file for a given base name"""
    import glob
    # Look for timestamped files first (e.g., sequential_writes_20260126_221500.csv)
    pattern = os.path.join(benchmarks_dir, f'{base_name}_*.csv')
    files = glob.glob(pattern)
    if files:
        # Return the most recent one (sorted by name, timestamp makes it chronological)
        return sorted(files)[-1]
    # Fall back to non-timestamped file
    fallback = os.path.join(benchmarks_dir, f'{base_name}.csv')
    if os.path.exists(fallback):
        return fallback
    return None

def extract_timestamp(csv_path):
    """Extract timestamp from CSV filename if present"""
    import re
    match = re.search(r'_(\d{8}_\d{6})\.csv$', csv_path)
    if match:
        return match.group(1)
    return None

def generate_all_visualizations(benchmarks_dir='.', output_dir='charts'):
    """Generate all visualization charts"""
    
    benchmarks = {
        'sequential_writes': 'Sequential Write Performance',
        'random_writes': 'Random Write Performance',
        'sequential_reads': 'Sequential Read Performance',
        'random_reads': 'Random Read Performance',
        'mixed_workload': 'Mixed Workload (50% Read, 50% Write)',
        'range_scans': 'Range Scan Performance',
        'bulk_writes': 'Bulk Write Performance (putAll)',
        'updates': 'Update Performance',
        'large_values': 'Large Value Performance (10KB)',
        'iteration': 'Full Iteration Performance'
    }
    
    print("Generating benchmark visualizations...\n")
    
    # Find the timestamp from the first available CSV
    timestamp = None
    for base_name in benchmarks.keys():
        csv_path = find_latest_csv(benchmarks_dir, base_name)
        if csv_path:
            timestamp = extract_timestamp(csv_path)
            if timestamp:
                break
    
    # Create timestamped output directory if we have a timestamp
    if timestamp:
        output_dir = f'charts_{timestamp}'
    os.makedirs(output_dir, exist_ok=True)
    
    for base_name, title in benchmarks.items():
        csv_path = find_latest_csv(benchmarks_dir, base_name)
        if csv_path is None:
            print(f"Warning: {base_name}.csv not found")
            continue
            
        df = load_benchmark_data(csv_path)
        
        if df is not None:
            # Use timestamp in output filenames if available
            suffix = f'_{timestamp}' if timestamp else ''
            
            # Performance comparison
            plot_performance_comparison(
                df, 
                f'{title} - Time Comparison',
                os.path.join(output_dir, f'{base_name}_comparison{suffix}.png')
            )
            
            # Speedup chart
            plot_speedup_chart(
                df,
                f'{title} - TidesDB Speedup',
                os.path.join(output_dir, f'{base_name}_speedup{suffix}.png')
            )
            
            # Throughput comparison
            plot_throughput_comparison(
                df,
                f'{title} - Throughput Comparison',
                os.path.join(output_dir, f'{base_name}_throughput{suffix}.png')
            )
    
    # Create summary table
    print("\nGenerating summary table...")
    suffix = f'_{timestamp}' if timestamp else ''
    create_summary_table(benchmarks_dir, os.path.join(output_dir, f'summary_table{suffix}.png'))
    
    print(f"\n✓ All visualizations created in: {output_dir}")
    print("\nBenchmark Analysis Complete!")
    print("=" * 60)
    print("Charts generated:")
    print("  - Performance comparisons (time)")
    print("  - Speedup charts")
    print("  - Throughput comparisons")
    print("  - Summary table")
    print("=" * 60)

if __name__ == '__main__':
    generate_all_visualizations()
