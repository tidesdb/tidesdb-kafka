#!/bin/bash

# TidesDB Kafka Streams -- Test and Benchmark Runner
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Source shared utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/utils.sh"

print_box "" "TidesDB Kafka Streams -- Test Runner"
echo

# Parse command line arguments
RUN_TESTS=false
RUN_BENCHMARKS=false
GENERATE_CHARTS=false
DATA_DIR=""

show_usage() {
    echo "Usage: $0 [options]"
    echo
    echo "Options:"
    echo "  -t, --tests            Run unit tests"
    echo "  -b, --benchmarks       Run benchmarks"
    echo "  -c, --charts           Generate charts from benchmark data"
    echo "  -a, --all              Run everything"
    echo "  -d, --data-dir <path>  Set data directory for benchmark databases"
    echo "  -h, --help             Show this help message"
    echo
    echo "Examples:"
    echo "  $0 -b                          # Run benchmarks with temp directory"
    echo "  $0 -b -d /mnt/fast-ssd/bench   # Run benchmarks on fast SSD"
    echo
}

if [ $# -eq 0 ]; then
    show_usage
    exit 0
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--tests)
            RUN_TESTS=true
            shift
            ;;
        -b|--benchmarks)
            RUN_BENCHMARKS=true
            shift
            ;;
        -d|--data-dir)
            if [ -z "$2" ] || [[ "$2" == -* ]]; then
                echo -e "${RED}Error: --data-dir requires a path argument${NC}"
                exit 1
            fi
            DATA_DIR="$2"
            shift 2
            ;;
        -c|--charts)
            GENERATE_CHARTS=true
            shift
            ;;
        -a|--all)
            RUN_TESTS=true
            RUN_BENCHMARKS=true
            GENERATE_CHARTS=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run unit tests
if [ "$RUN_TESTS" = true ]; then
    print_box "${BLUE}" "Running Unit Tests"
    echo
    
    # Check if Maven is available
    if ! command -v mvn &> /dev/null; then
        echo -e "${RED}✗ Maven not found!${NC}"
        echo
        echo "Please install Maven to run tests:"
        echo "  Debian/Ubuntu: sudo apt-get install maven"
        echo "  macOS: brew install maven"
        echo
        exit 1
    fi
    
    mvn test -Dtest=TidesDBStoreTest -DargLine="-Djava.library.path=/usr/local/lib"
    
    if [ $? -eq 0 ]; then
        echo
        echo -e "${GREEN}✓ Unit tests passed!${NC}"
        echo
    else
        echo
        echo -e "${RED}✗ Unit tests failed!${NC}"
        exit 1
    fi
fi

# Run benchmarks
if [ "$RUN_BENCHMARKS" = true ]; then
    print_box "${BLUE}" "Running Benchmarks"
    echo
    
    # Check if Maven is available
    if ! command -v mvn &> /dev/null; then
        echo -e "${RED}✗ Maven not found!${NC}"
        echo
        echo "Please install Maven to run benchmarks:"
        echo "  Debian/Ubuntu: sudo apt-get install maven"
        echo "  macOS: brew install maven"
        echo
        exit 1
    fi
    
    echo -e "${YELLOW} Note: Benchmarks may take 10-20 minutes to complete${NC}"
    echo
    
    # Build JVM args with library path and optional data directory
    JVM_ARGS="-Djava.library.path=/usr/local/lib"
    if [ -n "$DATA_DIR" ]; then
        echo -e "   Data directory: ${BLUE}${DATA_DIR}${NC}"
        JVM_ARGS="$JVM_ARGS -Dbenchmark.data.dir=$DATA_DIR"
    fi
    
    mvn test -Dtest=StateStoreBenchmark -DargLine="$JVM_ARGS"
    
    if [ $? -eq 0 ]; then
        echo
        echo -e "${GREEN}✓ Benchmarks completed!${NC}"
        echo -e "   CSV files saved to: ${BLUE}benchmarks/${NC}"
        echo
    else
        echo
        echo -e "${RED}✗ Benchmarks failed!${NC}"
        exit 1
    fi
fi

# Generate charts
if [ "$GENERATE_CHARTS" = true ]; then
    print_box "${BLUE}" "Generating Charts"
    echo
    
    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}✗ Python 3 not found. Please install Python 3.${NC}"
        exit 1
    fi
    
    # Check if benchmark data exists
    if [ ! -d "benchmarks" ] || [ -z "$(ls -A benchmarks/*.csv 2>/dev/null)" ]; then
        echo -e "${RED}✗ No benchmark data found. Run benchmarks first with -b flag.${NC}"
        exit 1
    fi
    
    # Set up Python virtual environment
    VENV_DIR="${SCRIPT_DIR}/.venv"
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating Python virtual environment..."
        python3 -m venv "$VENV_DIR"
    fi
    
    # Activate virtual environment and install dependencies
    echo "Installing Python dependencies..."
    source "$VENV_DIR/bin/activate"
    pip install -q -r benchmarks/requirements.txt
    
    # Generate charts
    cd benchmarks
    python visualize.py
    cd ..
    
    deactivate
    
    if [ $? -eq 0 ]; then
        echo
        echo -e "${GREEN}✓ Charts generated!${NC}"
        echo -e "   Charts saved to: ${BLUE}benchmarks/charts/${NC}"
        echo
        
        # List generated charts
        echo "Generated files:"
        ls -1 benchmarks/charts/*.png | sed 's/^/   /'
        echo
    else
        echo
        echo -e "${RED}✗ Chart generation failed!${NC}"
        exit 1
    fi
fi

# Print summary
print_box "${GREEN}" "All tasks completed successfully!"
echo

if [ "$RUN_BENCHMARKS" = true ] || [ "$GENERATE_CHARTS" = true ]; then
    echo "📊 View your results:"
    echo "   - CSV data: benchmarks/*.csv"
    if [ "$GENERATE_CHARTS" = true ]; then
        echo "   - Charts: benchmarks/charts/*.png"
    fi
    echo
fi

echo
