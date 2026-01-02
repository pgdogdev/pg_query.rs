#!/bin/bash

# Memory leak checker using valgrind for pg_query.rs
# Usage: ./check_leaks.sh [test_filter]
# Example: ./check_leaks.sh parse_tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
VALGRIND_LOG_DIR="valgrind_logs"
TEST_FILTER="${1:-}"

echo "=== Memory Leak Checker ==="
echo ""

# Check if valgrind is installed
if ! command -v valgrind &> /dev/null; then
    echo -e "${RED}Error: valgrind is not installed${NC}"
    echo "Install with: sudo pacman -S valgrind (Arch) or sudo apt install valgrind (Debian/Ubuntu)"
    exit 1
fi

# Create log directory
mkdir -p "$VALGRIND_LOG_DIR"

# Build tests in debug mode
echo "Building tests..."
cargo build --tests 2>&1 | tail -5

# Find test binaries
TEST_BINARIES=$(find target/debug/deps -maxdepth 1 -type f -executable ! -name "*.d" ! -name "*.so" 2>/dev/null | sort)

if [ -z "$TEST_BINARIES" ]; then
    echo -e "${RED}No test binaries found. Run 'cargo build --tests' first.${NC}"
    exit 1
fi

LEAK_COUNT=0
TESTED_COUNT=0

echo ""
echo "Running valgrind on test binaries..."
echo ""

for binary in $TEST_BINARIES; do
    binary_name=$(basename "$binary")

    # Skip non-test binaries (heuristic: test binaries usually have hash suffix)
    if [[ ! "$binary_name" =~ ^[a-z_]+-[a-f0-9]+$ ]]; then
        continue
    fi

    # Apply filter if provided
    if [ -n "$TEST_FILTER" ] && [[ ! "$binary_name" =~ $TEST_FILTER ]]; then
        continue
    fi

    log_file="$VALGRIND_LOG_DIR/${binary_name}.log"

    echo -n "Testing $binary_name... "

    # Run valgrind with leak checking
    valgrind \
        --leak-check=full \
        --show-leak-kinds=definite,indirect,possible \
        --track-origins=yes \
        --error-exitcode=1 \
        --log-file="$log_file" \
        "$binary" --test-threads=1 > /dev/null 2>&1 || true

    TESTED_COUNT=$((TESTED_COUNT + 1))

    # Check for leaks in the log
    definite_leaks=$(grep -oP "definitely lost: \K[0-9,]+" "$log_file" 2>/dev/null | tr -d ',' || echo "0")
    indirect_leaks=$(grep -oP "indirectly lost: \K[0-9,]+" "$log_file" 2>/dev/null | tr -d ',' || echo "0")
    possible_leaks=$(grep -oP "possibly lost: \K[0-9,]+" "$log_file" 2>/dev/null | tr -d ',' || echo "0")

    total_leaks=$((definite_leaks + indirect_leaks))

    if [ "$total_leaks" -gt 0 ]; then
        echo -e "${RED}LEAKS DETECTED${NC}"
        echo "  Definitely lost: $definite_leaks bytes"
        echo "  Indirectly lost: $indirect_leaks bytes"
        echo "  Possibly lost: $possible_leaks bytes"
        echo "  Log: $log_file"
        LEAK_COUNT=$((LEAK_COUNT + 1))
    elif [ "$possible_leaks" -gt 0 ]; then
        echo -e "${YELLOW}POSSIBLE LEAKS${NC} ($possible_leaks bytes) - see $log_file"
    else
        echo -e "${GREEN}OK${NC}"
    fi
done

echo ""
echo "=== Summary ==="
echo "Tested: $TESTED_COUNT binaries"
echo "Logs saved to: $VALGRIND_LOG_DIR/"

if [ "$LEAK_COUNT" -gt 0 ]; then
    echo -e "${RED}Memory leaks detected in $LEAK_COUNT binary(ies)${NC}"
    echo ""
    echo "To view leak details:"
    echo "  grep -A 20 'definitely lost' $VALGRIND_LOG_DIR/*.log"
    exit 1
else
    echo -e "${GREEN}No definite memory leaks detected${NC}"
    exit 0
fi
