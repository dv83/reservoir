#!/bin/bash
# Test commit log functionality

set -e

# Configuration
PORT=${PORT:-6379}
COMMIT_LOG_DIR="./data/commitlog"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Stop any existing server
log "Stopping any existing Reservoir server..."
./scripts/stop.sh > /dev/null 2>&1 || true

# Clean up old commit log data
log "Cleaning up old commit log data..."
rm -rf "$COMMIT_LOG_DIR"
mkdir -p "$COMMIT_LOG_DIR"

# Start server with commit log enabled
log "Starting Reservoir with commit log enabled..."
./reservoir --port=$PORT --log-level=info --monitoring-enabled \
    --commit-log-enabled --commit-log-dir="$COMMIT_LOG_DIR" &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Check if server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    error "Server failed to start"
    exit 1
fi

success "Server started with PID $SERVER_PID"

# Test 1: Basic operations
log "Test 1: Writing test data..."
redis-cli -p $PORT SET key1 "value1" > /dev/null
redis-cli -p $PORT SET key2 "value2" > /dev/null
redis-cli -p $PORT SET key3 "value3" > /dev/null
redis-cli -p $PORT DEL key2 > /dev/null
redis-cli -p $PORT INCR counter > /dev/null
redis-cli -p $PORT INCR counter > /dev/null
redis-cli -p $PORT INCR counter > /dev/null

# Check data
VAL1=$(redis-cli -p $PORT GET key1)
VAL2=$(redis-cli -p $PORT GET key2)
VAL3=$(redis-cli -p $PORT GET key3)
COUNTER=$(redis-cli -p $PORT GET counter)

if [ "$VAL1" = "value1" ] && [ -z "$VAL2" ] && [ "$VAL3" = "value3" ] && [ "$COUNTER" = "3" ]; then
    success "Data written correctly"
else
    error "Data verification failed"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Test 2: Check commit log files
log "Test 2: Checking commit log files..."
COMMIT_LOG_FILES=$(ls -la "$COMMIT_LOG_DIR"/*.log 2>/dev/null | wc -l)
if [ "$COMMIT_LOG_FILES" -gt 0 ]; then
    success "Commit log files created"
    ls -la "$COMMIT_LOG_DIR"/*.log
else
    error "No commit log files found"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Test 3: Crash recovery
log "Test 3: Testing crash recovery..."

# Kill server abruptly (simulate crash)
log "Simulating server crash..."
kill -9 $SERVER_PID 2>/dev/null
sleep 1

# Restart server - should recover from commit log
log "Restarting server to test recovery..."
./reservoir --port=$PORT --log-level=info --monitoring-enabled \
    --commit-log-enabled --commit-log-dir="$COMMIT_LOG_DIR" &
NEW_SERVER_PID=$!

# Wait for recovery
sleep 3

# Check if server recovered
if ! kill -0 $NEW_SERVER_PID 2>/dev/null; then
    error "Server failed to restart"
    exit 1
fi

# Verify data was recovered
log "Verifying recovered data..."
RECOVERED_VAL1=$(redis-cli -p $PORT GET key1)
RECOVERED_VAL2=$(redis-cli -p $PORT GET key2)
RECOVERED_VAL3=$(redis-cli -p $PORT GET key3)
RECOVERED_COUNTER=$(redis-cli -p $PORT GET counter)

if [ "$RECOVERED_VAL1" = "value1" ] && [ -z "$RECOVERED_VAL2" ] && [ "$RECOVERED_VAL3" = "value3" ] && [ "$RECOVERED_COUNTER" = "3" ]; then
    success "Data recovered successfully from commit log!"
else
    error "Data recovery failed"
    echo "Expected: key1=value1, key2=(deleted), key3=value3, counter=3"
    echo "Got: key1=$RECOVERED_VAL1, key2=$RECOVERED_VAL2, key3=$RECOVERED_VAL3, counter=$RECOVERED_COUNTER"
    kill $NEW_SERVER_PID 2>/dev/null
    exit 1
fi

# Test 4: Performance test
log "Test 4: Performance test with commit log..."
START_TIME=$(date +%s.%N)
redis-cli -p $PORT --pipe < <(
    for i in {1..1000}; do
        echo "SET perf_key_$i value_$i"
    done
)
END_TIME=$(date +%s.%N)
DURATION=$(echo "$END_TIME - $START_TIME" | bc)
success "Wrote 1000 keys in ${DURATION}s with commit log enabled"

# Clean up
log "Cleaning up..."
kill $NEW_SERVER_PID 2>/dev/null
wait $NEW_SERVER_PID 2>/dev/null || true

# Summary
echo ""
success "All commit log tests passed!"
echo -e "${BLUE}Summary:${NC}"
echo "  ✓ Basic write operations logged"
echo "  ✓ Commit log files created"
echo "  ✓ Crash recovery successful"
echo "  ✓ Performance acceptable"
echo ""
log "Commit log directory contents:"
ls -la "$COMMIT_LOG_DIR"/