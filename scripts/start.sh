#!/bin/bash
# Start Reservoir server

set -e

# Configuration
PORT=${PORT:-6379}
LOG_LEVEL=${LOG_LEVEL:-info}
DEBUG=${DEBUG:-false}
UI_PORT=${UI_PORT:-:8080}

FORCE_BUILD=${FORCE_BUILD:-false}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[START]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Check if reservoir binary exists or if source is newer
NEED_BUILD=false
if [ "$FORCE_BUILD" = "true" ]; then
    NEED_BUILD=true
    log "Force build requested..."
elif [ ! -f "./reservoir" ]; then
    NEED_BUILD=true
    log "Reservoir binary not found, building..."
else
    # Check if any Go source files are newer than the binary
    if find . -name "*.go" -newer "./reservoir" | grep -q .; then
        NEED_BUILD=true
        log "Source files are newer than binary, rebuilding..."
    fi
fi

if [ "$NEED_BUILD" = "true" ]; then
    log "Building reservoir binary..."
    # Clean up any old binaries first
    rm -f ./reservoir
    rm -f ./cmd/reservoir/reservoir
    
    go build -o reservoir ./cmd/reservoir
    if [ $? -ne 0 ]; then
        error "Failed to build reservoir binary"
        exit 1
    fi
    
    # Run unit tests after successful build
    log "Running unit tests..."
    go test ./internal/commands -v
    if [ $? -ne 0 ]; then
        error "Unit tests failed"
        exit 1
    fi
    success "Unit tests passed"
fi

# Check if server is already running
if lsof -i :$PORT > /dev/null 2>&1; then
    error "Port $PORT is already in use. Use './scripts/stop.sh' to stop existing server."
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Start the server
log "Starting Reservoir server on port $PORT..."
log "Log level: $LOG_LEVEL"
log "Debug mode: $DEBUG"


ARGS="--port $PORT --log-level $LOG_LEVEL --ui-port $UI_PORT"

if [ "$DEBUG" = "true" ]; then
    ARGS="$ARGS --debug"
fi



# Start server in background and save PID
nohup ./reservoir $ARGS > logs/reservoir.log 2>&1 &
SERVER_PID=$!

# Save PID for stop script
echo $SERVER_PID > .reservoir.pid

# Wait a moment and check if server started successfully
sleep 2

if kill -0 $SERVER_PID 2>/dev/null; then
    success "Reservoir server started successfully (PID: $SERVER_PID)"
    log "Server logs: logs/reservoir.log"
    log "Stop server: ./scripts/stop.sh"
    log "Test connection: redis-cli -p $PORT ping"

    
    # Run integration tests for __rand_int__ functionality if available
    if [ -f "./test_random_templates.sh" ] && [ "$NEED_BUILD" = "true" ]; then
        log "Running __rand_int__ template integration tests..."
        if ./test_random_templates.sh > /dev/null 2>&1; then
            success "Random template integration tests passed"
        else
            error "Random template integration tests failed, but server is running"
            log "Run './test_random_templates.sh' manually to see detailed results"
        fi
    fi
    
    # Run stress test if requested
    if [ "$1" = "--stress-test" ] || [ "$RUN_STRESS_TEST" = "true" ]; then
        log "Running connection stability stress test..."
        if ./scripts/stress_test.sh; then
            success "Stress test passed - server is stable under high load"
        else
            error "Stress test failed - check server stability"
            exit 1
        fi
    fi
else
    error "Failed to start server. Check logs/reservoir.log for details."
    exit 1
fi