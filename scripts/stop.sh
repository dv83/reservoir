#!/bin/bash
# Stop Reservoir server

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[STOP]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Check if PID file exists
if [ -f ".reservoir.pid" ]; then
    SERVER_PID=$(cat .reservoir.pid)
    log "Found PID file: $SERVER_PID"
    
    # Check if process is running
    if kill -0 $SERVER_PID 2>/dev/null; then
        log "Stopping Reservoir server (PID: $SERVER_PID)..."
        
        # Try graceful shutdown first
        kill $SERVER_PID
        
        # Wait up to 10 seconds for graceful shutdown
        for i in {1..10}; do
            if ! kill -0 $SERVER_PID 2>/dev/null; then
                success "Server stopped gracefully"
                rm -f .reservoir.pid
                exit 0
            fi
            sleep 1
        done
        
        # Force kill if still running
        log "Server not responding, forcing shutdown..."
        kill -9 $SERVER_PID 2>/dev/null || true
        
        # Double check
        if kill -0 $SERVER_PID 2>/dev/null; then
            error "Failed to stop server (PID: $SERVER_PID)"
            exit 1
        else
            success "Server stopped (force kill)"
            rm -f .reservoir.pid
        fi
    else
        log "Process $SERVER_PID not found, cleaning up PID file"
        rm -f .reservoir.pid
    fi
else
    log "No PID file found, checking for running processes..."
    
    # Try to find and kill any reservoir processes
    PIDS=$(pgrep -f "reservoir" 2>/dev/null || true)
    
    if [ -n "$PIDS" ]; then
        log "Found running reservoir processes: $PIDS"
        kill $PIDS
        sleep 2
        
        # Force kill if still running
        REMAINING=$(pgrep -f "reservoir" 2>/dev/null || true)
        if [ -n "$REMAINING" ]; then
            log "Force killing remaining processes: $REMAINING"
            kill -9 $REMAINING
        fi
        
        success "Stopped all reservoir processes"
    else
        log "No running reservoir processes found"
    fi
fi