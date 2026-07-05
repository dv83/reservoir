#!/bin/bash
# Stop Reservoir cluster

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[CLUSTER-STOP]${NC} $1"
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

# Configuration
NODE_PREFIX=${NODE_PREFIX:-node}
CLEAN_COMMIT_LOG=${CLEAN_COMMIT_LOG:-false}
COMMIT_LOG_DIR=${COMMIT_LOG_DIR:-"./data/cluster/commitlog"}

# Check if cluster info exists
if [ ! -f ".cluster.info" ]; then
    log "No cluster info found, attempting to stop all reservoir processes..."
    
    # Find all reservoir processes
    PIDS=$(pgrep -f "reservoir.*cluster-enabled" 2>/dev/null || true)
    
    if [ -n "$PIDS" ]; then
        log "Found cluster processes: $PIDS"
        
        # Try graceful shutdown first
        log "Attempting graceful shutdown..."
        kill $PIDS 2>/dev/null || true
        
        # Wait up to 10 seconds
        for i in {1..10}; do
            REMAINING=$(pgrep -f "reservoir.*cluster-enabled" 2>/dev/null || true)
            if [ -z "$REMAINING" ]; then
                success "All cluster processes stopped gracefully"
                break
            fi
            sleep 1
        done
        
        # Force kill if still running
        REMAINING=$(pgrep -f "reservoir.*cluster-enabled" 2>/dev/null || true)
        if [ -n "$REMAINING" ]; then
            log "Force killing remaining processes: $REMAINING"
            kill -9 $REMAINING 2>/dev/null || true
            success "All cluster processes stopped (forced)"
        fi
    else
        log "No cluster processes found"
    fi
    
    # Clean up any PID files
    rm -f pids/node*.pid 2>/dev/null || true
    
    exit 0
fi

# Load cluster info
source .cluster.info

log "Stopping $CLUSTER_SIZE-node Reservoir cluster..."

STOPPED_COUNT=0
FAILED_COUNT=0

# Stop all nodes
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    NODE_NAME="${NODE_PREFIX}${i}"
    PID_FILE="pids/${NODE_NAME}.pid"
    
    if [ -f "$PID_FILE" ]; then
        NODE_PID=$(cat "$PID_FILE")
        log "Stopping $NODE_NAME (PID: $NODE_PID)..."
        
        # Check if process is running
        if kill -0 $NODE_PID 2>/dev/null; then
            # Try graceful shutdown first
            kill $NODE_PID 2>/dev/null || true
            
            # Wait up to 35 seconds for graceful shutdown (main.go waits 30s for connections)
            GRACEFUL=false
            for j in {1..35}; do
                if ! kill -0 $NODE_PID 2>/dev/null; then
                    success "$NODE_NAME stopped gracefully"
                    GRACEFUL=true
                    STOPPED_COUNT=$((STOPPED_COUNT + 1))
                    break
                fi
                sleep 1
            done
            
            # Force kill if still running
            if [ "$GRACEFUL" = "false" ]; then
                if kill -0 $NODE_PID 2>/dev/null; then
                    log "Force killing $NODE_NAME..."
                    kill -9 $NODE_PID 2>/dev/null || true
                    
                    # Double check
                    sleep 1
                    if kill -0 $NODE_PID 2>/dev/null; then
                        error "Failed to stop $NODE_NAME (PID: $NODE_PID)"
                        FAILED_COUNT=$((FAILED_COUNT + 1))
                    else
                        success "$NODE_NAME stopped (forced)"
                        STOPPED_COUNT=$((STOPPED_COUNT + 1))
                    fi
                fi
            fi
        else
            log "$NODE_NAME process not found, cleaning up PID file"
            STOPPED_COUNT=$((STOPPED_COUNT + 1))
        fi
        
        # Remove PID file
        rm -f "$PID_FILE"
    else
        log "No PID file for $NODE_NAME"
    fi
done

# Clean up cluster info and PID directory
rm -f .cluster.info
if [ -d "pids" ] && [ -z "$(ls -A pids)" ]; then
    rmdir pids
fi

# Summary
log ""
if [ $FAILED_COUNT -eq 0 ]; then
    success "Cluster stopped successfully!"
    log "  Nodes stopped: $STOPPED_COUNT/$CLUSTER_SIZE"
else
    error "Some nodes failed to stop"
    log "  Nodes stopped: $STOPPED_COUNT/$CLUSTER_SIZE"
    log "  Failed: $FAILED_COUNT"
    
    # Try to clean up any remaining processes
    log "Checking for remaining cluster processes..."
    REMAINING=$(pgrep -f "reservoir.*cluster-enabled" 2>/dev/null || true)
    if [ -n "$REMAINING" ]; then
        warn "Found remaining processes: $REMAINING"
        log "Force killing remaining processes..."
        kill -9 $REMAINING 2>/dev/null || true
        success "Cleanup complete"
    fi
fi

# Clean commit log if requested
if [ "$CLEAN_COMMIT_LOG" = "true" ] && [ -d "$COMMIT_LOG_DIR" ]; then
    log "Cleaning commit log data..."
    rm -rf "$COMMIT_LOG_DIR"
    success "Commit log cleaned"
fi

# Check for any remaining reservoir processes
REMAINING_ALL=$(pgrep -f "reservoir" 2>/dev/null || true)
if [ -n "$REMAINING_ALL" ]; then
    warn "Other reservoir processes still running: $REMAINING_ALL"
    log "Use './scripts/stop.sh' to stop non-cluster instances"
fi