#!/bin/bash
# Restart Reservoir cluster

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[CLUSTER-RESTART]${NC} $1"
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

# Parse command line arguments
PRESERVE_CONFIG=true
FORCE_BUILD=false
ORIGINAL_ARGS=""

case "$1" in
    "--help"|"-h")
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --new-config           Don't preserve existing cluster configuration"
        echo "  --force-build          Force rebuild binary before restart"
        echo "  --help, -h             Show this help message"
        echo ""
        echo "The restart script will:"
        echo "1. Stop the existing cluster"
        echo "2. Preserve cluster configuration (unless --new-config is used)"
        echo "3. Start cluster with same or new configuration"
        echo ""
        echo "If no cluster info is found, you will be prompted to configure the cluster."
        exit 0
        ;;
esac

while [[ $# -gt 0 ]]; do
    case $1 in
        --new-config)
            PRESERVE_CONFIG=false
            shift
            ;;
        --force-build)
            FORCE_BUILD=true
            shift
            ;;
        *)
            # Pass unknown arguments to cluster-start.sh
            ORIGINAL_ARGS="$ORIGINAL_ARGS $1"
            shift
            ;;
    esac
done

log "Restarting Reservoir cluster..."

# Check if cluster is currently running
if [ -f ".cluster.info" ]; then
    source .cluster.info
    log "Found existing cluster: $CLUSTER_SIZE nodes"
    
    if [ "$PRESERVE_CONFIG" = "true" ]; then
        log "Preserving cluster configuration"
        
        # Save current config
        SAVED_CLUSTER_SIZE=$CLUSTER_SIZE
        SAVED_NODE_PREFIX=$NODE_PREFIX
        SAVED_BASE_PORT=$BASE_PORT
        SAVED_BASE_CLUSTER_PORT=$BASE_CLUSTER_PORT
        SAVED_BASE_MONITORING_PORT=$BASE_MONITORING_PORT
        SAVED_CLUSTER_ID=$CLUSTER_ID
    fi
else
    log "No existing cluster configuration found"
    PRESERVE_CONFIG=false
fi

# Stop existing cluster
log "Stopping existing cluster..."
./scripts/cluster-stop.sh

if [ $? -ne 0 ]; then
    error "Failed to stop existing cluster"
    exit 1
fi

# Wait a moment for ports to be released
sleep 2

# Start cluster with preserved or new configuration
if [ "$PRESERVE_CONFIG" = "true" ]; then
    log "Starting cluster with preserved configuration..."
    
    START_ARGS=""
    START_ARGS="$START_ARGS --size $SAVED_CLUSTER_SIZE"
    START_ARGS="$START_ARGS --base-port $SAVED_BASE_PORT"
    START_ARGS="$START_ARGS --base-cluster-port $SAVED_BASE_CLUSTER_PORT"
    START_ARGS="$START_ARGS --base-monitoring-port $SAVED_BASE_MONITORING_PORT"
    
    if [ -n "$SAVED_CLUSTER_ID" ]; then
        START_ARGS="$START_ARGS --cluster-id $SAVED_CLUSTER_ID"
    fi
    
    if [ "$FORCE_BUILD" = "true" ]; then
        START_ARGS="$START_ARGS --force-build"
    fi
    
    # Combine with any additional arguments passed to restart
    if [ -n "$ORIGINAL_ARGS" ]; then
        START_ARGS="$START_ARGS $ORIGINAL_ARGS"
    fi
    
    log "Starting with args: $START_ARGS"
    ./scripts/cluster-start.sh $START_ARGS
else
    log "Starting cluster with new configuration..."
    
    # Use original arguments or defaults
    START_ARGS="$ORIGINAL_ARGS"
    
    if [ "$FORCE_BUILD" = "true" ]; then
        START_ARGS="$START_ARGS --force-build"
    fi
    
    if [ -n "$START_ARGS" ]; then
        log "Starting with args: $START_ARGS"
        ./scripts/cluster-start.sh $START_ARGS
    else
        log "Starting with default configuration"
        ./scripts/cluster-start.sh
    fi
fi

if [ $? -eq 0 ]; then
    success "Cluster restarted successfully!"
    log ""
    log "Check cluster status: ./scripts/cluster-status.sh"
else
    error "Failed to restart cluster"
    exit 1
fi