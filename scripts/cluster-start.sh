#!/bin/bash
# Start Reservoir cluster

set -e

# Configuration defaults
CLUSTER_SIZE=${CLUSTER_SIZE:-3}
NODE_PREFIX=${NODE_PREFIX:-node}
BASE_PORT=${BASE_PORT:-6379}
BASE_CLUSTER_PORT=${BASE_CLUSTER_PORT:-7379}
BASE_MONITORING_PORT=${BASE_MONITORING_PORT:-8080}
LOG_LEVEL=${LOG_LEVEL:-info}
CLUSTER_ID=${CLUSTER_ID:-""}
FORCE_BUILD=${FORCE_BUILD:-false}
WAIT_FOR_CONVERGENCE=${WAIT_FOR_CONVERGENCE:-true}
COMMIT_LOG_ENABLED=${COMMIT_LOG_ENABLED:-true}
COMMIT_LOG_DIR=${COMMIT_LOG_DIR:-"./data/cluster/commitlog"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[CLUSTER-START]${NC} $1"
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

# Check if binary needs building
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
    log "Building reservoir binary with cluster support..."
    rm -f ./reservoir
    
    go build -o reservoir ./cmd/reservoir
    if [ $? -ne 0 ]; then
        error "Failed to build reservoir binary"
        exit 1
    fi
    success "Binary built successfully"
fi

# Parse command line arguments
case "$1" in
    "--help"|"-h")
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --size N                   Number of nodes (default: 3)"
        echo "  --base-port N              Base Redis port (default: 6379)"
        echo "  --base-cluster-port N      Base cluster port (default: 7379)"
        echo "  --base-monitoring-port N   Base monitoring port (default: 8080)"
        echo "  --cluster-id ID            Cluster ID (auto-generated if empty)"
        echo "  --log-level LEVEL          Log level (default: info)"
        echo "  --no-wait                  Don't wait for cluster convergence"
        echo "  --build, --force-build     Force rebuild binary before starting"
        echo ""
        echo "Environment variables:"
        echo "  CLUSTER_SIZE              Same as --size"
        echo "  BASE_PORT                 Same as --base-port"
        echo "  BASE_CLUSTER_PORT         Same as --base-cluster-port"
        echo "  BASE_MONITORING_PORT      Same as --base-monitoring-port"
        echo "  LOG_LEVEL                 Same as --log-level"
        echo "  CLUSTER_ID                Same as --cluster-id"
        echo ""
        echo "Examples:"
        echo "  $0                        # Start 3-node cluster with defaults"
        echo "  $0 --size 5               # Start 5-node cluster"
        echo "  $0 --cluster-id my-cluster # Use specific cluster ID"
        exit 0
        ;;
esac

# Parse command line options
while [[ $# -gt 0 ]]; do
    case $1 in
        --size)
            CLUSTER_SIZE="$2"
            shift 2
            ;;
        --base-port)
            BASE_PORT="$2"
            shift 2
            ;;
        --base-cluster-port)
            BASE_CLUSTER_PORT="$2"
            shift 2
            ;;
        --base-monitoring-port)
            BASE_MONITORING_PORT="$2"
            shift 2
            ;;
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --no-wait)
            WAIT_FOR_CONVERGENCE=false
            shift
            ;;
        --build|--force-build)
            FORCE_BUILD=true
            shift
            ;;
        --commit-log-enabled)
            COMMIT_LOG_ENABLED=true
            shift
            ;;
        --no-commit-log)
            COMMIT_LOG_ENABLED=false
            shift
            ;;
        --commit-log-dir)
            COMMIT_LOG_DIR="$2"
            shift 2
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate cluster size
if [ "$CLUSTER_SIZE" -lt 1 ]; then
    error "Cluster size must be at least 1"
    exit 1
fi

if [ "$CLUSTER_SIZE" -gt 10 ]; then
    warn "Large cluster size ($CLUSTER_SIZE) may require port adjustments"
fi

log "Starting $CLUSTER_SIZE-node Reservoir cluster..."
log "Redis ports: $BASE_PORT-$((BASE_PORT + CLUSTER_SIZE - 1))"
log "Cluster ports: $BASE_CLUSTER_PORT-$((BASE_CLUSTER_PORT + CLUSTER_SIZE - 1))"
log "Monitoring ports: $BASE_MONITORING_PORT-$((BASE_MONITORING_PORT + CLUSTER_SIZE - 1))"

# Check for port conflicts
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    REDIS_PORT=$((BASE_PORT + i))
    CLUSTER_PORT=$((BASE_CLUSTER_PORT + i))
    MONITORING_PORT=$((BASE_MONITORING_PORT + i))
    
    if lsof -i :$REDIS_PORT > /dev/null 2>&1; then
        error "Redis port $REDIS_PORT is already in use"
        exit 1
    fi
    
    if lsof -i :$CLUSTER_PORT > /dev/null 2>&1; then
        error "Cluster port $CLUSTER_PORT is already in use"
        exit 1
    fi
    
    if lsof -i :$MONITORING_PORT > /dev/null 2>&1; then
        error "Monitoring port $MONITORING_PORT is already in use"
        exit 1
    fi
done

# Create directories
mkdir -p logs/cluster
mkdir -p pids

# Create commit log directories for each node if enabled
if [ "$COMMIT_LOG_ENABLED" = "true" ]; then
    for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
        mkdir -p "${COMMIT_LOG_DIR}/${NODE_PREFIX}${i}"
    done
    log "Created commit log directories in $COMMIT_LOG_DIR"
fi

# Generate cluster ID if not provided
if [ -z "$CLUSTER_ID" ]; then
    CLUSTER_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
    log "Generated new cluster ID: $CLUSTER_ID"
fi

# Start first node (bootstrap node)
log "Starting bootstrap node (${NODE_PREFIX}0)..."

REDIS_PORT=$BASE_PORT
CLUSTER_PORT=$BASE_CLUSTER_PORT
MONITORING_PORT=$BASE_MONITORING_PORT

ARGS="--port $REDIS_PORT"
ARGS="$ARGS --log-level $LOG_LEVEL"
ARGS="$ARGS --ui-port :$MONITORING_PORT"
ARGS="$ARGS --cluster-enabled"
ARGS="$ARGS --cluster-node-addr 127.0.0.1"
ARGS="$ARGS --cluster-node-port $CLUSTER_PORT"

if [ -n "$CLUSTER_ID" ]; then
    ARGS="$ARGS --cluster-id $CLUSTER_ID"
fi

# Add commit log configuration for bootstrap node
if [ "$COMMIT_LOG_ENABLED" = "true" ]; then
    ARGS="$ARGS --commit-log-enabled --commit-log-dir ${COMMIT_LOG_DIR}/${NODE_PREFIX}0"
fi

# Start bootstrap node
nohup ./reservoir $ARGS > logs/cluster/${NODE_PREFIX}0.log 2>&1 &
BOOTSTRAP_PID=$!
echo $BOOTSTRAP_PID > pids/${NODE_PREFIX}0.pid

# Wait for bootstrap node to start
sleep 3

if ! kill -0 $BOOTSTRAP_PID 2>/dev/null; then
    error "Failed to start bootstrap node. Check logs/cluster/${NODE_PREFIX}0.log"
    exit 1
fi

success "Bootstrap node started (PID: $BOOTSTRAP_PID, Redis: $REDIS_PORT, Cluster: $CLUSTER_PORT, Monitoring: $MONITORING_PORT)"

# Start remaining nodes
SEED_NODE="127.0.0.1:$BASE_CLUSTER_PORT"

for i in $(seq 1 $((CLUSTER_SIZE - 1))); do
    log "Starting ${NODE_PREFIX}${i}..."
    
    REDIS_PORT=$((BASE_PORT + i))
    CLUSTER_PORT=$((BASE_CLUSTER_PORT + i))
    MONITORING_PORT=$((BASE_MONITORING_PORT + i))
    
    ARGS="--port $REDIS_PORT"
    ARGS="$ARGS --log-level $LOG_LEVEL"
    ARGS="$ARGS --ui-port :$MONITORING_PORT"
    ARGS="$ARGS --cluster-enabled"
    ARGS="$ARGS --cluster-node-addr 127.0.0.1"
    ARGS="$ARGS --cluster-node-port $CLUSTER_PORT"
    ARGS="$ARGS --cluster-seed-nodes $SEED_NODE"
    
    if [ -n "$CLUSTER_ID" ]; then
        ARGS="$ARGS --cluster-id $CLUSTER_ID"
    fi
    
    # Add commit log configuration for this node
    if [ "$COMMIT_LOG_ENABLED" = "true" ]; then
        ARGS="$ARGS --commit-log-enabled --commit-log-dir ${COMMIT_LOG_DIR}/${NODE_PREFIX}${i}"
    fi
    
    nohup ./reservoir $ARGS > logs/cluster/${NODE_PREFIX}${i}.log 2>&1 &
    NODE_PID=$!
    echo $NODE_PID > pids/${NODE_PREFIX}${i}.pid
    
    # Wait for node to start
    sleep 2
    
    if ! kill -0 $NODE_PID 2>/dev/null; then
        error "Failed to start ${NODE_PREFIX}${i}. Check logs/cluster/${NODE_PREFIX}${i}.log"
        # Clean up started nodes
        ./scripts/cluster-stop.sh > /dev/null 2>&1 || true
        exit 1
    fi
    
    success "${NODE_PREFIX}${i} started (PID: $NODE_PID, Redis: $REDIS_PORT, Cluster: $CLUSTER_PORT, Monitoring: $MONITORING_PORT)"
done

# Create cluster info file
cat > .cluster.info << EOF
CLUSTER_SIZE=$CLUSTER_SIZE
NODE_PREFIX=$NODE_PREFIX
BASE_PORT=$BASE_PORT
BASE_CLUSTER_PORT=$BASE_CLUSTER_PORT
BASE_MONITORING_PORT=$BASE_MONITORING_PORT
CLUSTER_ID=$CLUSTER_ID
STARTED_AT=$(date -Iseconds)
EOF

success "Cluster started successfully!"
log ""
log "Cluster Information:"
log "  Nodes: $CLUSTER_SIZE"
log "  Redis ports: $BASE_PORT-$((BASE_PORT + CLUSTER_SIZE - 1))"
log "  Cluster ports: $BASE_CLUSTER_PORT-$((BASE_CLUSTER_PORT + CLUSTER_SIZE - 1))"
log "  Monitoring ports: $BASE_MONITORING_PORT-$((BASE_MONITORING_PORT + CLUSTER_SIZE - 1))"
if [ "$COMMIT_LOG_ENABLED" = "true" ]; then
    log "  Commit log: enabled, isolated per node in $COMMIT_LOG_DIR"
else
    log "  Commit log: disabled"
fi
log ""
log "Test connections:"
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    REDIS_PORT=$((BASE_PORT + i))
    MONITORING_PORT=$((BASE_MONITORING_PORT + i))
    log "  ${NODE_PREFIX}${i}: redis-cli -p $REDIS_PORT ping"
    log "  ${NODE_PREFIX}${i} monitoring: http://localhost:$MONITORING_PORT/cluster"
done
log ""
log "Management:"
log "  Status: ./scripts/cluster-status.sh"
log "  Stop: ./scripts/cluster-stop.sh"
log "  Restart: ./scripts/cluster-restart.sh"

# Wait for cluster convergence if requested
if [ "$WAIT_FOR_CONVERGENCE" = "true" ] && [ "$CLUSTER_SIZE" -gt 1 ]; then
    log ""
    log "Waiting for cluster convergence..."
    
    # Wait up to 30 seconds for leader election
    for attempt in $(seq 1 30); do
        sleep 1
        
        # Check if we have a leader by testing the first node's API
        if curl -s "http://localhost:$BASE_MONITORING_PORT/api/cluster/status" | grep -q '"is_leader":true' 2>/dev/null; then
            success "Cluster converged - leader elected"
            
            # Test replication
            log "Testing cluster replication..."
            TEST_KEY="cluster_test_$(date +%s)"
            TEST_VALUE="test_value_$(date +%s)"
            
            # Write to first node
            if redis-cli -p $BASE_PORT set "$TEST_KEY" "$TEST_VALUE" > /dev/null 2>&1; then
                sleep 1
                
                # Check on second node if available
                if [ "$CLUSTER_SIZE" -gt 1 ]; then
                    SECOND_PORT=$((BASE_PORT + 1))
                    if redis-cli -p $SECOND_PORT get "$TEST_KEY" | grep -q "$TEST_VALUE" 2>/dev/null; then
                        success "Replication working - data synchronized across nodes"
                    else
                        warn "Replication test inconclusive - nodes may still be synchronizing"
                    fi
                fi
            fi
            
            break
        fi
        
        if [ $attempt -eq 30 ]; then
            warn "Cluster convergence timeout - nodes may still be electing leader"
            log "Check cluster status with: ./scripts/cluster-status.sh"
        fi
    done
fi

log ""
success "Cluster startup complete!"