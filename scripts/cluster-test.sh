#!/bin/bash
# Test Reservoir cluster functionality

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[CLUSTER-TEST]${NC} $1"
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

info() {
    echo -e "${CYAN}[INFO]${NC} $1"
}

# Parse command line arguments
TEST_REPLICATION=true
TEST_FAILOVER=false
TEST_PERFORMANCE=false
VERBOSE=false

case "$1" in
    "--help"|"-h")
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --no-replication       Skip replication tests"
        echo "  --test-failover        Test leader failover (stops/starts nodes)"
        echo "  --test-performance     Run performance benchmarks"
        echo "  --verbose, -v          Verbose output"
        echo "  --help, -h             Show this help message"
        echo ""
        echo "This script tests various cluster functionality:"
        echo "  - Basic connectivity"
        echo "  - Data replication between nodes"
        echo "  - Leader election"
        echo "  - Failover scenarios (if enabled)"
        echo "  - Performance benchmarks (if enabled)"
        exit 0
        ;;
esac

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-replication)
            TEST_REPLICATION=false
            shift
            ;;
        --test-failover)
            TEST_FAILOVER=true
            shift
            ;;
        --test-performance)
            TEST_PERFORMANCE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if cluster is running
if [ ! -f ".cluster.info" ]; then
    error "No cluster configuration found"
    log "Start a cluster first: ./scripts/cluster-start.sh"
    exit 1
fi

source .cluster.info

log "Testing $CLUSTER_SIZE-node Reservoir cluster..."
log "Started: $STARTED_AT"
log ""

# Test 1: Basic connectivity
log "Test 1: Basic Connectivity"
log "=========================="

CONNECTED_NODES=0
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    redis_port=$((BASE_PORT + i))
    node_name="${NODE_PREFIX}${i}"

    if redis-cli -p $redis_port ping > /dev/null 2>&1; then
        success "$node_name (port $redis_port): Connected"
        CONNECTED_NODES=$((CONNECTED_NODES + 1))
    else
        error "$node_name (port $redis_port): Not responding"
    fi
done

if [ $CONNECTED_NODES -eq $CLUSTER_SIZE ]; then
    success "All nodes responding ($CONNECTED_NODES/$CLUSTER_SIZE)"
else
    error "Some nodes not responding ($CONNECTED_NODES/$CLUSTER_SIZE)"
    exit 1
fi

log ""

# Test 2: Leader election
log "Test 2: Leader Election"
log "======================="

LEADER_COUNT=0
LEADER_NODE=""
LEADER_PORT=""

for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    monitoring_port=$((BASE_MONITORING_PORT + i))
    node_name="${NODE_PREFIX}${i}"

    # Check if this node is the leader (must have both is_current_node AND is_leader true)
    if curl -s --connect-timeout 2 "http://localhost:$monitoring_port/api/cluster/status" | grep -q '"is_current_node":true,"is_leader":true' 2>/dev/null; then
        LEADER_COUNT=$((LEADER_COUNT + 1))
        LEADER_NODE="$node_name"
        LEADER_PORT=$((BASE_PORT + i))
        success "Leader found: $node_name (port $((BASE_PORT + i)))"
    fi
done

if [ $LEADER_COUNT -eq 1 ]; then
    success "Healthy leader election: 1 leader"
elif [ $LEADER_COUNT -eq 0 ]; then
    error "No leader found - cluster may be starting up"
    exit 1
else
    error "Multiple leaders found: $LEADER_COUNT (split brain!)"
    exit 1
fi

log ""

# Test 3: Data replication
if [ "$TEST_REPLICATION" = "true" ]; then
    log "Test 3: Data Replication"
    log "========================"
    
    TEST_KEY="cluster_test_$(date +%s)"
    TEST_VALUE="test_value_$(date -Iseconds)"
    
    log "Writing test data to leader ($LEADER_NODE)..."
    if redis-cli -p $LEADER_PORT set "$TEST_KEY" "$TEST_VALUE" > /dev/null 2>&1; then
        success "Data written to leader"
    else
        error "Failed to write data to leader"
        exit 1
    fi
    
    # Wait for replication
    log "Waiting for replication (3 seconds)..."
    sleep 3
    
    # Test replication on all nodes
    REPLICATED_NODES=0
    for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
        redis_port=$((BASE_PORT + i))
        node_name="${NODE_PREFIX}${i}"

        RETRIEVED_VALUE=$(redis-cli -p $redis_port get "$TEST_KEY" 2>/dev/null || echo "")
        
        if [ "$RETRIEVED_VALUE" = "$TEST_VALUE" ]; then
            success "$node_name: Data replicated correctly"
            REPLICATED_NODES=$((REPLICATED_NODES + 1))
        else
            if [ "$VERBOSE" = "true" ]; then
                error "$node_name: Data not replicated (got: '$RETRIEVED_VALUE', expected: '$TEST_VALUE')"
            else
                error "$node_name: Data not replicated"
            fi
        fi
    done
    
    if [ $REPLICATED_NODES -eq $CLUSTER_SIZE ]; then
        success "Replication working: $REPLICATED_NODES/$CLUSTER_SIZE nodes have correct data"
    else
        warn "Partial replication: $REPLICATED_NODES/$CLUSTER_SIZE nodes have correct data"
        log "This may be normal if replication is still in progress"
    fi
    
    # Cleanup test data
    redis-cli -p $LEADER_PORT del "$TEST_KEY" > /dev/null 2>&1 || true
    
    log ""
fi

# Test 4: Performance benchmarks
if [ "$TEST_PERFORMANCE" = "true" ]; then
    log "Test 4: Performance Benchmarks"
    log "=============================="
    
    log "Running SET benchmark on leader ($LEADER_NODE)..."
    redis-benchmark -p $LEADER_PORT -t set -n 10000 -q || warn "SET benchmark failed"
    
    log "Running GET benchmark on leader ($LEADER_NODE)..."
    redis-benchmark -p $LEADER_PORT -t get -n 10000 -q || warn "GET benchmark failed"
    
    log "Running PING benchmark on leader ($LEADER_NODE)..."
    redis-benchmark -p $LEADER_PORT -t ping -n 10000 -q || warn "PING benchmark failed"
    
    log ""
fi

# Test 5: Failover (optional)
if [ "$TEST_FAILOVER" = "true" ] && [ $CLUSTER_SIZE -gt 1 ]; then
    log "Test 5: Leader Failover"
    log "======================"
    
    warn "This test will temporarily stop the leader node to test failover"
    log "Press Ctrl+C within 5 seconds to skip this test..."
    
    for i in {5..1}; do
        echo -n "$i... "
        sleep 1
    done
    echo ""
    
    # Find leader's PID and stop it
    LEADER_INDEX=${LEADER_NODE#$NODE_PREFIX}
    LEADER_PID_FILE="pids/${LEADER_NODE}.pid"
    
    if [ -f "$LEADER_PID_FILE" ]; then
        LEADER_PID=$(cat "$LEADER_PID_FILE")
        log "Stopping leader $LEADER_NODE (PID: $LEADER_PID)..."
        
        kill $LEADER_PID 2>/dev/null || true
        
        # Wait for new leader election
        log "Waiting for new leader election (15 seconds)..."
        sleep 15
        
        # Check for new leader
        NEW_LEADER_COUNT=0
        NEW_LEADER_NODE=""
        
        for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
            if [ $i -eq $LEADER_INDEX ]; then
                continue  # Skip the stopped node
            fi

            monitoring_port=$((BASE_MONITORING_PORT + i))
            node_name="${NODE_PREFIX}${i}"

            # Check if this node is the leader (must have both is_current_node AND is_leader true)
            if curl -s --connect-timeout 2 "http://localhost:$monitoring_port/api/cluster/status" | grep -q '"is_current_node":true,"is_leader":true' 2>/dev/null; then
                NEW_LEADER_COUNT=$((NEW_LEADER_COUNT + 1))
                NEW_LEADER_NODE="$node_name"
                success "New leader elected: $node_name"
            fi
        done
        
        if [ $NEW_LEADER_COUNT -eq 1 ]; then
            success "Failover successful: new leader is $NEW_LEADER_NODE"
        else
            error "Failover failed: $NEW_LEADER_COUNT leaders found"
        fi
        
        # Restart the stopped node
        log "Restarting stopped node $LEADER_NODE..."
        redis_port=$((BASE_PORT + LEADER_INDEX))
        cluster_port=$((BASE_CLUSTER_PORT + LEADER_INDEX))
        monitoring_port=$((BASE_MONITORING_PORT + LEADER_INDEX))

        ARGS="--port $redis_port"
        ARGS="$ARGS --log-level info"
        ARGS="$ARGS --monitoring-enabled --monitoring-addr :$monitoring_port"
        ARGS="$ARGS --cluster-enabled"
        ARGS="$ARGS --cluster-node-addr 127.0.0.1"
        ARGS="$ARGS --cluster-node-port $cluster_port"
        ARGS="$ARGS --cluster-seed-nodes 127.0.0.1:$BASE_CLUSTER_PORT"
        
        if [ -n "$CLUSTER_ID" ]; then
            ARGS="$ARGS --cluster-id $CLUSTER_ID"
        fi
        
        nohup ./reservoir $ARGS > logs/cluster/${LEADER_NODE}.log 2>&1 &
        NEW_PID=$!
        echo $NEW_PID > "$LEADER_PID_FILE"
        
        sleep 3
        
        if kill -0 $NEW_PID 2>/dev/null; then
            success "Node $LEADER_NODE restarted successfully (PID: $NEW_PID)"
        else
            error "Failed to restart node $LEADER_NODE"
        fi
    else
        error "Could not find PID file for leader $LEADER_NODE"
    fi
    
    log ""
fi

# Final summary
log "Test Summary"
log "============"
success "Connectivity: $CONNECTED_NODES/$CLUSTER_SIZE nodes responding"
success "Leadership: $LEADER_COUNT leader(s) elected"

if [ "$TEST_REPLICATION" = "true" ]; then
    if [ $REPLICATED_NODES -eq $CLUSTER_SIZE ]; then
        success "Replication: Working correctly"
    else
        warn "Replication: Partial ($REPLICATED_NODES/$CLUSTER_SIZE nodes)"
    fi
fi

if [ "$TEST_PERFORMANCE" = "true" ]; then
    success "Performance: Benchmarks completed"
fi

if [ "$TEST_FAILOVER" = "true" ]; then
    success "Failover: Tested (check results above)"
fi

log ""
success "Cluster testing completed!"
log "Use './scripts/cluster-status.sh' for detailed cluster status"