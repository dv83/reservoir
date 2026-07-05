#!/bin/bash
# Show Reservoir cluster status

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[CLUSTER-STATUS]${NC} $1"
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
DETAILED=false
JSON_OUTPUT=false
CONTINUOUS=false

case "$1" in
    "--help"|"-h")
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --detailed, -d         Show detailed node information"
        echo "  --json, -j             Output in JSON format"
        echo "  --continuous, -c       Continuously monitor (Ctrl+C to stop)"
        echo "  --help, -h             Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                     # Basic cluster status"
        echo "  $0 --detailed          # Detailed status with metrics"
        echo "  $0 --json              # JSON output for scripting"
        echo "  $0 --continuous        # Live monitoring"
        exit 0
        ;;
esac

while [[ $# -gt 0 ]]; do
    case $1 in
        --detailed|-d)
            DETAILED=true
            shift
            ;;
        --json|-j)
            JSON_OUTPUT=true
            shift
            ;;
        --continuous|-c)
            CONTINUOUS=true
            shift
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Function to get cluster status
get_cluster_status() {
    local timestamp=$(date -Iseconds)
    
    if [ ! -f ".cluster.info" ]; then
        if [ "$JSON_OUTPUT" = "true" ]; then
            echo '{"error": "No cluster configuration found", "timestamp": "'$timestamp'"}'
        else
            error "No cluster configuration found"
            log "Use './scripts/cluster-start.sh' to start a cluster"
        fi
        return 1
    fi
    
    source .cluster.info
    
    if [ "$JSON_OUTPUT" = "true" ]; then
        echo -n '{"cluster": {"size": '$CLUSTER_SIZE', "timestamp": "'$timestamp'", "nodes": ['
    else
        log "Cluster Status - $(date)"
        log "================================"
        log "Cluster Size: $CLUSTER_SIZE nodes"
        log "Started: $STARTED_AT"
        log ""
    fi
    
    local running_count=0
    local leader_count=0
    local leader_node=""
    
    for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
        local node_name="${NODE_PREFIX}${i}"
        local redis_port=$((BASE_PORT + i))
        local cluster_port=$((BASE_CLUSTER_PORT + i))
        local monitoring_port=$((BASE_MONITORING_PORT + i))
        local pid_file="pids/${node_name}.pid"
        
        local node_status="UNKNOWN"
        local is_running=false
        local is_leader=false
        local redis_responding=false
        local monitoring_responding=false
        local cluster_info=""
        
        # Check if process is running
        if [ -f "$pid_file" ]; then
            local node_pid=$(cat "$pid_file")
            if kill -0 $node_pid 2>/dev/null; then
                is_running=true
                node_status="RUNNING"
                running_count=$((running_count + 1))
            else
                node_status="DEAD"
            fi
        else
            node_status="NOT_STARTED"
        fi
        
        # Check Redis connectivity
        if [ "$is_running" = "true" ]; then
            if redis-cli -p $redis_port ping > /dev/null 2>&1; then
                redis_responding=true
            fi
            
            # Check monitoring API
            if curl -s --connect-timeout 2 "http://localhost:$monitoring_port/api/cluster/status" > /dev/null 2>&1; then
                monitoring_responding=true
                
                # Get cluster information
                cluster_info=$(curl -s --connect-timeout 2 "http://localhost:$monitoring_port/api/cluster/status" 2>/dev/null || echo '{}')
                
                # Check if this node is the leader
                if echo "$cluster_info" | grep -q '"is_leader":true' 2>/dev/null; then
                    is_leader=true
                    leader_count=$((leader_count + 1))
                    leader_node="$node_name"
                fi
            fi
        fi
        
        if [ "$JSON_OUTPUT" = "true" ]; then
            if [ $i -gt 0 ]; then echo -n ", "; fi
            echo -n '{"name": "'$node_name'", "redis_port": '$redis_port', "cluster_port": '$cluster_port', "monitoring_port": '$monitoring_port', "status": "'$node_status'", "is_running": '$is_running', "is_leader": '$is_leader', "redis_responding": '$redis_responding', "monitoring_responding": '$monitoring_responding'}'
        else
            local status_color=$RED
            if [ "$is_running" = "true" ]; then
                if [ "$redis_responding" = "true" ] && [ "$monitoring_responding" = "true" ]; then
                    status_color=$GREEN
                else
                    status_color=$YELLOW
                fi
            fi
            
            local leader_indicator=""
            if [ "$is_leader" = "true" ]; then
                leader_indicator=" ${GREEN}[LEADER]${NC}"
            fi
            
            echo -e "${status_color}$node_name${NC}$leader_indicator"
            echo "  Status: $node_status"
            echo "  Redis: $redis_port (responding: $redis_responding)"
            echo "  Cluster: $cluster_port"
            echo "  Monitoring: $monitoring_port (responding: $monitoring_responding)"
            
            if [ "$DETAILED" = "true" ] && [ "$monitoring_responding" = "true" ]; then
                local current_term=$(echo "$cluster_info" | grep -o '"current_term":[0-9]*' | cut -d: -f2 2>/dev/null || echo "0")
                local active_nodes=$(echo "$cluster_info" | grep -o '"active_nodes":[0-9]*' | cut -d: -f2 2>/dev/null || echo "0")
                local messages_sent=$(echo "$cluster_info" | grep -o '"messages_sent":[0-9]*' | cut -d: -f2 2>/dev/null || echo "0")
                local messages_received=$(echo "$cluster_info" | grep -o '"messages_received":[0-9]*' | cut -d: -f2 2>/dev/null || echo "0")
                
                echo "  Term: $current_term"
                echo "  Active Nodes: $active_nodes"
                echo "  Messages Sent: $messages_sent"
                echo "  Messages Received: $messages_received"
            fi
            
            echo ""
        fi
    done
    
    if [ "$JSON_OUTPUT" = "true" ]; then
        echo '], "summary": {"running": '$running_count', "leaders": '$leader_count', "leader_node": "'$leader_node'"}}'
    else
        log "Summary:"
        log "  Running nodes: $running_count/$CLUSTER_SIZE"
        log "  Leaders: $leader_count"
        
        if [ $leader_count -eq 0 ]; then
            warn "No leader elected - cluster may be starting up or have issues"
        elif [ $leader_count -gt 1 ]; then
            error "Multiple leaders detected - split brain condition!"
        else
            success "Cluster has a leader: $leader_node"
        fi
        
        if [ $running_count -eq 0 ]; then
            error "No nodes are running"
        elif [ $running_count -lt $CLUSTER_SIZE ]; then
            warn "Some nodes are not running ($running_count/$CLUSTER_SIZE)"
        else
            success "All nodes are running"
        fi
        
        log ""
        log "Management commands:"
        log "  Stop cluster: ./scripts/cluster-stop.sh"
        log "  Restart cluster: ./scripts/cluster-restart.sh"
        log "  View logs: tail -f logs/cluster/*.log"
        
        if [ $running_count -gt 0 ]; then
            log "  Test connection: redis-cli -p $BASE_PORT ping"
            log "  Monitoring: http://localhost:$BASE_MONITORING_PORT/cluster"
        fi
    fi
}

# Main execution
if [ "$CONTINUOUS" = "true" ]; then
    if [ "$JSON_OUTPUT" = "true" ]; then
        error "Continuous mode not supported with JSON output"
        exit 1
    fi
    
    log "Starting continuous monitoring (Ctrl+C to stop)..."
    log ""
    
    while true; do
        clear
        get_cluster_status
        sleep 5
    done
else
    get_cluster_status
fi