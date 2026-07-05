#!/bin/bash
# Restart Reservoir server

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[RESTART]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log "Restarting Reservoir server..."

# Always rebuild to ensure latest code
log "Building latest code..."
go build -o reservoir ./cmd/reservoir
if [ $? -ne 0 ]; then
    error "Failed to build reservoir binary"
    exit 1
fi

# Stop the server
log "Stopping server..."
./scripts/stop.sh

# Wait for port to be free (up to 10 seconds)
PORT=${PORT:-6379}
log "Waiting for port $PORT to be free..."
for i in {1..10}; do
    if ! lsof -i :$PORT > /dev/null 2>&1; then
        log "Port $PORT is now free"
        break
    fi
    if [ $i -eq 10 ]; then
        error "Port $PORT is still in use after 10 seconds"
        exit 1
    fi
    sleep 1
done

# Start the server
log "Starting server..."
./scripts/start.sh

if [ $? -eq 0 ]; then
    success "Server restarted successfully"
else
    error "Failed to restart server"
    exit 1
fi