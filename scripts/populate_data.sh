#!/bin/bash

HOST="localhost"
PORT="6379"
CLI="redis-cli -h $HOST -p $PORT"

echo "Populating Reservoir with test data..."

# users (Hashes)
echo "Creating User Profiles (Hash)..."
for i in {1..20}; do
    $CLI HSET "user:$i" name "User $i" email "user$i@example.com" role "user"
done

# sessions (Strings with TTL)
echo "Creating Active Sessions (String + TTL)..."
for i in {1..15}; do
    $CLI SET "session:$i" "active_session_data_payload_$i"
    $CLI EXPIRE "session:$i" 3600
done

# logs (Lists)
echo "Creating Application Logs (List)..."
for level in "info" "error" "debug"; do
    for i in {1..10}; do
        $CLI LPUSH "logs:$level" "Log entry $i: Something happened at $(date)"
    done
done

# tags (Sets)
echo "Creating Tags (Set)..."
$CLI SADD "tags:languages" "go" "rust" "cpp" "python" "javascript"
$CLI SADD "tags:databases" "redis" "postgres" "mysql" "mongodb" "reservoir"
$CLI SADD "tags:environment" "production" "staging" "development"

# large static content (String)
echo "Creating Large Static Content..."
$CLI SET "static:hero_image" "$(printf 'a%.0s' {1..5000})"
$CLI SET "static:config_json" "$(printf 'b%.0s' {1..2000})"

# Deferred commands
echo "Scheduling Deferred Commands..."
$CLI DEFER 300 SET "job:deferred_1" "pending"
$CLI DEFER 60 SET "job:deferred_2" "processing"

# Massive TTL data (Visual check)
echo "Creating Massive Temporary Cache (TTL)..."
for i in {1..200}; do
    TTL=$((10 + RANDOM % 600))
    $CLI SET "temp:massive:$i" "temporary_data_payload_$i"
    $CLI EXPIRE "temp:massive:$i" $TTL
done

# Massive Deferred commands (Visual check)
echo "Scheduling Massive Deferred Commands..."
for i in {1..100}; do
    DELAY=$((15 + RANDOM % 900))
    $CLI DEFER $DELAY SET "job:deferred_massive:$i" "pending_execution"
done

echo "Done! Data populated."
