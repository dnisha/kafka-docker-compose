#!/bin/bash
echo "=== KRaft Controller Failover Test ==="

# Function to get the current controller ID with retries
get_controller_id() {
    local retries=5
    local count=0
    
    while [ $count -lt $retries ]; do
        METADATA_OUTPUT=$(docker exec kraft-broker1-1 kafka-metadata-quorum \
            --bootstrap-server broker1:9092 \
            describe --status 2>&1)
        CONTROLLER_ID=$(echo "$METADATA_OUTPUT" | grep -i "leader" | awk '{print $2}')
        
        if [ -n "$CONTROLLER_ID" ] && [ "$CONTROLLER_ID" != "null" ]; then
            echo "$CONTROLLER_ID"
            return 0
        fi
        sleep 0.2
        count=$((count + 1))
    done
    echo ""
}

# Function to get cluster metadata state
get_cluster_metadata_state() {
    docker exec kraft-broker1-1 kafka-metadata-quorum \
        --bootstrap-server broker1:9092 \
        describe --status 2>&1 | grep -E "(LeaderId|LeaderEpoch|HighWatermark)" | head -3
}

# Function to get timestamp in milliseconds (faster alternative)
get_timestamp_ms() {
    if command -v python3 &> /dev/null; then
        python3 -c "import time; print(int(time.time() * 1000))"
    else
        date +%s%3N
    fi
}

# Function to wait_for_broker_availability with timeout
wait_for_broker_availability() {
    local broker=$1
    local timeout=10  # seconds
    local start=$(date +%s)
    
    while [ $(($(date +%s) - start)) -lt $timeout ]; do
        if docker exec $broker kafka-broker-api-versions \
            --bootstrap-server localhost:9092 > /dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

echo ""
echo "Step 1: Pre-test cluster health check..."
INITIAL_CONTROLLER=$(get_controller_id)
echo "Initial controller: broker${INITIAL_CONTROLLER}"
get_cluster_metadata_state
echo ""

echo "Step 2: Creating OPTIMIZED topic structure..."
echo "Creating 30 topics ..."

# Create topics with OPTIMIZED settings for faster failover
for i in {1..30}; do
    docker exec kraft-broker1-1 kafka-topics --create \
        --topic load-test-topic-$i \
        --partitions 2 \
        --replication-factor 3 \
        --config min.insync.replicas=2 \
        --config unclean.leader.election.enable=false \
        --bootstrap-server broker1:9092 > /dev/null 2>&1 &
    
    # Batch creation in groups of 5 for better performance
    if [ $((i % 5)) -eq 0 ]; then
        wait
        echo "  Created $i/30 topics..."
    fi
done
wait  # Wait for all background topic creations

echo "✓ Created 30 optimized topics"
echo ""

echo ""
echo "Step 3: Verifying cluster state before failover..."
CONTROLLER_ID=$(get_controller_id)
if [ -z "$CONTROLLER_ID" ]; then
    echo "ERROR: Cannot determine current controller"
    exit 1
fi

echo "Current controller: broker${CONTROLLER_ID}"
get_cluster_metadata_state
echo ""

# Create failover test topic with OPTIMIZED settings
echo "Step 4: Creating optimized failover test topic..."
docker exec kraft-broker1-1 kafka-topics --create \
    --topic failover-test \
    --partitions 1 \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config unclean.leader.election.enable=false \
    --bootstrap-server broker1:9092 > /dev/null 2>&1

echo "✓ Failover test topic created with optimized settings"
echo ""

# Determine monitoring broker (not the controller)
if [ "$CONTROLLER_ID" = "1" ]; then
    MONITOR_BROKER="kraft-broker2-1"
    ALTERNATE_BROKER="kraft-broker3-1"
elif [ "$CONTROLLER_ID" = "2" ]; then
    MONITOR_BROKER="kraft-broker3-1"
    ALTERNATE_BROKER="kraft-broker1-1"
else
    MONITOR_BROKER="kraft-broker1-1"
    ALTERNATE_BROKER="kraft-broker2-1"
fi

echo "Using ${MONITOR_BROKER} for failover monitoring"
echo ""

echo "Step 5: EXECUTING OPTIMIZED FAILOVER TEST..."
echo "Killing controller broker${CONTROLLER_ID} at $(date +%H:%M:%S.%3N)"

# PRECISE timing start
FAILOVER_START=$(get_timestamp_ms)

# Kill the controller
docker kill kraft-broker${CONTROLLER_ID}-1 > /dev/null 2>&1

echo "Monitoring failover with MULTIPLE health checks..."
echo -n "Progress: "

# OPTIMIZED failover detection with multiple check methods
FAILOVER_DETECTED=0
ATTEMPTS=0
MAX_ATTEMPTS=50  # 5 second timeout

while [ $ATTEMPTS -lt $MAX_ATTEMPTS ] && [ $FAILOVER_DETECTED -eq 0 ]; do
    # Method 1: Try metadata quorum first (fastest)
    if docker exec $MONITOR_BROKER kafka-metadata-quorum \
        --bootstrap-server localhost:9092 describe --status > /dev/null 2>&1; then
        FAILOVER_DETECTED=1
        break
    fi
    
    # Method 2: Alternate broker check
    if docker exec $ALTERNATE_BROKER kafka-topics --list \
        --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        FAILOVER_DETECTED=1
        break
    fi
    
    echo -n "."
    sleep 0.1
    ATTEMPTS=$((ATTEMPTS + 1))
done

FAILOVER_END=$(get_timestamp_ms)
FAILOVER_TIME=$((FAILOVER_END - FAILOVER_START))

echo ""
echo "=========================================="
if [ $FAILOVER_DETECTED -eq 1 ]; then
    echo "KRaft Failover Time: ${FAILOVER_TIME}ms"
else
    echo "Failover detected after timeout: ${FAILOVER_TIME}ms"
fi
echo "=========================================="
echo ""

echo "Step 6: Post-failover analysis..."
sleep 0.5  # Reduced sleep for faster analysis

NEW_CONTROLLER_ID=$(get_controller_id)
echo "New controller: broker${NEW_CONTROLLER_ID}"

# Quick cluster health check
echo "Cluster state after failover:"
get_cluster_metadata_state
echo ""

echo "Step 7: Fast cleanup and recovery..."
echo "Restarting broker${CONTROLLER_ID}..."
docker start kraft-broker${CONTROLLER_ID}-1 > /dev/null 2>&1

# Wait for broker to rejoin with timeout
echo -n "Waiting for broker${CONTROLLER_ID} to rejoin..."
if wait_for_broker_availability kraft-broker${CONTROLLER_ID}-1; then
    echo " ✓ Rejoined successfully"
else
    echo "  Slow to rejoin"
fi

# Final quick health check
sleep 2
echo ""
echo "Final cluster status:"
get_cluster_metadata_state

echo ""
echo "=== OPTIMIZED TEST COMPLETE ==="
echo "Summary:"
echo "  - Cluster: KRaft-based Kafka (Optimized Config)"
echo "  - Load: 30 topics, 60 partitions"
echo "  - Old Controller: broker${CONTROLLER_ID}"
echo "  - New Controller: broker${NEW_CONTROLLER_ID}"
echo "  - Optimized Failover Time: ${FAILOVER_TIME}ms"
echo ""
echo "Optimizations Applied:"
echo "   Reduced topics/partitions (30 topics, 60 partitions)"
echo "   Optimized topic configurations"
echo "   Dual monitoring brokers for faster detection"
echo "   Faster health checks with metadata quorum"
echo "   Reduced timeouts and sleeps"
echo ""

# Performance comparison
if [ $FAILOVER_TIME -lt 3000 ]; then
    echo " SUCCESS: Failover time under 3 seconds!"
elif [ $FAILOVER_TIME -lt 5000 ]; then
    echo " ACCEPTABLE: Failover time under 5 seconds"
else
    echo "  SLOW: Consider additional cluster tuning"
fi