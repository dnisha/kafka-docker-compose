#!/bin/bash
echo "=== ZooKeeper Controller Failover Test ==="

# Function to get the current controller ID
get_controller_id() {
    ZK_OUTPUT=$(docker exec zookeeper bash -c 'echo "get /controller" | zookeeper-shell localhost:2181' 2>/dev/null)
    CONTROLLER_ID=$(echo "$ZK_OUTPUT" | grep '"brokerid":' | cut -d: -f2 | cut -d, -f1 | tr -d '[:space:]"')
    echo "$CONTROLLER_ID"
}

echo ""
echo "Step 1: Creating topics..."
echo "Creating 30 topics (60 partitions total)..."

# Create 30 topics
for i in {1..30}; do
    docker exec broker1 kafka-topics --create \
        --topic load-test-topic-$i \
        --partitions 2 \
        --replication-factor 3 \
        --bootstrap-server broker1:29092 > /dev/null 2>&1
    
    if [ $((i % 10)) -eq 0 ]; then
        echo "  Created $i/30 topics..."
    fi
done

echo "✓ Created 30 topics with 60 partitions total"
echo ""

echo "Step 2: Finding current controller..."
CONTROLLER_ID=$(get_controller_id)

# Loop until a controller ID is found
while [ -z "$CONTROLLER_ID" ]; do
    echo "Waiting for ZooKeeper controller election..."
    sleep 1
    CONTROLLER_ID=$(get_controller_id)
done

echo "Current controller is: broker${CONTROLLER_ID}"
echo ""

echo "Step 3: Testing controller failover..."
echo "Killing controller broker${CONTROLLER_ID} container..."
START_TIME=$(date +%s%N | cut -b1-13)
docker kill broker${CONTROLLER_ID} > /dev/null 2>&1

# Wait for cluster recovery and measure time
echo "Waiting for new controller election and cluster availability..."
until docker exec broker1 kafka-topics --list --bootstrap-server broker1:29092 > /dev/null 2>&1; do
    sleep 0.2
done

END_TIME=$(date +%s%N | cut -b1-13)
FAILOVER_TIME=$((END_TIME - START_TIME))

echo ""
echo "=========================================="
echo "ZooKeeper-based failover time: ${FAILOVER_TIME}ms"
echo "=========================================="
echo ""

# Show the new controller
echo "Finding new controller..."
sleep 1
NEW_CONTROLLER_ID=$(get_controller_id)
echo "New controller is: broker${NEW_CONTROLLER_ID}"
echo ""

# Restart the killed broker for cleanup
echo "Step 4: Cleanup - Restarting the killed broker${CONTROLLER_ID}..."
docker start broker${CONTROLLER_ID} > /dev/null 2>&1

# Verify all brokers are back
sleep 5
echo "Verifying all brokers are registered..."
docker exec zookeeper bash -c 'echo "ls /brokers/ids" | zookeeper-shell localhost:2181' 2>&1 | grep "^\[" || echo "Brokers registering..."

echo ""
echo "=== Test Complete ==="
echo "Summary:"
echo "  - Cluster: ZooKeeper-based Kafka"
echo "  - Topics: 30 topics, 60 partitions"
echo "  - Old Controller: broker${CONTROLLER_ID}"
echo "  - New Controller: broker${NEW_CONTROLLER_ID}"
echo "  - Failover Time: ${FAILOVER_TIME}ms"
echo ""