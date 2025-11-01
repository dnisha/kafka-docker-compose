#!/bin/bash
echo "=== KRaft Controller Failover Test ==="

# Function to get the current controller ID
get_controller_id() {
    # Use kafka-metadata-quorum to get controller info (correct command for KRaft)
    METADATA_OUTPUT=$(docker exec kraft-broker1-1 kafka-metadata-quorum \
        --bootstrap-server localhost:9092 \
        describe --status 2>&1)
    
    # Extract leader ID from the output
    # Look for "LeaderId" or "Leader" in the output
    CONTROLLER_ID=$(echo "$METADATA_OUTPUT" | grep -i "leader" | grep -oE "[0-9]+" | head -1)
    
    echo "$CONTROLLER_ID"
}

# Function to get timestamp in milliseconds (uses Python)
get_timestamp_ms() {
    python3 -c "import time; print(int(time.time() * 1000))"
}

echo "Finding current controller..."
CONTROLLER_ID=$(get_controller_id)

# Loop until a controller ID is found
while [ -z "$CONTROLLER_ID" ]; do
    echo "Waiting for KRaft controller election..."
    sleep 1
    CONTROLLER_ID=$(get_controller_id)
done

echo "Current controller is: broker${CONTROLLER_ID}"

# Create test topic
echo "Creating test topic..."
docker exec kraft-broker1-1 kafka-topics --create \
  --topic failover-test \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server localhost:9092

# Determine test broker (not the controller)
if [ "$CONTROLLER_ID" = "1" ]; then
    TEST_BROKER="kraft-broker2-1"
elif [ "$CONTROLLER_ID" = "2" ]; then
    TEST_BROKER="kraft-broker3-1"
else
    TEST_BROKER="kraft-broker1-1"
fi

# Start timing and kill controller's container
echo "Killing controller broker${CONTROLLER_ID} container..."
START_TIME=$(get_timestamp_ms)
docker kill kraft-broker${CONTROLLER_ID}-1

# Wait for cluster recovery and measure time
echo "Waiting for new controller election and cluster availability..."
until docker exec $TEST_BROKER kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    sleep 0.1
done

END_TIME=$(get_timestamp_ms)
FAILOVER_TIME=$((END_TIME - START_TIME))

echo "KRaft-based failover time: ${FAILOVER_TIME}ms"

# Show the new controller
echo "Finding new controller..."
sleep 1
NEW_CONTROLLER_ID=$(get_controller_id)
echo "New controller is: broker${NEW_CONTROLLER_ID}"

# Restart the killed broker for cleanup
echo "Restarting the killed broker${CONTROLLER_ID}..."
docker start kraft-broker${CONTROLLER_ID}-1

# Verify all brokers are back
sleep 5
echo "Verifying all brokers are available:"
docker exec $TEST_BROKER kafka-broker-api-versions \
    --bootstrap-server localhost:9092 | head -10