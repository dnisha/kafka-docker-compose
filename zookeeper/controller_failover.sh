#!/bin/bash
echo "=== ZooKeeper Controller Failover Test ==="

# Function to get the current controller ID reliably using basic tools and single-line command
get_controller_id() {
    # Execute zookeeper-shell commands in a single line within the container
    # This avoids issues with multiline <<EOF redirection across docker exec
    ZK_OUTPUT=$(docker exec zookeeper bash -c 'echo "get /controller" | zookeeper-shell localhost:2181')
    
    # Use grep/cut/tr to reliably extract the brokerid from the JSON output
    CONTROLLER_ID=$(echo "$ZK_OUTPUT" | grep '"brokerid":' | cut -d: -f2 | cut -d, -f1 | tr -d '[:space:]"')

    echo "$CONTROLLER_ID"
}

echo "Finding current controller..."
CONTROLLER_ID=$(get_controller_id)

# Loop until a controller ID is found
while [ -z "$CONTROLLER_ID" ]; do
    echo "Waiting for ZooKeeper controller election..."
    sleep 1
    CONTROLLER_ID=$(get_controller_id)
done

echo "Current controller is: broker${CONTROLLER_ID}"

# Create test topic
echo "Creating test topic..."
docker exec broker1 kafka-topics --create \
  --topic failover-test \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server broker1:29092

# Start timing and kill controller's container
echo "Killing controller broker${CONTROLLER_ID} container..."
START_TIME=$(date +%s%N | cut -b1-13) # Get current time in milliseconds
docker kill broker${CONTROLLER_ID}

# Wait for cluster recovery and measure time
echo "Waiting for new controller election and cluster availability..."
until docker exec broker1 kafka-topics --list --bootstrap-server broker1:29092 > /dev/null 2>&1; do
    sleep 0.2
done

END_TIME=$(date +%s%N | cut -b1-13) # Get current time in milliseconds
FAILOVER_TIME=$((END_TIME - START_TIME))

echo "ZooKeeper-based failover time: ${FAILOVER_TIME}ms"

# Show the new controller
echo "New controller is:"
NEW_CONTROLLER_ID=$(get_controller_id)
echo "New controller is: broker${NEW_CONTROLLER_ID}"

# Restart the killed broker for cleanup
echo "Restarting the killed broker${CONTROLLER_ID}..."
docker start broker${CONTROLLER_ID}

# Verify all brokers are back
sleep 5
echo "Verifying all brokers are registered:"
docker exec zookeeper bash -c 'echo "ls /brokers/ids" | zookeeper-shell localhost:2181'
