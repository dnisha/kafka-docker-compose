#!/bin/bash
echo "=== Kafka Cluster Diagnostic ==="

echo "1. Checking running containers..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n2. Checking ZooKeeper controller..."
docker exec zookeeper zookeeper-shell localhost:2181 << EOF
get /controller
quit
EOF

echo -e "\n3. Checking registered brokers in ZooKeeper..."
docker exec zookeeper zookeeper-shell localhost:2181 << EOF
ls /brokers/ids
quit
EOF

echo -e "\n4. Testing Kafka connectivity..."
# Internal ports are 29092, 29093, 29094
for port in 29092 29093 29094; do
    broker_id=$((port - 29091)) # Gets 1, 2, or 3
    echo "Broker${broker_id} (Port $port):"
    # We can execute the command from inside the broker1 container to test internal connectivity
    docker exec broker1 kafka-topics --list --bootstrap-server broker${broker_id}:$port 2>&1 | head -3
done
