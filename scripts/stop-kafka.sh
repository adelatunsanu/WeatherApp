#!/bin/bash

# Modify this path as needed
KAFKA_DIR="$HOME/kafka_2.13-3.9.0"

echo "Stopping Kafka..."
"$KAFKA_DIR/bin/kafka-server-stop.sh"

echo "Stopping Zookeeper..."
"$KAFKA_DIR/bin/zookeeper-server-stop.sh"

echo "==== All services stopped ===="