#!/bin/bash

# Modify these paths as needed
KAFKA_DIR="$HOME/kafka_2.13-3.9.0"
ZOOKEEPER_CONFIG="$KAFKA_DIR/config/zookeeper.properties"
KAFKA_CONFIG="$KAFKA_DIR/config/server.properties"
LOG_DIR="$KAFKA_DIR/logs"

echo "==== Kafka & Zookeeper Startup Script (WSL2) ===="

# Disable IPv6 (fix for Kafka in WSL2)
echo "Disabling IPv6 in WSL2 to fix Kafka connectivity..."
sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Start Zookeeper
echo "Starting Zookeeper..."
nohup "$KAFKA_DIR/bin/zookeeper-server-start.sh" "$ZOOKEEPER_CONFIG" > "$LOG_DIR/zookeeper.log" 2>&1 &

# Wait a few seconds for Zookeeper to initialize
sleep 10

# Start Kafka
echo "Starting Kafka..."
nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_CONFIG" > "$LOG_DIR/kafka.log" 2>&1 &

echo "==== All services started ===="
echo "Zookeeper log: $LOG_DIR/zookeeper.log"
echo "Kafka log:     $LOG_DIR/kafka.log"