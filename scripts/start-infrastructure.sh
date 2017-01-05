#!/bin/bash

# Start infrastructure (Kafka, Zookeeper, MongoDB)
# This script uses Docker Compose to start all required services

set -e

echo "================================"
echo "Starting Infrastructure"
echo "================================"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "[1] Starting Zookeeper, Kafka, MongoDB, and Mongo Express..."
docker-compose up -d

echo ""
echo "[2] Waiting for services to be ready..."
sleep 10

echo ""
echo "[3] Creating Kafka topics..."
docker exec -it twitter-trend-momentum_kafka_1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic tweets \
  --partitions 3 \
  --replication-factor 1 \
  2>/dev/null || echo "Topic 'tweets' already exists"

docker exec -it twitter-trend-momentum_kafka_1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic trends \
  --partitions 2 \
  --replication-factor 1 \
  2>/dev/null || echo "Topic 'trends' already exists"

echo ""
echo "[4] Infrastructure is ready!"
echo ""
echo "Services:"
echo "  - Kafka: localhost:9092"
echo "  - Zookeeper: localhost:2181"
echo "  - MongoDB: localhost:27017"
echo "  - Mongo Express: http://localhost:8081"
echo ""
echo "To stop infrastructure: docker-compose down"
