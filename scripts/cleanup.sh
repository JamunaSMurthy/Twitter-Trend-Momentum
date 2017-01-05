#!/bin/bash

# Cleanup script - stops all services and removes data

echo "================================"
echo "Cleaning up..."
echo "================================"
echo ""

# Stop Spark jobs if running
echo "[1] Stopping Spark jobs..."
pkill -f "KafkaSparkProcessor" || echo "  No Spark jobs found"

# Stop producer if running
echo "[2] Stopping producer..."
pkill -f "KafkaTwitterProducer" || echo "  No producer found"

# Stop Docker containers
echo "[3] Stopping Docker containers..."
docker-compose down -v

echo "[4] Cleaning build artifacts..."
rm -rf target/
rm -rf checkpoint/

echo ""
echo "Cleanup complete!"
echo ""
echo "To restart:"
echo "  bash scripts/start-infrastructure.sh"
echo "  bash scripts/run-producer.sh    (Terminal 2)"
echo "  bash scripts/run-processor.sh   (Terminal 3)"
