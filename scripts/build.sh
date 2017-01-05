#!/bin/bash

# Build script for Twitter Trend Momentum project
# This script builds the complete Maven project and creates JAR files

set -e

echo "================================"
echo "Twitter Trend Momentum - Build"
echo "================================"
echo ""

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "ERROR: Maven is not installed. Please install Maven first."
    exit 1
fi

echo "[1] Cleaning previous builds..."
mvn clean

echo ""
echo "[2] Building the project..."
mvn package -DskipTests

echo ""
echo "[3] Build completed successfully!"
echo ""
echo "Output JARs:"
echo "  - Producer: target/twitter-trend-momentum-1.0.0-jar-with-dependencies.jar"
echo ""
echo "Next steps:"
echo "  1. Update input/oAuth-tokens.txt with your Twitter API credentials"
echo "  2. Run 'bash scripts/start-infrastructure.sh' to start Kafka and MongoDB"
echo "  3. Run 'bash scripts/run-producer.sh' to start the Twitter producer"
echo "  4. Run 'bash scripts/run-processor.sh' to start the Spark processor"
