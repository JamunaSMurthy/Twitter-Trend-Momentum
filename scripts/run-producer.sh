#!/bin/bash

# Run Twitter Producer
# Streams tweets from Twitter API to Kafka topic

set -e

PRODUCER_JAR="target/twitter-trend-momentum-1.0.0-jar-with-dependencies.jar"
TOKEN_FILE="input/oAuth-tokens.txt"
KAFKA_TOPIC="tweets"
SEARCH_KEYWORDS="cryptocurrency,bitcoin,ethereum,twitter,technology,trending"

echo "================================"
echo "Twitter Trend Momentum - Producer"
echo "================================"
echo ""

# Check if JAR exists
if [ ! -f "$PRODUCER_JAR" ]; then
    echo "ERROR: Producer JAR not found at $PRODUCER_JAR"
    echo "Please run 'bash scripts/build.sh' first"
    exit 1
fi

# Check if token file exists
if [ ! -f "$TOKEN_FILE" ]; then
    echo "ERROR: OAuth token file not found at $TOKEN_FILE"
    echo "Please create input/oAuth-tokens.txt with your Twitter API credentials"
    echo "Format:"
    echo "  Line 1: Consumer Key"
    echo "  Line 2: Consumer Secret"
    echo "  Line 3: Access Token"
    echo "  Line 4: Access Token Secret"
    exit 1
fi

echo "[1] Configuration:"
echo "  JAR: $PRODUCER_JAR"
echo "  Token File: $TOKEN_FILE"
echo "  Kafka Topic: $KAFKA_TOPIC"
echo "  Search Keywords: $SEARCH_KEYWORDS"
echo ""

echo "[2] Starting Twitter Producer..."
echo ""

java -Xmx2g -cp "$PRODUCER_JAR" org.streaming.KafkaTwitterProducer "$TOKEN_FILE" "$KAFKA_TOPIC" "$SEARCH_KEYWORDS"
