#!/bin/bash

# Run Spark Processor
# Processes tweets from Kafka topic and performs trend analysis/sentiment analysis

set -e

SPARK_HOME="${SPARK_HOME:-.}"
PROCESSOR_JAR="target/twitter-trend-momentum-1.0.0.jar"
KAFKA_TOPIC="tweets"

echo "================================"
echo "Twitter Trend Momentum - Processor"
echo "================================"
echo ""

# Check if Spark is installed
if ! command -v spark-submit &> /dev/null; then
    echo "ERROR: Spark is not installed or SPARK_HOME is not set"
    echo "Please install Apache Spark and set SPARK_HOME environment variable"
    exit 1
fi

# Build Scala version of JAR
echo "[1] Building Scala Spark processor..."
mvn -pl . package -DskipTests -am

if [ ! -f "$PROCESSOR_JAR" ]; then
    echo "ERROR: Processor JAR not found at $PROCESSOR_JAR"
    exit 1
fi

echo "[2] Submitting Spark job..."
echo ""

spark-submit \
  --class org.streaming.KafkaSparkProcessor \
  --master local[6] \
  --executor-memory 5g \
  --total-executor-cores 6 \
  --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.5,org.mongodb.spark:mongo-spark-connector_2.12:2.4.5,edu.stanford.nlp:stanford-corenlp:3.9.2 \
  "$PROCESSOR_JAR" "$KAFKA_TOPIC"
