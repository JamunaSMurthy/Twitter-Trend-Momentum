# Quick Start Guide

## Prerequisites
- Java 8+
- Maven 3.6+
- Docker & Docker Compose
- Twitter API credentials

## 5-Minute Setup

### 1. Create Twitter API Credential File
```bash
cat > input/oAuth-tokens.txt << EOF
YOUR_CONSUMER_KEY
YOUR_CONSUMER_SECRET
YOUR_ACCESS_TOKEN
YOUR_ACCESS_TOKEN_SECRET
EOF
```

### 2. Build Project
```bash
bash scripts/build.sh
```

### 3. Start Services (Terminal 1)
```bash
bash scripts/start-infrastructure.sh
```

### 4. Run Producer (Terminal 2)
```bash
bash scripts/run-producer.sh
```

### 5. Run Processor (Terminal 3)
```bash
bash scripts/run-processor.sh
```

### 6. Monitor Results
Open in browser: http://localhost:8081 (Mongo Express)

## Files Modified
- `pom.xml` - Maven dependencies
- `src/main/java/org/streaming/KafkaTwitterProducer.java` - Complete producer
- `src/main/scala/org/streaming/KafkaSparkProcessor.scala` - Trend algorithm
- `docker-compose.yml` - Infrastructure services
- `scripts/*` - Build and run scripts

## Next Steps
See [README_FULL.md](README_FULL.md) for detailed documentation.
