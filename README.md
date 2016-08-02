# Twitter Trend Momentum
**Real-Time Twitter Trend Analysis with Momentum Detection**

Detect emerging trends by measuring **trend acceleration** (not just popularity). The system streams tweets from Twitter API, analyzes sentiment, and calculates momentum metrics to identify rapidly growing hashtags.

> 📄 **Based on**: [A real-time twitter trend analysis and visualization framework](https://www.igi-global.com/article/a-real-time-twitter-trend-analysis-and-visualization-framework/222625) - Published in *International Journal on Semantic Web and Information Systems* (IJSWIS), 2019
> 
> Authors: Jamuna S Murthy, GM Siddesh, KG Srinivasa  
> See [CITATIONS.md](CITATIONS.md) for full BibTeX citation

## 🚀 Quick Start (5 min)

### Prerequisites
```bash
# Install Maven (if not already installed)
brew install maven

# Verify installations
java -version    # Java 8+
mvn -version     # Maven 3.6+
docker --version # Docker & Compose required
```

### 1. Setup Twitter API Credentials
Create `input/oAuth-tokens.txt` with 4 lines:
```
YOUR_CONSUMER_KEY
YOUR_CONSUMER_SECRET
YOUR_ACCESS_TOKEN
YOUR_ACCESS_TOKEN_SECRET
```
Get credentials from: https://developer.twitter.com/en/portal/dashboard

### 2. Build Project
```bash
bash scripts/build.sh
```

### 3. Start in 3 Terminals

**Terminal 1 - Infrastructure (Kafka, MongoDB, Zookeeper)**
```bash
bash scripts/start-infrastructure.sh
```
Wait for message: "Infrastructure is ready!"

**Terminal 2 - Twitter Producer**
```bash
bash scripts/run-producer.sh
```
Wait for: "Starting to consume tweets from queue..."

**Terminal 3 - Spark Processor**
```bash
bash scripts/run-processor.sh
```
When you see tweet processing, it's working!

### 4. View Results
Open browser: **http://localhost:8081**

MongoDB Mongo Express UI shows real-time trends with:
- Hashtag name
- Sentiment score (0-4 scale)
- Trend velocity (frequency change rate)
- Trend momentum (acceleration)
- Trend score (composite 0-1)

---

## 🎯 What Makes This Special: Trend Momentum Algorithm

Traditional trending tools show **what's popular**. This system shows **what's accelerating**.

### How It Works

**Trend Score = (Frequency × 0.4) + (Sentiment × 0.3) + (Momentum × 0.3)**

1. **Frequency**: How many tweets
2. **Sentiment**: Average emotion (Stanford CoreNLP)
3. **Momentum**: Rate of acceleration (key innovation!)

**Example:**
- `#Bitcoin` has 1000 tweets/hour (established trend) → Lower momentum
- `#NewCoin` goes from 10→20→50 tweets/hour (accelerating) → Higher momentum ⚡

---

## 🏗 Architecture

<img src="images/architecture.png" alt="Twitter Trend Momentum Architecture" width="100%">

**System Flow:**

```
┌─────────────────┐
│  Twitter API    │
│  (100k+ tweets) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│ KafkaTwitterProducer (Java)     │
│ • OAuth authentication          │
│ • Extract location & hashtags   │─────┐
│ • Queue tweets                  │     │
└─────────────────────────────────┘     │
         │                               │
         ▼                               ▼
      ┌──────────┐                 ┌──────────────────┐
      │  Kafka   │────────────────▶│ Spark Streaming  │
      │  Topic   │                 │ 15-sec batches   │
      └──────────┘                 └──────────────────┘
                                            │
                                    ┌───────┴────────────┐
                                    │                    │
                                    ▼                    ▼
                          ┌──────────────────┐   ┌──────────────┐
                          │ Stanford CoreNLP │   │ Trend Momentum│
                          │  Sentiment (0-4) │   │  Algorithm   │
                          └──────────────────┘   └──────────────┘
                                    │                    │
                                    └───────┬────────────┘
                                            ▼
                                   ┌──────────────────┐
                                   │    MongoDB       │
                                   │  (Store results) │
                                   └──────────────────┘
                                            │
                                            ▼
                                   ┌──────────────────┐
                                   │ Mongo Express UI │
                                   │ http://8081      │
                                   └──────────────────┘
```

---

## 📊 Key Features

✅ **Real-Time Streaming** - Process 50-200 tweets/second  
✅ **Sentiment Analysis** - Stanford CoreNLP 3.9.2  
✅ **Trend Momentum** - Detect accelerating trends  
✅ **Multi-Country** - Track trends by location  
✅ **Web Dashboard** - MongoDB Mongo Express UI  
✅ **Docker Ready** - All services containerized  
✅ **Scalable** - Built on Spark + Kafka architecture  

---

## 🛠 Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Streaming | Apache Spark | 2.4.5 |
| Message Broker | Apache Kafka | 2.4.1 |
| Sentiment Analysis | Stanford CoreNLP | 3.9.2 |
| Database | MongoDB | 4.4+ |
| Languages | Java 8 + Scala 2.12 | - |
| Build | Maven | 3.6+ |
| Containerization | Docker Compose | - |

---

## 📁 Project Structure

```
├── src/main/
│   ├── java/org/streaming/
│   │   ├── KafkaTwitterProducer.java      # Twitter streaming producer
│   │   └── util/ConfigLoader.java         # Configuration manager
│   └── scala/org/streaming/
│       ├── KafkaSparkProcessor.scala      # Trend momentum algorithm
│       ├── util/TrendAnalysisUtils.scala  # Utility functions
│       └── ml/MockDataGenerator.scala     # Test data generator
├── scripts/
│   ├── build.sh                  # Maven compile
│   ├── start-infrastructure.sh   # Docker services
│   ├── run-producer.sh          # Start producer
│   ├── run-processor.sh         # Start processor
│   └── test.sh                  # Run tests
├── docker-compose.yml            # Infrastructure definition
├── pom.xml                       # Maven dependencies
└── input/oAuth-tokens.txt        # Twitter credentials (CREATE THIS)
```

---

## 🔍 Querying MongoDB Results

### Using Mongo Express (Web UI)
http://localhost:8081 → Select `twitter` → `realtime_trends`

### Using MongoDB CLI
```bash
docker exec -it twitter-trend-momentum_mongodb_1 mongosh -u root -p password --authenticationDatabase admin
```

```javascript
// Top trending hashtags now
db.realtime_trends.find().sort({trend_score: -1}).limit(10)

// Find emerging trends (high momentum)
db.realtime_trends.find({trend_momentum: {$gt: 0.5}})

// Trends by country
db.realtime_trends.find({country: "USA"}).sort({trend_score: -1}).limit(10)

// Last 30 minutes
db.realtime_trends.find({
  timestamp: {$gte: Math.floor(Date.now()/1000) - 1800}
})
```

---

## 📝 Configuration

Edit `src/main/resources/application.properties`:

```properties
# Kafka
kafka.bootstrap.servers=localhost:9092

# MongoDB
mongodb.uri=mongodb://root:password@localhost:27017/twitter

# Twitter Search Keywords  
twitter.search.keywords=cryptocurrency,bitcoin,trending,technology

# Spark Configuration
spark.streaming.batch.interval=15000
```

---

## 🐛 Troubleshooting

### "Port already in use"
```bash
# Kill existing services
lsof -i :9092 | grep -v PID | awk '{print $2}' | xargs kill -9
lsof -i :27017 | grep -v PID | awk '{print $2}' | xargs kill -9
```

### "No tweets being streamed"
1. Verify Twitter credentials in `input/oAuth-tokens.txt`
2. Check Twitter API rate limits
3. Try different keywords

### "Sentiment analysis slow"
- Stanford CoreNLP is CPU-intensive
- This is normal for first batch (loads models)
- Subsequent batches are faster

### Clean Reset
```bash
bash scripts/cleanup.sh
# Then restart from "Start in 3 Terminals"
```

---

## 📚 Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Fast setup reference
- **[VERIFICATION.md](VERIFICATION.md)** - ✅ Academic paper compliance verification
- **[ALGORITHM.md](ALGORITHM.md)** - Technical algorithm specification
- **[CITATIONS.md](CITATIONS.md)** - Original paper BibTeX & citations
- **[CHANGELOG.md](CHANGELOG.md)** - Version history & roadmap

---

## Performance Expectations

| Metric | Expected |
|--------|----------|
| Tweet Ingestion Rate | 50-200 tweets/sec |
| Processing Batch Size | 1000-5000 tweets/15sec |
| End-to-End Latency | 30-45 seconds |
| Memory Usage (All) | 6-8 GB |
| MongoDB Write Rate | 500-1000 doc/15sec |

---

## Next Steps

1. ✅ Install Maven: `brew install maven`
2. ✅ Add Twitter credentials to `input/oAuth-tokens.txt`
3. ✅ Follow "Quick Start" above
4. ✅ Open http://localhost:8081 to see trends

---

## 📖 Technology Details

See [ALGORITHM.md](ALGORITHM.md) for:
- Mathematical formulation of Trend Momentum Algorithm
- Sentiment analysis methodology
- Temporal sliding windows
- Performance characteristics

---

## 📞 Support

**Author**: Jamuna Srinivasa Murthy  
**Email**: jamunamurthy.s@gmail.com

For issues, feature requests, or questions:

- **Report Issues**: Open a GitHub Issue with details
- **Check Documentation**: See [ALGORITHM.md](ALGORITHM.md) for technical details
- **Review Examples**: Check [QUICKSTART.md](QUICKSTART.md) for usage examples
- **Contact**: Send email to jamunamurthy.s@gmail.com

---

## License
MIT License - See [LICENSE](LICENSE)

---

**Status**: ✅ Ready for use (requires Maven installation)  
**Version**: 1.0.0  
**Last Updated**: March 2024
