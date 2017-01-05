# Trend Momentum Algorithm - Detailed Specification

## Overview

The **Trend Momentum Algorithm** is the core novelty of this project. It detects emerging trends by measuring the **acceleration** of trend growth, not just the frequency.

## Mathematical Formulation

### 1. Trend Velocity (v)
Measures how fast the frequency is changing:

$$v_t = \frac{f_t - f_{t-1}}{f_{t-1}}$$

Where:
- $v_t$ = velocity at time t
- $f_t$ = frequency (tweet count) at time t
- $f_{t-1}$ = frequency at previous time window

**Interpretation**:
- $v > 0$: Trend is growing
- $v < 0$: Trend is declining
- $v > 0.5$: Trend is accelerating rapidly

### 2. Trend Acceleration (a)
Measures the change in velocity (momentum):

$$a_t = v_t - v_{t-1}$$

Where:
- $a_t$ = acceleration at time t
- $v_t$ = velocity at current time
- $v_{t-1}$ = velocity at previous time

**Interpretation**:
- $a > 0.1$: Strong positive momentum
- $a < -0.1$: Losing momentum
- $a \approx 0$: Steady state

### 3. Frequency Score
Normalized by square root (diminishing returns):

$$f_{score} = \frac{\sqrt{frequency}}{10}$$

Clamped to [0, 1]

### 4. Sentiment Score
Normalized to [0, 1] from Stanford CoreNLP scale [0, 4]:

$$s_{score} = \frac{sentiment + 1.0}{10}$$

Where sentiment is the weighted average of all tweet sentiments in the window.

**Sentiment Scale**:
- 0.0 = Very Negative
- 1.0 = Negative
- 2.0 = Neutral
- 3.0 = Positive
- 4.0 = Very Positive

### 5. Momentum Score
Boosts positive momentum, penalizes negative:

$$m_{score} = \frac{1 + acceleration}{2}$$

This gives:
- $m_{score} = 1.0$ when $a = 1.0$ (strong positive momentum)
- $m_{score} = 0.5$ when $a = 0$ (no momentum)
- $m_{score} = 0$ when $a = -1.0$ (strong negative momentum)

### 6. Overall Trend Score
Weighted combination of all factors:

$$T = (f_{score} \times 0.4) + (s_{score} \times 0.3) + (m_{score} \times 0.3)$$

Where:
- **Frequency weight (0.4)**: Ensures trends with volume are prioritized
- **Sentiment weight (0.3)**: Positive sentiment amplifies scores
- **Momentum weight (0.3)**: Emphasis on acceleration (key novelty)

**Result**: $T \in [0, 1]$

## Algorithm Design Decisions

### Why Square Root for Frequency?
```
Frequency: 10    → Score: 0.316
Frequency: 100   → Score: 1.0 (clamped)
```
Square root provides diminishing returns - we care about the presence of a trend, not absolute volume.

### Why Weighted Sentiment Analysis?
Not all sentences contribute equally:
- Longer sentences = more meaningful sentiment
- Weighting by length gives them more influence
- Average sentiment can be misleading

$$s_{weighted} = \frac{\sum (sentiment_i × length_i)}{\sum length_i}$$

### Why Focus on Momentum?
Traditional approaches (trending tweets, most popular) miss the story:
- A hashtag with 100 tweets/hour might be established
- A hashtag with 10→50→100 tweets (accelerating) is **emerging**

Momentum captures the narrative arc of trends.

## Temporal Sliding Windows

### Processing Timeline
```
T=0s    T=15s   T=30s   T=45s   T=60s   T=75s
|-------|-------|-------|-------|-------|
└─────────────────┬─────────────────┘
         Window 1 (60s span)
                  └─────────────────┬─────────────────┘
                          Window 2 (sliding 15s)
```

- **Batch Interval**: 15 seconds (Spark processes every 15s)
- **Window Duration**: 60 seconds (analyze last 60s of data)
- **Slide Duration**: 15 seconds (window moves every 15s)

This creates overlapping windows that detect momentum changes quickly.

## Sentiment Analysis Pipeline

### Tweet Cleaning
1. Remove retweet markers (RT)
2. Remove special characters (except punctuation)
3. Remove URLs and @mentions
4. Normalize whitespace
5. Convert to lowercase

### Stanford CoreNLP Annotations
```
Tweet: "I love Bitcoin! It's amazing for financial freedom!"

Annotators:
1. Tokenize: ["I", "love", "Bitcoin", "!", "It", "'s", "amazing", ...]
2. POS Tag: ["PRP", "VBP", "NNP", ".", ...]
3. Lemmatize: ["i", "love", "bitcoin", "!", ...]
4. Parse: [Dependency tree structure]
5. Sentiment: [0, 1, 2, 3, 4] per sentence

Result: Sentiment = 4.0 (Very Positive)
```

### Weighted Sentiment
If tweet has multiple sentences:
```
Sentence 1 (10 words): Sentiment = 3.0
Sentence 2 (15 words): Sentiment = 2.0

Weighted = (3.0 × 10 + 2.0 × 15) / (10 + 15) 
         = (30 + 30) / 25 
         = 2.4
```

## Trend Classification

Based on velocity and momentum, trends are classified:

| Velocity | Momentum | Type | Example |
|----------|----------|------|---------|
| v > 0.5  | a > 0.1  | 🚀 EMERGING_POSITIVE | New meme gaining traction |
| v > 0.5  | a < -0.1 | 📉 EMERGING_DECLINING | Flash trend cooling off |
| v > 0.2  | -        | 📈 RISING | Established trend growing |
| v < -0.2 | -        | 📉 FALLING | Trend losing interest |
| \|v\| < 0.1 | \|a\| < 0.05 | ➡️ STABLE | Plateau'd trend |
| -        | -        | ⚪ NEUTRAL | Minimal activity |

## Implementation Details

### Hashtag Tracking State
```scala
hashtagHistory: Map[String, ListBuffer[(timestamp, frequency, velocity)]]
```

For each hashtag, we maintain:
- **Timestamp**: When measurement was taken
- **Frequency**: Tweet count in window
- **Velocity**: Rate of change

We keep only the last 5 measurements to calculate acceleration.

### Aggregation Strategy

**Current Approach** (streaming):
```scalar
tweets → extract hashtags → group by (hashtag, country, 15-sec window)
       → count frequency, average sentiment
       → calculate velocity & acceleration
       → output metrics
```

**Time Complexity**: O(n) where n = number of tweets
**Space**: O(h × c) where h = hashtags, c = countries

### Handling Edge Cases

**First Measurement**:
- Previous frequency unknown
- Velocity = 1.0 if current frequency > 0
- Acceleration = 0 (no previous velocity)

**Zero Frequency**:
- If no tweets in window, skip trend calculation
- Prevents noise from inactive hashtags

**Sentiment Errors**:
- If CoreNLP fails, use default sentiment = 2.0 (neutral)
- Log errors for debugging

## Performance Characteristics

### Computational Cost
- **Sentiment Analysis**: ~100-200ms per tweet (slowest)
- **Grouping/Aggregation**: ~1ms per 1000 tweets
- **Trend Calculation**: ~0.1ms per hashtag

### Memory Usage
- **Stanford CoreNLP**: ~500MB (loaded once)
- **Hashtag History**: ~100 bytes per hashtag × max 10K hashtags = 1MB
- **Kafka Buffering**: ~50MB (configurable)

### Throughput
- **Producer**: 50-200 tweets/sec
- **Processor**: 1000-5000 tweets/15-sec batch
- **Bottleneck**: Stanford CoreNLP sentiment analysis

## Optimization Opportunities

### Caching
```scala
@transient lazy val pipeline = new StanfordCoreNLP(props)
// Reuse pipeline across batches
```

### Partitioning
```scala
// Process different hashtags on different executors
df.repartition(col("hashtag")).mapPartitions(...)
```

### Approximation
For very high volume:
- Sample tweets (1/10)
- Use lightweight sentiment model (Twitter-specific)
- Increase window sizes

### Persistence
```scala
hashtagHistory.mapValues(_.persist())  // Cache in memory
```

## Evaluation Metrics

### Trend Detection Accuracy
```
Precision = Correctly detected emerging trends / All detected trends
Recall = Correctly detected emerging trends / All actual emerging trends
F1-Score = 2 × (Precision × Recall) / (Precision + Recall)
```

### Latency
```
End-to-end latency = Time from tweet posted to trend score in DB
Expected: 30-45 seconds (15s batch + processing + I/O)
```

### Throughput
```
Tweets/second = Total tweets / Time period
Expected for 6-core system: 1000-2000 tweets/15-second batch
```

## Future Enhancements

1. **Multi-language Support**: Train sentiment models for other languages
2. **Geographical Heat Maps**: Visualize trends by region
3. **Predictive Momentum**: Use ML to predict future trend trajectory
4. **Anomaly Detection**: Flag sudden unusual spikes
5. **Topic Modeling**: Group related hashtags into topics
6. **Cross-platform**: Add Instagram, TikTok, Reddit data
7. **GPU Acceleration**: Use GPU for sentiment analysis
8. **Real-time Dashboard**: Live visualization of trend momentum

---

**Algorithm Version**: 1.0
**Last Updated**: March 2024
