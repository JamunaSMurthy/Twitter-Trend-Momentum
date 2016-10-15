package org.streaming

import java.io.IOException
import java.sql.DriverManager
import com.mongodb.MongoClient
import java.util.Properties
import twitter4j.TwitterObjectFactory

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, LongType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.math.{abs, pow, sqrt}

/**
 * KafkaSparkProcessor - Processes tweets from Kafka, performs sentiment analysis,
 * and computes Trend Momentum metrics
 * 
 * The Trend Momentum Algorithm:
 * 1. Tracks hashtag frequency and sentiment over sliding windows
 * 2. Calculates trend velocity (rate of change in frequency)
 * 3. Computes trend acceleration (momentum)
 * 4. Detects emerging and decaying trends
 * 5. Scores trends by strength and momentum
 */
object KafkaSparkProcessor {
  
  private val logger = LogManager.getLogger(this.getClass)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[6]")
    .appName("Twitter Trend Momentum Analyzer")
    .config("spark.mongodb.input.uri", "mongodb://localhost/twitter.realtime_trends")
    .config("spark.mongodb.output.uri", "mongodb://localhost/twitter.realtime_trends")
    .config("spark.executor.cores", "6")
    .config("spark.executor.memory", "5g")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._
  import spark.sql

  // Sentiment type enumeration
  sealed trait SENTIMENT_TYPE {
    def value: Int
  }
  case object VERY_NEGATIVE extends SENTIMENT_TYPE { val value = 0 }
  case object NEGATIVE extends SENTIMENT_TYPE { val value = 1 }
  case object NEUTRAL extends SENTIMENT_TYPE { val value = 2 }
  case object POSITIVE extends SENTIMENT_TYPE { val value = 3 }
  case object VERY_POSITIVE extends SENTIMENT_TYPE { val value = 4 }
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE { val value = -1 }

  val nlpProps: Properties = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  // Data structures to track trend momentum
  val hashtagHistory: MutableMap[String, ListBuffer[(Long, Int, Double)]] = MutableMap()
  
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")

    if (args.length != 1) {
      logger.error("Invalid arguments. Usage: KafkaSparkProcessor <topic-name>")
      println("Usage: KafkaSparkProcessor <topic-name>")
      return
    }

    val Array(topics) = args
    logger.info("Starting Kafka Spark Processor for topics: {}", topics)

    // Create streaming context with 15-second batch intervals
    val ssc = new StreamingContext(sc, Seconds(15))
    ssc.checkpoint("checkpoint")

    // Configure Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "twitter-trend-momentum-processor",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicSet = topics.split(",").toSet

    // Create Kafka stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topicSet, kafkaParams)
    )

    val tweets = stream.map(record => record.value).cache()

    // Process tweets: extract hashtags and sentiment
    val hashtagSentimentRDD = processTweet(tweets).cache()

    // Calculate trend momentum metrics
    val trendMomentumRDD = calculateTrendMomentum(hashtagSentimentRDD).cache()

    // Define schema for storing trend data
    val schema = new StructType()
      .add(StructField("timestamp", LongType, nullable = true))
      .add(StructField("hashtag", StringType, nullable = true))
      .add(StructField("sentiment_score", DoubleType, nullable = true))
      .add(StructField("sentiment_type", StringType, nullable = true))
      .add(StructField("country", StringType, nullable = true))
      .add(StructField("frequency", IntegerType, nullable = true))
      .add(StructField("trend_velocity", DoubleType, nullable = true))
      .add(StructField("trend_momentum", DoubleType, nullable = true))
      .add(StructField("trend_score", DoubleType, nullable = true))

    val counter = sc.longAccumulator("counter")

    // Write results to MongoDB
    trendMomentumRDD.foreachRDD((rdd: RDD[(String, Double, String, String, Int, Double, Double, Double)],
                                  time: org.apache.spark.streaming.Time) => {
      try {
        val timestamp = time.milliseconds / 1000
        
        val newRDD = rdd.map(r =>
          Row(timestamp, r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8)
        )

        val df = spark.createDataFrame(newRDD, schema).cache()

        if (counter.isZero) {
          resetDatabase()
        }
        counter.add(1)

        // Remove duplicates and sort by trend score
        val process_df = df.dropDuplicates(Seq("timestamp", "hashtag", "country"))
          .orderBy(org.apache.spark.sql.functions.desc("trend_score"))

        logger.info("Top trends at timestamp {}: ", timestamp)
        process_df.filter(col("trend_score") > 0.5).show(10, truncate = false)

        // Write to MongoDB
        process_df.repartition(10).write.format("mongo")
          .mode("append").save()

      } catch {
        case e: Exception => logger.error("Error processing batch", e)
      }
    })

    tweets.count().map(cnt => "Received " + cnt + " kafka messages.").print()

    logger.info("Starting streaming context...")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Calculate Trend Momentum metrics
   * 
   * Trend Momentum = (Current Frequency - Previous Frequency) / Previous Frequency
   * Trend Score = Sentiment * sqrt(Frequency) * Momentum (normalized)
   */
  def calculateTrendMomentum(hashtagSentiments: DStream[(String, Double, String, String)])
      : DStream[(String, Double, String, String, Int, Double, Double, Double)] = {

    hashtagSentiments
      .map { case (hashtag, sentiment, sentType, country) => 
        ((hashtag, country), (sentiment, 1))
      }
      .reduceByKeyAndWindow(
        { case ((sentA, countA), (sentB, countB)) => 
          (sentA + sentB, countA + countB)
        },
        Seconds(60),  // Window duration
        Seconds(15)   // Slide duration
      )
      .map { case ((hashtag, country), (totalSent, count)) =>
        val avgSentiment = totalSent / count
        val frequency = count
        val trendVelocity = calculateVelocity(hashtag, frequency)
        val acceleration = calculateAcceleration(hashtag, trendVelocity)
        val trendScore = calculateTrendScore(frequency, avgSentiment, acceleration)
        
        (hashtag, avgSentiment, sentimentTypeFromScore(avgSentiment), country, 
         frequency, trendVelocity, acceleration, trendScore)
      }
  }

  /**
   * Calculate trend velocity (rate of change in frequency)
   */
  def calculateVelocity(hashtag: String, currentFreq: Int): Double = {
    synchronized {
      val currentTime = System.currentTimeMillis()
      val previousFreq = hashtagHistory.get(hashtag)
        .flatMap(_.lastOption)
        .map(_._2)
        .getOrElse(0)

      val velocity = if (previousFreq > 0) {
        (currentFreq - previousFreq).toDouble / previousFreq
      } else if (currentFreq > 0) {
        1.0
      } else {
        0.0
      }

      // Update history
      if (!hashtagHistory.contains(hashtag)) {
        hashtagHistory(hashtag) = ListBuffer()
      }
      hashtagHistory(hashtag) += ((currentTime, currentFreq, velocity))

      // Keep only recent history (last 5 measurements)
      if (hashtagHistory(hashtag).length > 5) {
        hashtagHistory(hashtag).remove(0)
      }

      velocity
    }
  }

  /**
   * Calculate trend acceleration (rate of change of velocity)
   */
  def calculateAcceleration(hashtag: String, currentVelocity: Double): Double = {
    synchronized {
      val history = hashtagHistory.getOrElse(hashtag, ListBuffer())
      if (history.length >= 2) {
        val previousVelocity = history(history.length - 1)._3
        currentVelocity - previousVelocity
      } else {
        0.0
      }
    }
  }

  /**
   * Calculate trend score combining frequency, sentiment, and momentum
   * Higher score = stronger, more positive trend with upward momentum
   */
  def calculateTrendScore(frequency: Int, sentiment: Double, momentum: Double): Double = {
    // Normalize inputs
    val freqScore = sqrt(frequency) / 10.0  // Square root for diminishing returns
    val sentScore = (sentiment + 1.0) / 10.0  // Normalize from -1 to 1 scale
    val momScore = (1.0 + momentum) / 2.0  // Boost positive momentum, penalize negative
    
    // Combined score with weights
    val score = (freqScore * 0.4) + (sentScore * 0.3) + (momScore * 0.3)
    math.min(math.max(score, 0.0), 1.0)  // Clamp between 0 and 1
  }

  /**
   * Convert sentiment score to categorical type
   */
  def sentimentTypeFromScore(score: Double): String = {
    score match {
      case s if s < 1.0 => "VERY_NEGATIVE"
      case s if s < 2.0 => "NEGATIVE"
      case s if s < 3.0 => "NEUTRAL"
      case s if s < 4.0 => "POSITIVE"
      case s if s < 5.0 => "VERY_POSITIVE"
      case _ => "UNKNOWN"
    }
  }

  def processTweet(tweets: DStream[String]): DStream[(String, Double, String, String)] = {
    val metricsStream = tweets.flatMap { eTweet => 
      val retList = ListBuffer[String]()
      
      for (tag <- eTweet.split(" ")) {
        if (tag.startsWith("#") && tag.replaceAll("\\s", "").length > 1) {
          val tweetObj = eTweet.split(" /TLOC/ ")
          
          if (tweetObj.length >= 2) {
            val country = getCountryInfo(tweetObj(0))
            val tweetClean = cleanTweet(tweetObj(1))

            try {
              val (sentimentScore, _) = detectSentiment(tweetClean)
              retList += (tag + " /TLOC/ " + sentimentScore + " /TLOC/ SENTIMENT /TLOC/ " + country)
            } catch {
              case e: IOException => logger.error("Error detecting sentiment", e)
            }
          }
        }
      }
      retList.toList
    }

    val processedTweet = metricsStream.map { line =>
      val Array(tag, sentiScore, _, location) = line.split(" /TLOC/ ")
      (tag.replaceAll("([\\w]*RT)|[^a-zA-Z0-9#]", ""),
        sentiScore.toDouble, 
        sentimentTypeFromScore(sentiScore.toDouble), 
        location)
    }

    processedTweet
  }

  /**
   * Clean tweet text for sentiment analysis
   */
  def cleanTweet(tweet: String): String = {
    tweet
      .replaceAll("(\\b\\w*RT)|[^a-zA-Z0-9\\s.,!@]", "")
      .replaceAll("(http\\S+)", "")
      .replaceAll("(@\\w+)", "Foo")
      .replaceAll("^(Foo)", "")
      .replaceAll("\\s+", " ")
      .trim
  }

  /**
   * Detect sentiment using Stanford CoreNLP
   */
  def detectSentiment(message: String): (Double, SENTIMENT_TYPE) = {
    try {
      if (message.trim.isEmpty) {
        return (2.0, NEUTRAL)
      }

      val pipeline = new StanfordCoreNLP(nlpProps)
      val annotation = pipeline.process(message)
      
      var sentiments: ListBuffer[Double] = ListBuffer()
      var sizes: ListBuffer[Int] = ListBuffer()
      var longest = 0
      var mainSentiment = 2

      for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
        val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
        val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
        val partText = sentence.toString

        if (partText.length > longest) {
          mainSentiment = sentiment
          longest = partText.length
        }

        sentiments += sentiment.toDouble
        sizes += partText.length
      }

      val weightedSentiments = (sentiments, sizes).zipped.map((s, sz) => s * sz)
      val weightedSentiment = if (sizes.nonEmpty) {
        weightedSentiments.sum / sizes.sum
      } else {
        2.0
      }

      val sentimentScore = math.max(0, math.min(4, weightedSentiment))
      val sentimentType = weightedSentiment match {
        case s if s < 1.0 => VERY_NEGATIVE
        case s if s < 2.0 => NEGATIVE
        case s if s < 3.0 => NEUTRAL
        case s if s < 4.0 => POSITIVE
        case _ => VERY_POSITIVE
      }

      (sentimentScore, sentimentType)
    } catch {
      case e: Exception => 
        logger.error("Sentiment detection failed", e)
        (2.0, NEUTRAL)
    }
  }

  /**
   * Extract country from location string
   */
  def getCountryInfo(location: String): String = {
    if (location == null || location.isEmpty) {
      return "UNKNOWN"
    }

    var country = "UNKNOWN"
    if (location.contains(",")) {
      val locArray = location.split(",")
      country = locArray(locArray.length - 1)
        .replaceAll("""[\p{Punct}]""", "")
        .trim
      
      if (country.length == 2 && locArray.length == 2) {
        country = "USA"
      }
    } else {
      country = location.trim
    }

    if (country.isEmpty) "UNKNOWN" else country.toUpperCase
  }

  /**
   * Reset MongoDB database
   */
  def resetDatabase(): Unit = {
    try {
      val mongo = new MongoClient("localhost", 27017)
      val database = mongo.getDatabase("twitter")
      database.drop()
      logger.info("Database reset successfully")
      mongo.close()
    } catch {
      case e: Exception => logger.error("Error resetting database", e)
    }
  }
}
