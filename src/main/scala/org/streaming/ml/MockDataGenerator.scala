package org.streaming.ml

import scala.math.random
import scala.collection.mutable

/**
 * Mock Data Generator for testing without Twitter API
 * Generates synthetic tweets for testing the processor
 */
object MockDataGenerator {

  private val hashtags = Array(
    "#bitcoin", "#ethereum", "#crypto", "#blockchain", "#defi",
    "#nft", "#web3", "#tech", "#startup", "#ai",
    "#python", "#rust", "#golang", "#javascript", "#java"
  )

  private val sentiments = Array(
    "I love this project!",
    "This is amazing work",
    "Really impressed with this",
    "Not sure about this",
    "This is okay I guess",
    "This is terrible",
    "Worst thing ever",
    "Absolutely horrible"
  )

  private val locations = Array(
    "San Francisco, USA", "New York, USA", "London, UK",
    "Tokyo, Japan", "Berlin, Germany", "Toronto, Canada",
    "Sydney, Australia", "Singapore", "Mumbai, India",
    "Dubai, UAE"
  )

  /**
   * Generate a mock tweet with random hashtag, sentiment, and location
   */
  def generateMockTweet(): String = {
    val location = locations((random * locations.length).toInt)
    val hashtag = hashtags((random * hashtags.length).toInt)
    val sentiment = sentiments((random * sentiments.length).toInt)
    
    s"$location /TLOC/ User @twitter$hashtag says: $sentiment #trending"
  }

  /**
   * Generate batch of mock tweets
   */
  def generateMockTweets(count: Int): List[String] = {
    (1 to count).map(_ => generateMockTweet()).toList
  }

  /**
   * Generate trending spike (simulate trend momentum)
   */
  def generateTrendingSpike(hashtag: String, count: Int): List[String] = {
    val tweets = mutable.ListBuffer[String]()
    for (i <- 1 to count) {
      val location = locations((random * locations.length).toInt)
      val sentiment = sentiments((random * sentiments.length).toInt)
      tweets += s"$location /TLOC/ $hashtag talks about: $sentiment #trending"
    }
    tweets.toList
  }

  def main(args: Array[String]): Unit = {
    // Generate and print sample tweets
    println("=== Sample Mock Tweets ===")
    val tweets = generateMockTweets(5)
    tweets.foreach(println)
    
    println("\n=== Trending Spike for #bitcoin ===")
    val spike = generateTrendingSpike("#bitcoin", 10)
    spike.foreach(println)
  }
}
