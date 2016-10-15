package org.streaming.util

/**
 * Utility functions for trend analysis
 */
object TrendAnalysisUtils {

  /**
   * Calculate trend strength (0-1 scale)
   * Combines frequency, sentiment polarity, and consistency
   */
  def calculateTrendStrength(frequency: Int, sentiment: Double, consistency: Double): Double = {
    val normalizedFreq = Math.min(frequency / 100.0, 1.0)
    val sentimentStrength = Math.abs(sentiment - 2.0) / 2.0  // Distance from neutral
    
    (normalizedFreq * 0.4) + (sentimentStrength * 0.3) + (consistency * 0.3)
  }

  /**
   * Exponential moving average for real-time trend smoothing
   */
  def exponentialMovingAverage(currentValue: Double, previousEMA: Double, alpha: Double): Double = {
    (currentValue * alpha) + (previousEMA * (1 - alpha))
  }

  /**
   * Calculate z-score for anomaly detection
   */
  def calculateZScore(value: Double, mean: Double, stdDev: Double): Double = {
    if (stdDev == 0) 0.0 else (value - mean) / stdDev
  }

  /**
   * Normalize value to 0-1 range
   */
  def normalize(value: Double, min: Double, max: Double): Double = {
    if (max == min) 0.5 else (value - min) / (max - min)
  }

  /**
   * Calculate trend divergence (comparing two sentiment streams)
   */
  def calculateDivergence(trend1Sentiment: Double, trend2Sentiment: Double): Double = {
    Math.abs(trend1Sentiment - trend2Sentiment) / 2.0
  }

  /**
   * Identify trend type based on velocity and momentum
   */
  def identifyTrendType(velocity: Double, momentum: Double): String = {
    (velocity, momentum) match {
      case (v, m) if v > 0.5 && m > 0.1 => "EMERGING_POSITIVE"
      case (v, m) if v > 0.5 && m < -0.1 => "EMERGING_DECLINING"
      case (v, m) if v > 0.2 => "RISING"
      case (v, m) if v < -0.2 => "FALLING"
      case (v, m) if Math.abs(v) < 0.1 && Math.abs(m) < 0.05 => "STABLE"
      case _ => "NEUTRAL"
    }
  }
}
