package org.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * KafkaTwitterProducer - Streams tweets from Twitter API and publishes to Kafka topic
 * 
 * This producer:
 * 1. Authenticates with Twitter API using OAuth tokens
 * 2. Filters tweets by search keywords and language
 * 3. Captures tweets and associated location information
 * 4. Publishes tweets to a Kafka topic for downstream processing
 */
public class KafkaTwitterProducer {
    
    private static final Logger logger = LogManager.getLogger(KafkaTwitterProducer.class);
    private static final int QUEUE_SIZE = 1000;
    private static final int SLEEP_TIME_MS = 100;
    private static final String LOCATION_SEPARATOR = " /TLOC/ ";

    public static void main(String[] args) throws Exception {
        logger.info("Starting Kafka Twitter Producer...");

        if (args.length < 3) {
            logger.error("Invalid arguments. Usage: KafkaTwitterProducer <token-file-path> <topic-name> <twitter-search-keywords>");
            System.out.println("Usage: KafkaTwitterProducer <token-file-path> <topic-name> <twitter-search-keywords>");
            System.exit(1);
        }

        String tokenFilePath = args[0];
        String topicName = args[1];
        String searchKeywordsStr = args[2];

        // Load authentication tokens
        List<String> tokens = getAuthTokens(tokenFilePath);
        if (tokens.size() < 4) {
            logger.error("Invalid token file format. Expected 4 tokens: consumerKey, consumerSecret, accessToken, accessTokenSecret");
            System.exit(1);
        }

        String consumerKey = tokens.get(0).trim();
        String consumerSecret = tokens.get(1).trim();
        String accessToken = tokens.get(2).trim();
        String accessTokenSecret = tokens.get(3).trim();

        String[] searchWords = searchKeywordsStr.split(",");
        logger.info("Twitter Search Keywords: {}", Arrays.toString(searchWords));

        // Create queue to hold tweets
        final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);

        // Configure Twitter Stream
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false)
          .setOAuthConsumerKey(consumerKey)
          .setOAuthConsumerSecret(consumerSecret)
          .setOAuthAccessToken(accessToken)
          .setOAuthAccessTokenSecret(accessTokenSecret);

        logger.info("Authenticating with Twitter API...");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        // Create status listener to capture tweets
        StatusListener listener = createStatusListener(queue);
        twitterStream.addListener(listener);

        // Configure filter query
        FilterQuery query = new FilterQuery(searchWords);
        query.language("en");
        
        logger.info("Starting tweet filtering for keywords: {}", Arrays.toString(searchWords));
        twitterStream.filter(query);

        // Start Kafka producer to send tweets
        configKafkaProducer(queue, topicName);
    }

    /**
     * Creates a StatusListener to handle incoming tweets
     */
    private static StatusListener createStatusListener(LinkedBlockingQueue<Status> queue) {
        return new StatusListener() {
            @Override
            public void onStatus(Status status) {
                if (!queue.offer(status)) {
                    logger.warn("Queue is full, dropping tweet: {}", status.getId());
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.debug("Status deletion notice: {}", statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                logger.warn("Twitter track limitation notice: {} tweets limited", numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                logger.debug("Scrub geo event - userId: {}, upToStatusId: {}", userId, upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                logger.warn("Twitter stall warning: {}", stallWarning.getMessage());
            }

            @Override
            public void onException(Exception e) {
                logger.error("Exception in Twitter stream listener", e);
            }
        };
    }

    /**
     * Configures Kafka producer and sends tweets to Kafka topic
     */
    private static void configKafkaProducer(LinkedBlockingQueue<Status> queue, String topic) throws Exception {
        logger.info("Configuring Kafka Producer for topic: {}", topic);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        int messageCount = 0;
        long startTime = System.currentTimeMillis();

        logger.info("Starting to consume tweets from queue and send to Kafka...");
        while (true) {
            try {
                Status tweet = queue.poll(SLEEP_TIME_MS, TimeUnit.MILLISECONDS);
                
                if (tweet == null) {
                    continue;
                }

                String location = extractLocation(tweet);
                String tweetText = tweet.getText();

                // Only send tweets with hashtags and location
                if (tweet.getHashtagEntities().length > 0 && !location.isEmpty()) {
                    String message = location + LOCATION_SEPARATOR + tweetText;
                    
                    // Send to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic, 
                        String.valueOf(tweet.getId()), 
                        message
                    );
                    
                    Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Failed to send message to Kafka", exception);
                        } else {
                            messageCount++;
                            if (messageCount % 100 == 0) {
                                long elapsed = System.currentTimeMillis() - startTime;
                                double rate = (messageCount / (elapsed / 1000.0));
                                logger.info("Sent {} messages at {:.2f} msg/sec", messageCount, rate);
                            }
                        }
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Twitter Producer interrupted", e);
                break;
            } catch (Exception e) {
                logger.error("Error processing tweet", e);
            }
        }
    }

    /**
     * Extracts location from tweet, handling null cases
     */
    private static String extractLocation(Status tweet) {
        if (tweet.getUser() != null && tweet.getUser().getLocation() != null) {
            String location = tweet.getUser().getLocation().trim();
            return location.isEmpty() ? "UNKNOWN" : location;
        }
        return "UNKNOWN";
    }

    /**
     * Reads OAuth tokens from file
     * Expected format: one token per line (consumerKey, consumerSecret, accessToken, accessTokenSecret)
     */
    private static List<String> getAuthTokens(String filePath) {
        final List<String> tokens = new ArrayList<>();
        try {
            File f = new File(filePath);
            if (!f.exists()) {
                logger.error("Token file not found: {}", filePath);
                return tokens;
            }

            BufferedReader reader = new BufferedReader(new FileReader(f));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty() && !line.startsWith("#")) {
                    tokens.add(line.trim());
                }
            }
            reader.close();

            logger.info("Successfully loaded {} authentication tokens", tokens.size());
        } catch (IOException e) {
            logger.error("Error reading authentication tokens from file: {}", filePath, e);
        }
        return tokens;
    }
}
