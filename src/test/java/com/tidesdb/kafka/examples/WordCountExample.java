package com.tidesdb.kafka.examples;

import com.tidesdb.kafka.store.TidesDBStoreSupplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Example Kafka Streams application using TidesDB for state storage.
 * 
 * This example demonstrates:
 * - Word count with TidesDB state store
 * - Windowed aggregations
 * - Interactive queries
 * - Graceful shutdown
 * 
 * To run this example:
 * 1. Start Kafka and create topics: input-topic, output-topic
 * 2. Run this application
 * 3. Produce messages to input-topic
 * 4. Observe results in output-topic
 */
public class WordCountExample {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-tidesdb-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Use temp directory for state
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-tidesdb");

        StreamsBuilder builder = new StreamsBuilder();

        // Example 1: Simple word count with TidesDB
        KStream<String, String> textLines = builder.stream("input-topic");

        KTable<String, Long> wordCounts = textLines
            .flatMapValues(textLine -> {
                String[] words = textLine.toLowerCase().split("\\W+");
                return java.util.Arrays.asList(words);
            })
            .groupBy((key, word) -> word)
            .count(Materialized.as(new TidesDBStoreSupplier("word-counts")));

        // Output results
        wordCounts.toStream()
            .map((word, count) -> KeyValue.pair(word, word + ":" + count))
            .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Example 2: Windowed aggregation with TidesDB
        KStream<String, String> events = builder.stream("events-topic");

        TimeWindowedKStream<String, String> windowedStream = events
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)));

        windowedStream
            .count()
            .toStream()
            .map((windowedKey, count) -> {
                String key = windowedKey.key();
                long start = windowedKey.window().start();
                long end = windowedKey.window().end();
                String value = String.format("%s,%d,%d,%d", key, start, end, count);
                return KeyValue.pair(key, value);
            })
            .to("windowed-output", Produced.with(Serdes.String(), Serdes.String()));

        // Build and start the topology
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutting down gracefully...");
                streams.close(Duration.ofSeconds(10));
                latch.countDown();
            }
        });

        try {
            System.out.println("Starting Kafka Streams with TidesDB...");
            streams.start();
            System.out.println("Application started successfully!");
            System.out.println("Press Ctrl+C to stop");
            latch.await();
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    /**
     * Advanced example with custom aggregation
     */
    public static class UserActivityAggregator {
        
        public static void run(String bootstrapServers) {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-activity-tidesdb");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> activities = builder.stream("user-activities");

            activities
                .groupByKey()
                .aggregate(
                    UserStats::new,
                    (userId, activity, stats) -> {
                        stats.addActivity(activity);
                        return stats;
                    },
                    Materialized.<String, UserStats>as(new TidesDBStoreSupplier("user-stats"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new UserStatsSerde())
                )
                .toStream()
                .mapValues(UserStats::toString)
                .to("user-stats-output");

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();
        }
    }

    /**
     * Example user statistics class
     */
    public static class UserStats {
        private int activityCount = 0;
        private long lastActivityTime = 0;

        public void addActivity(String activity) {
            activityCount++;
            lastActivityTime = System.currentTimeMillis();
        }

        public int getActivityCount() {
            return activityCount;
        }

        public long getLastActivityTime() {
            return lastActivityTime;
        }

        @Override
        public String toString() {
            return String.format("count=%d,lastActivity=%d", activityCount, lastActivityTime);
        }
    }

    /**
     * Custom serde for UserStats
     */
    public static class UserStatsSerde implements org.apache.kafka.common.serialization.Serde<UserStats> {
        @Override
        public org.apache.kafka.common.serialization.Serializer<UserStats> serializer() {
            return (topic, data) -> {
                if (data == null) return null;
                String str = data.activityCount + "," + data.lastActivityTime;
                return str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            };
        }

        @Override
        public org.apache.kafka.common.serialization.Deserializer<UserStats> deserializer() {
            return (topic, data) -> {
                if (data == null) return null;
                String str = new String(data, java.nio.charset.StandardCharsets.UTF_8);
                String[] parts = str.split(",");
                UserStats stats = new UserStats();
                stats.activityCount = Integer.parseInt(parts[0]);
                stats.lastActivityTime = Long.parseLong(parts[1]);
                return stats;
            };
        }
    }
}
