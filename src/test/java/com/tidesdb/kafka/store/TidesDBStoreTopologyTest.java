package com.tidesdb.kafka.store;

import com.tidesdb.LogLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs {@link TidesDBStore} inside real Kafka Streams topologies, so the
 * registration, wrapping and changelog wiring Kafka does around a store is
 * exercised rather than mocked.
 */
class TidesDBStoreTopologyTest {

    @TempDir
    File stateDir;

    private static final TidesDBStoreConfig QUIET =
        TidesDBStoreConfig.builder().logLevel(LogLevel.NONE).build();

    private Properties props(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return props;
    }

    @Test
    @DisplayName("Should back a DSL aggregation through Materialized.as(supplier)")
    void testDslCount() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Long> counts = builder
            .stream("words", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy((k, word) -> word)
            .count(Materialized.<String, Long>as(new TidesDBStoreSupplier("word-counts", QUIET))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));
        counts.toStream().to("counts", Produced.with(Serdes.String(), Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props("dsl-count"))) {
            TestInputTopic<String, String> in =
                driver.createInputTopic("words", new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, Long> out = driver.createOutputTopic("counts",
                new StringDeserializer(), Serdes.Long().deserializer());

            for (String w : Arrays.asList("kafka", "tidesdb", "kafka", "streams", "kafka")) {
                in.pipeInput(null, w);
            }

            Map<String, Long> latest = out.readKeyValuesToMap();
            assertThat(latest).containsEntry("kafka", 3L)
                .containsEntry("tidesdb", 1L)
                .containsEntry("streams", 1L);

            KeyValueStore<String, Long> store = driver.getKeyValueStore("word-counts");
            assertThat(store.get("kafka")).isEqualTo(3L);
            assertThat(store.approximateNumEntries()).isGreaterThanOrEqualTo(3);

            List<String> keys = new ArrayList<>();
            try (KeyValueIterator<String, Long> it = store.all()) {
                it.forEachRemaining(kv -> keys.add(kv.key));
            }
            assertThat(keys).containsExactlyInAnyOrder("kafka", "tidesdb", "streams");

            // the changelog topic is written through Kafka's logging wrapper
            TestOutputTopic<Bytes, Long> changelog = driver.createOutputTopic(
                "dsl-count-word-counts-changelog",
                Serdes.Bytes().deserializer(), Serdes.Long().deserializer());
            assertThat(changelog.getQueueSize()).isGreaterThan(0);

            // one database per task, under <state.dir>/<application.id>/<task>/<store>
            File[] taskDirs = new File(stateDir, "dsl-count").listFiles(File::isDirectory);
            assertThat(taskDirs).isNotNull();
            assertThat(Arrays.stream(taskDirs).map(d -> new File(d, "word-counts")))
                .anyMatch(File::isDirectory);
        }
    }

    @Test
    @DisplayName("Should serve a Processor API store built with Stores.keyValueStoreBuilder")
    void testProcessorApi() {
        Topology topology = new Topology();
        topology.addSource("src", new StringDeserializer(), new StringDeserializer(), "events");
        topology.addProcessor("latest", () -> new Processor<String, String, String, String>() {
            private KeyValueStore<String, String> store;

            @Override
            public void init(ProcessorContext<String, String> context) {
                store = context.getStateStore("latest-store");
            }

            @Override
            public void process(Record<String, String> record) {
                if (record.value() == null) {
                    store.delete(record.key());
                } else {
                    store.put(record.key(), record.value());
                }
            }
        }, "src");
        topology.addStateStore(
            Stores.keyValueStoreBuilder(new TidesDBStoreSupplier("latest-store", QUIET),
                                        Serdes.String(), Serdes.String()),
            "latest");

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props("papi"))) {
            TestInputTopic<String, String> in =
                driver.createInputTopic("events", new StringSerializer(), new StringSerializer());
            in.pipeInput("user:1", "alice");
            in.pipeInput("user:2", "bob");
            in.pipeInput("user:1", "alice-v2");
            in.pipeInput("user:2", (String) null);

            KeyValueStore<String, String> store = driver.getKeyValueStore("latest-store");
            assertThat(store.get("user:1")).isEqualTo("alice-v2");
            assertThat(store.get("user:2")).isNull();

            List<KeyValue<String, String>> prefixed = new ArrayList<>();
            try (KeyValueIterator<String, String> it =
                     store.prefixScan("user:", Serdes.String().serializer())) {
                it.forEachRemaining(prefixed::add);
            }
            assertThat(prefixed).containsExactly(KeyValue.pair("user:1", "alice-v2"));

            List<String> reversed = new ArrayList<>();
            in.pipeInput("user:3", "carol");
            try (KeyValueIterator<String, String> it = store.reverseAll()) {
                it.forEachRemaining(kv -> reversed.add(kv.key));
            }
            assertThat(reversed).containsExactly("user:3", "user:1");
        }
    }
}
