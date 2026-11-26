package ru.pavelg.eventpipeline.verify;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.pavelg.eventpipeline.model.Event;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

final class KafkaTopicSnapshot implements AutoCloseable {

    private static final Duration API_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration SCAN_TIMEOUT = Duration.ofSeconds(45);

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper;

    KafkaTopicSnapshot(List<String> bootstrapServers, ObjectMapper mapper) {
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.CLIENT_ID_CONFIG, "hw2-verify",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none",
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) API_TIMEOUT.toMillis()
        ), new StringDeserializer(), new StringDeserializer());
        this.mapper = mapper;
    }

    List<Event> read(String topic) {
        var partitions = consumer.partitionsFor(topic, API_TIMEOUT).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
        if (partitions.isEmpty()) {
            throw new IllegalStateException("Topic has no partitions: " + topic);
        }

        consumer.assign(partitions);
        var starts = consumer.beginningOffsets(partitions, API_TIMEOUT);
        var ends = consumer.endOffsets(partitions, API_TIMEOUT);
        starts.forEach(consumer::seek);
        var pending = new HashSet<>(partitions);
        pending.removeIf(partition -> starts.get(partition) >= ends.get(partition));
        var events = new ArrayList<Event>();
        long deadline = System.nanoTime() + SCAN_TIMEOUT.toNanos();

        while (!pending.isEmpty()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out reading Kafka snapshot: " + pending);
            }
            var records = consumer.poll(Duration.ofMillis(250));
            for (var partition : pending) {
                for (var record : records.records(partition)) {
                    if (record.offset() < ends.get(partition)) {
                        events.add(mapper.readValue(record.value(), Event.class));
                    }
                }
            }
            pending.removeIf(partition -> consumer.position(partition, API_TIMEOUT) >= ends.get(partition));
        }
        return events;
    }

    @Override
    public void close() {
        consumer.close(CloseOptions.timeout(API_TIMEOUT));
    }
}
