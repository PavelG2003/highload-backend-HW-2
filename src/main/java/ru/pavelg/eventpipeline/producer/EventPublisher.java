package ru.pavelg.eventpipeline.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import ru.pavelg.eventpipeline.model.Event;

import java.util.concurrent.CompletableFuture;

final class EventPublisher {

    private static final String TOPIC = "events";

    private final KafkaTemplate<Object, Object> kafka;

    EventPublisher(KafkaTemplate<Object, Object> kafka) {
        this.kafka = kafka;
    }

    CompletableFuture<RecordMetadata> send(Event event, String key) {
        return kafka.send(TOPIC, key, event).thenApply(SendResult::getRecordMetadata);
    }
}
