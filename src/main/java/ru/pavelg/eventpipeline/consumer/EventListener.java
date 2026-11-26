package ru.pavelg.eventpipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import ru.pavelg.eventpipeline.model.Event;
import tools.jackson.databind.ObjectMapper;

final class EventListener {

    private final ConsumerConfiguration.Settings settings;
    private final EventSink sink;
    private final ObjectMapper mapper;

    EventListener(ConsumerConfiguration.Settings settings, EventSink sink, ObjectMapper mapper) {
        this.settings = settings;
        this.sink = sink;
        this.mapper = mapper;
    }

    @KafkaListener(id = "events-listener", idIsGroup = false, topics = "${app.consumer.topic:events}",
            containerFactory = "eventsListenerFactory")
    void process(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws InterruptedException {
        Event event = mapper.readValue(record.value(), Event.class);
        Thread.sleep(settings.processingDelayMs());
        if (Boolean.TRUE.equals(event.payload().get("poison"))) {
            throw new PoisonEventException(event.eventId());
        }

        sink.write(event, settings.id(), record.topic(), record.partition(), record.offset());
        acknowledgment.acknowledge();
    }
}
