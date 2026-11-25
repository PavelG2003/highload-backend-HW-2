package ru.pavelg.eventpipeline.producer;

import ru.pavelg.eventpipeline.model.Event;
import ru.pavelg.eventpipeline.model.EventType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Used by a single producer thread. */
final class EventGenerator {

    private static final EventType[] EVENT_TYPES = EventType.values();
    private static final double POISON_PROBABILITY = 0.02;

    private final String userPrefix;
    private final long[] sequences;
    private int nextUserIndex;

    EventGenerator(String producerId, int userCount) {
        this.userPrefix = producerId + "-" + UUID.randomUUID() + "-";
        this.sequences = new long[userCount];
    }

    Event next() {
        int index = nextUserIndex;
        nextUserIndex = (nextUserIndex + 1) % sequences.length;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return new Event(
                UUID.randomUUID(),
                userPrefix + index,
                EVENT_TYPES[random.nextInt(EVENT_TYPES.length)],
                ++sequences[index],
                Instant.now(),
                Map.of("poison", random.nextDouble() < POISON_PROBABILITY)
        );
    }
}
