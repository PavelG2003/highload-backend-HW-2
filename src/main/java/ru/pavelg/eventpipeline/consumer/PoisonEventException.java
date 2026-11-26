package ru.pavelg.eventpipeline.consumer;

import java.util.UUID;

final class PoisonEventException extends RuntimeException {

    PoisonEventException(UUID eventId) {
        super("Event " + eventId + " has payload.poison=true");
    }
}
