package ru.pavelg.eventpipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record Event(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("user_id") String userId,
        EventType type,
        long seq,
        @JsonProperty("produced_at") Instant producedAt,
        Map<String, Object> payload
) {
}
