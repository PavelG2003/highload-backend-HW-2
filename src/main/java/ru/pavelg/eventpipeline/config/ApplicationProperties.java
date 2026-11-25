package ru.pavelg.eventpipeline.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app")
public record ApplicationProperties(@NotNull Role role) {

    public enum Role {
        PRODUCER,
        CONSUMER,
        VERIFY
    }
}
