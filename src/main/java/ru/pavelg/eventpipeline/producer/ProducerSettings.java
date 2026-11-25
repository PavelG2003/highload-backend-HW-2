package ru.pavelg.eventpipeline.producer;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.producer")
record ProducerSettings(
        @NotBlank String id,
        @Min(1) @Max(1000) int eventsPerSecond,
        @Min(1) @Max(10000) int usersPerProducer,
        @NotNull KeyMode keyMode
) {
    enum KeyMode {
        USER_ID,
        NONE
    }
}
