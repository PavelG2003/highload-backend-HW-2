package ru.pavelg.eventpipeline.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

final class ConsumerFailureHandling {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFailureHandling.class);
    private static final long RETRY_DELAY_MS = 1000;
    private static final long POISON_RETRIES = 2;

    private ConsumerFailureHandling() {
    }

    static DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        var errorHandler = new DefaultErrorHandler((record, failure) -> {
            if (!isPoison(failure)) {
                throw new IllegalStateException("Only poison events can be sent to the DLT", failure);
            }
            recoverer.accept(record, failure);
            log.info("Poison event published to DLT: topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
        }, new FixedBackOff(RETRY_DELAY_MS, FixedBackOff.UNLIMITED_ATTEMPTS));
        errorHandler.setClassifications(Map.of(), true);
        errorHandler.setBackOffFunction((record, failure) -> isPoison(failure)
                ? new FixedBackOff(RETRY_DELAY_MS, POISON_RETRIES)
                : null);
        errorHandler.setCommitRecovered(true);
        errorHandler.setAckAfterHandle(false);
        errorHandler.setRetryListeners((record, failure, attempt) -> log.warn(
                "Processing failed: topic={} partition={} offset={} attempt={} cause={}",
                record.topic(), record.partition(), record.offset(), attempt,
                NestedExceptionUtils.getMostSpecificCause(failure).getMessage()));
        return errorHandler;
    }

    private static boolean isPoison(Exception failure) {
        return NestedExceptionUtils.getMostSpecificCause(failure) instanceof PoisonEventException;
    }
}
