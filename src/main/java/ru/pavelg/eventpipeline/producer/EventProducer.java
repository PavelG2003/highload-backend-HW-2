package ru.pavelg.eventpipeline.producer;

import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaProducerException;
import ru.pavelg.eventpipeline.model.Event;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class EventProducer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

    private final ProducerSettings settings;
    private final EventPublisher publisher;
    private final Function<Event, String> keyFor;
    private volatile Thread worker;
    private CountDownLatch stopRequested;

    EventProducer(ProducerSettings settings, EventPublisher publisher, Function<Event, String> keyFor) {
        this.settings = settings;
        this.publisher = publisher;
        this.keyFor = keyFor;
    }

    @Override
    public synchronized void start() {
        if (worker != null) {
            return;
        }
        stopRequested = new CountDownLatch(1);
        worker = new Thread(this::produce, settings.id());
        worker.setDaemon(false);
        worker.start();
    }

    @Override
    public synchronized void stop() {
        if (worker == null) {
            return;
        }
        stopRequested.countDown();
        try {
            worker.join();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for producer {} to stop", settings.id(), exception);
        } finally {
            worker = null;
        }
    }

    @Override
    public boolean isRunning() {
        return worker != null;
    }

    private void produce() {
        EventGenerator generator = new EventGenerator(settings.id(), settings.usersPerProducer());
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / settings.eventsPerSecond();
        long confirmed = 0;
        log.info("Producer {} started: target_rate={}/s users={}",
                settings.id(), settings.eventsPerSecond(), settings.usersPerProducer());

        try {
            while (stopRequested.getCount() != 0) {
                long startedAt = System.nanoTime();
                Event event = generator.next();
                if (!sendUntilConfirmed(event, keyFor.apply(event))) {
                    break;
                }
                confirmed++;
                if (confirmed % settings.eventsPerSecond() == 0) {
                    log.info("Producer {} confirmed {} events", settings.id(), confirmed);
                }
                long remaining = intervalNanos - (System.nanoTime() - startedAt);
                if (remaining > 0) {
                    stopRequested.await(remaining, TimeUnit.NANOSECONDS);
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.warn("Producer {} was interrupted", settings.id(), exception);
        } finally {
            log.info("Producer {} stopped: confirmed={}", settings.id(), confirmed);
        }
    }

    private boolean sendUntilConfirmed(Event event, String key) throws InterruptedException {
        while (true) {
            try {
                var metadata = publisher.send(event, key).join();
                log.debug("Confirmed event_id={} user_id={} seq={} partition={} offset={}",
                        event.eventId(), event.userId(), event.seq(), metadata.partition(), metadata.offset());
                return true;
            } catch (RuntimeException failure) {
                Throwable cause = failure;
                while ((cause instanceof CompletionException || cause instanceof KafkaProducerException)
                        && cause.getCause() != null) {
                    cause = cause.getCause();
                }
                log.error("Producer {} did not confirm event_id={} user_id={} seq={}",
                        settings.id(), event.eventId(), event.userId(), event.seq(), cause);
                if (!(cause instanceof RetriableException) || stopRequested.await(1, TimeUnit.SECONDS)) {
                    return false;
                }
            }
        }
    }
}
