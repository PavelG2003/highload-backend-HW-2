package ru.pavelg.eventpipeline.consumer;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.pavelg.eventpipeline.model.Event;

final class EventSink {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    EventSink(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));
    }

    void initialize() {
        transaction.executeWithoutResult(status -> {
            // Serialize schema creation across the three consumer containers.
            jdbc.execute("SELECT pg_advisory_xact_lock(2642026)");
            new ResourceDatabasePopulator(new ClassPathResource("db/sink-schema.sql"))
                    .execute(jdbc.getDataSource());
        });
    }

    void write(Event event, String consumerId, String topic, int partition, long offset) {
        transaction.executeWithoutResult(status -> {
            // Keep write_order aligned with commit order for each user, even across partitions.
            jdbc.queryForObject("SELECT pg_advisory_xact_lock(2642027, hashtext(?))", String.class, event.userId());
            int inserted = jdbc.update("""
                    INSERT INTO processed_events (event_id, user_id, seq, consumer_id, source_topic, source_partition, source_offset)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (event_id) DO NOTHING
                    """, event.eventId(), event.userId(), event.seq(), consumerId, topic, partition, offset);
            if (inserted == 0) {
                jdbc.update("""
                        INSERT INTO sink_counters (counter_name, counter_value)
                        VALUES ('duplicates', 1)
                        ON CONFLICT (counter_name) DO UPDATE
                        SET counter_value = sink_counters.counter_value + 1
                        """);
            }
        });
    }
}
