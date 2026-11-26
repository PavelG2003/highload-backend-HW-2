package ru.pavelg.eventpipeline.verify;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.pavelg.eventpipeline.model.Event;
import ru.pavelg.eventpipeline.sink.SinkConfiguration;
import tools.jackson.databind.ObjectMapper;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.role", havingValue = "verify")
@Import(SinkConfiguration.class)
class VerifyConfiguration {

    @Bean
    ApplicationRunner verifyEvents(JdbcTemplate jdbc, KafkaProperties kafka, ObjectMapper mapper,
                                   @Value("${app.verify.github-login}") String login,
                                   @Value("${app.consumer.topic:events}") String topic,
                                   @Value("${app.consumer.dlt-topic}") String dltTopic) {
        return args -> {
            System.out.printf("%s %s%n", login, OffsetDateTime.now().withNano(0));
            try (var snapshot = new KafkaTopicSnapshot(kafka.getBootstrapServers(), mapper)) {
                var source = snapshot.read(topic);
                var dlt = snapshot.read(dltTopic);
                var sink = jdbc.query("""
                        SELECT event_id, user_id, seq FROM processed_events
                        WHERE source_topic = ? ORDER BY write_order
                        """, (row, number) -> new Sequence(row.getObject("event_id", UUID.class),
                        row.getString("user_id"), row.getLong("seq")), topic);
                long duplicates = jdbc.queryForObject("""
                        SELECT COALESCE((SELECT counter_value FROM sink_counters
                                         WHERE counter_name = 'duplicates'), 0)
                        """, Long.class);
                printReport(source, dlt, sink, duplicates);
            } catch (RuntimeException failure) {
                System.err.println("verify failed: " + failure.getMessage());
                throw failure;
            }
        };
    }

    private static void printReport(List<Event> source, List<Event> dlt, List<Sequence> sink, long duplicates) {
        var delivered = new HashMap<String, NavigableSet<Long>>();
        var deliveredIds = new HashSet<UUID>();
        var maxWritten = new HashMap<String, Long>();
        long outOfOrder = 0;
        for (var row : sink) {
            addDelivered(delivered, row.userId(), row.seq());
            deliveredIds.add(row.eventId());
            long previous = maxWritten.getOrDefault(row.userId(), 0L);
            if (row.seq() < previous) {
                outOfOrder++;
            }
            maxWritten.put(row.userId(), Math.max(previous, row.seq()));
        }
        var dltIds = new HashSet<UUID>();
        for (var event : dlt) {
            addDelivered(delivered, event.userId(), event.seq());
            deliveredIds.add(event.eventId());
            dltIds.add(event.eventId());
        }
        var expectedLast = new TreeMap<String, Long>();
        delivered.forEach((user, seqs) -> expectedLast.put(user, seqs.last()));
        var missingIds = new HashSet<UUID>();
        var sourceIds = new HashSet<UUID>();
        for (var event : source) {
            expectedLast.merge(event.userId(), event.seq(), Math::max);
            sourceIds.add(event.eventId());
            if (!deliveredIds.contains(event.eventId())) {
                missingIds.add(event.eventId());
            }
        }

        System.out.printf("source_events=%d sink_events=%d dlt_events=%d users=%d%n",
                sourceIds.size(), sink.size(), dltIds.size(), expectedLast.size());
        long missingSequences = 0;
        for (var user : expectedLast.entrySet()) {
            var seqs = delivered.get(user.getKey());
            long missing = user.getValue() - (seqs == null ? 0 : seqs.size());
            if (missing > 0) {
                System.out.printf("user_id=%s missing_seq=%d%n", user.getKey(), missing);
                missingSequences += missing;
            }
        }
        System.out.printf("missing_seq=%d missing_source_event_ids=%d%n", missingSequences, missingIds.size());
        System.out.printf("duplicates_dropped=%d sink_out_of_order=%d%n", duplicates, outOfOrder);
    }

    private static void addDelivered(Map<String, NavigableSet<Long>> delivered, String user, long seq) {
        delivered.computeIfAbsent(user, ignored -> new TreeSet<>()).add(seq);
    }

    private record Sequence(UUID eventId, String userId, long seq) {
    }
}
