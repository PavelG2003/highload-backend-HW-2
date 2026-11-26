package ru.pavelg.eventpipeline.consumer;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.validation.annotation.Validated;
import ru.pavelg.eventpipeline.sink.SinkConfiguration;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
@EnableConfigurationProperties(ConsumerConfiguration.Settings.class)
@Import(SinkConfiguration.class)
class ConsumerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfiguration.class);

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> eventsListenerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            Settings settings,
            @Qualifier("deadLetterTemplate") KafkaTemplate<Object, Object> deadLetterTemplate
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        var container = factory.getContainerProperties();
        container.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        container.setSyncCommits(true);
        container.setAssignmentCommitOption(ContainerProperties.AssignmentCommitOption.NEVER);
        var recoverer = new DeadLetterPublishingRecoverer(deadLetterTemplate,
                (record, failure) -> new TopicPartition(settings.dltTopic(), record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        factory.setCommonErrorHandler(ConsumerFailureHandling.errorHandler(recoverer));
        return factory;
    }

    @Bean(defaultCandidate = false)
    KafkaTemplate<Object, Object> deadLetterTemplate(KafkaTemplate<Object, Object> kafkaTemplate, Settings settings) {
        // Keep the original JSON string instead of serializing it as a JSON string value.
        return new KafkaTemplate<>(kafkaTemplate.getProducerFactory(), false, Map.of(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.CLIENT_ID_CONFIG, settings.id() + "-dlt"
        ));
    }

    @Bean
    EventListener eventListener(Settings settings, JdbcTemplate sinkJdbc, ObjectMapper mapper) {
        var sink = new EventSink(sinkJdbc);
        sink.initialize();
        log.info("Consumer {} initialized: processing_delay_ms={}", settings.id(), settings.processingDelayMs());
        return new EventListener(settings, sink, mapper);
    }

    @Validated
    @ConfigurationProperties(prefix = "app.consumer")
    record Settings(@NotBlank String id, @PositiveOrZero long processingDelayMs, @NotBlank String dltTopic) {
    }
}
