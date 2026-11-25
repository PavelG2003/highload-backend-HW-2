package ru.pavelg.eventpipeline.producer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.role", havingValue = "producer")
@EnableConfigurationProperties(ProducerSettings.class)
class ProducerConfiguration {

    @Bean
    EventProducer eventProducer(ProducerSettings settings, KafkaTemplate<Object, Object> kafka) {
        return new EventProducer(settings, new EventPublisher(kafka), event -> switch (settings.keyMode()) {
            case USER_ID -> event.userId();
            case NONE -> null;
        });
    }
}
