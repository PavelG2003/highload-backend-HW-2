package ru.pavelg.eventpipeline.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ApplicationProperties.class)
public class BootstrapConfiguration {

    private static final Logger log = LoggerFactory.getLogger(BootstrapConfiguration.class);

    @Bean
    ApplicationRunner logSelectedRole(ApplicationProperties properties) {
        return arguments -> {
            if (properties.role() != ApplicationProperties.Role.PRODUCER) {
                log.info("Application skeleton initialized: role={}. Implementation pending.", properties.role());
            }
        };
    }
}
