package ru.pavelg.eventpipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.pavelg.eventpipeline.config.ApplicationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ApplicationProperties.class)
public class EventPipelineApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventPipelineApplication.class, args);
	}

}
