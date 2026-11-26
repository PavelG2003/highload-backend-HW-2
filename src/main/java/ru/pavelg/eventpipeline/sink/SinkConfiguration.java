package ru.pavelg.eventpipeline.sink;

import jakarta.validation.constraints.NotBlank;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.validation.annotation.Validated;

// Imported by the roles that need the sink.
@EnableConfigurationProperties(SinkConfiguration.Settings.class)
public class SinkConfiguration {

    @Bean
    JdbcTemplate sinkJdbc(Settings settings) {
        var dataSource = new PGSimpleDataSource();
        dataSource.setURL(settings.url());
        dataSource.setUser(settings.username());
        dataSource.setPassword(settings.password());
        dataSource.setConnectTimeout(5);
        return new JdbcTemplate(dataSource);
    }

    @Validated
    @ConfigurationProperties(prefix = "app.sink")
    record Settings(@NotBlank String url, @NotBlank String username, @NotBlank String password) {
    }
}
