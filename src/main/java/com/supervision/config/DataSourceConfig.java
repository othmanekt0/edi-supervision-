package com.supervision.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    // --- Supervision (base cible, @Primary) ---
    @Value("${spring.datasource.url}")
    private String supervisionUrl;
    @Value("${spring.datasource.username}")
    private String supervisionUsername;
    @Value("${spring.datasource.password}")
    private String supervisionPassword;

    // --- Stambia ---
    @Value("${stambia.datasource.url}")
    private String stambiaUrl;
    @Value("${stambia.datasource.username}")
    private String stambiaUsername;
    @Value("${stambia.datasource.password}")
    private String stambiaPassword;

    // --- Airflow (base native Airflow, lecture seule via AirflowDao) ---
    @Value("${airflow.datasource.url}")
    private String airflowUrl;
    @Value("${airflow.datasource.username}")
    private String airflowUsername;
    @Value("${airflow.datasource.password}")
    private String airflowPassword;

    // ---------------------------------------------------------------
    // Supervision
    // ---------------------------------------------------------------
    @Primary
    @Bean(name = "supervisionDataSource")
    public DataSource supervisionDataSource() {
        return DataSourceBuilder.create()
                .url(supervisionUrl)
                .username(supervisionUsername)
                .password(supervisionPassword)
                .build();
    }

    @Bean(name = "supervisionJdbcTemplate")
    public JdbcTemplate supervisionJdbcTemplate(
            @Qualifier("supervisionDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------
    // Stambia (inchangé)
    // ---------------------------------------------------------------
    @Bean(name = "stambiaDataSource")
    public DataSource stambiaDataSource() {
        return DataSourceBuilder.create()
                .url(stambiaUrl)
                .username(stambiaUsername)
                .password(stambiaPassword)
                .build();
    }

    @Bean(name = "stambiaJdbcTemplate")
    public JdbcTemplate stambiaJdbcTemplate(
            @Qualifier("stambiaDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------
    // Airflow (NOUVEAU — lecture seule, ne participe pas à Flyway)
    // ---------------------------------------------------------------
    @Bean(name = "airflowDataSource")
    public DataSource airflowDataSource() {
        return DataSourceBuilder.create()
                .url(airflowUrl)
                .username(airflowUsername)
                .password(airflowPassword)
                .build();
    }

    @Bean(name = "airflowJdbcTemplate")
    public JdbcTemplate airflowJdbcTemplate(
            @Qualifier("airflowDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------
    // Flyway (sur supervisionDataSource uniquement, inchangé)
    // ---------------------------------------------------------------
    @Bean(initMethod = "migrate")
    public Flyway flyway() {
        return Flyway.configure()
                .dataSource(supervisionUrl, supervisionUsername, supervisionPassword)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .load();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}