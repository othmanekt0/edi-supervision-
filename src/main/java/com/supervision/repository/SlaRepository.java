package com.supervision.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
@Slf4j
public class SlaRepository {

    @Qualifier("supervisionDataSource")
    private final DataSource supervisionDataSource;

    /**
     * Retourne le max_duration_sec du SLA actif pour une interface donnée,
     * à une date donnée (généralement la date de début de l'exécution).
     */
    public Optional<Integer> findActiveMaxDurationSec(Long interfaceId, Instant atDate) {
        if (interfaceId == null) {
            return Optional.empty();
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(supervisionDataSource);

        String sql = """
            SELECT max_duration_sec
            FROM dim_sla
            WHERE interface_id = ?
              AND actif_depuis <= ?
              AND (actif_jusqu_a IS NULL OR actif_jusqu_a >= ?)
            ORDER BY actif_depuis DESC
            LIMIT 1
            """;

        Timestamp ts = Timestamp.from(atDate);

        List<Integer> result = jdbcTemplate.query(sql,
                (rs, rowNum) -> rs.getInt("max_duration_sec"),
                interfaceId, ts, ts);

        return result.stream().findFirst();
    }



    public <T> T queryForObject(String sql, Class<T> type, Object... args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(supervisionDataSource);
        try {
            return jdbcTemplate.queryForObject(sql, type, args);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }

    // À ajouter dans SlaRepository.java
    public String queryCronAttendu(Long interfaceId) {
        if (interfaceId == null) return null;
        JdbcTemplate jdbcTemplate = new JdbcTemplate(supervisionDataSource);
        List<String> rows = jdbcTemplate.queryForList(
                "SELECT cron_attendu FROM dim_sla WHERE interface_id = ? AND actif_jusqu_a IS NULL LIMIT 1",
                String.class, interfaceId);
        return rows.isEmpty() ? null : rows.get(0);
    }
}