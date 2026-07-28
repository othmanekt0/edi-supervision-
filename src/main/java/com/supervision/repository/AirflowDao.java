package com.supervision.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.ZoneOffset;
import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.time.OffsetDateTime;

/**
 * Accès direct (JDBC) à la base Airflow native.
 * Lecture seule — jamais d'écriture dans la base Airflow.
 */
@Repository
@Slf4j
public class AirflowDao {

    private final JdbcTemplate airflowJdbcTemplate;

    public AirflowDao(@Qualifier("airflowDataSource") DataSource airflowDataSource) {
        this.airflowJdbcTemplate = new JdbcTemplate(airflowDataSource);
    }

    public Map<String, Object> getDag(String dagId) {
        List<Map<String, Object>> rows = airflowJdbcTemplate.queryForList("""
                SELECT dag_id, dag_display_name, is_paused, schedule_interval, fileloc
                FROM dag
                WHERE dag_id = ?
                """, dagId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> getAllActiveDags() {
        return airflowJdbcTemplate.queryForList("""
                SELECT dag_id, dag_display_name, is_paused, schedule_interval, fileloc
                FROM dag
                WHERE is_active = true
                """);
    }

    public List<Map<String, Object>> getTaskInstancesForRun(String dagId, String runId) {
        return airflowJdbcTemplate.queryForList("""
                SELECT task_id, state, start_date, end_date, duration, hostname, try_number
                FROM task_instance
                WHERE dag_id = ? AND run_id = ?
                """, dagId, runId);
    }

    public String findRunIdByDagIdAndExecutionDate(String dagId, LocalDateTime executionDate) {
        // Format exact utilisé par Airflow : scheduled__2026-07-17T09:12:00+00:00
        String scheduledRunId = "scheduled__" + executionDate.atOffset(ZoneOffset.UTC)
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx"));

        List<String> rows = airflowJdbcTemplate.queryForList(
                "SELECT run_id FROM dag_run WHERE dag_id = ? AND run_id = ?",
                String.class, dagId, scheduledRunId);

        // Fallback : chercher par execution_date (pour les runs manuels)
        if (rows.isEmpty()) {
            OffsetDateTime executionDateUtc = executionDate.atOffset(ZoneOffset.UTC);
            rows = airflowJdbcTemplate.queryForList(
                    "SELECT run_id FROM dag_run WHERE dag_id = ? AND execution_date = ?",
                    String.class, dagId, executionDateUtc);
        }

        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> getSlaMissesSince(LocalDateTime since) {
        return airflowJdbcTemplate.queryForList("""
                SELECT dag_id, task_id, execution_date, timestamp, description
                FROM sla_miss
                WHERE timestamp > ?
                ORDER BY timestamp
                """, Timestamp.valueOf(since));
    }

    public Integer getTaskInstanceTryNumber(String dagId, String runId, String taskId) {
        List<Integer> rows = airflowJdbcTemplate.queryForList(
                "SELECT try_number FROM task_instance WHERE dag_id = ? AND run_id = ? AND task_id = ?",
                Integer.class, dagId, runId, taskId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, Integer> getXComMetrics(String dagId, String runId) {
        List<Map<String, Object>> rows = airflowJdbcTemplate.queryForList("""
            SELECT key, value
            FROM xcom
            WHERE dag_id = ? AND run_id = ? AND task_id = 'push_metrics'
              AND key IN ('rows_read', 'rows_written', 'rows_rejected')
            """, dagId, runId);

        Map<String, Integer> result = new java.util.HashMap<>();
        for (Map<String, Object> row : rows) {
            String key = (String) row.get("key");
            Object raw = row.get("value");
            if (raw != null) {
                try {
                    // XCom stocké en bytea JSON dans Airflow
                    String str = new String((byte[]) raw).replaceAll("[^0-9]", "");
                    result.put(key, Integer.parseInt(str));
                } catch (Exception e) {
                    log.warn("XCom non parseable : key={} value={}", key, raw);
                }
            }
        }
        return result;
    }
}