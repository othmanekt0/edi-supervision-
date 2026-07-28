package com.supervision.alert;

import com.supervision.alert.util.CronGraceCalculator;
import com.supervision.dto.AlertSeverity;
import com.supervision.dto.AlertType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * Symétrique de AirflowAlertManager.checkNotTriggered(), mais côté Stambia
 * (source_id = 1). Composant séparé et purement "scan périodique" — le
 * AlertManager Stambia existant reste réactif (décision 2 du plan de
 * couverture KPI du 17/07/2026).
 *
 * Contrairement à Airflow, Stambia ne produit aucun signal CDC en l'absence
 * de déclenchement (pas de heartbeat de scheduler) : le calcul se base
 * uniquement sur le dernier fact_execution connu pour l'interface, exactement
 * comme le fait déjà checkNotTriggered() côté Airflow.
 *
 * Aligné sur le style de AlertManager (enums AlertType/AlertSeverity, colonne
 * message, ON CONFLICT (execution_id, alert_type) WHERE resolved_at IS NULL)
 * plutôt que sur le SQL brut de AirflowAlertManager, pour rester cohérent
 * avec le seul index unique réel de fact_alert.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class StambiaAlertManager {

    private static final int SOURCE_ID_STAMBIA = 1;

    @Qualifier("supervisionDataSource")
    private final DataSource supervisionDataSource;

    private final CronGraceCalculator cronGraceCalculator;

    private JdbcTemplate jdbc() {
        return new JdbcTemplate(supervisionDataSource);
    }

    // ================================================================
    // SCAN PÉRIODIQUE (toutes les 5 min) — miroir de AirflowAlertManager
    // ================================================================

    @Scheduled(fixedDelay = 5 * 60 * 1000) // 5 minutes
    public void scanForSilentStambiaFailures() {
        checkStambiaNotTriggered();
    }

    private void checkStambiaNotTriggered() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        List<Map<String, Object>> slaRows = jdbc().queryForList("""
            SELECT s.sla_id, s.interface_id, s.cron_attendu,
                   i.code, i.actif
            FROM dim_sla s
            JOIN dim_interface i ON i.interface_id = s.interface_id
            WHERE s.cron_attendu IS NOT NULL
              AND i.actif = true
              AND EXISTS (
                  SELECT 1 FROM fact_execution e
                  WHERE e.interface_id = s.interface_id
                    AND e.source_id = ?
              )
            """, SOURCE_ID_STAMBIA);

        for (Map<String, Object> row : slaRows) {
            String cron        = (String) row.get("cron_attendu");
            Long   interfaceId = toLong(row.get("interface_id"));
            String ifaceCode   = (String) row.get("code");

            // Dernier run connu pour cette interface — on part de là, pas de "now"
            List<Map<String, Object>> lastRunRows = jdbc().queryForList("""
                SELECT execution_id, start_datetime
                FROM fact_execution
                WHERE interface_id = ? AND source_id = ?
                  AND start_datetime IS NOT NULL
                ORDER BY start_datetime DESC
                LIMIT 1
                """, interfaceId, SOURCE_ID_STAMBIA);

            if (lastRunRows.isEmpty()) {
                continue;
            }

            Map<String, Object> lastRun = lastRunRows.get(0);
            LocalDateTime lastStart      = toLocalDateTime(lastRun.get("start_datetime"));
            Long executionId             = toLong(lastRun.get("execution_id"));

            if (lastStart == null) {
                continue;
            }

            // Le tir attendu juste APRÈS le dernier run réel — fixe, ne dépend pas de "now"
            LocalDateTime expected = cronGraceCalculator.nextExpectedFireTime(cron, lastStart);
            if (expected == null) {
                continue;
            }

            Duration grace = cronGraceCalculator.computeDynamicGrace(cron, expected);

            if (expected.plus(grace).isAfter(now)) {
                continue;
            }

            String message = String.format(
                    "Interface %s non déclenchée depuis %s (attendu, grâce %ds dépassée)",
                    ifaceCode, expected, grace.toSeconds()
            );

            // Nom distinct de DAG_NOT_TRIGGERED pour garder la distinction Stambia/Airflow
            // dans les dashboards (cf. plan de couverture KPI, point 2.1).
            createAlertIfAbsent(executionId, AlertType.SESSION_NOT_TRIGGERED, AlertSeverity.CRITICAL, message);
        }
    }

    // ================================================================
    // INSERTION — même pattern que AlertManager.createAlertIfAbsent()
    // ================================================================

    private void createAlertIfAbsent(Long executionId, AlertType type, AlertSeverity severity, String message) {
        Integer rows = jdbc().update("""
                INSERT INTO fact_alert (execution_id, alert_type, severity, triggered_at, message)
                VALUES (?, ?, ?, NOW(), ?)
                ON CONFLICT (execution_id, alert_type) WHERE (resolved_at IS NULL) DO NOTHING
                """,
                executionId, type.name(), severity.name(), message
        );

        if (rows != null && rows > 0) {
            log.warn("Alerte {} créée pour execution_id={} : {}", type, executionId, message);
        } else {
            log.debug("Alerte {} déjà existante (ouverte) pour execution_id={}, pas de doublon créé", type, executionId);
        }
    }

    // ================================================================
    // UTILITAIRES PRIVÉS
    // ================================================================

    private Long toLong(Object val) {
        if (val == null) return null;
        return ((Number) val).longValue();
    }

    private LocalDateTime toLocalDateTime(Object val) {
        if (val == null) return null;
        if (val instanceof java.sql.Timestamp ts) return ts.toLocalDateTime();
        if (val instanceof LocalDateTime ldt) return ldt;
        return null;
    }
}