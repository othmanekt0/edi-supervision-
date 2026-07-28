package com.supervision.alert;

import com.supervision.alert.util.CronGraceCalculator;
import com.supervision.repository.AirflowDao;
import com.supervision.repository.SlaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class AirflowAlertManager {

    // --- Seuils ---
    private static final int    REPEATED_FAILURE_THRESHOLD = 3;
    private static final int    RETRY_THRESHOLD            = 3;
    private static final Duration LATE_START_GRACE         = Duration.ofMinutes(15);
    private static final Duration DEFAULT_STUCK_THRESHOLD  = Duration.ofHours(2);

    private final JdbcTemplate        jdbcTemplate; // supervision
    private final AirflowDao          airflowDao;
    private final SlaRepository       slaRepository;
    private final CronGraceCalculator cronGraceCalculator;

    public AirflowAlertManager(
            @Qualifier("supervisionDataSource") DataSource dataSource,
            AirflowDao airflowDao,
            SlaRepository slaRepository,
            CronGraceCalculator cronGraceCalculator) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.airflowDao   = airflowDao;
        this.slaRepository = slaRepository;
        this.cronGraceCalculator = cronGraceCalculator;
    }

    // ================================================================
    // ALERTES RÉACTIVES (déclenchées par un événement CDC)
    // ================================================================

    public void handleSlaMiss(String dagId, LocalDateTime executionDate,
                              LocalDateTime triggeredAt, String description) {
        log.warn("handleSlaMiss : dag={} executionDate={}", dagId, executionDate);

        String runId = airflowDao.findRunIdByDagIdAndExecutionDate(dagId, executionDate);
        if (runId == null) {
            log.warn("Aucun run_id pour sla_miss dag={} execution_date={}", dagId, executionDate);
            return;
        }
        Long interfaceId = findInterfaceIdByCode(dagId);
        if (interfaceId == null) {
            log.warn("Interface introuvable pour dag_id={}", dagId);
            return;
        }
        Long executionId = findExecutionId(runId, interfaceId);
        if (executionId == null) {
            log.warn("FACT_EXECUTION introuvable pour dag={} run={}", dagId, runId);
            return;
        }

        String alertType = resolveSlaMissAlertType(description);
        String severity  = resolveSlaMissSeverity(alertType);
        LocalDateTime at = triggeredAt != null ? triggeredAt : LocalDateTime.now(ZoneOffset.UTC);
        String message = String.format("SLA miss détecté par Airflow pour dag=%s : %s",
                dagId, description != null ? description : "aucun détail");

        insertAlert(executionId, alertType, severity, at, message);
    }

    public void handleTaskFailure(Long executionId, String dagId, String runId,
                                  String taskId, LocalDateTime triggeredAt) {
        if (executionId == null) return;
        LocalDateTime at = triggeredAt != null ? triggeredAt : LocalDateTime.now(ZoneOffset.UTC);
        String message = String.format("Tâche en échec : dag=%s run=%s task=%s", dagId, runId, taskId);
        insertAlert(executionId, "TASK_FAILED", "MEDIUM", at, message);
        log.warn("FACT_ALERT TASK_FAILED : dag={} run={} task={}", dagId, runId, taskId);
    }

    public void handleDurationBreach(Long executionId, Long interfaceId, Integer durationSeconds) {
        if (executionId == null || durationSeconds == null) return;

        Optional<Integer> maxDuration = slaRepository.findActiveMaxDurationSec(
                interfaceId, Instant.now());
        if (maxDuration.isEmpty() || maxDuration.get() <= 0 || durationSeconds <= maxDuration.get()) return;

        LocalDateTime at = resolveEndDatetime(executionId);
        String message = String.format("Durée %ds dépasse le SLA de %ds", durationSeconds, maxDuration.get());
        insertAlert(executionId, "SLA_BREACH", "CRITICAL", at, message);
        log.warn("FACT_ALERT SLA_BREACH : executionId={} duree={}s max={}s",
                executionId, durationSeconds, maxDuration.get());
    }

    /**
     * FIX (28/07/2026 - bug B3 RETRY_EXHAUSTED, mismatch de valeur de statut) :
     * currentStatus reçu ici est toujours normalisé en "FAILURE" par
     * AirflowLoader.normalizeExecutionStatus (case "FAILED" -> "FAILURE").
     * La comparaison utilisait auparavant la constante "FAILED", qui ne
     * correspond jamais à la valeur réellement transmise : la condition
     * !"FAILED".equalsIgnoreCase(currentStatus) était donc toujours vraie et
     * la méthode retournait systématiquement sans jamais évaluer retryCount.
     * Confirmé en test : run avec status=FAILURE en base et retry_count=3,
     * aucune alerte DAG_ABORTED créée malgré start_datetime renseignée et
     * end_datetime nulle. Remplacé "FAILED" par "FAILURE" ici.
     */
    public void handleDagAborted(Long executionId, String currentStatus) {
        if (executionId == null || !"FAILURE".equalsIgnoreCase(currentStatus)) return;

        Boolean hasStart = queryBoolean(
                "SELECT start_datetime IS NOT NULL FROM fact_execution WHERE execution_id = ?",
                executionId);
        Boolean hasEnd = queryBoolean(
                "SELECT end_datetime IS NOT NULL FROM fact_execution WHERE execution_id = ?",
                executionId);

        if (!Boolean.TRUE.equals(hasStart) || Boolean.TRUE.equals(hasEnd)) return;

        insertAlert(executionId, "DAG_ABORTED", "CRITICAL", LocalDateTime.now(ZoneOffset.UTC),
                "DAG interrompu sans end_datetime (abandon détecté)");
        log.warn("FACT_ALERT DAG_ABORTED : executionId={}", executionId);
    }

    public void handleUpstreamFailed(Long executionId, List<Map<String, Object>> stepRows) {
        if (executionId == null || stepRows == null) return;

        boolean any = stepRows.stream()
                .anyMatch(s -> "UPSTREAM_FAILED".equalsIgnoreCase((String) s.get("status")));
        if (!any) return;

        insertAlert(executionId, "UPSTREAM_FAILED", "MEDIUM", LocalDateTime.now(ZoneOffset.UTC),
                "Au moins une tâche en amont a échoué (upstream_failed)");
        log.warn("FACT_ALERT UPSTREAM_FAILED : executionId={}", executionId);
    }

    /**
     * FIX (28/07/2026 - bug B3 RETRY_EXHAUSTED) : voir NOTE sur handleDagAborted
     * ci-dessus, même cause exacte. "FAILED" remplacé par "FAILURE". C'est ce
     * remplacement qui corrige le bug B3 (aucune alerte RETRY_EXHAUSTED créée
     * malgré retry_count=3 en base sur un run FAILURE).
     */
    public void handleRetryExhausted(Long executionId, String currentStatus, int retryCount) {
        if (executionId == null || !"FAILURE".equalsIgnoreCase(currentStatus)) return;
        if (retryCount < RETRY_THRESHOLD) return;

        String message = String.format("Échec après épuisement des retries (retryCount=%d)", retryCount);
        insertAlert(executionId, "RETRY_EXHAUSTED", "CRITICAL", LocalDateTime.now(ZoneOffset.UTC), message);
        log.warn("FACT_ALERT RETRY_EXHAUSTED : executionId={} retryCount={}", executionId, retryCount);
    }

    /**
     * FIX (28/07/2026 - bug B3 RETRY_EXHAUSTED) : même mismatch "FAILED" vs
     * "FAILURE", à deux endroits dans cette méthode : le early-return sur
     * currentStatus, et le filtre allMatch sur l'historique des statuts lus
     * en base (qui contiennent aussi "FAILURE", jamais "FAILED"). Les deux
     * remplacés par "FAILURE".
     */
    public void handleRepeatedFailure(Long executionId, Long interfaceId, String currentStatus) {
        if (executionId == null || interfaceId == null) return;
        if (!"FAILURE".equalsIgnoreCase(currentStatus)) return;

        List<String> lastStatuses = jdbcTemplate.queryForList("""
                SELECT status FROM fact_execution
                WHERE interface_id = ?
                ORDER BY start_datetime DESC
                LIMIT ?
                """, String.class, interfaceId, REPEATED_FAILURE_THRESHOLD);

        if (lastStatuses.size() < REPEATED_FAILURE_THRESHOLD) return;

        boolean allFailed = lastStatuses.stream()
                .allMatch(s -> "FAILURE".equalsIgnoreCase(s));
        if (!allFailed) return;

        String message = String.format("%d échecs consécutifs sur cette interface", REPEATED_FAILURE_THRESHOLD);
        insertAlert(executionId, "REPEATED_FAILURE", "CRITICAL", LocalDateTime.now(ZoneOffset.UTC), message);
        log.warn("FACT_ALERT REPEATED_FAILURE : interfaceId={}", interfaceId);
    }

    public void handleManualOverride(Long executionId, String triggerType) {
        if (executionId == null || triggerType == null) return;
        String tt = triggerType.toLowerCase();
        if (!tt.contains("manual") && !tt.contains("external")) return;

        String message = "Déclenchement manuel/externe détecté (triggerType=" + triggerType + ")";
        insertAlert(executionId, "MANUAL_OVERRIDE", "LOW", LocalDateTime.now(ZoneOffset.UTC), message);
        log.info("FACT_ALERT MANUAL_OVERRIDE : executionId={} triggerType={}", executionId, triggerType);
    }

    public void handleLateStart(Long executionId, Long interfaceId, LocalDateTime startDatetime) {
        if (executionId == null || startDatetime == null) return;

        String cronAttendu = slaRepository.queryCronAttendu(interfaceId);
        if (cronAttendu == null) return;

        LocalDateTime expected = cronGraceCalculator.previousExpectedFireTime(cronAttendu, startDatetime);
        if (expected == null) return;

        Duration delay = Duration.between(expected, startDatetime);
        if (delay.compareTo(LATE_START_GRACE) <= 0) return;

        String message = String.format("Démarrage tardif : attendu %s, retard de %dmin",
                expected, delay.toMinutes());
        insertAlert(executionId, "LATE_START", "MEDIUM", LocalDateTime.now(ZoneOffset.UTC), message);
        log.warn("FACT_ALERT LATE_START : executionId={} retard={}min",
                executionId, delay.toMinutes());
    }

    // ================================================================
    // SCAN PÉRIODIQUE (toutes les 5 min)
    // ================================================================

    @Scheduled(fixedDelay = 5 * 60 * 1000) // 5 minutes
    public void scanForSilentFailures() {
        checkNotTriggered();
        checkStuckRunning();
    }

    private void checkNotTriggered() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);

        List<Map<String, Object>> slaRows = jdbcTemplate.queryForList("""
            SELECT s.sla_id, s.interface_id, s.cron_attendu,
                   i.code, i.actif
            FROM dim_sla s
            JOIN dim_interface i ON i.interface_id = s.interface_id
            WHERE s.cron_attendu IS NOT NULL
              AND i.actif = true
              AND EXISTS (
                  SELECT 1 FROM fact_execution e
                  WHERE e.interface_id = s.interface_id
                    AND e.source_id = 2
              )
            """);

        for (Map<String, Object> row : slaRows) {
            String cron        = (String) row.get("cron_attendu");
            Long   interfaceId = toLong(row.get("interface_id"));
            String ifaceCode   = (String) row.get("code");

            // Dernier run connu pour cette interface — on part de là, pas de "now"
            List<Map<String, Object>> lastRunRows = jdbcTemplate.queryForList("""
                SELECT execution_id, start_datetime
                FROM fact_execution
                WHERE interface_id = ? AND source_id = 2
                  AND start_datetime IS NOT NULL
                ORDER BY start_datetime DESC
                LIMIT 1
                """, interfaceId);

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

            String message = String.format("Interface %s non déclenchée depuis %s (grâce %ds dépassée)",
                    ifaceCode, expected, grace.toSeconds());
            insertAlert(executionId, "DAG_NOT_TRIGGERED", "CRITICAL", now, message);
            log.warn("FACT_ALERT DAG_NOT_TRIGGERED : interface={} attendu={} grace={}s",
                    ifaceCode, expected, grace.toSeconds());
        }
    }

    private void checkStuckRunning() {
        LocalDateTime now           = LocalDateTime.now(ZoneOffset.UTC);
        LocalDateTime defaultCutoff = now.minus(DEFAULT_STUCK_THRESHOLD);

        List<Map<String, Object>> runningRows = jdbcTemplate.queryForList("""
                SELECT execution_id, interface_id, start_datetime
                FROM fact_execution
                WHERE UPPER(status) = 'RUNNING'
                  AND source_id = 2
                  AND start_datetime < ?
                """, Timestamp.valueOf(defaultCutoff));

        for (Map<String, Object> row : runningRows) {
            Long          executionId = toLong(row.get("execution_id"));
            Long          interfaceId = toLong(row.get("interface_id"));
            LocalDateTime start       = toLocalDateTime(row.get("start_datetime"));
            if (start == null) continue;

            Duration threshold = DEFAULT_STUCK_THRESHOLD;
            Optional<Integer> maxDur = slaRepository.findActiveMaxDurationSec(interfaceId, Instant.now());
            if (maxDur.isPresent()) {
                threshold = Duration.ofSeconds(maxDur.get() * 2L);
            }
            if (Duration.between(start, now).compareTo(threshold) < 0) continue;

            String message = String.format("Exécution RUNNING depuis plus de %ds sans conclusion",
                    threshold.toSeconds());
            insertAlert(executionId, "STUCK_RUNNING", "MEDIUM", now, message);
            log.warn("FACT_ALERT STUCK_RUNNING : executionId={}", executionId);
        }
    }

    // ================================================================
    // INSERTION — idempotente via l'index unique partiel de fact_alert
    // (execution_id, alert_type) WHERE resolved_at IS NULL, identique à
    // celui utilisé par AlertManager.createAlertIfAbsent().
    // ================================================================

    private void insertAlert(Long executionId, String alertType, String severity,
                             LocalDateTime triggeredAt, String message) {
        Integer rows = jdbcTemplate.update("""
                INSERT INTO fact_alert (execution_id, alert_type, severity, triggered_at, message)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (execution_id, alert_type) WHERE (resolved_at IS NULL) DO NOTHING
                """,
                executionId,
                alertType,
                severity,
                Timestamp.valueOf(triggeredAt != null ? triggeredAt : LocalDateTime.now(ZoneOffset.UTC)),
                message);

        if (rows == null || rows == 0) {
            log.debug("Alerte {} déjà existante (ouverte) pour execution_id={}, pas de doublon créé",
                    alertType, executionId);
        }
    }

    // ================================================================
    // UTILITAIRES PRIVÉS
    // ================================================================

    private Long findInterfaceIdByCode(String dagId) {
        List<Long> rows = jdbcTemplate.queryForList(
                "SELECT interface_id FROM dim_interface WHERE code = ?",
                Long.class, dagId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Long findExecutionId(String runId, Long interfaceId) {
        List<Long> rows = jdbcTemplate.queryForList(
                "SELECT execution_id FROM fact_execution WHERE source_execution_ref = ? AND interface_id = ?",
                Long.class, runId, interfaceId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private LocalDateTime resolveEndDatetime(Long executionId) {
        List<Timestamp> rows = jdbcTemplate.queryForList(
                "SELECT end_datetime FROM fact_execution WHERE execution_id = ?",
                Timestamp.class, executionId);
        if (rows.isEmpty() || rows.get(0) == null) return LocalDateTime.now(ZoneOffset.UTC);
        return rows.get(0).toLocalDateTime();
    }

    private Boolean queryBoolean(String sql, Object... args) {
        try {
            return jdbcTemplate.queryForObject(sql, Boolean.class, args);
        } catch (Exception e) {
            return null;
        }
    }

    private String resolveSlaMissAlertType(String description) {
        if (description == null) return "SLA_BREACH";
        String d = description.toLowerCase();
        if (d.contains("timeout")) return "TIMEOUT";
        if (d.contains("failure") || d.contains("failed")) return "TASK_FAILED";
        return "SLA_BREACH";
    }

    private String resolveSlaMissSeverity(String alertType) {
        return switch (alertType) {
            case "SLA_BREACH", "TIMEOUT" -> "CRITICAL";
            case "TASK_FAILED"           -> "MEDIUM";
            default                      -> "LOW";
        };
    }

    private Long toLong(Object val) {
        if (val == null) return null;
        return ((Number) val).longValue();
    }

    private LocalDateTime toLocalDateTime(Object val) {
        if (val == null) return null;
        if (val instanceof Timestamp ts) return ts.toLocalDateTime();
        if (val instanceof LocalDateTime ldt) return ldt;
        return null;
    }
}