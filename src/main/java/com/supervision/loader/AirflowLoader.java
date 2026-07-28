package com.supervision.loader;

import com.supervision.alert.AirflowAlertManager;
import com.supervision.alert.ExecutionStatusResolver;
import com.supervision.dto.DagDTO;
import com.supervision.dto.DagRunDTO;
import com.supervision.dto.SlaMissDTO;
import com.supervision.dto.TaskInstanceDTO;
import com.supervision.repository.AirflowDao;
import com.supervision.repository.SlaRepository;
import com.supervision.util.AirflowLogClient;
import com.supervision.util.AirflowVersionReader;
import com.supervision.util.DagFileReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class AirflowLoader {

    private static final int  AIRFLOW_SOURCE_ID = 2;
    private static final int  PROD_ENV_ID       = 3;
    private static final int  DEV_ENV_ID        = 1;
    private static final Long DEFAULT_CLIENT_ID = 1L;

    private final JdbcTemplate            jdbcTemplate;
    private final AirflowDao              airflowDao;
    private final AirflowAlertManager     alertManager;
    private final ExecutionStatusResolver statusResolver;
    private final AirflowLogClient        airflowLogClient;
    private final AirflowVersionReader    versionReader;
    private final DagFileReader           dagFileReader;
    private final SlaRepository           slaRepository;

    public AirflowLoader(
            @Qualifier("supervisionDataSource") DataSource dataSource,
            AirflowDao airflowDao,
            AirflowAlertManager alertManager,
            ExecutionStatusResolver statusResolver,
            AirflowLogClient airflowLogClient,
            AirflowVersionReader versionReader,
            DagFileReader dagFileReader,
            SlaRepository slaRepository) {
        this.jdbcTemplate     = new JdbcTemplate(dataSource);
        this.airflowDao       = airflowDao;
        this.alertManager     = alertManager;
        this.statusResolver   = statusResolver;
        this.airflowLogClient = airflowLogClient;
        this.versionReader    = versionReader;
        this.dagFileReader    = dagFileReader;
        this.slaRepository    = slaRepository;
    }

    // ================================================================
    // dag_run → fact_execution
    // ================================================================

    public void upsertExecutionFromDagRun(DagRunDTO dto) {
        Long interfaceId = resolveOrCreateInterface(dto.getDagId());

        String normalizedStatus  = normalizeExecutionStatus(dto.getStatus());
        String normalizedTrigger = normalizeTriggerType(dto.getTriggerType());
        Integer duration         = computeDuration(dto.getStartDatetime(), dto.getEndDatetime());

        Map<String, Object> row = jdbcTemplate.queryForMap("""
                INSERT INTO fact_execution (
                    source_execution_ref, interface_id, source_id, env_id,
                    status, trigger_type, start_datetime, end_datetime,
                    duration_seconds, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                ON CONFLICT (source_execution_ref, source_id)
                DO UPDATE SET
                    status           = EXCLUDED.status,
                    start_datetime   = COALESCE(fact_execution.start_datetime, EXCLUDED.start_datetime),
                    end_datetime     = EXCLUDED.end_datetime,
                    duration_seconds = EXCLUDED.duration_seconds,
                    trigger_type     = EXCLUDED.trigger_type,
                    ingested_at      = NOW()
                RETURNING execution_id, (xmax = 0) AS is_insert
                """,
                dto.getRunId(), interfaceId, AIRFLOW_SOURCE_ID, resolveEnvId(dto.getDagId()),
                normalizedStatus, normalizedTrigger,
                toTs(dto.getStartDatetime()), toTs(dto.getEndDatetime()),
                duration);

        Long    executionId = ((Number) row.get("execution_id")).longValue();
        boolean isInsert    = Boolean.TRUE.equals(row.get("is_insert"));

        if (dto.getEndDatetime() != null) {
            alertManager.handleDurationBreach(executionId, interfaceId, duration);
        }

        recomputeAggregatedFields(executionId, interfaceId,
                dto.getDagId(), dto.getRunId(), normalizedStatus, normalizedTrigger,
                isInsert);
    }

    // ================================================================
    // task_instance → fact_execution_step
    // ================================================================

    public void upsertStepFromTaskInstance(TaskInstanceDTO dto) {
        Long executionId = resolveExecutionId(dto.getRunId());
        if (executionId == null) {
            log.debug("FACT_EXECUTION introuvable pour run={}, task {} ignorée",
                    dto.getRunId(), dto.getTaskId());
            return;
        }

        String normalizedState = normalizeStepStatus(dto.getState());
        String sourceStepRef   = dto.getDagId() + "." + dto.getRunId() + "." + dto.getTaskId();
        String errorMessage    = null;

        if ("FAILURE".equals(normalizedState)) {
            errorMessage = resolveTaskErrorMessage(dto);
        }

        jdbcTemplate.update("""
                INSERT INTO fact_execution_step (
                    execution_id, parent_step_id, source_step_ref,
                    step_name, step_type, status,
                    start_datetime, end_datetime, error_message
                ) VALUES (?, NULL, ?, ?, 'TASK', ?, ?, ?, ?)
                ON CONFLICT (source_step_ref) DO UPDATE SET
                    status        = EXCLUDED.status,
                    end_datetime  = EXCLUDED.end_datetime,
                    error_message = COALESCE(EXCLUDED.error_message, fact_execution_step.error_message)
                """,
                executionId, sourceStepRef, dto.getTaskId(),
                normalizedState,
                toTs(dto.getStartDatetime()), toTs(dto.getEndDatetime()),
                errorMessage);

        Long interfaceId = resolveInterfaceIdFromExecution(executionId);
        recomputeAggregatedFields(executionId, interfaceId,
                dto.getDagId(), dto.getRunId(), null, null, false);

        if ("FAILURE".equals(normalizedState) || "UPSTREAM_FAILED".equals(normalizedState)) {
            alertManager.handleTaskFailure(executionId, dto.getDagId(),
                    dto.getRunId(), dto.getTaskId(), dto.getEndDatetime());
        }
    }

    // ================================================================
    // dag → dim_interface (+ dim_sla si nouvelle interface)
    // ================================================================

    public void upsertInterfaceFromDagEvent(DagDTO dto) {
        boolean actif = dto.getIsPaused() == null || !dto.getIsPaused();

        List<Long> existing = jdbcTemplate.queryForList(
                "SELECT interface_id FROM dim_interface WHERE code = ?",
                Long.class, dto.getDagId());

        if (!existing.isEmpty()) {
            jdbcTemplate.update("""
                    UPDATE dim_interface
                    SET libelle = COALESCE(?, libelle), actif = ?
                    WHERE code = ?
                    """,
                    dto.getDisplayName(), actif, dto.getDagId());
            return;
        }

        Long interfaceId = jdbcTemplate.queryForObject("""
                INSERT INTO dim_interface (code, libelle, direction, client_id, actif)
                VALUES (?, ?, 'OUT', ?, ?)
                RETURNING interface_id
                """,
                Long.class,
                dto.getDagId(),
                dto.getDisplayName() != null ? dto.getDisplayName() : dto.getDagId(),
                DEFAULT_CLIENT_ID,
                actif);

        log.info("DIM_INTERFACE créée : {} (id={})", dto.getDagId(), interfaceId);

        Integer maxDurationSec = dagFileReader.readDagrunTimeoutSeconds(dto.getFileloc());
        jdbcTemplate.update("""
                INSERT INTO dim_sla (interface_id, max_duration_sec, cron_attendu, actif_depuis)
                VALUES (?, ?, ?, ?)
                """,
                interfaceId, maxDurationSec,
                stripJsonQuotes(dto.getScheduleInterval()), LocalDate.now());

        log.info("DIM_SLA créée pour {} (max={}s cron={})",
                dto.getDagId(), maxDurationSec, dto.getScheduleInterval());
    }

    // ================================================================
    // sla_miss → fact_alert (délégué à AirflowAlertManager)
    // ================================================================

    public void handleSlaMiss(SlaMissDTO dto) {
        alertManager.handleSlaMiss(
                dto.getDagId(),
                dto.getExecutionDate(),
                dto.getTriggeredAt(),
                dto.getDescription());
    }

    // ================================================================
    // Recalcul des champs agrégés de fact_execution
    // ================================================================

    /**
     * NOTE (28/07/2026 - fix race condition test B2) :
     * Cette méthode est appelée indépendamment par upsertExecutionFromDagRun
     * (event dag_run) ET upsertStepFromTaskInstance (event task_instance),
     * sans garantie d'ordre entre les deux topics Kafka. Un appel déclenché
     * par task_instance passe toujours dagStatusOrNull=null et relit le
     * statut courant en base — si l'event dag_run (passage à FAILED) n'est
     * pas encore traité à ce moment, currentStatus peut encore valoir
     * RUNNING/QUEUED, ce qui change la branche empruntée par
     * ExecutionStatusResolver.resolve() et peut renvoyer une Resolution(null,
     * null) prématurée qui écraserait un futur bon résultat si on ne s'en
     * protège pas.
     * Fix : on ne met à jour error_code/error_message que si la nouvelle
     * résolution apporte une vraie valeur (non nulle et non vide) OU s'il
     * n'y a encore aucune valeur en base. On ne régresse jamais un message
     * déjà présent vers null suite à un appel arrivé "trop tôt".
     *
     * NOTE (28/07/2026 - fix test B3, race condition RETRY_EXHAUSTED) :
     * retryCount est calculé ICI localement (somme des try_number de
     * taskRows) puis passé directement à statusResolver.resolve(), qui le
     * transmet à checkRetryExhausted. Avant ce fix, checkRetryExhausted
     * relisait retry_count depuis fact_execution en base — or à ce stade de
     * la méthode, l'UPDATE qui écrit ce même retryCount n'a pas encore été
     * exécuté (il arrive plus bas). Au dernier appel d'un run (celui qui
     * fait passer retry_count de 2 à 3, seuil d'épuisement), le check lisait
     * donc encore l'ancienne valeur en base, ratait le seuil, et
     * RETRY_EXHAUSTED n'était jamais déclenchée. Confirmé en test manuel :
     * run FAILURE avec retry_count=3 en base, aucune alerte RETRY_EXHAUSTED
     * créée. Ne plus jamais relire retry_count en base dans resolve()/
     * checkRetryExhausted — toujours utiliser la valeur calculée ici.
     */
    private void recomputeAggregatedFields(Long executionId, Long interfaceId,
                                           String dagId, String runId,
                                           String dagStatusOrNull, String triggerType,
                                           boolean isInsert) {
        List<Map<String, Object>> taskRows = airflowDao.getTaskInstancesForRun(dagId, runId);

        String serverName = taskRows.stream()
                .map(r -> (String) r.get("hostname"))
                .filter(h -> h != null && !h.isBlank())
                .distinct()
                .reduce((a, b) -> a + " | " + b)
                .orElse(null);

        int retryCount = taskRows.stream()
                .mapToInt(r -> r.get("try_number") != null
                        ? ((Number) r.get("try_number")).intValue() : 0)
                .sum();

        List<Map<String, Object>> stepRows = jdbcTemplate.queryForList(
                "SELECT status, error_message FROM fact_execution_step WHERE execution_id = ?",
                executionId);

        String currentStatus = dagStatusOrNull != null ? dagStatusOrNull :
                jdbcTemplate.queryForObject(
                        "SELECT status FROM fact_execution WHERE execution_id = ?",
                        String.class, executionId);

        ExecutionStatusResolver.Resolution resolution = statusResolver.resolve(
                currentStatus, dagId, runId, stepRows, interfaceId, executionId, retryCount);

        // Lecture XCom métriques (rows_*)
        Map<String, Integer> xcom = airflowDao.getXComMetrics(dagId, runId);
        Integer rowsRead     = xcom.get("rows_read");
        Integer rowsWritten  = xcom.get("rows_written");
        Integer rowsRejected = xcom.get("rows_rejected");

        // Ne jamais régresser error_code/error_message vers une valeur vide
        // suite à un appel "arrivé trop tôt" (avant que dag_run ne soit FAILED
        // ou avant que la task en échec ne soit visible dans stepRows).
        // On ne pousse la nouvelle valeur que si elle est réellement informative.
        String newErrorCode    = blankToNull(resolution.errorCode());
        String newErrorMessage = blankToNull(resolution.errorMessage());

        jdbcTemplate.update("""
                UPDATE fact_execution
                SET server_name    = ?,
                    retry_count    = ?,
                    error_code     = COALESCE(?, error_code),
                    error_message  = COALESCE(?, error_message),
                    rows_read      = COALESCE(?, rows_read),
                    rows_written   = COALESCE(?, rows_written),
                    rows_rejected  = COALESCE(?, rows_rejected)
                WHERE execution_id = ?
                """,
                serverName, retryCount,
                newErrorCode, newErrorMessage,
                rowsRead, rowsWritten, rowsRejected,
                executionId);

        alertManager.handleDagAborted(executionId, currentStatus);
        alertManager.handleUpstreamFailed(executionId, stepRows);
        alertManager.handleRetryExhausted(executionId, currentStatus, retryCount);
        alertManager.handleRepeatedFailure(executionId, interfaceId, currentStatus);

        if (isInsert && triggerType != null) {
            alertManager.handleManualOverride(executionId, triggerType);
        }

        LocalDateTime startDatetime = queryStartDatetime(executionId);
        if (startDatetime != null) {
            alertManager.handleLateStart(executionId, interfaceId, startDatetime);
        }
    }

    /** Convertit une chaîne vide ou blanche en null, pour ne jamais écraser une valeur existante via COALESCE. */
    private String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }

    // ================================================================
    // resolveOrCreateInterface
    // ================================================================

    private Long resolveOrCreateInterface(String dagId) {
        List<Long> rows = jdbcTemplate.queryForList(
                "SELECT interface_id FROM dim_interface WHERE code = ?",
                Long.class, dagId);

        if (!rows.isEmpty()) return rows.get(0);

        Map<String, Object> dag = airflowDao.getDag(dagId);
        DagDTO dto = DagDTO.builder()
                .dagId(dagId)
                .displayName(dag != null ? (String) dag.get("dag_display_name") : dagId)
                .isPaused(dag != null ? (Boolean) dag.get("is_paused") : Boolean.FALSE)
                .scheduleInterval(dag != null ? (String) dag.get("schedule_interval") : null)
                .fileloc(dag != null ? (String) dag.get("fileloc") : null)
                .build();

        upsertInterfaceFromDagEvent(dto);

        return jdbcTemplate.queryForObject(
                "SELECT interface_id FROM dim_interface WHERE code = ?",
                Long.class, dagId);
    }

    // ================================================================
    // Utilitaires privés
    // ================================================================

    /**
     * Dérive l'env_id depuis le dag_id.
     * Convention : dag_id préfixé par "test_" → DEV, sinon PROD.
     * Évite le codage en dur de PROD_ENV_ID = 3 pour tous les DAGs y compris les DAGs de test.
     */
    private int resolveEnvId(String dagId) {
        if (dagId != null && dagId.startsWith("test_")) {
            log.debug("DAG {} tagué DEV (préfixe test_)", dagId);
            return DEV_ENV_ID;
        }
        return PROD_ENV_ID;
    }

    private Long resolveExecutionId(String runId) {
        List<Long> rows = jdbcTemplate.queryForList(
                "SELECT execution_id FROM fact_execution WHERE source_execution_ref = ? AND source_id = ?",
                Long.class, runId, AIRFLOW_SOURCE_ID);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private Long resolveInterfaceIdFromExecution(Long executionId) {
        List<Long> rows = jdbcTemplate.queryForList(
                "SELECT interface_id FROM fact_execution WHERE execution_id = ?",
                Long.class, executionId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private LocalDateTime queryStartDatetime(Long executionId) {
        List<Timestamp> rows = jdbcTemplate.queryForList(
                "SELECT start_datetime FROM fact_execution WHERE execution_id = ?",
                Timestamp.class, executionId);
        if (rows.isEmpty() || rows.get(0) == null) return null;
        return rows.get(0).toLocalDateTime();
    }

    private String resolveTaskErrorMessage(TaskInstanceDTO dto) {
        Integer tryNumber = airflowDao.getTaskInstanceTryNumber(
                dto.getDagId(), dto.getRunId(), dto.getTaskId());
        if (tryNumber == null || tryNumber <= 0) {
            return "Erreur non disponible (task instance introuvable dans la base Airflow)";
        }
        try {
            String logContent = airflowLogClient.getTaskLog(
                    dto.getDagId(), dto.getRunId(), dto.getTaskId(), tryNumber);
            return airflowLogClient.extractErrorLine(logContent);
        } catch (Exception e) {
            log.warn("Erreur récupération log pour {}/{}/{} : {}",
                    dto.getDagId(), dto.getRunId(), dto.getTaskId(), e.getMessage());
            return "Erreur non disponible (API Airflow indisponible)";
        }
    }

    private String normalizeExecutionStatus(String value) {
        if (value == null) return null;
        return switch (value.toUpperCase()) {
            case "FAILED"   -> "FAILURE";
            case "RUNNING"  -> "RUNNING";
            case "SUCCESS"  -> "SUCCESS";
            case "QUEUED"   -> "QUEUED";
            case "SKIPPED"  -> "SKIPPED";
            default         -> value.toUpperCase();
        };
    }

    private String normalizeStepStatus(String value) {
        if (value == null) return null;
        return switch (value.toUpperCase()) {
            case "FAILED"            -> "FAILURE";
            case "SCHEDULED"         -> "QUEUED";
            case "RUNNING"           -> "RUNNING";
            case "SUCCESS"           -> "SUCCESS";
            case "SKIPPED"           -> "SKIPPED";
            case "UPSTREAM_FAILED"   -> "UPSTREAM_FAILED";
            case "DEFERRED"          -> "DEFERRED";
            case "QUEUED"            -> "QUEUED";
            default                  -> value.toUpperCase();
        };
    }

    private String normalizeTriggerType(String value) {
        if (value == null) return null;
        return switch (value.toUpperCase()) {
            case "MANUAL"    -> "MANUAL";
            case "SCHEDULED" -> "SCHEDULED";
            case "EXTERNAL"  -> "API";
            case "DATASET"   -> "EVENT";
            default          -> "MANUAL";
        };
    }

    private Integer computeDuration(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) return null;
        return (int) Duration.between(start, end).toSeconds();
    }

    private Timestamp toTs(LocalDateTime ldt) {
        return ldt == null ? null : Timestamp.valueOf(ldt);
    }

    private String stripJsonQuotes(String value) {
        if (value == null) return null;
        String trimmed = value.trim();
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() > 1) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }
}