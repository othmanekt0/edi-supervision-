package com.supervision.loader;

import com.supervision.alert.AlertManager;
import com.supervision.dto.ActionDTO;
import com.supervision.dto.ExecutionDTO;
import com.supervision.dto.StepDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SupervisionLoader {

    private final JdbcTemplate jdbcTemplate;
    private final AlertManager alertManager;

    public SupervisionLoader(
            @Qualifier("supervisionDataSource") DataSource dataSource,
            AlertManager alertManager) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.alertManager = alertManager;
    }

    public Long upsertExecution(ExecutionDTO dto) {
        Long interfaceId = resolveInterfaceId(dto.getInterfaceCode());
        Integer envId = resolveEnvId(dto.getEnvironment());

        Long executionId = jdbcTemplate.queryForObject("""
            INSERT INTO fact_execution (
                source_execution_ref,
                interface_id,
                source_id,
                env_id,
                status,
                trigger_type,
                start_datetime,
                end_datetime,
                duration_seconds,
                server_name,
                error_message,
                ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON CONFLICT (source_execution_ref, source_id)
            DO UPDATE SET
                status           = CASE
                                      WHEN fact_execution.status = 'FAILURE' AND EXCLUDED.status <> 'FAILURE'
                                      THEN fact_execution.status
                                      ELSE EXCLUDED.status
                                    END,
                end_datetime     = EXCLUDED.end_datetime,
                duration_seconds = EXCLUDED.duration_seconds,
                error_message    = CASE
                                      WHEN fact_execution.status = 'FAILURE' AND EXCLUDED.status <> 'FAILURE'
                                      THEN fact_execution.error_message
                                      ELSE EXCLUDED.error_message
                                    END,
                ingested_at      = NOW()
            RETURNING execution_id
            """,
                Long.class,
                dto.getSourceExecutionRef(),
                interfaceId,
                dto.getSourceId(),
                envId,
                dto.getStatus(),
                dto.getTriggerType(),
                toTimestamp(dto.getStartDatetime()),
                toTimestamp(dto.getEndDatetime()),
                dto.getDurationSeconds(),
                dto.getServerName(),
                dto.getErrorMessage()
        );

        log.info("FACT_EXECUTION upsert OK : {} (id={})", dto.getSourceExecutionRef(), executionId);

        alertManager.checkAndCreateAlerts(dto, executionId, interfaceId);

        return executionId;
    }

    public void upsertStep(StepDTO dto) {
        // Cas 1 : le parent est une session principale -> sous-session
        Long executionId = resolveExecutionId(dto.getParentExecutionRef());

        if (executionId != null) {
            jdbcTemplate.update("""
                    INSERT INTO fact_execution_step (
                        execution_id,
                        parent_step_id,
                        source_step_ref,
                        step_name,
                        step_type,
                        status,
                        start_datetime,
                        end_datetime,
                        error_message
                    ) VALUES (?, NULL, ?, ?, 'SUB_SESSION', ?, ?, ?, ?)
                    ON CONFLICT (source_step_ref) DO UPDATE SET
                        status        = EXCLUDED.status,
                        end_datetime  = EXCLUDED.end_datetime,
                        error_message = EXCLUDED.error_message
                    """,
                    executionId,
                    dto.getStepExecutionRef(),
                    dto.getStepName(),
                    dto.getStatus(),
                    toTimestamp(dto.getStartDatetime()),
                    toTimestamp(dto.getEndDatetime()),
                    dto.getErrorMessage()
            );
            log.info("FACT_EXECUTION_STEP (sous-session) insert OK : {}", dto.getStepName());

            // NOUVEAU : une sous-session peut elle-même être le parent
            // attendu par des actions orphelines déjà en attente
            resolvePendingChildren(dto.getStepExecutionRef());
            return;
        }

        // Cas 2 : le parent est une sous-session -> action
        Long parentStepId = resolveStepIdByRef(dto.getParentExecutionRef());

        if (parentStepId == null) {
            log.warn("Parent introuvable ni en execution ni en step : {}", dto.getParentExecutionRef());
            return;
        }

        executionId = resolveExecutionIdFromStep(parentStepId);

        if (executionId == null) {
            log.warn("Impossible de remonter a la session principale depuis step : {}", parentStepId);
            return;
        }

        jdbcTemplate.update("""
                INSERT INTO fact_execution_step (
                    execution_id,
                    parent_step_id,
                    source_step_ref,
                    step_name,
                    step_type,
                    status,
                    start_datetime,
                    end_datetime,
                    error_message
                ) VALUES (?, ?, ?, ?, 'SUB_SESSION', ?, ?, ?, ?)
                ON CONFLICT (source_step_ref) DO UPDATE SET
                    status        = EXCLUDED.status,
                    end_datetime  = EXCLUDED.end_datetime,
                    error_message = EXCLUDED.error_message
                """,
                executionId,
                parentStepId,
                dto.getStepExecutionRef(),
                dto.getStepName(),
                dto.getStatus(),
                toTimestamp(dto.getStartDatetime()),
                toTimestamp(dto.getEndDatetime()),
                dto.getErrorMessage()
        );
        log.info("FACT_EXECUTION_STEP (sous-session, imbriquée) insert OK : {}", dto.getStepName());

        resolvePendingChildren(dto.getStepExecutionRef());
    }

    // --- Étape C : upsert des actions (stb_log_action_act) ---
    public void upsertAction(ActionDTO dto) {
        Long executionId;
        Long parentStepId;

        String parentRef = dto.getParentRef();
        boolean isRootAction = parentRef == null || parentRef.isBlank() || parentRef.equals("0");

        if (isRootAction) {
            // Action racine : rattachée directement à la session/sous-session via sess_id
            executionId = resolveExecutionId(dto.getSessId());

            if (executionId != null) {
                // rattachée à une session PRINCIPALE
                parentStepId = null;
            } else {
                // rattachée à une SOUS-SESSION (confirmé sur données réelles : 104 cas)
                parentStepId = resolveStepIdByRef(dto.getSessId());
                if (parentStepId == null) {
                    log.warn("Action {} : sess_id {} introuvable (ni execution ni step), mise en buffer (pending_action)",
                            dto.getSourceStepRef(), dto.getSessId());
                    bufferPendingAction(dto, dto.getSessId());
                    return;
                }
                executionId = resolveExecutionIdFromStep(parentStepId);
                if (executionId == null) {
                    log.warn("Action {} : impossible de remonter à l'execution depuis step {}",
                            dto.getSourceStepRef(), parentStepId);
                    return;
                }
            }
        } else {
            // Action imbriquée dans une autre action
            parentStepId = resolveStepIdByRef(parentRef);
            if (parentStepId == null) {
                log.warn("Action {} : action parente {} introuvable (pas encore consommée ?), mise en buffer (pending_action)",
                        dto.getSourceStepRef(), parentRef);
                bufferPendingAction(dto, parentRef);
                return;
            }
            executionId = resolveExecutionIdFromStep(parentStepId);
            if (executionId == null) {
                log.warn("Action {} : impossible de remonter à l'execution depuis step parent {}",
                        dto.getSourceStepRef(), parentStepId);
                return;
            }
        }

        insertActionStep(executionId, parentStepId, dto.getSourceStepRef(), dto.getStepName(),
                dto.getStatus(), dto.getStartDatetime(), dto.getEndDatetime(), dto.getErrorMessage());

        // NOUVEAU : cette action vient d'être créée, elle peut débloquer des
        // orphelins qui l'attendaient comme parent (résolution récursive)
        resolvePendingChildren(dto.getSourceStepRef());
    }

    /**
     * Factorise l'INSERT dans fact_execution_step + l'escalade, pour être
     * appelée à la fois par upsertAction() (chemin normal) et
     * resolvePendingChildren() (chemin différé depuis le buffer).
     */
    private void insertActionStep(Long executionId, Long parentStepId, String sourceStepRef,
                                  String stepName, String status, ZonedDateTime startDatetime,
                                  ZonedDateTime endDatetime, String errorMessage) {
        jdbcTemplate.update("""
                INSERT INTO fact_execution_step (
                    execution_id,
                    parent_step_id,
                    source_step_ref,
                    step_name,
                    step_type,
                    status,
                    start_datetime,
                    end_datetime,
                    error_message
                ) VALUES (?, ?, ?, ?, 'ACTION', ?, ?, ?, ?)
                ON CONFLICT (source_step_ref) DO UPDATE SET
                    status        = EXCLUDED.status,
                    end_datetime  = EXCLUDED.end_datetime,
                    error_message = EXCLUDED.error_message
                """,
                executionId,
                parentStepId,
                sourceStepRef,
                stepName,
                status,
                toTimestamp(startDatetime),
                toTimestamp(endDatetime),
                errorMessage
        );
        log.info("FACT_EXECUTION_STEP (action) insert OK : {}", stepName);

        // --- Escalade vers FACT_EXECUTION ---
        // ⚠️ Confirmé sur le dump réel que sess_ret_code ne vaut JAMAIS -1 en
        // pratique (toujours 1 ou NULL), même quand des actions internes sont
        // en FAILURE (15 058 sessions/29 853 concernées). Sans cette
        // escalade, AlertManager ne verrait donc JAMAIS ces échecs.
        if ("FAILURE".equals(status)) {
            escalateFailureToExecution(executionId, stepName);
        }
    }

    /**
     * NOUVEAU (fermeture point ouvert #2) : au lieu de jeter une action dont
     * le parent n'est pas encore en base, on la stocke dans pending_action.
     * Elle sera résolue automatiquement dès que son parent sera inséré (voir
     * resolvePendingChildren).
     */
    private void bufferPendingAction(ActionDTO dto, String parentRef) {
        jdbcTemplate.update("""
                INSERT INTO pending_action (
                    parent_ref, source_step_ref, step_name, status,
                    start_datetime, end_datetime, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (source_step_ref) DO NOTHING
                """,
                parentRef,
                dto.getSourceStepRef(),
                dto.getStepName(),
                dto.getStatus(),
                toTimestamp(dto.getStartDatetime()),
                toTimestamp(dto.getEndDatetime()),
                dto.getErrorMessage()
        );
        log.info("Action {} mise en attente dans pending_action (parent {} introuvable pour l'instant)",
                dto.getSourceStepRef(), parentRef);
    }

    /**
     * NOUVEAU (fermeture point ouvert #2) : appelée après l'insertion réussie
     * de tout step ou action (identifié par newlyCreatedRef). Cherche dans
     * pending_action les orphelins qui attendaient précisément ce step comme
     * parent, les insère à leur tour, puis se rappelle elle-même
     * récursivement — un orphelin tout juste débloqué peut lui-même être le
     * parent attendu par un autre orphelin plus profond dans la chaîne.
     */
    private void resolvePendingChildren(String newlyCreatedRef) {
        List<Map<String, Object>> pendingRows = jdbcTemplate.queryForList("""
                SELECT source_step_ref, step_name, status, start_datetime, end_datetime, error_message
                FROM pending_action WHERE parent_ref = ?
                """, newlyCreatedRef);

        if (pendingRows.isEmpty()) {
            return;
        }

        Long parentStepId = resolveStepIdByRef(newlyCreatedRef);
        if (parentStepId == null) {
            log.warn("resolvePendingChildren : step {} introuvable alors qu'il vient d'être inséré, incohérence inattendue",
                    newlyCreatedRef);
            return;
        }
        Long executionId = resolveExecutionIdFromStep(parentStepId);
        if (executionId == null) {
            log.warn("resolvePendingChildren : execution introuvable depuis step {} ", parentStepId);
            return;
        }

        for (Map<String, Object> row : pendingRows) {
            String childRef = (String) row.get("source_step_ref");
            String stepName = (String) row.get("step_name");
            String status = (String) row.get("status");
            Timestamp start = (Timestamp) row.get("start_datetime");
            Timestamp end = (Timestamp) row.get("end_datetime");
            String errorMessage = (String) row.get("error_message");

            insertActionStep(
                    executionId,
                    parentStepId,
                    childRef,
                    stepName,
                    status,
                    start != null ? start.toInstant().atZone(java.time.ZoneOffset.UTC) : null,
                    end != null ? end.toInstant().atZone(java.time.ZoneOffset.UTC) : null,
                    errorMessage
            );

            log.info("Action orpheline {} résolue automatiquement (parent {} maintenant disponible)",
                    childRef, newlyCreatedRef);

            jdbcTemplate.update("DELETE FROM pending_action WHERE source_step_ref = ?", childRef);

            // Récursion : cet enfant qu'on vient de débloquer peut lui-même
            // être le parent attendu par un orphelin plus profond
            resolvePendingChildren(childRef);
        }
    }

    private void escalateFailureToExecution(Long executionId, String failedStepName) {
        if (executionId == null) return;
        jdbcTemplate.update("""
                UPDATE fact_execution
                SET status = 'FAILURE',
                    error_message = CASE
                        WHEN error_message IS NULL OR error_message = '' THEN ?
                        WHEN error_message LIKE '%' || ? || '%' THEN error_message
                        ELSE error_message || '; ' || ?
                    END
                WHERE execution_id = ?
                """,
                failedStepName, failedStepName, failedStepName, executionId
        );
        log.info("FACT_EXECUTION {} escaladée en FAILURE suite à l'échec de l'action '{}'",
                executionId, failedStepName);

        alertManager.raiseErrorAlert(executionId, "Action en échec : " + failedStepName);
    }

    private Long resolveInterfaceId(String code) {
        if (code == null) return null;
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT interface_id FROM dim_interface WHERE code = ?",
                    Long.class, code
            );
        } catch (Exception e) {
            log.warn("Interface introuvable dans DIM_INTERFACE : {}", code);
            return null;
        }
    }

    private Integer resolveEnvId(String environment) {
        if (environment == null) return null;
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT env_id FROM dim_environment WHERE nom = ?",
                    Integer.class, environment
            );
        } catch (Exception e) {
            log.warn("Environnement introuvable dans DIM_ENVIRONMENT : {}", environment);
            return null;
        }
    }

    private Long resolveExecutionId(String sourceExecutionRef) {
        if (sourceExecutionRef == null) return null;
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT execution_id FROM fact_execution WHERE source_execution_ref = ?",
                    Long.class, sourceExecutionRef
            );
        } catch (Exception e) {
            return null;
        }
    }

    // Cherche un step par son source_step_ref (sess_id ou act_id Stambia)
    private Long resolveStepIdByRef(String sourceStepRef) {
        if (sourceStepRef == null) return null;
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT step_id FROM fact_execution_step WHERE source_step_ref = ?",
                    Long.class, sourceStepRef
            );
        } catch (Exception e) {
            log.warn("Step parent introuvable pour ref : {}", sourceStepRef);
            return null;
        }
    }

    private Long resolveExecutionIdFromStep(Long stepId) {
        if (stepId == null) return null;
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT execution_id FROM fact_execution_step WHERE step_id = ?",
                    Long.class, stepId
            );
        } catch (Exception e) {
            log.warn("execution_id introuvable depuis step_id : {}", stepId);
            return null;
        }
    }

    private Timestamp toTimestamp(ZonedDateTime zdt) {
        if (zdt == null) return null;
        return Timestamp.from(zdt.toInstant());
    }
}