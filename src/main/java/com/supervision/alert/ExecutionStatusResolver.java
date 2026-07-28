package com.supervision.alert;

import com.supervision.repository.SlaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Résolution du statut/erreur d'une exécution Airflow.
 * Priorité d'évaluation :
 * 1) TIMEOUT           : duration_seconds > max_duration_sec (dim_sla)
 * 2) DAG_NEVER_STARTED : run failed mais start_datetime null
 * 3) DAG_ABORTED       : start_datetime renseigné mais end_datetime null
 * 4) UPSTREAM_FAILED   : au moins une task en upstream_failed
 * 5) RETRY_EXHAUSTED   : retry_count >= seuil et DAG failed
 * 6) TASK_FAILED       : DAG pas failed mais au moins une task failed
 * 7) DAG_FAILED        : cas générique, aucune règle ci-dessus ne matche
 *
 * NOTE (28/07/2026 - fix test B2) : les messages d'erreur remontés incluent
 * désormais le détail technique réel de la task en échec (déjà récupéré par
 * AirflowLogClient et stocké dans fact_execution_step.error_message), au lieu
 * d'une phrase générique de classification uniquement. La phrase générique
 * reste utilisée en repli si aucun message de step n'est disponible.
 * Prérequis : la requête qui construit stepRows doit désormais sélectionner
 * la colonne error_message en plus de status (voir AirflowLoader.recomputeAggregatedFields).
 *
 * NOTE (28/07/2026 - fix test B3, race condition RETRY_EXHAUSTED) :
 * checkRetryExhausted lisait auparavant retry_count directement en base via
 * queryRetryCount(). Or resolve() est appelé DANS recomputeAggregatedFields
 * AVANT que l'UPDATE qui écrit le retryCount fraîchement calculé ne soit
 * exécuté. Au dernier appel d'un run (celui qui fait passer retry_count de
 * 2 à 3, seuil d'épuisement), checkRetryExhausted lisait donc encore
 * l'ancienne valeur (2) en base, ratait le seuil, et l'alerte RETRY_EXHAUSTED
 * n'était jamais déclenchée pour ce run (confirmé en test : run FAILURE avec
 * retry_count=3 en base, aucune alerte RETRY_EXHAUSTED créée).
 * Fix : retryCount est désormais calculé une seule fois dans
 * recomputeAggregatedFields et passé explicitement à resolve(), qui le
 * transmet à checkRetryExhausted. Plus aucune relecture base pour cette
 * valeur — queryRetryCount() est supprimée.
 */
@Component
@RequiredArgsConstructor
public class ExecutionStatusResolver {

    public static final String CODE_DAG_FAILED        = "DAG_FAILED";
    public static final String CODE_TASK_FAILED       = "TASK_FAILED";
    public static final String CODE_TIMEOUT           = "TIMEOUT";
    public static final String CODE_DAG_NEVER_STARTED = "DAG_NEVER_STARTED";
    public static final String CODE_DAG_ABORTED       = "DAG_ABORTED";
    public static final String CODE_UPSTREAM_FAILED   = "UPSTREAM_FAILED";
    public static final String CODE_RETRY_EXHAUSTED   = "RETRY_EXHAUSTED";

    private static final int RETRY_THRESHOLD = 3;

    private final SlaRepository slaRepository;

    public record Resolution(String errorCode, String errorMessage) {}

    /**
     * @param dagStatus      statut brut du dag_run (déjà normalisé en majuscules)
     * @param dagId          identifiant du DAG
     * @param runId          identifiant du run
     * @param stepRows       lignes de fact_execution_step (Map avec clés "status" et "error_message")
     * @param interfaceId    id de l'interface dans dim_interface (pour la vérif SLA)
     * @param executionId    id de fact_execution (pour récupérer les champs nécessaires)
     * @param retryCount     nombre de tentatives déjà calculé par l'appelant (recomputeAggregatedFields),
     *                       à ne PAS relire en base ici — voir NOTE fix test B3 ci-dessus.
     */
    public Resolution resolve(String dagStatus,
                              String dagId,
                              String runId,
                              List<Map<String, Object>> stepRows,
                              Long interfaceId,
                              Long executionId,
                              int retryCount) {

        boolean dagFailed       = "FAILURE".equalsIgnoreCase(dagStatus);
        boolean anyTaskFailed   = stepRows.stream()
                .anyMatch(s -> "FAILURE".equalsIgnoreCase((String) s.get("status")));
        boolean anyUpstreamFailed = stepRows.stream()
                .anyMatch(s -> "UPSTREAM_FAILED".equalsIgnoreCase((String) s.get("status")));

        // Rien d'anormal → pas d'error_code
        if (!dagFailed && !anyTaskFailed && !anyUpstreamFailed) {
            return new Resolution(null, null);
        }

        // 1) TIMEOUT
        Resolution timeout = checkTimeout(interfaceId, executionId);
        if (timeout != null) return timeout;

        if (dagFailed) {

            // 2) DAG_NEVER_STARTED
            Resolution neverStarted = checkNeverStarted(executionId);
            if (neverStarted != null) return neverStarted;

            // 3) DAG_ABORTED
            Resolution aborted = checkAborted(executionId);
            if (aborted != null) return aborted;

            // 4) UPSTREAM_FAILED
            if (anyUpstreamFailed) {
                String detail = findFailedStepErrorMessage(stepRows);
                String msg = detail != null
                        ? "Dépendance amont en échec : " + detail
                        : "Le DAG a échoué à cause d'une dépendance amont (task en upstream_failed)";
                return new Resolution(CODE_UPSTREAM_FAILED, msg);
            }

            // 5) RETRY_EXHAUSTED
            Resolution retryExhausted = checkRetryExhausted(retryCount);
            if (retryExhausted != null) return retryExhausted;
        }

        // 6) TASK_FAILED (DAG pas failed, mais une task l'est)
        if (!dagFailed && anyTaskFailed) {
            String detail = findFailedStepErrorMessage(stepRows);
            String msg = detail != null ? detail : "L'une des tasks est failed";
            return new Resolution(CODE_TASK_FAILED, msg);
        }

        // 7) DAG_FAILED générique
        String detail = anyTaskFailed ? findFailedStepErrorMessage(stepRows) : null;
        String msg = detail != null
                ? detail
                : (anyTaskFailed ? "Le DAG a échoué ; de plus, l'une de ses tasks est failed" : "");
        return new Resolution(CODE_DAG_FAILED, msg);
    }

    // ------------------------------------------------------------------
    // Extraction du message d'erreur réel d'une step en échec
    // ------------------------------------------------------------------

    /**
     * Cherche le premier message d'erreur non vide parmi les steps en FAILURE
     * ou UPSTREAM_FAILED. Ce message provient d'AirflowLogClient (log réel de
     * la task), déjà stocké dans fact_execution_step.error_message par
     * AirflowLoader.upsertStepFromTaskInstance. Retourne null si aucune step
     * en échec n'a de message exploitable (repli sur phrase générique côté appelant).
     */
    private String findFailedStepErrorMessage(List<Map<String, Object>> stepRows) {
        return stepRows.stream()
                .filter(s -> "FAILURE".equalsIgnoreCase((String) s.get("status"))
                        || "UPSTREAM_FAILED".equalsIgnoreCase((String) s.get("status")))
                .map(s -> (String) s.get("error_message"))
                .filter(msg -> msg != null && !msg.isBlank())
                .findFirst()
                .orElse(null);
    }

    // ------------------------------------------------------------------
    // Checks privés — lisent fact_execution via SlaRepository et JdbcTemplate
    // ------------------------------------------------------------------

    /**
     * Compare duration_seconds de fact_execution au max_duration_sec du SLA actif.
     * Utilise SlaRepository déjà existant (style identique à côté Stambia).
     */
    private Resolution checkTimeout(Long interfaceId, Long executionId) {
        if (interfaceId == null || executionId == null) return null;

        Optional<Integer> maxDuration = slaRepository.findActiveMaxDurationSec(
                interfaceId, Instant.now());
        if (maxDuration.isEmpty()) return null;

        // On relit duration_seconds depuis fact_execution
        Integer duration = queryDurationSeconds(executionId);
        if (duration == null || duration <= maxDuration.get()) return null;

        return new Resolution(CODE_TIMEOUT,
                "Le DAG a dépassé la durée maximale autorisée (SLA: "
                        + maxDuration.get() + "s, durée réelle: " + duration + "s)");
    }

    private Resolution checkNeverStarted(Long executionId) {
        if (executionId == null) return null;
        Boolean hasStart = queryHasStartDatetime(executionId);
        if (Boolean.TRUE.equals(hasStart)) return null;
        return new Resolution(CODE_DAG_NEVER_STARTED,
                "Le DAG n'a jamais démarré (aucune start_datetime renseignée)");
    }

    private Resolution checkAborted(Long executionId) {
        if (executionId == null) return null;
        Boolean hasStart = queryHasStartDatetime(executionId);
        Boolean hasEnd   = queryHasEndDatetime(executionId);
        if (!Boolean.TRUE.equals(hasStart) || Boolean.TRUE.equals(hasEnd)) return null;
        return new Resolution(CODE_DAG_ABORTED,
                "Le DAG a été interrompu brutalement (démarré mais jamais terminé proprement)");
    }

    /**
     * @param retryCount valeur déjà calculée par l'appelant (recomputeAggregatedFields),
     *                    jamais relue en base ici — voir NOTE fix test B3 en tête de classe.
     */
    private Resolution checkRetryExhausted(int retryCount) {
        if (retryCount < RETRY_THRESHOLD) return null;
        return new Resolution(CODE_RETRY_EXHAUSTED,
                "Le DAG a échoué après épuisement des tentatives ("
                        + retryCount + " tentatives, seuil: " + RETRY_THRESHOLD + ")");
    }

    // ------------------------------------------------------------------
    // Lectures SQL directes sur fact_execution
    // On passe par SlaRepository qui a déjà un JdbcTemplate supervision
    // ------------------------------------------------------------------

    private Integer queryDurationSeconds(Long executionId) {
        return slaRepository.queryForObject(
                "SELECT duration_seconds FROM fact_execution WHERE execution_id = ?",
                Integer.class, executionId);
    }

    private Boolean queryHasStartDatetime(Long executionId) {
        return slaRepository.queryForObject(
                "SELECT start_datetime IS NOT NULL FROM fact_execution WHERE execution_id = ?",
                Boolean.class, executionId);
    }

    private Boolean queryHasEndDatetime(Long executionId) {
        return slaRepository.queryForObject(
                "SELECT end_datetime IS NOT NULL FROM fact_execution WHERE execution_id = ?",
                Boolean.class, executionId);
    }
}