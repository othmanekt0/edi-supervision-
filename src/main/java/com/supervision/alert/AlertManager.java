package com.supervision.alert;

import com.supervision.dto.AlertSeverity;
import com.supervision.dto.AlertType;
import com.supervision.dto.ExecutionDTO;
import com.supervision.repository.SlaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlertManager {

    @Qualifier("supervisionDataSource")
    private final DataSource supervisionDataSource;

    private final SlaRepository slaRepository;

    private JdbcTemplate jdbc() {
        return new JdbcTemplate(supervisionDataSource);
    }

    /**
     * Vérifie une exécution terminée et crée les alertes nécessaires (ERROR, SLA_BREACH).
     * Ne fait rien si l'exécution est encore RUNNING (endDatetime null).
     */
    public void checkAndCreateAlerts(ExecutionDTO dto, Long executionId, Long interfaceId) {
        if (executionId == null) {
            log.warn("executionId null, impossible de vérifier les alertes pour {}", dto.getSourceExecutionRef());
            return;
        }

        if (dto.getEndDatetime() == null) {
            // Exécution encore en cours : rien à vérifier
            return;
        }

        if ("FAILURE".equals(dto.getStatus())) {
            createAlertIfAbsent(
                    executionId,
                    AlertType.ERROR,
                    AlertSeverity.HIGH,
                    "Exécution en échec : " + safeMessage(dto.getErrorMessage())
            );
        }

        checkSlaBreach(dto, executionId, interfaceId);
    }

    /**
     * NOUVEAU (point ouvert #1) : créé une alerte ERROR suite à une escalade
     * directe depuis SupervisionLoader.escalateFailureToExecution(), c'est-à-dire
     * quand une action interne échoue avant même que le message de fin de
     * session (avec son endDatetime) ne soit arrivé. Contrairement à
     * checkAndCreateAlerts(), on ne vérifie pas endDatetime ici : l'échec est
     * déjà avéré au niveau du step, inutile d'attendre la fin de la session
     * pour prévenir. Le ON CONFLICT de createAlertIfAbsent() évite les
     * doublons si plusieurs actions échouent sur la même exécution, ou si
     * checkAndCreateAlerts() crée aussi une alerte ERROR plus tard pour la
     * même exécution.
     */
    public void raiseErrorAlert(Long executionId, String message) {
        if (executionId == null) {
            log.warn("executionId null, impossible de créer l'alerte d'escalade");
            return;
        }
        createAlertIfAbsent(executionId, AlertType.ERROR, AlertSeverity.HIGH, safeMessage(message));
    }

    private void checkSlaBreach(ExecutionDTO dto, Long executionId, Long interfaceId) {
        if (interfaceId == null || dto.getDurationSeconds() == null || dto.getStartDatetime() == null) {
            return;
        }

        slaRepository.findActiveMaxDurationSec(interfaceId, dto.getStartDatetime().toInstant())
                .ifPresent(maxDurationSec -> {
                    if (dto.getDurationSeconds() > maxDurationSec) {
                        String message = String.format(
                                "Durée %ds dépasse le SLA de %ds",
                                dto.getDurationSeconds(), maxDurationSec
                        );
                        createAlertIfAbsent(executionId, AlertType.SLA_BREACH, AlertSeverity.MEDIUM, message);
                    }
                });
    }

    private void createAlertIfAbsent(Long executionId, AlertType type, AlertSeverity severity, String message) {
        Integer rows = jdbc().update("""
                INSERT INTO fact_alert (execution_id, alert_type, severity, triggered_at, message)
                VALUES (?, ?, ?, NOW(), ?)
                ON CONFLICT (execution_id, alert_type) WHERE (resolved_at IS NULL) DO NOTHING
                """,
                executionId, type.name(), severity.name(), message
        );

        if (rows != null && rows > 0) {
            log.info("Alerte {} créée pour execution_id={} : {}", type, executionId, message);
        } else {
            log.debug("Alerte {} déjà existante (ouverte) pour execution_id={}, pas de doublon créé", type, executionId);
        }
    }

    private String safeMessage(String msg) {
        if (msg == null) return "aucun détail";
        return msg.length() > 400 ? msg.substring(0, 400) + "..." : msg;
    }
}