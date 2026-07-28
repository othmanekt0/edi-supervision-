package com.supervision.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.supervision.dto.ActionDTO;
import com.supervision.dto.ExecutionDTO;
import com.supervision.dto.StepDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
@Component
public class StambiaTransformer {

    private static final DateTimeFormatter STAMBIA_FORMAT =
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");

    private static final ZoneId STAMBIA_ZONE = ZoneId.of("Europe/Paris");
    private static final ZoneId UTC = ZoneId.of("UTC");

    public ExecutionDTO toExecutionDTO(JsonNode payload) {
        return ExecutionDTO.builder()
                .sourceExecutionRef(getText(payload, "sess_id"))
                .interfaceCode(getText(payload, "sess_name"))
                .sourceId(1)
                .environment(normalizeEnvironment(getText(payload, "sess_conf")))
                .status(normalizeStatus(getInt(payload, "sess_ret_code")))
                .triggerType(normalizeTriggerType(getText(payload, "sess_launch_mode")))
                .startDatetime(parseDate(getText(payload, "sess_begin_date")))
                .endDatetime(parseDate(getText(payload, "sess_end_date")))
                .durationSeconds(getDurationSeconds(payload))
                .serverName(getText(payload, "sess_engine_host"))
                .errorMessage(getText(payload, "sess_ret_msg"))
                .build();
    }

    public StepDTO toStepDTO(JsonNode payload) {
        return StepDTO.builder()
                .parentExecutionRef(getText(payload, "sess_parent_id"))
                .stepExecutionRef(getText(payload, "sess_id"))
                .stepName(getText(payload, "sess_name"))
                .status(normalizeStatus(getInt(payload, "sess_ret_code")))
                .startDatetime(parseDate(getText(payload, "sess_begin_date")))
                .endDatetime(parseDate(getText(payload, "sess_end_date")))
                .durationSeconds(getDurationSeconds(payload))
                .errorMessage(getText(payload, "sess_ret_msg"))
                .build();
    }

    // --- Étape B : mapping des actions (stb_log_action_act) ---
    // ⚠️ CONFIRMÉ le 13/07/2026 sur données réelles du dump + information_schema.columns :
    // la clé de hiérarchie entre actions est portée par act_father_engine_id (vide = racine,
    // sinon act_id du parent), PAS par act_parent_iter (qui ne vaut que 0/1 malgré son nom).
    public ActionDTO toActionDTO(JsonNode payload) {
        return ActionDTO.builder()
                .sourceStepRef(getText(payload, "act_id"))
                .sessId(getText(payload, "sess_id"))
                .parentRef(getText(payload, "act_father_engine_id"))
                .stepName(getText(payload, "act_real_name"))
                .actType(getText(payload, "act_type"))
                .status(normalizeActionStatus(
                        getInt(payload, "act_ret_code"),
                        getText(payload, "act_type"),
                        getText(payload, "act_end_date")))
                .startDatetime(parseDate(getText(payload, "act_begin_date")))
                .endDatetime(parseDate(getText(payload, "act_end_date")))
                .errorMessage(getText(payload, "act_ret_msg"))
                .build();
    }

    // --- Méthodes utilitaires ---

    private ZonedDateTime parseDate(String raw) {
        if (raw == null || raw.isBlank() || raw.equals("null")) return null;
        try {
            LocalDateTime ldt = LocalDateTime.parse(raw, STAMBIA_FORMAT);
            return ldt.atZone(STAMBIA_ZONE).withZoneSameInstant(UTC);
        } catch (Exception e) {
            log.warn("Impossible de parser la date Stambia : {}", raw);
            return null;
        }
    }

    private String normalizeStatus(Integer code) {
        if (code == null) return "RUNNING";
        return switch (code) {
            case 1  -> "SUCCESS";
            case -1 -> "FAILURE";
            case 0  -> "RUNNING";
            default -> "UNKNOWN";
        };
    }

    /**
     * ✅ CONFIRMÉ le 14/07/2026 sur le dump réel complet (556 051 actions) :
     * les 27 Process avec act_ret_code=-1 ont TOUS (100%) au moins un descendant
     * réellement en échec dans leur sous-arbre. -1 est un vrai échec, identique
     * à la sémantique sur les Code. Pas de traitement spécial nécessaire.
     *
     * Valeur -2 observée (rare, 36 cas, quasi tous en données de test manuelles) :
     * hypothèse "action non exécutée / skippée" (act_nb_exe=0 sur l'unique cas
     * réel identifié), non confirmée formellement — traitée comme UNKNOWN pour
     * l'instant plutôt que FAILURE, pour éviter une fausse alerte si le sens réel
     * s'avère différent.
     */
    private String normalizeActionStatus(Integer code, String actType, String endDate) {
        if (code == null) return "UNKNOWN";
        return switch (code) {
            case 1  -> "SUCCESS";
            case -1 -> "FAILURE";
            case 0  -> "RUNNING";
            default -> "UNKNOWN"; // couvre -2 et toute autre valeur non documentée
        };
    }

    private String normalizeTriggerType(String raw) {
        if (raw == null) return "UNKNOWN";
        return switch (raw.toUpperCase()) {
            case "SCHEDULE"        -> "SCHEDULED";
            case "WEB_INTERACTIVE" -> "MANUAL";
            case "WEB_SERVICE"     -> "API";
            case "ACTION"          -> "EVENT";
            default                -> "UNKNOWN";
        };
    }

    private String normalizeEnvironment(String raw) {
        if (raw == null) return "UNKNOWN";
        return switch (raw.toLowerCase()) {
            case "prod"    -> "PROD";
            case "preprod" -> "PREPROD";
            case "dev"     -> "DEV";
            default        -> raw.toUpperCase();
        };
    }

    private Long getDurationSeconds(JsonNode payload) {
        JsonNode node = payload.get("sess_duration");
        if (node == null || node.isNull()) return null;
        return node.asLong() / 1000;
    }

    private String getText(JsonNode node, String field) {
        JsonNode n = node.get(field);
        if (n == null || n.isNull()) return null;
        String val = n.asText();
        return val.isBlank() ? null : val;
    }

    private Integer getInt(JsonNode node, String field) {
        JsonNode n = node.get(field);
        if (n == null || n.isNull()) return null;
        return n.asInt();
    }
}