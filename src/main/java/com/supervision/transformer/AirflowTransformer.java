package com.supervision.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.supervision.dto.DagDTO;
import com.supervision.dto.DagRunDTO;
import com.supervision.dto.SlaMissDTO;
import com.supervision.dto.TaskInstanceDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@RequiredArgsConstructor
@Slf4j
public class AirflowTransformer {

    private static final Pattern TABLE_FROM_TOPIC = Pattern.compile("^.+\\.(.+)$");

    private final ObjectMapper objectMapper;

    // ------------------------------------------------------------------
    // Point d'entrée : parse l'enveloppe Debezium brute
    // ------------------------------------------------------------------

    /**
     * Résultat du parsing de l'enveloppe Debezium.
     * data == null signifie "événement à ignorer" (delete, payload invalide).
     * before == null pour les insertions (op="c").
     */
    public record DebeziumEvent(String tableName, String operation, JsonNode data, JsonNode before) {
        public boolean shouldIgnore() {
            return data == null || "d".equals(operation);
        }
    }

    public DebeziumEvent parse(String rawMessage, String topic) throws Exception {
        JsonNode raw = objectMapper.readTree(rawMessage);
        if (raw == null || raw.isNull()) return null;

        JsonNode envelope = raw.path("payload");
        if (envelope.isMissingNode() || envelope.isNull()) envelope = raw;

        String operation = text(envelope, "op");
        if (operation == null || operation.isBlank()) return null;

        String tableName = resolveTableName(topic, envelope.path("source"));
        if (tableName == null || tableName.isBlank()) {
            log.warn("Table introuvable pour le topic {}", topic);
            return null;
        }

        // Pour un delete, on retourne l'événement marqué "à ignorer"
        if ("d".equals(operation)) {
            return new DebeziumEvent(tableName, operation, null, null);
        }

        JsonNode data = envelope.path("after");
        if (data == null || data.isMissingNode() || data.isNull()) {
            log.warn("Payload Debezium invalide (after manquant) sur {}", topic);
            return null;
        }

        JsonNode before = envelope.path("before");

        return new DebeziumEvent(tableName, operation, data, before);
    }

    // ------------------------------------------------------------------
    // Mappings JsonNode → DTO
    // ------------------------------------------------------------------

    public DagRunDTO toDagRunDTO(JsonNode data) {
        return DagRunDTO.builder()
                .dagId(requiredText(data, "dag_id"))
                .runId(requiredText(data, "run_id"))
                .status(text(data, "state"))
                .triggerType(text(data, "run_type"))
                .startDatetime(dateTime(data, "start_date"))
                .endDatetime(dateTime(data, "end_date"))
                .build();
    }

    public TaskInstanceDTO toTaskInstanceDTO(JsonNode data) {
        return TaskInstanceDTO.builder()
                .dagId(requiredText(data, "dag_id"))
                .runId(requiredText(data, "run_id"))
                .taskId(requiredText(data, "task_id"))
                .state(text(data, "state"))
                .startDatetime(dateTime(data, "start_date"))
                .endDatetime(dateTime(data, "end_date"))
                .build();
    }

    public DagDTO toDagDTO(JsonNode data) {
        return DagDTO.builder()
                .dagId(requiredText(data, "dag_id"))
                .displayName(text(data, "dag_display_name"))
                .isPaused(bool(data, "is_paused"))
                .scheduleInterval(text(data, "schedule_interval"))
                .fileloc(text(data, "fileloc"))
                .build();
    }

    public SlaMissDTO toSlaMissDTO(JsonNode data) {
        return SlaMissDTO.builder()
                .dagId(requiredText(data, "dag_id"))
                .executionDate(dateTime(data, "execution_date"))
                .triggeredAt(dateTime(data, "timestamp"))
                .description(text(data, "description"))
                .build();
    }

    /**
     * Retourne true si l'update dag ne concerne que des champs techniques
     * (last_parsed_time, next_dagrun, etc.) — pas de changement métier.
     * Dans ce cas, le consumer peut ignorer l'événement.
     */
    public boolean isDagTechnicalUpdateOnly(JsonNode before, JsonNode after) {
        // Sans REPLICA IDENTITY FULL, before est toujours vide pour les updates dag.
        // On filtre sur after uniquement : si is_active=true et is_paused inchangeable
        // à déduire, on ignore. La vraie solution est REPLICA IDENTITY FULL.
        // Pour l'instant : on laisse passer uniquement si before a du contenu utile.
        if (before == null || before.isNull() || before.isMissingNode()
                || !before.isObject() || before.isEmpty()) {
            // before vide → on ne peut pas comparer → on ignore l'update dag
            log.debug("Update dag ignoré (before vide, REPLICA IDENTITY non configuré) : {}",
                    after.path("dag_id").asText());
            return true; // ← on filtre
        }
        for (String field : new String[]{
                "dag_id", "is_paused", "is_active", "schedule_interval",
                "fileloc", "owners", "dag_display_name", "has_import_errors"}) {
            JsonNode b = before.path(field);
            JsonNode a = after.path(field);
            if (!b.equals(a)) {
                log.debug("Champ métier modifié : {} | before={} | after={}", field, b, a);
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Utilitaires
    // ------------------------------------------------------------------

    private String resolveTableName(String topic, JsonNode sourceNode) {
        String fromSource = text(sourceNode, "table");
        if (fromSource != null && !fromSource.isBlank()) {
            return fromSource;
        }
        Matcher matcher = TABLE_FROM_TOPIC.matcher(topic);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private String requiredText(JsonNode node, String field) {
        String value = text(node, field);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Champ obligatoire manquant dans le payload Debezium : " + field);
        }
        return value;
    }

    public String text(JsonNode node, String field) {
        JsonNode fieldNode = node.path(field);
        if (fieldNode.isMissingNode() || fieldNode.isNull()) return null;
        return fieldNode.asText();
    }

    private Boolean bool(JsonNode node, String field) {
        JsonNode fieldNode = node.path(field);
        if (fieldNode.isMissingNode() || fieldNode.isNull()) return null;
        return fieldNode.asBoolean();
    }

    private LocalDateTime dateTime(JsonNode node, String field) {
        JsonNode fieldNode = node.path(field);
        if (fieldNode.isMissingNode() || fieldNode.isNull()) return null;

        if (fieldNode.isNumber()) {
            return Instant.ofEpochMilli(fieldNode.asLong())
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime();
        }

        String raw = fieldNode.asText();
        if (raw == null || raw.isBlank()) return null;

        try {
            return LocalDateTime.parse(raw);
        } catch (DateTimeParseException ignored) {}

        try {
            return OffsetDateTime.parse(raw).toLocalDateTime();
        } catch (DateTimeParseException ignored) {}

        throw new IllegalArgumentException("Format date invalide pour le champ " + field + " : " + raw);
    }
}