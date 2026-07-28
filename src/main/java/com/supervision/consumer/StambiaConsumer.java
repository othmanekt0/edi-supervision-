package com.supervision.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.supervision.dto.ActionDTO;
import com.supervision.dto.ExecutionDTO;
import com.supervision.dto.StepDTO;
import com.supervision.loader.SupervisionLoader;
import com.supervision.transformer.StambiaTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.messaging.handler.annotation.Payload;
import javax.sql.DataSource;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consommateur Kafka pour les deux topics Stambia.
 *
 * Filtrage précoce d'interface (ajouté le 21/07/2026)
 * -------------------------------------------------------
 * Si l'interface d'une session racine est inconnue de dim_interface
 * (interfaceCode absent de la base), la session est ignorée silencieusement
 * AVANT tout appel à SupervisionLoader.  Les sous-sessions et actions qui
 * en dépendent sont également ignorées grâce à deux caches ConcurrentHashSet
 * qui propagent le filtre de façon transitive :
 *
 *   ignoredSessIds  → sess_id des sessions/sous-sessions ignorées
 *   ignoredActIds   → act_id des actions ignorées (cascade depuis ignoredSessIds)
 *
 * Le cache est non borné mais ne croît que sur les interfaces inconnues,
 * cas normalement rare si le référentiel dim_interface est bien tenu.
 * Il est purgé uniquement au redémarrage de l'application.
 * Un log horaire avertit si le cache grossit de façon inattendue.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StambiaConsumer {

    private final StambiaTransformer transformer;
    private final SupervisionLoader  loader;
    private final ObjectMapper       objectMapper;

    /**
     * JdbcTemplate pointant sur la base supervision, utilisé uniquement
     * pour la vérification précoce de dim_interface.
     * Même patron que AlertManager : @Qualifier sur un champ final,
     * câblé automatiquement par @RequiredArgsConstructor de Lombok.
     */
    @Qualifier("supervisionDataSource")
    private final DataSource supervisionDataSource;

    // -------------------------------------------------------------------------
    // Cache des identifiants ignorés (interface racine inconnue de dim_interface)
    // -------------------------------------------------------------------------

    /** sess_id des sessions racines et sous-sessions rejetées. */
    private final Set<String> ignoredSessIds = ConcurrentHashMap.newKeySet();

    /** act_id des actions rejetées (cascade depuis ignoredSessIds). */
    private final Set<String> ignoredActIds  = ConcurrentHashMap.newKeySet();

    // -------------------------------------------------------------------------
    // Listener — sessions et sous-sessions
    // -------------------------------------------------------------------------

    @KafkaListener(
            topics  = "stambia.log.stb_log_session_sess",
            groupId = "supervision-consumer-group-2"
    )
    public void consumeSession(@Payload(required = false) String payload) {
        if (payload == null) {
            log.debug("[StambiaConsumer] Tombstone reçu sur session topic, ignoré");
            return;
        }
        try {
            JsonNode root  = objectMapper.readTree(payload);
            JsonNode after = extractAfter(root, "session");
            if (after == null) return;

            String sessParentId = nullableText(after, "sess_parent_id");

            if (sessParentId == null || sessParentId.isBlank()) {
                // ── Session racine ────────────────────────────────────────────
                handleRootSession(after);
            } else {
                // ── Sous-session ──────────────────────────────────────────────
                handleSubSession(after, sessParentId);
            }

        } catch (IllegalArgumentException e) {
            log.warn("[StambiaConsumer] Payload session malformé, ignoré : {}", e.getMessage());
        } catch (Exception e) {
            log.error("[StambiaConsumer] Erreur inattendue consumeSession", e);
        }
    }

    @KafkaListener(
            topics  = "stambia.log.stb_log_action_act",
            groupId = "supervision-consumer-group-2"
    )
    public void consumeAction(@Payload(required = false) String payload) {
        if (payload == null) {
            log.debug("[StambiaConsumer] Tombstone reçu sur action topic, ignoré");
            return;
        }
        try {
            JsonNode root  = objectMapper.readTree(payload);
            JsonNode after = extractAfter(root, "action");
            if (after == null) return;

            ActionDTO dto = transformer.toActionDTO(after);

            // ── Filtre cascade : est-ce que le parent (sessId ou parentRef action)
            //    fait partie des identifiants ignorés ?
            if (isActionIgnored(dto)) {
                ignoredActIds.add(dto.getSourceStepRef());
                log.debug("[StambiaConsumer] Action {} ignorée (parent filtré)", dto.getSourceStepRef());
                return;
            }

            loader.upsertAction(dto);

        } catch (IllegalArgumentException e) {
            log.warn("[StambiaConsumer] Payload action malformé, ignoré : {}", e.getMessage());
        } catch (Exception e) {
            log.error("[StambiaConsumer] Erreur inattendue consumeAction", e);
        }
    }

    // -------------------------------------------------------------------------
    // Logique de traitement interne
    // -------------------------------------------------------------------------

    /** Traite une session racine : vérifie l'interface avant tout appel au loader. */
    private void handleRootSession(JsonNode after) {
        ExecutionDTO dto = transformer.toExecutionDTO(after);



        if (!isInterfaceKnown(dto.getInterfaceCode())) {
            String sessId = dto.getSourceExecutionRef();
            ignoredSessIds.add(sessId);
            log.warn("[StambiaConsumer] Interface inconnue '{}' pour session racine {} — " +
                            "session et descendants ignorés silencieusement (ajouter l'interface dans dim_interface)",
                    dto.getInterfaceCode(), sessId);
            return;
        }

        loader.upsertExecution(dto);
    }

    /**
     * Traite une sous-session.
     * Si son parent direct (sessParentId) est dans ignoredSessIds,
     * la sous-session est également ignorée et ajoutée au cache pour
     * que ses propres enfants soient filtrés à leur tour (propagation transitive).
     */
    private void handleSubSession(JsonNode after, String sessParentId) {
        if (ignoredSessIds.contains(sessParentId)) {
            String sessId = nullableText(after, "sess_id");
            if (sessId != null) ignoredSessIds.add(sessId);
            log.debug("[StambiaConsumer] Sous-session {} ignorée (parent filtré {})",
                    sessId, sessParentId);
            return;
        }

        StepDTO dto = transformer.toStepDTO(after);
        loader.upsertStep(dto);
    }

    /**
     * Détermine si une action doit être ignorée.
     * Une action est ignorée si :
     *   - son sessId (session/sous-session directe) est dans ignoredSessIds, OU
     *   - son parentRef (action parente) est dans ignoredActIds.
     */
    private boolean isActionIgnored(ActionDTO dto) {
        if (dto.getSessId() != null && ignoredSessIds.contains(dto.getSessId())) {
            return true;
        }
        if (dto.getParentRef() != null && ignoredActIds.contains(dto.getParentRef())) {
            return true;
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Vérification dim_interface
    // -------------------------------------------------------------------------

    /**
     * Interroge dim_interface pour vérifier que l'interfaceCode est connu.
     * Utilise un JdbcTemplate créé à la volée depuis supervisionDataSource
     * (même pattern que SupervisionLoader.resolveInterfaceId).
     */
    private boolean isInterfaceKnown(String interfaceCode) {
        if (interfaceCode == null || interfaceCode.isBlank()) {
            return false;
        }
        try {
            JdbcTemplate jdbc = new JdbcTemplate(supervisionDataSource);
            Integer count = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM dim_interface WHERE code = ?",
                    Integer.class,
                    interfaceCode
            );
            return count != null && count > 0;
        } catch (Exception e) {
            // En cas d'erreur SQL (base indisponible, etc.), on laisse passer
            // pour ne pas bloquer l'ingestion ; SupervisionLoader loggera son propre WARN.
            log.warn("[StambiaConsumer] Impossible de vérifier dim_interface pour '{}' : {}",
                    interfaceCode, e.getMessage());
            return true;
        }
    }

    // -------------------------------------------------------------------------
    // Surveillance périodique du cache
    // -------------------------------------------------------------------------

    /**
     * Log horaire de la taille des caches d'identifiants ignorés.
     * Permet de détecter une dérive (référentiel non tenu) sans surcharger les logs.
     */
    @Scheduled(fixedRate = 3_600_000) // toutes les heures
    public void logIgnoredCacheSize() {
        int sessCount = ignoredSessIds.size();
        int actCount  = ignoredActIds.size();
        if (sessCount > 0 || actCount > 0) {
            log.warn("[StambiaConsumer] Cache interfaces inconnues — " +
                            "{} sess_id ignorés, {} act_id ignorés. " +
                            "Vérifier dim_interface pour les interfaces manquantes.",
                    sessCount, actCount);
        }
    }

    // -------------------------------------------------------------------------
    // Utilitaires de parsing JSON
    // -------------------------------------------------------------------------

    /**
     * Extrait le nœud {@code payload.after} ou {@code after} selon la présence du wrapper Debezium.
     * Retourne null si le nœud est absent ou nul (cas DELETE / tombstone).
     */
    private JsonNode extractAfter(JsonNode root, String context) {
        JsonNode payload = root.has("payload") ? root.get("payload") : root;
        JsonNode after   = payload.get("after");

        if (after == null || after.isNull()) {
            log.warn("[StambiaConsumer] Pas de 'after' dans le payload {} (DELETE/tombstone ?), ignoré", context);
            return null;
        }
        return after;
    }

    /** Retourne la valeur texte d'un champ JSON, ou null si absent/nul. */
    private String nullableText(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) return null;
        String val = child.asText("").trim();
        return val.isEmpty() ? null : val;
    }
}