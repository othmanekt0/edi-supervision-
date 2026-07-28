package com.supervision.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Job de nettoyage : résout automatiquement les alertes restées ouvertes
 * plus de 7 jours (resolved_at IS NULL), pour éviter de fausser
 * indéfiniment le KPI "temps moyen de résolution" du Dashboard 3.
 *
 * Règle métier (décidée le 17/07/2026, point 3 du plan de couverture KPI) :
 * une alerte non acquittée/non résolue après 7 jours est considérée
 * comme close par défaut, faute de mécanisme de résolution automatique
 * plus fin (cf. discussion option 1 vs option 2).
 */
@Component
public class AlertResolutionJob {

    private static final Logger log = LoggerFactory.getLogger(AlertResolutionJob.class);

    private static final int DELAI_RESOLUTION_JOURS = 7;

    private final JdbcTemplate jdbcTemplate;

    public AlertResolutionJob(@Qualifier("supervisionJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Tourne une fois par jour (pas besoin d'une fréquence plus fine
     * pour une résolution à J+7).
     */
    @Scheduled(fixedDelay = 24 * 60 * 60 * 1000) // 24h
    public void resolveStaleAlerts() {
        int updated = jdbcTemplate.update("""
                UPDATE fact_alert
                SET resolved_at = now()
                WHERE resolved_at IS NULL
                  AND triggered_at < now() - make_interval(days => ?)
                """, DELAI_RESOLUTION_JOURS);

        if (updated > 0) {
            log.warn("AlertResolutionJob : {} alerte(s) résolue(s) automatiquement (> {} jours sans résolution)",
                    updated, DELAI_RESOLUTION_JOURS);
        } else {
            log.debug("AlertResolutionJob : aucune alerte à résoudre automatiquement");
        }
    }
}