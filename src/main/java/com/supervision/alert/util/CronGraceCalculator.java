package com.supervision.alert.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Calcul partagé de la marge de tolérance (grâce dynamique) avant de déclencher
 * une alerte "non déclenché" (DAG_NOT_TRIGGERED côté Airflow, SESSION_NOT_TRIGGERED
 * côté Stambia).
 *
 * Extrait de AirflowAlertManager le 17/07/2026 (point 2.3 du plan de couverture KPI)
 * pour être réutilisé tel quel par StambiaAlertManager, et éviter toute divergence
 * future entre les deux implémentations.
 *
 * Ne dépend d'aucune donnée spécifique à une source (Stambia ou Airflow) : prend un
 * cron et une date de référence en entrée, retourne une durée.
 */
@Component
@Slf4j
public class CronGraceCalculator {

    private static final int      GRACE_MULTIPLIER = 3;
    private static final Duration GRACE_FLOOR      = Duration.ofSeconds(30);
    private static final Duration GRACE_CEILING    = Duration.ofMinutes(15);

    /**
     * Prochain tir attendu du cron, strictement après {@code after}.
     * Retourne null si le cron est invalide.
     */
    public LocalDateTime nextExpectedFireTime(String cron, LocalDateTime after) {
        try {
            CronExpression expr = CronExpression.parse(normalizeCron(cron));
            return expr.next(after);
        } catch (Exception e) {
            log.warn("Cron invalide dans dim_sla : '{}' ({})", cron, e.getMessage());
            return null;
        }
    }

    /**
     * Dernier tir attendu du cron avant ou égal à {@code before}.
     * Retourne null si le cron est invalide ou si aucun tir n'est trouvé
     * dans la fenêtre de recherche (2 jours en arrière).
     */
    public LocalDateTime previousExpectedFireTime(String cron, LocalDateTime before) {
        try {
            CronExpression expr = CronExpression.parse(normalizeCron(cron));
            LocalDateTime candidate = before.minusDays(2);
            LocalDateTime last = null;
            LocalDateTime next = expr.next(candidate);
            while (next != null && !next.isAfter(before)) {
                last = next;
                next = expr.next(next);
            }
            return last;
        } catch (Exception e) {
            log.warn("Cron invalide dans dim_sla : '{}' ({})", cron, e.getMessage());
            return null;
        }
    }

    /**
     * Période du cron (durée entre deux tirs consécutifs), calculée autour
     * de {@code referenceFireTime}. Retourne null si le calcul échoue.
     */
    public Duration cronPeriod(String cron, LocalDateTime referenceFireTime) {
        try {
            LocalDateTime previous = previousExpectedFireTime(cron, referenceFireTime.minusNanos(1));
            if (previous == null) return null;
            return Duration.between(previous, referenceFireTime);
        } catch (Exception e) {
            log.warn("Impossible de calculer la période du cron '{}' ({})", cron, e.getMessage());
            return null;
        }
    }

    /**
     * Marge de tolérance dynamique avant de déclencher une alerte "non déclenché" :
     * période du cron x GRACE_MULTIPLIER, bornée entre GRACE_FLOOR et GRACE_CEILING.
     */
    public Duration computeDynamicGrace(String cron, LocalDateTime referenceFireTime) {
        Duration period = cronPeriod(cron, referenceFireTime);
        if (period == null || period.isZero() || period.isNegative()) {
            return GRACE_CEILING;
        }

        Duration grace = period.multipliedBy(GRACE_MULTIPLIER);
        if (grace.compareTo(GRACE_FLOOR) < 0)   return GRACE_FLOOR;
        if (grace.compareTo(GRACE_CEILING) > 0) return GRACE_CEILING;
        return grace;
    }

    private String normalizeCron(String cron) {
        if (cron == null) return null;
        String trimmed = cron.trim();
        int fields = trimmed.split("\\s+").length;
        return fields == 5 ? "0 " + trimmed : trimmed;
    }
}