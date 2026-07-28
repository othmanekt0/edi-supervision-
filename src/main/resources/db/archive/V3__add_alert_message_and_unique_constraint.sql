-- Ajout d'un message descriptif sur l'alerte
ALTER TABLE fact_alert ADD COLUMN message VARCHAR(500);

COMMENT ON COLUMN fact_alert.message IS 'Détail lisible de l''alerte (ex: message d''erreur, dépassement SLA en secondes)';

-- Contrainte d'idempotence : une seule alerte OUVERTE par (execution_id, alert_type)
DROP INDEX IF EXISTS idx_alert_unresolved;

CREATE UNIQUE INDEX idx_alert_unique_open
    ON fact_alert (execution_id, alert_type)
    WHERE resolved_at IS NULL;