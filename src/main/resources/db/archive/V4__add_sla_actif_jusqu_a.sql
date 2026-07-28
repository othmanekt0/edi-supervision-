ALTER TABLE dim_sla ADD COLUMN actif_jusqu_a DATE;

COMMENT ON COLUMN dim_sla.actif_jusqu_a IS 'Date de fin de validité du SLA. NULL = toujours actif';