-- V5__fusion_airflow_schema.sql
-- Adaptation du schéma Stambia pour accueillir le flux Airflow

-- 1. Formaliser step_type (créée manuellement, pas encore dans les migrations)
ALTER TABLE fact_execution_step
    ADD COLUMN IF NOT EXISTS step_type VARCHAR(50) DEFAULT 'TASK';

-- 2. Élargir chk_execution_status pour inclure les statuts Airflow
--    Airflow envoie : failed, queued (nouveaux), skipped (côté steps)
--    On normalise en majuscules côté Java donc on garde la casse majuscule
ALTER TABLE fact_execution
DROP CONSTRAINT chk_execution_status;

ALTER TABLE fact_execution
    ADD CONSTRAINT chk_execution_status
        CHECK (status IN (
                          'SUCCESS', 'FAILURE', 'WARNING',
                          'RUNNING', 'KILLED',
                          'QUEUED', 'SKIPPED'
            ));

-- 3. Élargir chk_step_status pour inclure les statuts Airflow
ALTER TABLE fact_execution_step
DROP CONSTRAINT chk_step_status;

ALTER TABLE fact_execution_step
    ADD CONSTRAINT chk_step_status
        CHECK (status IN (
                          'SUCCESS', 'FAILURE', 'RUNNING', 'UNKNOWN',
                          'SKIPPED', 'UPSTREAM_FAILED', 'QUEUED', 'DEFERRED'
            ));

-- 4. Élargir chk_alert_type pour inclure tous les types Airflow
ALTER TABLE fact_alert
DROP CONSTRAINT chk_alert_type;

ALTER TABLE fact_alert
    ADD CONSTRAINT chk_alert_type
        CHECK (alert_type IN (
                              'SLA_BREACH', 'ERROR', 'TIMEOUT',
                              'TASK_FAILED', 'DAG_ABORTED', 'UPSTREAM_FAILED',
                              'RETRY_EXHAUSTED', 'REPEATED_FAILURE', 'MANUAL_OVERRIDE',
                              'LATE_START', 'DAG_NOT_TRIGGERED', 'STUCK_RUNNING'
            ));