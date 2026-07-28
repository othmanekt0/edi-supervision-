-- src/main/resources/db/migration/V2__add_source_step_ref.sql
ALTER TABLE fact_execution_step
    ADD COLUMN source_step_ref VARCHAR(200);

CREATE UNIQUE INDEX idx_fact_execution_step_ref
    ON fact_execution_step (source_step_ref)
    WHERE source_step_ref IS NOT NULL;