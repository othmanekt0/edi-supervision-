-- V6__fix_id_column_types.sql

ALTER TABLE dim_environment  ALTER COLUMN env_id     TYPE BIGINT;
ALTER TABLE dim_source       ALTER COLUMN source_id  TYPE BIGINT;

ALTER TABLE fact_execution   ALTER COLUMN env_id     TYPE BIGINT;
ALTER TABLE fact_execution   ALTER COLUMN source_id  TYPE BIGINT;