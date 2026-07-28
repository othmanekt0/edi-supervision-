-- =============================================================================
-- V1__init_supervision_schema.sql
-- Supervision EDI — Modèle en étoile (Kimball)
-- Flyway migration initiale
-- =============================================================================

-- -----------------------------------------------------------------------------
-- EXTENSIONS
-- -----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- utile pour gen_random_uuid si besoin

-- -----------------------------------------------------------------------------
-- SEQUENCES
-- -----------------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS dim_client_seq       START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS dim_interface_seq    START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS dim_sla_seq          START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS fact_execution_seq   START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS fact_step_seq        START 1 INCREMENT 1;
CREATE SEQUENCE IF NOT EXISTS fact_alert_seq       START 1 INCREMENT 1;

-- =============================================================================
-- DIMENSIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIM_SOURCE — origines techniques (Stambia, Airflow, ...)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_source (
                            source_id   INT          NOT NULL,
                            nom         VARCHAR(100) NOT NULL,
                            version     VARCHAR(50),
                            CONSTRAINT pk_dim_source PRIMARY KEY (source_id)
);

-- -----------------------------------------------------------------------------
-- DIM_ENVIRONMENT — DEV, PREPROD, PROD
-- -----------------------------------------------------------------------------
CREATE TABLE dim_environment (
                                 env_id  INT          NOT NULL,
                                 nom     VARCHAR(50)  NOT NULL,
                                 CONSTRAINT pk_dim_environment PRIMARY KEY (env_id),
                                 CONSTRAINT uq_dim_environment_nom UNIQUE (nom)
);

-- -----------------------------------------------------------------------------
-- DIM_CLIENT — référentiel des partenaires EDI (alimentation manuelle)
-- -----------------------------------------------------------------------------
CREATE TABLE dim_client (
                            client_id       BIGINT       NOT NULL DEFAULT nextval('dim_client_seq'),
                            code            VARCHAR(20)  NOT NULL,
                            nom             VARCHAR(200) NOT NULL,
                            contact_email   VARCHAR(200),
                            actif           BOOLEAN      NOT NULL DEFAULT TRUE,
                            CONSTRAINT pk_dim_client  PRIMARY KEY (client_id),
                            CONSTRAINT uq_dim_client_code UNIQUE (code)
);

-- -----------------------------------------------------------------------------
-- DIM_INTERFACE — lien logique entre le code source et le contexte métier
-- -----------------------------------------------------------------------------
CREATE TABLE dim_interface (
                               interface_id    BIGINT       NOT NULL DEFAULT nextval('dim_interface_seq'),
                               code            VARCHAR(100) NOT NULL,
                               libelle         VARCHAR(200),
                               direction       VARCHAR(3)   NOT NULL,  -- IN | OUT
                               format          VARCHAR(20),            -- EDIFACT, X12, JSON, XML, CSV
                               client_id       BIGINT,
                               actif           BOOLEAN      NOT NULL DEFAULT TRUE,
                               CONSTRAINT pk_dim_interface      PRIMARY KEY (interface_id),
                               CONSTRAINT uq_dim_interface_code UNIQUE (code),
                               CONSTRAINT chk_direction         CHECK (direction IN ('IN','OUT')),
                               CONSTRAINT chk_format            CHECK (format IN ('EDIFACT','X12','JSON','XML','CSV')),
                               CONSTRAINT fk_interface_client   FOREIGN KEY (client_id)
                                   REFERENCES dim_client (client_id)
                                   ON DELETE SET NULL
);

-- -----------------------------------------------------------------------------
-- DIM_SLA — engagements de service, versionnés dans le temps
-- -----------------------------------------------------------------------------
CREATE TABLE dim_sla (
                         sla_id              BIGINT       NOT NULL DEFAULT nextval('dim_sla_seq'),
                         interface_id        BIGINT       NOT NULL,
                         max_duration_sec    INT,
                         cron_attendu        VARCHAR(100),
                         actif_depuis        DATE         NOT NULL DEFAULT CURRENT_DATE,
                         CONSTRAINT pk_dim_sla          PRIMARY KEY (sla_id),
                         CONSTRAINT fk_sla_interface    FOREIGN KEY (interface_id)
                             REFERENCES dim_interface (interface_id)
                             ON DELETE CASCADE
);

CREATE INDEX idx_sla_interface ON dim_sla (interface_id);

-- =============================================================================
-- FACTS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- FACT_EXECUTION — table centrale, une ligne = une exécution unifiée
-- -----------------------------------------------------------------------------
CREATE TABLE fact_execution (
                                execution_id            BIGINT          NOT NULL DEFAULT nextval('fact_execution_seq'),
                                interface_id            BIGINT,
                                source_id               INT             NOT NULL,
                                env_id                  INT,
                                source_execution_ref    VARCHAR(200)    NOT NULL,
                                start_datetime          TIMESTAMPTZ,
                                end_datetime            TIMESTAMPTZ,
                                duration_seconds        INT,
                                status                  VARCHAR(20)     NOT NULL DEFAULT 'RUNNING',
                                trigger_type            VARCHAR(20),
                                rows_read               BIGINT,
                                rows_written            BIGINT,
                                rows_rejected           BIGINT,
                                error_code              VARCHAR(50),
                                error_message           TEXT,
                                retry_count             INT             NOT NULL DEFAULT 0,
                                server_name             VARCHAR(100),
                                ingested_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

                                CONSTRAINT pk_fact_execution        PRIMARY KEY (execution_id),
                                CONSTRAINT uq_execution_ref_source  UNIQUE (source_execution_ref, source_id),
                                CONSTRAINT chk_execution_status     CHECK (status IN ('SUCCESS','FAILURE','WARNING','RUNNING','KILLED')),
                                CONSTRAINT chk_trigger_type         CHECK (trigger_type IN ('SCHEDULED','MANUAL','API','EVENT')),
                                CONSTRAINT fk_execution_interface   FOREIGN KEY (interface_id)
                                    REFERENCES dim_interface (interface_id)
                                    ON DELETE SET NULL,
                                CONSTRAINT fk_execution_source      FOREIGN KEY (source_id)
                                    REFERENCES dim_source (source_id),
                                CONSTRAINT fk_execution_env         FOREIGN KEY (env_id)
                                    REFERENCES dim_environment (env_id)
                                    ON DELETE SET NULL
);

-- Index obligatoires (spécifiés dans le cahier des charges)
CREATE INDEX idx_execution_interface_date
    ON fact_execution (interface_id, start_datetime DESC);

CREATE INDEX idx_execution_status_date
    ON fact_execution (status, start_datetime DESC);

-- Index complémentaires utiles pour les dashboards
CREATE INDEX idx_execution_ingested   ON fact_execution (ingested_at DESC);
CREATE INDEX idx_execution_server     ON fact_execution (server_name);

-- -----------------------------------------------------------------------------
-- FACT_EXECUTION_STEP — détail par étape (sous-session ou task Airflow)
-- -----------------------------------------------------------------------------
CREATE TABLE fact_execution_step (
                                     step_id             BIGINT       NOT NULL DEFAULT nextval('fact_step_seq'),
                                     execution_id        BIGINT       NOT NULL,
                                     parent_step_id      BIGINT,                     -- hiérarchie des sous-étapes
                                     step_name           VARCHAR(300),
                                     start_datetime      TIMESTAMPTZ,
                                     end_datetime        TIMESTAMPTZ,
                                     status              VARCHAR(20),
                                     rows_processed      BIGINT,
                                     error_message       TEXT,

                                     CONSTRAINT pk_fact_step             PRIMARY KEY (step_id),
                                     CONSTRAINT chk_step_status          CHECK (status IN ('SUCCESS','FAILURE','WARNING','RUNNING','KILLED')),
                                     CONSTRAINT fk_step_execution        FOREIGN KEY (execution_id)
                                         REFERENCES fact_execution (execution_id)
                                         ON DELETE CASCADE,
                                     CONSTRAINT fk_step_parent           FOREIGN KEY (parent_step_id)
                                         REFERENCES fact_execution_step (step_id)
                                         ON DELETE SET NULL
);

-- Index obligatoire (mentionné dans le cahier des charges)
CREATE INDEX idx_step_execution ON fact_execution_step (execution_id);

-- Index complémentaire pour la hiérarchie
CREATE INDEX idx_step_parent ON fact_execution_step (parent_step_id);

-- -----------------------------------------------------------------------------
-- FACT_ALERT — historisation des alertes
-- -----------------------------------------------------------------------------
CREATE TABLE fact_alert (
                            alert_id        BIGINT          NOT NULL DEFAULT nextval('fact_alert_seq'),
                            execution_id    BIGINT          NOT NULL,
                            alert_type      VARCHAR(50)     NOT NULL,   -- SLA_BREACH, ERROR, TIMEOUT
                            severity        VARCHAR(20),
                            triggered_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                            acknowledged_at TIMESTAMPTZ,
                            resolved_at     TIMESTAMPTZ,

                            CONSTRAINT pk_fact_alert        PRIMARY KEY (alert_id),
                            CONSTRAINT chk_alert_type       CHECK (alert_type IN ('SLA_BREACH','ERROR','TIMEOUT')),
                            CONSTRAINT chk_alert_severity   CHECK (severity IN ('CRITICAL','HIGH','MEDIUM','LOW')),
                            CONSTRAINT fk_alert_execution   FOREIGN KEY (execution_id)
                                REFERENCES fact_execution (execution_id)
                                ON DELETE CASCADE
);

CREATE INDEX idx_alert_execution    ON fact_alert (execution_id);
CREATE INDEX idx_alert_triggered    ON fact_alert (triggered_at DESC);
CREATE INDEX idx_alert_unresolved   ON fact_alert (resolved_at) WHERE resolved_at IS NULL;

-- =============================================================================
-- SEED DATA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIM_SOURCE — données initiales
-- -----------------------------------------------------------------------------
INSERT INTO dim_source (source_id, nom, version) VALUES
                                                     (1, 'Stambia',  NULL),
                                                     (2, 'Airflow',  NULL);

-- -----------------------------------------------------------------------------
-- DIM_ENVIRONMENT — données initiales
-- -----------------------------------------------------------------------------
INSERT INTO dim_environment (env_id, nom) VALUES
                                              (1, 'DEV'),
                                              (2, 'PREPROD'),
                                              (3, 'PROD');

-- =============================================================================
-- COMMENTS (documentation inline pour les outils de BI)
-- =============================================================================
COMMENT ON TABLE dim_client           IS 'Référentiel des partenaires EDI — alimentation manuelle';
COMMENT ON TABLE dim_interface        IS 'Lien logique entre le code source ETL et le contexte métier';
COMMENT ON TABLE dim_source           IS 'Petite dimension technique : origine des exécutions (Stambia, Airflow)';
COMMENT ON TABLE dim_environment      IS 'Environnements de déploiement (DEV, PREPROD, PROD)';
COMMENT ON TABLE dim_sla              IS 'Engagements de service par interface, versionnés dans le temps';
COMMENT ON TABLE fact_execution       IS 'Table centrale : une ligne = une exécution unifiée (session Stambia ou dag_run Airflow)';
COMMENT ON TABLE fact_execution_step  IS 'Détail par étape : sous-session Stambia ou task Airflow';
COMMENT ON TABLE fact_alert           IS 'Historisation des alertes déclenchées sur les exécutions';

COMMENT ON COLUMN fact_execution.source_execution_ref IS 'SESSION_ID Stambia ou run_id Airflow — clé métier de déduplication';
COMMENT ON COLUMN fact_execution.status               IS 'SUCCESS | FAILURE | WARNING | RUNNING | KILLED';
COMMENT ON COLUMN fact_execution.trigger_type         IS 'SCHEDULED | MANUAL | API | EVENT';
COMMENT ON COLUMN fact_execution_step.parent_step_id  IS 'NULL pour les étapes racines ; renseigné pour les sous-étapes imbriquées';
COMMENT ON COLUMN dim_interface.direction             IS 'IN (réception) ou OUT (émission)';
COMMENT ON COLUMN dim_sla.cron_attendu                IS 'Expression cron décrivant la planification théorique de l''interface';