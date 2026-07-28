-- ============================================================================
-- V7__schema_consolide.sql
-- Migration consolidée — schéma complet EDI Supervision
--
-- Ce fichier remplace l'ensemble des migrations V1 à V7 précédentes.
-- Il reconstruit l'état réel de la base (y compris les changements appliqués
-- manuellement en SQL via DBeaver, non versionnés jusqu'ici).
--
-- ⚠️ IMPORTANT AVANT EXÉCUTION :
--   - Ne PAS exécuter ce fichier sur une base qui contient déjà ces tables
--     sans avoir au préalable vidé/renommé le schéma existant, sinon les
--     CREATE TABLE échoueront (pas de IF NOT EXISTS ici, volontairement,
--     pour éviter de masquer un schéma divergent).
--   - Sur un environnement Flyway déjà initialisé, utiliser
--     `flyway baseline` ou repartir d'un schéma vide selon votre stratégie
--     de bascule (voir notes de fin de fichier).
-- ============================================================================


-- ============================================================================
-- SECTION 1 — Séquences
-- (déduites des DEFAULT nextval(...) du schéma existant)
-- ============================================================================

CREATE SEQUENCE IF NOT EXISTS dim_client_seq;
CREATE SEQUENCE IF NOT EXISTS dim_interface_seq;
CREATE SEQUENCE IF NOT EXISTS dim_sla_seq;
CREATE SEQUENCE IF NOT EXISTS fact_execution_seq;
CREATE SEQUENCE IF NOT EXISTS fact_step_seq;
CREATE SEQUENCE IF NOT EXISTS fact_alert_seq;


-- ============================================================================
-- SECTION 2 — Tables de dimension sans dépendance
-- ============================================================================

-- public.dim_client
CREATE TABLE public.dim_client (
                                   client_id int8 DEFAULT nextval('dim_client_seq'::regclass) NOT NULL,
                                   code varchar(20) NOT NULL,
                                   nom varchar(200) NOT NULL,
                                   contact_email varchar(200) NULL,
                                   actif bool DEFAULT true NOT NULL,
                                   CONSTRAINT pk_dim_client PRIMARY KEY (client_id),
                                   CONSTRAINT uq_dim_client_code UNIQUE (code)
);

-- public.dim_environment
CREATE TABLE public.dim_environment (
                                        env_id int8 NOT NULL,
                                        nom varchar(50) NOT NULL,
                                        CONSTRAINT pk_dim_environment PRIMARY KEY (env_id),
                                        CONSTRAINT uq_dim_environment_nom UNIQUE (nom)
);

-- public.dim_source
CREATE TABLE public.dim_source (
                                   source_id int8 NOT NULL,
                                   nom varchar(100) NOT NULL,
                                   "version" varchar(50) NULL,
                                   CONSTRAINT pk_dim_source PRIMARY KEY (source_id)
);


-- ============================================================================
-- SECTION 3 — dim_interface (dépend de dim_client)
-- ============================================================================

CREATE TABLE public.dim_interface (
                                      interface_id int8 DEFAULT nextval('dim_interface_seq'::regclass) NOT NULL,
                                      code varchar(100) NOT NULL,
                                      libelle varchar(200) NULL,
                                      direction varchar(3) NOT NULL,
                                      format varchar(20) NULL,
                                      client_id int8 NULL,
                                      actif bool DEFAULT true NOT NULL,
                                      CONSTRAINT chk_direction CHECK (((direction)::text = ANY ((ARRAY['IN'::character varying, 'OUT'::character varying])::text[]))),
	CONSTRAINT chk_format CHECK (((format)::text = ANY ((ARRAY['EDIFACT'::character varying, 'X12'::character varying, 'JSON'::character varying, 'XML'::character varying, 'CSV'::character varying])::text[]))),
	CONSTRAINT pk_dim_interface PRIMARY KEY (interface_id),
	CONSTRAINT uq_dim_interface_code UNIQUE (code),
	CONSTRAINT fk_interface_client FOREIGN KEY (client_id) REFERENCES public.dim_client(client_id) ON DELETE SET NULL
);


-- ============================================================================
-- SECTION 4 — dim_sla (dépend de dim_interface)
-- ============================================================================

CREATE TABLE public.dim_sla (
                                sla_id int8 DEFAULT nextval('dim_sla_seq'::regclass) NOT NULL,
                                interface_id int8 NOT NULL,
                                max_duration_sec int4 NULL,
                                cron_attendu varchar(100) NULL,
                                actif_depuis date DEFAULT CURRENT_DATE NOT NULL,
                                actif_jusqu_a date NULL,
                                CONSTRAINT pk_dim_sla PRIMARY KEY (sla_id),
                                CONSTRAINT fk_sla_interface FOREIGN KEY (interface_id) REFERENCES public.dim_interface(interface_id) ON DELETE CASCADE
);
CREATE INDEX idx_sla_interface ON public.dim_sla USING btree (interface_id);


-- ============================================================================
-- SECTION 5 — fact_execution (dépend de dim_interface, dim_source, dim_environment)
-- ============================================================================

CREATE TABLE public.fact_execution (
                                       execution_id int8 DEFAULT nextval('fact_execution_seq'::regclass) NOT NULL,
                                       interface_id int8 NULL,
                                       source_id int8 NOT NULL,
                                       env_id int8 NULL,
                                       source_execution_ref varchar(200) NOT NULL,
                                       start_datetime timestamptz NULL,
                                       end_datetime timestamptz NULL,
                                       duration_seconds int4 NULL,
                                       status varchar(20) DEFAULT 'RUNNING'::character varying NOT NULL,
                                       trigger_type varchar(20) NULL,
                                       rows_read int8 NULL,
                                       rows_written int8 NULL,
                                       rows_rejected int8 NULL,
                                       error_code varchar(50) NULL,
                                       error_message text NULL,
                                       retry_count int4 DEFAULT 0 NOT NULL,
                                       server_name varchar(100) NULL,
                                       ingested_at timestamptz DEFAULT now() NOT NULL,
                                       CONSTRAINT chk_execution_status CHECK (((status)::text = ANY ((ARRAY['SUCCESS'::character varying, 'FAILURE'::character varying, 'WARNING'::character varying, 'RUNNING'::character varying, 'KILLED'::character varying, 'QUEUED'::character varying, 'SKIPPED'::character varying])::text[]))),
	CONSTRAINT chk_trigger_type CHECK (((trigger_type)::text = ANY ((ARRAY['SCHEDULED'::character varying, 'MANUAL'::character varying, 'API'::character varying, 'EVENT'::character varying, 'UNKNOWN'::character varying])::text[]))),
	CONSTRAINT pk_fact_execution PRIMARY KEY (execution_id),
	CONSTRAINT uq_execution_ref_source UNIQUE (source_execution_ref, source_id),
	CONSTRAINT fk_execution_env FOREIGN KEY (env_id) REFERENCES public.dim_environment(env_id) ON DELETE SET NULL,
	CONSTRAINT fk_execution_interface FOREIGN KEY (interface_id) REFERENCES public.dim_interface(interface_id) ON DELETE SET NULL,
	CONSTRAINT fk_execution_source FOREIGN KEY (source_id) REFERENCES public.dim_source(source_id)
);
CREATE INDEX idx_execution_ingested ON public.fact_execution USING btree (ingested_at DESC);
CREATE INDEX idx_execution_interface_date ON public.fact_execution USING btree (interface_id, start_datetime DESC);
CREATE INDEX idx_execution_server ON public.fact_execution USING btree (server_name);
CREATE INDEX idx_execution_status_date ON public.fact_execution USING btree (status, start_datetime DESC);


-- ============================================================================
-- SECTION 6 — fact_execution_step (dépend de fact_execution, auto-référence)
-- ============================================================================

CREATE TABLE public.fact_execution_step (
                                            step_id int8 DEFAULT nextval('fact_step_seq'::regclass) NOT NULL,
                                            execution_id int8 NOT NULL,
                                            parent_step_id int8 NULL,
                                            step_name varchar(300) NULL,
                                            start_datetime timestamptz NULL,
                                            end_datetime timestamptz NULL,
                                            status varchar(20) NULL,
                                            rows_processed int8 NULL,
                                            error_message text NULL,
                                            source_step_ref varchar(200) NULL,
                                            step_type varchar(20) NULL,
                                            CONSTRAINT chk_step_status CHECK (((status)::text = ANY ((ARRAY['SUCCESS'::character varying, 'FAILURE'::character varying, 'RUNNING'::character varying, 'UNKNOWN'::character varying, 'SKIPPED'::character varying, 'UPSTREAM_FAILED'::character varying, 'QUEUED'::character varying, 'DEFERRED'::character varying])::text[]))),
	CONSTRAINT pk_fact_step PRIMARY KEY (step_id),
	CONSTRAINT fk_step_execution FOREIGN KEY (execution_id) REFERENCES public.fact_execution(execution_id) ON DELETE CASCADE,
	CONSTRAINT fk_step_parent FOREIGN KEY (parent_step_id) REFERENCES public.fact_execution_step(step_id) ON DELETE SET NULL
);
CREATE UNIQUE INDEX idx_fact_execution_step_ref ON public.fact_execution_step USING btree (source_step_ref);
CREATE INDEX idx_step_execution ON public.fact_execution_step USING btree (execution_id);
CREATE INDEX idx_step_parent ON public.fact_execution_step USING btree (parent_step_id);


-- ============================================================================
-- SECTION 7 — fact_alert (dépend de fact_execution)
-- ============================================================================

CREATE TABLE public.fact_alert (
                                   alert_id int8 DEFAULT nextval('fact_alert_seq'::regclass) NOT NULL,
                                   execution_id int8 NOT NULL,
                                   alert_type varchar(50) NOT NULL,
                                   severity varchar(20) NULL,
                                   triggered_at timestamptz DEFAULT now() NOT NULL,
                                   acknowledged_at timestamptz NULL,
                                   resolved_at timestamptz NULL,
                                   message varchar(500) NULL,
                                   CONSTRAINT chk_alert_severity CHECK (((severity)::text = ANY ((ARRAY['CRITICAL'::character varying, 'HIGH'::character varying, 'MEDIUM'::character varying, 'LOW'::character varying])::text[]))),
	CONSTRAINT chk_alert_type CHECK (((alert_type)::text = ANY (ARRAY[('SLA_BREACH'::character varying)::text, ('ERROR'::character varying)::text, ('TIMEOUT'::character varying)::text, ('TASK_FAILED'::character varying)::text, ('DAG_ABORTED'::character varying)::text, ('UPSTREAM_FAILED'::character varying)::text, ('RETRY_EXHAUSTED'::character varying)::text, ('REPEATED_FAILURE'::character varying)::text, ('MANUAL_OVERRIDE'::character varying)::text, ('LATE_START'::character varying)::text, ('DAG_NOT_TRIGGERED'::character varying)::text, ('STUCK_RUNNING'::character varying)::text, ('SESSION_NOT_TRIGGERED'::character varying)::text]))),
	CONSTRAINT pk_fact_alert PRIMARY KEY (alert_id),
	CONSTRAINT fk_alert_execution FOREIGN KEY (execution_id) REFERENCES public.fact_execution(execution_id) ON DELETE CASCADE
);
CREATE INDEX idx_alert_execution ON public.fact_alert USING btree (execution_id);
CREATE INDEX idx_alert_triggered ON public.fact_alert USING btree (triggered_at DESC);
CREATE UNIQUE INDEX idx_alert_unique_open ON public.fact_alert USING btree (execution_id, alert_type) WHERE (resolved_at IS NULL);


-- ============================================================================
-- SECTION 8 — pending_action (table indépendante, buffer applicatif)
-- ============================================================================

CREATE TABLE public.pending_action (
                                       pending_id bigserial NOT NULL,
                                       parent_ref varchar(100) NOT NULL,
                                       source_step_ref varchar(100) NOT NULL,
                                       step_name varchar(255) NULL,
                                       status varchar(20) NULL,
                                       start_datetime timestamp NULL,
                                       end_datetime timestamp NULL,
                                       error_message text NULL,
                                       buffered_at timestamp DEFAULT now() NOT NULL,
                                       CONSTRAINT pending_action_pkey PRIMARY KEY (pending_id),
                                       CONSTRAINT pending_action_source_step_ref_key UNIQUE (source_step_ref)
);
CREATE INDEX idx_pending_action_parent_ref ON public.pending_action USING btree (parent_ref);


-- ============================================================================
-- NOTES DE MIGRATION — à lire avant d'intégrer ce fichier au projet
-- ============================================================================
--
-- 1. Table `flyway_schema_history` :
--    Volontairement EXCLUE de ce fichier — c'est une table interne gérée
--    par Flyway lui-même, elle ne doit jamais être créée manuellement dans
--    une migration.
--
-- 2. Stratégie de bascule recommandée pour remplacer V1-V7 par ce V8 :
--    a) Sur un environnement DEV/TEST vide ou jetable :
--       - Renommer/supprimer les anciens fichiers V1__..sql à V7__..sql
--         du dossier de migration (les sortir du repo ou les archiver
--         dans un dossier "archive/" hors du chemin scanné par Flyway)
--       - Placer ce fichier comme V1__schema_consolide.sql (nouvelle base)
--       - `flyway clean` puis `flyway migrate` sur un environnement de test
--         pour valider que le schéma se recrée à l'identique
--    b) Sur un environnement déjà en production avec historique Flyway existant :
--       - NE PAS supprimer les migrations déjà appliquées dans
--         flyway_schema_history (Flyway comparerait les checksums et échouerait)
--       - Garder ce fichier comme prochaine version (ex: V8) uniquement
--         à des fins de DOCUMENTATION / référence du schéma consolidé,
--         sans le ré-exécuter tel quel (il ferait des CREATE TABLE en doublon)
--       - Pour repartir propre sur un futur nouvel environnement, utiliser
--         `flyway baseline -baselineVersion=8` puis démarrer les nouvelles
--         migrations à partir de V9
--
-- 3. Changements appliqués manuellement via DBeaver :
--    Ce fichier reflète l'état RÉEL de la base au moment de l'export DDL
--    (colonnes, contraintes, index confirmés directement en base).
--    Toute modification future doit repasser par un fichier de migration
--    versionné — plus de changement direct en DBeaver sans migration
--    associée, pour éviter de re-diverger entre code et base.
--
-- 4. Dette identifiée précédemment (Flyway V7 "pending_action + chk_alert_type") :
--    Les deux éléments (table `pending_action` et contrainte `chk_alert_type`
--    incluant les 13 valeurs actuelles) sont bien présents dans ce schéma
--    consolidé — cette dette est donc résorbée par ce fichier.
-- ============================================================================