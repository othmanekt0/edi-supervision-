-- =============================================================================
-- Initialisation de la base Stambia MOCK
-- Structure identique à la vraie base Stambia (schéma "log")
-- Basé sur l'analyse des vrais logs : session_sess.csv, action_act.csv, delivery_dlv.csv
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS log;

-- -----------------------------------------------------------------------------
-- stb_log_delivery_dlv — référentiel des interfaces fonctionnelles
-- Colonnes identifiées depuis delivery_dlv.csv (30 lignes analysées)
-- Colonnes : dlv_id, dlv_clo, dlv_blo, dlv_format, dlv_name, proc_id,
--            dlv_conf, dlv_tstamp, dlv_version, dlv_user, dlv_comment,
--            pck_id, checksum
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS log.stb_log_delivery_dlv (
                                                        dlv_id          VARCHAR(100)  PRIMARY KEY,
    dlv_clo         TEXT,
    dlv_blo         BYTEA,
    dlv_format      VARCHAR(50),
    dlv_name        VARCHAR(300),
    proc_id         VARCHAR(100),
    dlv_conf        VARCHAR(100),
    dlv_tstamp      NUMERIC,
    dlv_version     VARCHAR(50),
    dlv_user        VARCHAR(100),
    dlv_comment     VARCHAR(500),
    pck_id          VARCHAR(100),
    checksum        VARCHAR(100)
    );

-- -----------------------------------------------------------------------------
-- stb_log_session_sess — table principale capturée par Debezium
-- Colonnes identifiées depuis session_sess.csv (500 lignes analysées)
-- Colonnes : sess_id, sess_name, sess_ret_code, sess_begin_date, sess_end_date,
--            sess_iter, sess_ret_msg, sess_engine_host, sess_engine_port,
--            dlv_id, sess_launch_mode, sess_execution_mode, sess_guest_host,
--            sess_conf, sess_parent_id, v_version, sess_parent_iter,
--            sess_act_root_id, sess_begin_tstamp, sess_duration,
--            sess_tstamp_offset, sess_last_tstamp, sess_inact_timeout,
--            sess_launch_user, sess_begin_ts
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS log.stb_log_session_sess (
                                                        sess_id             VARCHAR(100)    PRIMARY KEY,
    sess_name           VARCHAR(300),
    sess_ret_code       INTEGER,                        -- 1=SUCCESS, -1=FAILURE, 0=RUNNING
    sess_begin_date     VARCHAR(30),                    -- format : 'yyyy/MM/dd HH:mm:ss.SSS'
    sess_end_date       VARCHAR(30),                    -- NULL si en cours
    sess_iter           NUMERIC,
    sess_ret_msg        VARCHAR(1000),                  -- message d'erreur si échec
    sess_engine_host    VARCHAR(100),                   -- serveur d'exécution ex: 1.7.5.8
    sess_engine_port    NUMERIC,
    dlv_id              VARCHAR(100) REFERENCES log.stb_log_delivery_dlv(dlv_id),
    sess_launch_mode    VARCHAR(50),                    -- WEB_SERVICE, WEB_INTERACTIVE, SCHEDULE, ACTION
    sess_execution_mode VARCHAR(50),
    sess_guest_host     VARCHAR(100),
    sess_conf           VARCHAR(50),                    -- prod, preprod, dev
    sess_parent_id      VARCHAR(100),                   -- NULL si session principale, rempli si sous-session
    v_version           VARCHAR(50),
    sess_parent_iter    NUMERIC,
    sess_act_root_id    VARCHAR(100),
    sess_begin_tstamp   NUMERIC,
    sess_duration       BIGINT,                         -- millisecondes
    sess_tstamp_offset  NUMERIC,
    sess_last_tstamp    NUMERIC,
    sess_inact_timeout  NUMERIC,
    sess_launch_user    VARCHAR(100),
    sess_begin_ts       TIMESTAMP
    );

-- -----------------------------------------------------------------------------
-- stb_log_action_act — actions en erreur liées aux sessions
-- Colonnes identifiées depuis action_act.csv (13978 lignes analysées)
-- Colonnes : sess_id, sess_iter, act_id, act_iter, act_name, act_type,
--            act_begin_date, act_end_date, act_ret_code, act_ret_msg,
--            act_father_engine_id, act_parent_iter, act_real_name,
--            act_nb_exe, act_nb_bnd_exe, act_is_begin, act_num
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS log.stb_log_action_act (
                                                      sess_id             VARCHAR(100) REFERENCES log.stb_log_session_sess(sess_id),
    sess_iter           NUMERIC,
    act_id              VARCHAR(100),
    act_iter            NUMERIC,
    act_name            VARCHAR(500),                   -- chemin complet ex: interface/step/substep
    act_type            VARCHAR(50),                    -- Code, Mapping, etc.
    act_begin_date      VARCHAR(30),
    act_end_date        VARCHAR(30),
    act_ret_code        NUMERIC,                        -- 1=SUCCESS, -1=FAILURE
    act_ret_msg         VARCHAR(1000),
    act_father_engine_id VARCHAR(100),
    act_parent_iter     NUMERIC,
    act_real_name       VARCHAR(300),
    act_nb_exe          NUMERIC,
    act_nb_bnd_exe      NUMERIC,
    act_is_begin        NUMERIC,
    act_num             NUMERIC,
    PRIMARY KEY (sess_id, act_id, act_iter)
    );

-- -----------------------------------------------------------------------------
-- Rôle de réplication pour Debezium
-- -----------------------------------------------------------------------------
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium_user') THEN
CREATE ROLE debezium_user WITH LOGIN PASSWORD 'debezium_pass' REPLICATION;
END IF;
END$$;

GRANT USAGE  ON SCHEMA log TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA log TO debezium_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA log GRANT SELECT ON TABLES TO debezium_user;

-- -----------------------------------------------------------------------------
-- Données de test réalistes basées sur les vrais logs Stambia
-- -----------------------------------------------------------------------------

-- Interfaces (delivery_dlv)
INSERT INTO log.stb_log_delivery_dlv (dlv_id, dlv_name, dlv_conf, dlv_version)
VALUES
    ('_2f9MUW_vEfGXkecuqx0sqg',  'delivery',                     'prod', 'Version_1'),
    ('_xmnkwXBxEfGXkecuqx0sqg',  'delivery_main',                'prod', 'Version_1'),
    ('_Xmzt4G_vEfGXkecuqx0sqg',  'load_response_api_order',      'prod', 'Version_1'),
    ('_UsUHABJKEfGaip7_cUsMHQ',  'shipment_out_notification_main','prod', 'Version_1')
    ON CONFLICT DO NOTHING;

-- Sessions principales (sess_parent_id NULL → FACT_EXECUTION)
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id,
    sess_engine_host, dlv_id
) VALUES
      ('644004af019efdf444e38cb01b18eb3d', 'delivery',      1,
       '2026/06/25 08:45:04.740', '2026/06/25 08:45:05.359', 619,
       'WEB_SERVICE', 'prod', NULL,
       '1.7.5.8', '_2f9MUW_vEfGXkecuqx0sqg'),

      ('644004af019efdf6937024202fd1b32b', 'delivery_main', 1,
       '2026/06/25 08:47:35.888', '2026/06/25 08:47:37.428', 1540,
       'WEB_INTERACTIVE', 'prod', NULL,
       '1.7.5.8', '_xmnkwXBxEfGXkecuqx0sqg'),

      ('64400488019efe45bd0ecb806f9b7037', 'shipment_out_notification_main', 1,
       '2026/06/25 10:14:03.910', '2026/06/25 10:14:15.000', 11090,
       'SCHEDULE', 'prod', NULL,
       '1.7.5.8', '_UsUHABJKEfGaip7_cUsMHQ')
    ON CONFLICT DO NOTHING;

-- Sous-sessions (sess_parent_id rempli → FACT_EXECUTION_STEP)
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id,
    sess_engine_host
) VALUES
    ('644004a3019efdffcde57535434fa35f',
     'traitement fichier en cours : batch_lot1.xlsx', 1,
     '2026/06/25 08:57:40.630', '2026/06/25 08:57:40.803', 173,
     'ACTION', 'prod', '644004af019efdf6937024202fd1b32b',
     '1.7.5.8')
    ON CONFLICT DO NOTHING;

-- Actions en erreur (exemples depuis action_act.csv)
INSERT INTO log.stb_log_action_act (
    sess_id, sess_iter, act_id, act_iter,
    act_name, act_type,
    act_begin_date, act_end_date,
    act_ret_code, act_num
) VALUES
      ('64400488019efe45bd0ecb806f9b7037', 1,
       'f49313abf2300df38403c4328b24a454', 1,
       'shipment_out_notification_main/Transport_KAFKA_to_File/L1-log/Drop of load table',
       'Code',
       '2026/06/25 10:14:03.970', '2026/06/25 10:14:04.063',
       1, 5),

      ('64400488019efe45bd0ecb806f9b7037', 1,
       '3ef2f713ed05d637aabe15d3c29f4c80', 1,
       'shipment_out_notification_main/Transport_KAFKA_to_File/L1-log/Creation of load table',
       'Code',
       '2026/06/25 10:14:04.071', '2026/06/25 10:14:04.086',
       1, 7)
    ON CONFLICT DO NOTHING;