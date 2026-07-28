-- =============================================================================
-- SEED DATA — base supervision (dev local)
-- Couvre tous les dashboards 1 à 5
-- Données couvrant les 30 derniers jours (juillet 2026)
-- =============================================================================
-- Lancer avec :
--   docker exec -i pg-supervision psql -U supervision -d supervision < seed_supervision_data.sql
-- =============================================================================

-- Nettoyage préalable (dans l'ordre des FK)
TRUNCATE fact_alert    RESTART IDENTITY CASCADE;
TRUNCATE fact_execution_step RESTART IDENTITY CASCADE;
TRUNCATE fact_execution      RESTART IDENTITY CASCADE;
TRUNCATE dim_sla             RESTART IDENTITY CASCADE;
TRUNCATE dim_interface       RESTART IDENTITY CASCADE;
TRUNCATE dim_client          RESTART IDENTITY CASCADE;
-- dim_source et dim_environment sont des seed Flyway — on ne les touche pas

-- =============================================================================
-- 1. dim_client
-- =============================================================================
INSERT INTO dim_client (client_id, code, nom, contact_email, actif) VALUES
  (1, 'CLIENT_A',  'Client A — Retail',          'ops@clienta.com',   true),
  (2, 'CLIENT_B',  'Client B — Logistique',       'edi@clientb.fr',    true),
  (3, 'CLIENT_C',  'Client C — Pharmacie',        'it@clientc.eu',     true),
  (4, 'CLIENT_D',  'Client D — Inactif',          'ancien@clientd.com',false);

-- =============================================================================
-- 2. dim_interface  (source_id : 1=Stambia, 2=Airflow)
-- =============================================================================
INSERT INTO dim_interface (interface_id, code, libelle, direction, format, client_id, actif) VALUES
  -- Interfaces Stambia (source_id=1)
  (1,  'Delivery',              'Livraisons Client A',          'OUT', 'EDIFACT', 1, true),
  (2,  'Orders',                'Commandes Client A',           'IN',  'EDIFACT', 1, true),
  (3,  'Invoice_ClientB',       'Factures Client B',            'OUT', 'XML',     2, true),
  (4,  'Stock_ClientC',         'Stocks Client C',              'OUT', 'CSV',     3, true),
  -- Interfaces Airflow (source_id=2)
  (5,  'dag_orders_clientA',    'DAG Commandes Client A',       'OUT', 'JSON',    1, true),
  (6,  'dag_invoice_clientB',   'DAG Factures Client B',        'OUT', 'XML',     2, true),
  (7,  'dag_stock_clientC',     'DAG Stocks Client C',          'OUT', 'CSV',     3, true),
  (8,  'dag_delivery_clientA',  'DAG Livraisons Client A',      'OUT', 'EDIFACT', 1, true),
  (9,  'dag_reporting_weekly',  'DAG Rapport hebdomadaire',     'OUT', 'JSON',    1, true);

-- =============================================================================
-- 3. dim_sla
-- =============================================================================
INSERT INTO dim_sla (interface_id, max_duration_sec, cron_attendu, actif_depuis, actif_jusqu_a) VALUES
  (1, 300,  '0 6 * * 1-5',  '2026-01-01', NULL),   -- Delivery : SLA 5 min, lun-ven 6h
  (2, 600,  '0 8 * * 1-5',  '2026-01-01', NULL),   -- Orders : SLA 10 min
  (3, 900,  '0 7 * * *',    '2026-01-01', NULL),   -- Invoice_ClientB : SLA 15 min, quotidien
  (4, 1200, '0 5 * * *',    '2026-01-01', NULL),   -- Stock_ClientC : SLA 20 min
  (5, 300,  '0 6 * * 1-5',  '2026-01-01', NULL),   -- dag_orders_clientA
  (6, 600,  '0 7 * * *',    '2026-01-01', NULL),   -- dag_invoice_clientB
  (7, 900,  '0 5 * * *',    '2026-01-01', NULL),   -- dag_stock_clientC
  (8, 300,  '0 6 * * 1-5',  '2026-01-01', NULL),   -- dag_delivery_clientA
  (9, 1800, '0 8 * * 1',    '2026-01-01', NULL);   -- dag_reporting_weekly : SLA 30 min, lundi

-- =============================================================================
-- 4. fact_execution — ~60 lignes couvrant le mois de juillet 2026
--    env_id : 1=DEV, 2=PREPROD, 3=PROD
--    source_id : 1=Stambia, 2=Airflow
-- =============================================================================

INSERT INTO fact_execution
  (execution_id, interface_id, source_id, env_id, source_execution_ref,
   start_datetime, end_datetime, duration_seconds,
   status, trigger_type, rows_read, rows_written, rows_rejected,
   error_code, error_message, retry_count, server_name, ingested_at)
VALUES

-- ── Interface 1 : Delivery (Stambia, PROD) ──────────────────────────────────
(1001, 1, 1, 3, 'STB-SESS-1001',
  '2026-07-21 06:01:00+01','2026-07-21 06:03:45+01', 225,
  'SUCCESS','SCHEDULED', 1200, 1200, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(1002, 1, 1, 3, 'STB-SESS-1002',
  '2026-07-18 06:00:30+01','2026-07-18 06:07:15+01', 465,   -- dépasse SLA 300s
  'SUCCESS','SCHEDULED', 980, 975, 5, NULL, NULL, 0, 'etl-srv-01', NOW()),

(1003, 1, 1, 3, 'STB-SESS-1003',
  '2026-07-17 06:01:00+01','2026-07-17 06:09:00+01', 480,   -- dépasse SLA
  'FAILURE','SCHEDULED', 500, 0, 0, NULL, 'Connection timeout to target system', 0, 'etl-srv-01', NOW()),

(1004, 1, 1, 3, 'STB-SESS-1004',
  '2026-07-16 06:00:00+01','2026-07-16 06:04:10+01', 250,
  'SUCCESS','SCHEDULED', 1100, 1100, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(1005, 1, 1, 3, 'STB-SESS-1005',
  '2026-07-15 06:00:00+01','2026-07-15 06:03:30+01', 210,
  'SUCCESS','SCHEDULED', 1050, 1050, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(1006, 1, 1, 3, 'STB-SESS-1006',
  '2026-07-14 06:01:00+01','2026-07-14 06:05:20+01', 320,  -- légèrement au-dessus SLA
  'WARNING','SCHEDULED', 1300, 1295, 5, NULL, '5 lignes rejetées', 0, 'etl-srv-01', NOW()),

(1007, 1, 1, 3, 'STB-SESS-1007',
  '2026-07-11 06:00:00+01','2026-07-11 06:04:00+01', 240,
  'SUCCESS','SCHEDULED', 1150, 1150, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(1008, 1, 1, 3, 'STB-SESS-1008',
  '2026-07-10 06:00:00+01','2026-07-10 06:03:00+01', 180,
  'SUCCESS','SCHEDULED', 950,  950,  0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(1009, 1, 1, 3, 'STB-SESS-1009',
  '2026-07-09 06:00:00+01','2026-07-09 06:12:00+01', 720,  -- gros dépassement SLA
  'FAILURE','SCHEDULED', 0, 0, 0, NULL, 'SFTP host unreachable', 0, 'etl-srv-01', NOW()),

(1010, 1, 1, 3, 'STB-SESS-1010',
  '2026-07-08 06:00:00+01','2026-07-08 06:04:30+01', 270,
  'SUCCESS','SCHEDULED', 1000, 1000, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

-- ── Interface 2 : Orders (Stambia, PROD) ────────────────────────────────────
(2001, 2, 1, 3, 'STB-SESS-2001',
  '2026-07-21 08:01:00+01','2026-07-21 08:06:30+01', 390,
  'SUCCESS','SCHEDULED', 320, 320, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(2002, 2, 1, 3, 'STB-SESS-2002',
  '2026-07-18 08:00:00+01','2026-07-18 08:05:00+01', 300,   -- exactement au SLA
  'SUCCESS','SCHEDULED', 280, 280, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(2003, 2, 1, 3, 'STB-SESS-2003',
  '2026-07-17 08:01:00+01','2026-07-17 08:14:00+01', 780,   -- dépasse SLA 600s
  'FAILURE','SCHEDULED', 100, 0, 0, NULL, 'XML parse error on line 42', 0, 'etl-srv-02', NOW()),

(2004, 2, 1, 3, 'STB-SESS-2004',
  '2026-07-16 08:00:00+01','2026-07-16 08:04:00+01', 240,
  'SUCCESS','SCHEDULED', 310, 310, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

(2005, 2, 1, 3, 'STB-SESS-2005',
  '2026-07-15 08:00:00+01','2026-07-15 08:03:00+01', 180,
  'SUCCESS','SCHEDULED', 290, 290, 0, NULL, NULL, 0, 'etl-srv-01', NOW()),

-- ── Interface 3 : Invoice_ClientB (Stambia, PROD) ───────────────────────────
(3001, 3, 1, 3, 'STB-SESS-3001',
  '2026-07-21 07:00:00+01','2026-07-21 07:08:00+01', 480,
  'SUCCESS','SCHEDULED', 45, 45, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

(3002, 3, 1, 3, 'STB-SESS-3002',
  '2026-07-20 07:01:00+01','2026-07-20 07:10:00+01', 540,
  'SUCCESS','SCHEDULED', 52, 52, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

(3003, 3, 1, 3, 'STB-SESS-3003',
  '2026-07-19 07:00:00+01','2026-07-19 07:07:30+01', 450,
  'SUCCESS','SCHEDULED', 38, 38, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

(3004, 3, 1, 3, 'STB-SESS-3004',
  '2026-07-18 07:00:00+01','2026-07-18 07:22:00+01', 1320, -- dépasse SLA 900s
  'FAILURE','SCHEDULED', 20, 0, 0, NULL, 'Database lock timeout', 0, 'etl-srv-02', NOW()),

(3005, 3, 1, 3, 'STB-SESS-3005',
  '2026-07-17 07:00:00+01','2026-07-17 07:06:00+01', 360,
  'SUCCESS','SCHEDULED', 41, 41, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

-- ── Interface 4 : Stock_ClientC (Stambia, PROD) ─────────────────────────────
(4001, 4, 1, 3, 'STB-SESS-4001',
  '2026-07-21 05:00:00+01','2026-07-21 05:15:00+01', 900,
  'SUCCESS','SCHEDULED', 5000, 5000, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

(4002, 4, 1, 3, 'STB-SESS-4002',
  '2026-07-20 05:01:00+01','2026-07-20 05:19:00+01', 1080,
  'SUCCESS','SCHEDULED', 4800, 4800, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

(4003, 4, 1, 3, 'STB-SESS-4003',
  '2026-07-19 05:00:00+01','2026-07-19 05:23:00+01', 1380, -- dépasse SLA 1200s
  'WARNING','SCHEDULED', 5100, 5050, 50, NULL, '50 lignes rejetées', 0, 'etl-srv-02', NOW()),

(4004, 4, 1, 3, 'STB-SESS-4004',
  '2026-07-18 05:00:00+01','2026-07-18 05:14:00+01', 840,
  'SUCCESS','SCHEDULED', 4900, 4900, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

(4005, 4, 1, 3, 'STB-SESS-4005',
  '2026-07-17 05:00:00+01','2026-07-17 05:11:00+01', 660,
  'SUCCESS','SCHEDULED', 4700, 4700, 0, NULL, NULL, 0, 'etl-srv-02', NOW()),

-- ── Interface 5 : dag_orders_clientA (Airflow, PROD) ────────────────────────
(5001, 5, 2, 3, 'dag_orders_clientA__scheduled__2026-07-21T05:00:00+00:00',
  '2026-07-21 06:00:00+01','2026-07-21 06:03:10+01', 190,
  'SUCCESS','SCHEDULED', 320, 320, 0, NULL, NULL, 0, 'worker-01', NOW()),

(5002, 5, 2, 3, 'dag_orders_clientA__scheduled__2026-07-18T05:00:00+00:00',
  '2026-07-18 06:00:30+01','2026-07-18 06:06:00+01', 330,  -- dépasse SLA 300s
  'SUCCESS','SCHEDULED', 295, 295, 0, NULL, NULL, 1, 'worker-01', NOW()),

(5003, 5, 2, 3, 'dag_orders_clientA__scheduled__2026-07-17T05:00:00+00:00',
  '2026-07-17 06:01:00+01','2026-07-17 06:11:00+01', 600,  -- gros dépassement
  'FAILURE','SCHEDULED', 0, 0, 0, 'CONNECTION_ERROR', 'Connection to EDI gateway refused', 2, 'worker-01', NOW()),

(5004, 5, 2, 3, 'dag_orders_clientA__scheduled__2026-07-16T05:00:00+00:00',
  '2026-07-16 06:00:00+01','2026-07-16 06:02:30+01', 150,
  'SUCCESS','SCHEDULED', 280, 280, 0, NULL, NULL, 0, 'worker-01', NOW()),

(5005, 5, 2, 3, 'dag_orders_clientA__scheduled__2026-07-15T05:00:00+00:00',
  '2026-07-15 06:00:00+01','2026-07-15 06:04:00+01', 240,
  'SUCCESS','SCHEDULED', 310, 310, 0, NULL, NULL, 0, 'worker-01', NOW()),

-- ── Interface 6 : dag_invoice_clientB (Airflow, PROD) ───────────────────────
(6001, 6, 2, 3, 'dag_invoice_clientB__scheduled__2026-07-21T06:00:00+00:00',
  '2026-07-21 07:00:00+01','2026-07-21 07:09:00+01', 540,
  'SUCCESS','SCHEDULED', 48, 48, 0, NULL, NULL, 0, 'worker-02', NOW()),

(6002, 6, 2, 3, 'dag_invoice_clientB__scheduled__2026-07-20T06:00:00+00:00',
  '2026-07-20 07:01:00+01','2026-07-20 07:08:00+01', 420,
  'SUCCESS','SCHEDULED', 53, 53, 0, NULL, NULL, 0, 'worker-02', NOW()),

(6003, 6, 2, 3, 'dag_invoice_clientB__scheduled__2026-07-18T06:00:00+00:00',
  '2026-07-18 07:00:00+01','2026-07-18 07:20:00+01', 1200,  -- au SLA
  'WARNING','SCHEDULED', 30, 28, 2, NULL, '2 lignes rejetées', 1, 'worker-02', NOW()),

(6004, 6, 2, 3, 'dag_invoice_clientB__scheduled__2026-07-17T06:00:00+00:00',
  '2026-07-17 07:00:00+01','2026-07-17 07:06:00+01', 360,
  'SUCCESS','SCHEDULED', 40, 40, 0, NULL, NULL, 0, 'worker-02', NOW()),

-- ── Interface 7 : dag_stock_clientC (Airflow, PROD) ─────────────────────────
(7001, 7, 2, 3, 'dag_stock_clientC__scheduled__2026-07-21T04:00:00+00:00',
  '2026-07-21 05:00:00+01','2026-07-21 05:13:00+01', 780,
  'SUCCESS','SCHEDULED', 6200, 6200, 0, NULL, NULL, 0, 'worker-02', NOW()),

(7002, 7, 2, 3, 'dag_stock_clientC__scheduled__2026-07-20T04:00:00+00:00',
  '2026-07-20 05:00:00+01','2026-07-20 05:17:00+01', 1020,
  'SUCCESS','SCHEDULED', 5900, 5900, 0, NULL, NULL, 0, 'worker-02', NOW()),

(7003, 7, 2, 3, 'dag_stock_clientC__scheduled__2026-07-18T04:00:00+00:00',
  '2026-07-18 05:01:00+01','2026-07-18 05:25:00+01', 1440,  -- dépasse SLA 900s
  'FAILURE','SCHEDULED', 2000, 0, 0, 'TIMEOUT', 'DAG timeout after 1440s', 2, 'worker-02', NOW()),

(7004, 7, 2, 3, 'dag_stock_clientC__scheduled__2026-07-17T04:00:00+00:00',
  '2026-07-17 05:00:00+01','2026-07-17 05:11:00+01', 660,
  'SUCCESS','SCHEDULED', 6000, 6000, 0, NULL, NULL, 0, 'worker-02', NOW()),

-- ── Interface 8 : dag_delivery_clientA (Airflow, PROD) ──────────────────────
(8001, 8, 2, 3, 'dag_delivery_clientA__scheduled__2026-07-21T05:00:00+00:00',
  '2026-07-21 06:00:00+01','2026-07-21 06:04:30+01', 270,
  'SUCCESS','SCHEDULED', 1500, 1500, 0, NULL, NULL, 0, 'worker-01', NOW()),

(8002, 8, 2, 3, 'dag_delivery_clientA__scheduled__2026-07-18T05:00:00+00:00',
  '2026-07-18 06:00:00+01','2026-07-18 06:03:00+01', 180,
  'SUCCESS','SCHEDULED', 1400, 1400, 0, NULL, NULL, 0, 'worker-01', NOW()),

(8003, 8, 2, 3, 'dag_delivery_clientA__scheduled__2026-07-17T05:00:00+00:00',
  '2026-07-17 06:01:00+01','2026-07-17 06:07:00+01', 360,
  'SUCCESS','SCHEDULED', 1350, 1350, 0, NULL, NULL, 0, 'worker-01', NOW()),

(8004, 8, 2, 3, 'dag_delivery_clientA__manual__2026-07-15T10:00:00+00:00',
  '2026-07-15 11:00:00+01','2026-07-15 11:02:00+01', 120,
  'SUCCESS','MANUAL', 800, 800, 0, NULL, NULL, 0, 'worker-01', NOW()),

-- ── Interface 9 : dag_reporting_weekly (Airflow, PROD) ──────────────────────
(9001, 9, 2, 3, 'dag_reporting_weekly__scheduled__2026-07-21T07:00:00+00:00',
  '2026-07-21 08:00:00+01','2026-07-21 08:28:00+01', 1680,  -- dépasse SLA 1800s? non, 1680 < 1800 OK
  'SUCCESS','SCHEDULED', 15000, 15000, 0, NULL, NULL, 0, 'worker-02', NOW()),

(9002, 9, 2, 3, 'dag_reporting_weekly__scheduled__2026-07-14T07:00:00+00:00',
  '2026-07-14 08:00:00+01','2026-07-14 08:33:00+01', 1980,  -- dépasse SLA 1800s
  'SUCCESS','SCHEDULED', 14200, 14200, 0, NULL, NULL, 1, 'worker-02', NOW()),

(9003, 9, 2, 3, 'dag_reporting_weekly__scheduled__2026-07-07T07:00:00+00:00',
  '2026-07-07 08:01:00+01','2026-07-07 08:45:00+01', 2640,  -- gros dépassement
  'FAILURE','SCHEDULED', 0, 0, 0, 'MEMORY_ERROR', 'OOM: container memory limit reached', 2, 'worker-02', NOW()),

-- ── Exécutions DEV (env_id=1) — pour tester le filtre env ───────────────────
(901,  5, 2, 1, 'dag_orders_clientA__manual__DEV-2026-07-21',
  '2026-07-21 09:00:00+01','2026-07-21 09:05:00+01', 300,
  'SUCCESS','MANUAL', 10, 10, 0, NULL, NULL, 0, 'worker-dev', NOW()),

(902,  6, 2, 1, 'dag_invoice_clientB__manual__DEV-2026-07-21',
  '2026-07-21 09:10:00+01','2026-07-21 09:14:00+01', 240,
  'FAILURE','MANUAL', 0, 0, 0, 'CONFIG_ERROR', 'Missing env variable SFTP_HOST_DEV', 0, 'worker-dev', NOW()),

-- ── Exécution RUNNING (en cours) — pour Dashboard 1 ─────────────────────────
(9999, 1, 1, 3, 'STB-SESS-9999',
  NOW() - INTERVAL '2 minutes', NULL, NULL,
  'RUNNING','SCHEDULED', NULL, NULL, NULL, NULL, NULL, 0, 'etl-srv-01', NOW());


-- =============================================================================
-- 5. fact_execution_step — quelques steps pour Dashboard 3 (analyse erreurs)
-- =============================================================================
INSERT INTO fact_execution_step
  (step_id, execution_id, parent_step_id, source_step_ref,
   step_name, step_type, start_datetime, end_datetime, status, error_message)
VALUES
  (101, 1003, NULL, 'STB-STEP-101', 'ConnectSFTP',   'ACTION',    '2026-07-17 06:01:00+01','2026-07-17 06:09:00+01','FAILURE', 'Connection timeout to target system'),
  (102, 2003, NULL, 'STB-STEP-102', 'ParseXML',      'ACTION',    '2026-07-17 08:01:00+01','2026-07-17 08:14:00+01','FAILURE', 'XML parse error on line 42'),
  (103, 3004, NULL, 'STB-STEP-103', 'InsertOracle',  'ACTION',    '2026-07-18 07:00:00+01','2026-07-18 07:22:00+01','FAILURE', 'Database lock timeout'),
  (201, 5003, NULL, 'dag_orders_clientA.orders_run_2026-07-17.push_to_edi',
               'push_to_edi','TASK',   '2026-07-17 06:01:00+01','2026-07-17 06:11:00+01','FAILURE', 'Connection to EDI gateway refused'),
  (202, 7003, NULL, 'dag_stock_clientC.stock_run_2026-07-18.load_warehouse',
               'load_warehouse','TASK','2026-07-18 05:01:00+01','2026-07-18 05:25:00+01','FAILURE', 'DAG timeout after 1440s'),
  (203, 9003, NULL, 'dag_reporting.weekly_2026-07-07.generate_report',
               'generate_report','TASK','2026-07-07 08:01:00+01','2026-07-07 08:45:00+01','FAILURE', 'OOM: container memory limit reached');


-- =============================================================================
-- 6. fact_alert
-- =============================================================================
INSERT INTO fact_alert
  (execution_id, alert_type, severity, triggered_at, resolved_at, message)
VALUES

-- Alertes SLA_BREACH (dépassement de durée)
(1002, 'SLA_BREACH', 'MEDIUM', '2026-07-18 06:07:15+01', '2026-07-19 08:00:00+01',
  'Delivery (Client A) : durée 465s > SLA 300s'),
(1003, 'SLA_BREACH', 'HIGH',   '2026-07-17 06:09:00+01', NULL,
  'Delivery (Client A) : durée 480s > SLA 300s — exécution en FAILURE'),
(1006, 'SLA_BREACH', 'MEDIUM', '2026-07-14 06:05:20+01', '2026-07-15 06:00:00+01',
  'Delivery (Client A) : durée 320s > SLA 300s'),
(1009, 'SLA_BREACH', 'HIGH',   '2026-07-09 06:12:00+01', '2026-07-16 09:00:00+01',
  'Delivery (Client A) : durée 720s > SLA 300s — SFTP unreachable'),
(2003, 'SLA_BREACH', 'HIGH',   '2026-07-17 08:14:00+01', NULL,
  'Orders (Client A) : durée 780s > SLA 600s — XML parse error'),
(3004, 'SLA_BREACH', 'HIGH',   '2026-07-18 07:22:00+01', NULL,
  'Invoice ClientB : durée 1320s > SLA 900s — DB lock timeout'),
(4003, 'SLA_BREACH', 'MEDIUM', '2026-07-19 05:23:00+01', '2026-07-20 06:00:00+01',
  'Stock ClientC : durée 1380s > SLA 1200s'),
(5002, 'SLA_BREACH', 'MEDIUM', '2026-07-18 06:06:00+01', '2026-07-19 07:00:00+01',
  'DAG orders Client A : durée 330s > SLA 300s'),
(5003, 'SLA_BREACH', 'HIGH',   '2026-07-17 06:11:00+01', NULL,
  'DAG orders Client A : durée 600s > SLA 300s — connexion refusée'),
(7003, 'SLA_BREACH', 'HIGH',   '2026-07-18 05:25:00+01', NULL,
  'DAG stock ClientC : durée 1440s > SLA 900s — timeout'),
(9002, 'SLA_BREACH', 'MEDIUM', '2026-07-14 08:33:00+01', '2026-07-15 09:00:00+01',
  'DAG reporting : durée 1980s > SLA 1800s'),
(9003, 'SLA_BREACH', 'HIGH',   '2026-07-07 08:45:00+01', '2026-07-14 09:00:00+01',
  'DAG reporting : durée 2640s > SLA 1800s — OOM error'),

-- Alertes ERROR (échecs applicatifs)
(1003, 'ERROR', 'HIGH',   '2026-07-17 06:09:00+01', NULL,
  'Delivery : FAILURE — Connection timeout to target system'),
(2003, 'ERROR', 'HIGH',   '2026-07-17 08:14:00+01', NULL,
  'Orders : FAILURE — XML parse error on line 42'),
(3004, 'ERROR', 'HIGH',   '2026-07-18 07:22:00+01', NULL,
  'Invoice ClientB : FAILURE — Database lock timeout'),
(5003, 'ERROR', 'HIGH',   '2026-07-17 06:11:00+01', NULL,
  'DAG orders : FAILURE — Connection to EDI gateway refused'),
(7003, 'ERROR', 'CRITICAL','2026-07-18 05:25:00+01', NULL,
  'DAG stock ClientC : FAILURE — DAG timeout'),
(9003, 'ERROR', 'CRITICAL','2026-07-07 08:45:00+01', '2026-07-14 09:00:00+01',
  'DAG reporting : FAILURE — OOM error'),

-- Alerte TIMEOUT (exécution bloquée trop longtemps en RUNNING)
(9999, 'TIMEOUT', 'HIGH', NOW(), NULL,
  'Delivery : exécution RUNNING depuis > 5 min sans fin — possible blocage'),

-- Alerte DAG_NOT_TRIGGERED (détectée par scan périodique)
-- On simule une interface qui n'a pas tourné vendredi 11 juillet
(NULL, 'DAG_NOT_TRIGGERED', 'MEDIUM', '2026-07-11 07:30:00+01', '2026-07-12 08:00:00+01',
  'dag_invoice_clientB : attendu à 07:00 (cron 0 7 * * *), pas de dag_run détecté');

-- Note : l'alerte DAG_NOT_TRIGGERED sans execution_id est insérée manuellement
-- pour simuler une détection par le scan @Scheduled (dans la réalité il n'y a
-- pas de fact_execution associée). La FK est nullable.

-- =============================================================================
-- Vérifications rapides
-- =============================================================================
SELECT 'dim_client'        AS table_name, COUNT(*) AS lignes FROM dim_client
UNION ALL
SELECT 'dim_interface',    COUNT(*) FROM dim_interface
UNION ALL
SELECT 'dim_sla',          COUNT(*) FROM dim_sla
UNION ALL
SELECT 'fact_execution',   COUNT(*) FROM fact_execution
UNION ALL
SELECT 'fact_execution_step', COUNT(*) FROM fact_execution_step
UNION ALL
SELECT 'fact_alert',       COUNT(*) FROM fact_alert
ORDER BY table_name;
