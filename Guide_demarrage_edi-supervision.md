# Guide de démarrage local — Projet `edi-supervision`

> **Destinataire :** Encadrant / Reviewer
> **Stack :** Java 21 · Spring Boot · Docker Compose · PostgreSQL · Kafka · Debezium · Grafana
> **OS supposé :** Windows — toutes les commandes sont en **PowerShell**

---

## Prérequis

| Outil | Version minimale | Vérification |
|---|---|---|
| Docker Desktop | 24+ | `docker --version` |
| Docker Compose | v2 | `docker compose version` |
| Java JDK | 21 | `java --version` |
| Git | — | `git --version` |

> **Ressources Docker recommandées :** allouer au moins **4 Go de RAM** → Docker Desktop → Settings → Resources.

---

## Étape 0 — Récupérer le projet

```powershell
git clone https://github.com/othmanekt0/edi-supervision-.git
cd edi-supervision-
```

> **Si tu n'as pas Git installé**, télécharger l'archive ZIP depuis GitHub (bouton **Code → Download ZIP**), extraire, puis ouvrir PowerShell dans le dossier extrait.

---

## Étape 1 — Démarrer la stack Docker

```powershell
docker compose up -d
```

Attendre 2-4 minutes, puis vérifier :

```powershell
docker compose ps
```

Tous les services doivent être `healthy` ou `running`.

> `airflow-init` doit être `Exited (0)` — c'est normal, c'est un container one-shot.

### Si `kafka-connect` reste unhealthy

```powershell
docker compose restart kafka-connect
```

---

## Étape 2 — Initialiser le schéma de supervision (Flyway)

D'abord compiler le projet :

```powershell
.\mvnw.cmd compile
```

Puis lancer la migration :

```powershell
.\mvnw.cmd flyway:migrate `
  "-Dflyway.url=jdbc:postgresql://localhost:5432/supervision" `
  "-Dflyway.user=supervision" `
  "-Dflyway.password=supervision" `
  "-Dflyway.locations=classpath:db/migration"
```

Résultat attendu : `Successfully applied 1 migration to schema "public"`

Vérifier les tables :

```powershell
docker exec -it pg-supervision psql -U supervision -d supervision -c "\dt"
```

---

## Étape 3 — Enregistrer les connecteurs Debezium

### 3a. Créer les publications PostgreSQL

```powershell
docker exec -it pg-stambia psql -U stambia -d stambia -c "CREATE PUBLICATION debezium_pub FOR TABLE log.stb_log_session_sess, log.stb_log_action_act, log.stb_log_delivery_dlv;"

docker exec -it pg-airflow psql -U airflow -d airflow -c "CREATE PUBLICATION debezium_pub FOR TABLE public.dag_run, public.task_instance, public.dag, public.sla_miss;"
```

### 3b. Enregistrer les connecteurs

```powershell
$body = Get-Content -Raw .\debezium-connector.json
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -Body $body

$body = Get-Content -Raw .\airflow-connector.json
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors" -ContentType "application/json" -Body $body
```

### 3c. Vérifier le statut

```powershell
Invoke-RestMethod -Uri "http://localhost:8083/connectors/stambia-connector/status"
Invoke-RestMethod -Uri "http://localhost:8083/connectors/airflow-connector/status"
```

Le champ `state` doit être `RUNNING` pour le connector et toutes les tasks.

### 3d. Vérifier les topics Kafka

Ouvrir http://localhost:8080 — au moins ces topics doivent apparaître :
- `stambia.log.stb_log_session_sess`
- `stambia.log.stb_log_action_act`
- `stambia.log.stb_log_delivery_dlv`
- `airflow.public.dag`

> Les topics `dag_run`, `task_instance`, `sla_miss` apparaîtront lors de l'exécution des DAGs.

---

## Étape 4 — Charger les données de test

> ⚠️ **Charger le seed AVANT de démarrer Spring Boot** (voir explication en fin de guide).

```powershell
$insert = @"
INSERT INTO dim_source (source_id, nom) VALUES (1, 'Stambia'), (2, 'Airflow') ON CONFLICT DO NOTHING;
INSERT INTO dim_environment (env_id, nom) VALUES (1, 'DEV'), (2, 'PREPROD'), (3, 'PROD') ON CONFLICT DO NOTHING;

"@
$seed = Get-Content .\seed_supervision_data.sql -Raw
$insert + $seed | docker exec -i pg-supervision psql -U supervision -d supervision
```

Vérifier :

```powershell
docker exec -it pg-supervision psql -U supervision -d supervision -c "SELECT COUNT(*) FROM fact_execution;"
docker exec -it pg-supervision psql -U supervision -d supervision -c "SELECT COUNT(*) FROM dim_client;"
```

Résultats attendus : 48 exécutions, 4 clients.

---

## Étape 5 — Démarrer l'application Spring Boot

```powershell
.\mvnw.cmd spring-boot:run
```

Attendre `Started EdiSupervisionApplication` dans la console.

Vérifier dans un second terminal :

```powershell
Invoke-RestMethod -Uri "http://localhost:8090/actuator/health"
```

Résultat attendu : `status : UP`

---

## Étape 6 — Vérifier Grafana

1. Ouvrir http://localhost:3000 (login : `admin` / `admin`)
2. **Connections → Data Sources** → `supervision-pg` et `supervision-pg-drill` doivent être actives
3. **Dashboards → EDI Supervision** → 8 dashboards listés
4. Ouvrir `dashboard_1_ops` → les panels doivent afficher des données

> Si les datasources n'apparaissent pas :
> ```powershell
> docker compose restart grafana
> ```

---

## Étape 7 — Valider le flux CDC Airflow en temps réel

Cette étape valide la chaîne complète : **Airflow DAG → pg-airflow → Debezium → Kafka → Spring Boot → pg-supervision**.

1. Ouvrir Airflow UI : http://localhost:8085 (login `airflow` / `airflow`)
2. Déclencher successivement (bouton **Trigger DAG** ▶) les 4 DAGs suivants, en attendant ~30-60s entre chaque :
   - `test_success`
   - `test_failure`
   - `test_retry`
   - `test_long_running`
3. Vérifier que chaque `dag_run` a bien créé une ligne dans `fact_execution` :

```powershell
docker exec -it pg-supervision psql -U supervision -d supervision -c "SELECT di.code, fe.status, fe.retry_count FROM fact_execution fe JOIN dim_interface di ON fe.interface_id = di.interface_id WHERE fe.source_id = 2 AND fe.ingested_at > NOW() - INTERVAL '10 minutes' ORDER BY fe.execution_id DESC;"
```

**Résultat attendu (4 lignes) :**

| code | status | retry_count |
|---|---|---|
| test_long_running | SUCCESS | 1 |
| test_retry | FAILURE | 3 |
| test_failure | FAILURE | 1 |
| test_success | SUCCESS | 1 |

> **Note :** `error_message` peut afficher un faux `DURATION_BREACH` pour les DAGs `test_*` (voir points résiduels ci-dessous) — comportement connu, sans impact sur la supervision réelle des interfaces déjà configurées.

---

## Étape 8 — Tester manuellement l'injection Stambia (CDC en direct)

Cette étape complète l'Étape 7 (qui valide le flux Airflow) en validant la chaîne équivalente côté Stambia : **INSERT SQL dans `pg-stambia` → Debezium → Kafka → Spring Boot → `pg-supervision`**.

Contrairement à Airflow, il n'y a pas d'UI pour "déclencher" une session Stambia : on simule l'ETL en insérant directement des lignes dans les tables sources `log.stb_log_session_sess` et `log.stb_log_action_act` (schéma `log` de `pg-stambia`, capturé par Debezium exactement comme le ferait le vrai moteur Stambia).

> **Rappel de mapping** (voir `StambiaTransformer`) :
> - `sess_ret_code` : `1` = SUCCESS, `-1` = FAILURE, `0` = RUNNING (NULL est aussi traité comme RUNNING)
> - `sess_launch_mode` : `SCHEDULE` → `SCHEDULED`, `WEB_INTERACTIVE` → `MANUAL`, `WEB_SERVICE` → `API`, `ACTION` → `EVENT`
> - `sess_conf` : `prod` / `preprod` / `dev` → `PROD` / `PREPROD` / `DEV`
> - `sess_parent_id` : `NULL` = session principale (alimente `fact_execution`) ; rempli = sous-session (alimente `fact_execution_step`)
> - Dans `stb_log_action_act`, la hiérarchie parent/enfant passe par `act_father_engine_id` (vide = racine), **pas** par `act_parent_iter`
> - Les dates suivent le format Stambia `'yyyy/MM/dd HH:mm:ss.SSS'`, interprétées en `Europe/Paris` puis converties en UTC

### 8a. Test 1 — Session SUCCESS simple

```powershell
$sql = @"
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id, sess_engine_host, dlv_id
) VALUES (
    'test_manual_success_001', 'delivery', 1,
    '2026/07/31 09:00:00.000', '2026/07/31 09:00:04.500', 4500,
    'SCHEDULE', 'prod', NULL, '1.7.5.8', '_2f9MUW_vEfGXkecuqx0sqg'
);
"@
$sql | docker exec -i pg-stambia psql -U stambia -d stambia
```

Résultat attendu dans `fact_execution` : `status = SUCCESS`, `trigger_type = SCHEDULED`, `duration_seconds = 4`.

### 8b. Test 2 — Session FAILURE avec message d'erreur

```powershell
$sql = @"
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id, sess_engine_host, dlv_id
) VALUES (
    'test_manual_failure_001', 'delivery', -1,
    '2026/07/31 09:05:00.000', '2026/07/31 09:05:08.200', 8200,
    'SCHEDULE', 'prod', NULL, '1.7.5.8', '_2f9MUW_vEfGXkecuqx0sqg'
);
UPDATE log.stb_log_session_sess SET sess_ret_msg = 'Connection refused to target FTP'
  WHERE sess_id = 'test_manual_failure_001';
"@
$sql | docker exec -i pg-stambia psql -U stambia -d stambia
```

Résultat attendu : `status = FAILURE`, `error_message = 'Connection refused to target FTP'`. Vérifier aussi qu'une alerte `ERROR` a été créée dans `fact_alert`.

### 8c. Test 3 — Session RUNNING (en cours), pour valider l'affichage temps réel

```powershell
$sql = @"
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id, sess_engine_host, dlv_id
) VALUES (
    'test_manual_running_001', 'delivery_main', NULL,
    '2026/07/31 09:10:00.000', NULL, NULL,
    'WEB_INTERACTIVE', 'prod', NULL, '1.7.5.8', '_xmnkwXBxEfGXkecuqx0sqg'
);
"@
$sql | docker exec -i pg-stambia psql -U stambia -d stambia
```

Résultat attendu : ligne `RUNNING` visible sur `dashboard_1_ops` (temps réel), `trigger_type = MANUAL`. Pour clôturer la session ensuite (simuler la fin du traitement) :

```powershell
$sql = @"
UPDATE log.stb_log_session_sess
SET sess_ret_code = 1, sess_end_date = '2026/07/31 09:10:12.000', sess_duration = 12000
WHERE sess_id = 'test_manual_running_001';
"@
$sql | docker exec -i pg-stambia psql -U stambia -d stambia
```

### 8d. Test 4 — Session avec sous-session (hiérarchie `fact_execution_step`)

```powershell
$sql = @"
-- Session principale
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id, sess_engine_host, dlv_id
) VALUES (
    'test_manual_parent_001', 'shipment_out_notification_main', 1,
    '2026/07/31 09:15:00.000', '2026/07/31 09:15:11.000', 11000,
    'SCHEDULE', 'prod', NULL, '1.7.5.8', '_UsUHABJKEfGaip7_cUsMHQ'
);

-- Sous-session (rattachée via sess_parent_id)
INSERT INTO log.stb_log_session_sess (
    sess_id, sess_name, sess_ret_code,
    sess_begin_date, sess_end_date, sess_duration,
    sess_launch_mode, sess_conf, sess_parent_id, sess_engine_host
) VALUES (
    'test_manual_child_001', 'traitement fichier en cours : test.xlsx', 1,
    '2026/07/31 09:15:02.000', '2026/07/31 09:15:03.000', 1000,
    'ACTION', 'prod', 'test_manual_parent_001', '1.7.5.8'
);
"@
$sql | docker exec -i pg-stambia psql -U stambia -d stambia
```

Résultat attendu : `test_manual_parent_001` dans `fact_execution`, `test_manual_child_001` dans `fact_execution_step` rattachée au bon `execution_id` via `source_step_ref`.

### 8e. Test 5 — Action en échec (analyse d'erreurs, `fact_execution_step` racine)

```powershell
$sql = @"
INSERT INTO log.stb_log_action_act (
    sess_id, sess_iter, act_id, act_iter,
    act_name, act_real_name, act_type,
    act_begin_date, act_end_date,
    act_ret_code, act_ret_msg, act_father_engine_id, act_num
) VALUES (
    'test_manual_parent_001', 1,
    'test_action_fail_001', 1,
    'shipment_out_notification_main/Transport_KAFKA_to_File/L1-log/Write file',
    'Write file',
    'Code',
    '2026/07/31 09:15:05.000', '2026/07/31 09:15:06.500',
    -1, 'Disk quota exceeded', NULL, 8
);
"@
$sql | docker exec -i pg-stambia psql -U stambia -d stambia
```

Résultat attendu : ligne visible sur `dashboard_3_analyse_erreurs` (Analyse des erreurs) avec le message `Disk quota exceeded`.

### 8f. Vérification globale après les tests

```powershell
docker exec -it pg-supervision psql -U supervision -d supervision -c "SELECT source_execution_ref, status, trigger_type, environment, duration_seconds FROM fact_execution fe JOIN dim_environment de ON fe.env_id = de.env_id WHERE source_execution_ref LIKE 'test_manual_%' ORDER BY execution_id;"
```

> ⚠️ Si les lignes n'apparaissent pas après ~10-20s, vérifier le statut du connecteur (`stambia-connector`, Étape 3c) et les logs Spring Boot (`StambiaConsumer`).

### 8g. Nettoyage (optionnel)

```powershell
docker exec -it pg-stambia psql -U stambia -d stambia -c "DELETE FROM log.stb_log_action_act WHERE sess_id LIKE 'test_manual_%'; DELETE FROM log.stb_log_session_sess WHERE sess_id LIKE 'test_manual_%';"
```

> Ceci nettoie uniquement la base source Stambia. Les lignes déjà répliquées dans `pg-supervision` (`fact_execution`, `fact_execution_step`, `fact_alert`) ne sont pas supprimées automatiquement (pas de gestion des `DELETE` côté CDC) — à nettoyer manuellement si besoin via `DELETE ... WHERE source_execution_ref LIKE 'test_manual_%'` côté `pg-supervision`.

---

## Récapitulatif des ports

| Service | URL | Credentials |
|---|---|---|
| Spring Boot | http://localhost:8090/actuator/health | — |
| Grafana | http://localhost:3000 | admin / admin |
| Airflow | http://localhost:8085 | airflow / airflow |
| Kafka UI | http://localhost:8080 | — |
| Kafka Connect API | http://localhost:8083/connectors | — |
| pg-supervision | localhost:5432 | supervision / supervision |
| pg-stambia | localhost:5433 | stambia / stambia |
| pg-airflow | localhost:5434 | airflow / airflow |

---

## Points résiduels connus (non bloquants)

| Point | Cause | Impact |
|---|---|---|
| Faux `DURATION_BREACH` sur DAGs `test_*` | `max_duration_sec` est `NULL` dans `dim_sla` pour les DAGs créés dynamiquement, et `handleDurationBreach` traite `NULL` comme `0` | Alerte de durée déclenchée à tort sur les DAGs de test ; n'affecte pas les interfaces réelles où le SLA est configuré |
| WARN `password authentication failed` intermittent sur port 5434 | Cause non confirmée dans le code — `AirflowDao` utilise correctement `@Qualifier("airflowDataSource")`, donc ce n'est pas un mauvais datasource injecté. Cause probable : contention/latence au démarrage sur le pool de connexions. À investiguer si le WARN persiste | Intermittent, le retry applicatif réussit — aucune perte de données observée |
| WARN `Cron invalide 'null'` dans `AirflowAlertManager` | DAGs `test_*` déclenchés manuellement, sans `schedule` → `dim_sla.cron_attendu = null` | Attendu ; le calcul SLA sur cron ne s'applique pas aux DAGs manuels |
| `UPDATE dag` toujours ignorés | `REPLICA IDENTITY` non configuré sur la table `dag` côté Airflow | Attendu et documenté dans le code ; seuls les `INSERT` (`dag_run`) sont exploités |

---

## Résolution des problèmes courants

### ❌ `kafka-connect` unhealthy
```powershell
docker compose restart kafka-connect
```

### ❌ Flyway — No migrations found
```powershell
.\mvnw.cmd compile
# Puis relancer flyway:migrate
```

### ❌ Flyway — checksum mismatch
```powershell
docker exec -it pg-supervision psql -U supervision -d supervision -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
.\mvnw.cmd flyway:migrate "-Dflyway.url=jdbc:postgresql://localhost:5432/supervision" "-Dflyway.user=supervision" "-Dflyway.password=supervision" "-Dflyway.locations=classpath:db/migration"
```

### ❌ Connecteur Debezium FAILED — publication autocreation disabled
Le connecteur cherche par défaut `dbz_publication`. Vérifier que `publication.name: debezium_pub` est bien présent dans `debezium-connector.json` et `airflow-connector.json`.

### ❌ Connecteur `stambia-connector` FAILED — erreur de parsing JSON/booléen
Vérifier qu'il n'y a pas d'espaces parasites dans les valeurs du fichier `debezium-connector.json`, en particulier :
```json
"key.converter.schemas.enable": "false"
```
(sans espace après `false`).

### ❌ Connecteur Debezium FAILED — slot déjà existant
```powershell
docker exec -it pg-stambia psql -U stambia -d stambia -c "SELECT pg_drop_replication_slot('debezium_stambia_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_stambia_slot');"
docker exec -it pg-airflow psql -U airflow -d airflow -c "SELECT pg_drop_replication_slot('debezium_airflow_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_airflow_slot');"
Invoke-RestMethod -Method Delete -Uri "http://localhost:8083/connectors/stambia-connector"
Invoke-RestMethod -Method Delete -Uri "http://localhost:8083/connectors/airflow-connector"
# Reprendre depuis l'Étape 3b
```

### ❌ Connecteur Debezium FAILED — redémarrer la task
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors/stambia-connector/tasks/0/restart" -ContentType "application/json"
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/connectors/airflow-connector/tasks/0/restart" -ContentType "application/json"
```

### ❌ Seed — erreur `fk_execution_env` ou `execution_id NOT NULL`
Vérifier que `dim_source`/`dim_environment` sont bien insérés avant le seed (Étape 4), et que la colonne `execution_id` de `fact_alert` accepte `NULL` (migration `V1__schema_consolide.sql`).

### ❌ Panels Grafana "datasource not found" / dashboards manquants
Vérifier `grafana/provisioning/datasources/datasources.yml` — les UIDs `bfssjum1dvoqob` et `cfqvza7ykhudce` doivent être présents, puis :
```powershell
docker compose restart grafana
```

---

*Guide validé par test complet depuis zéro — juillet 2026*
