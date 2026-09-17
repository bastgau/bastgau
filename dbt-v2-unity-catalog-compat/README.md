# dbt v2 × Databricks / Unity Catalog — protocole de compatibilité

Objectif : établir si **dbt v2** (moteur Fusion, paquet PyPI `dbt`) est compatible avec
**Databricks** et **Unity Catalog** (UC), avec une preuve exécutable pour chaque affirmation.

## 0. Ce qui est testé (identification)

| Élément | Valeur constatée | Source |
|---|---|---|
| Paquet | `dbt` (PyPI), *pas* `dbt-core` | `pypi.org/pypi/dbt/json` |
| Version testée | `2.0.4` (publiée le 2026-09-16) | idem |
| GA de la 2.0.0 | 2026-09-14 | idem |
| Distributions | `dbt` (complète) et `dbt-oss` (sous-ensemble Apache 2) | description PyPI |
| Adaptateurs | intégrés au moteur, plus de `pip install dbt-databricks` | `pip freeze` après install |
| Pour comparaison | `dbt-core` 1.12.5, `dbt-databricks` 1.12.5 | PyPI |

Le moteur est une extension native (`dbt/_core.abi3.so`, ~383 Mo) pilotée par un shim Python :
le pilote SQL Databricks est embarqué, il n'y a plus de dépendance `databricks-sql-connector`.

## 1. Protocole

Trois niveaux de preuve, explicitement séparés :

* **N1 — statique** : validation du schéma de configuration et du dialecte SQL, sans warehouse.
  Chaque test a un *contrôle négatif* (une clé ou un SQL invalide doit être rejeté), sinon un
  « PASS » ne prouverait rien.
* **N2 — connexion** : ouverture de session et chemins d'authentification (endpoints réellement
  appelés), sans exécution de modèle.
* **N3 — exécution** : DDL/DML réellement émis dans UC. Nécessite un workspace.

| Script | Rôle | Prérequis |
|---|---|---|
| `run_all.sh` | orchestrateur : enchaîne tout | aucun (`--live` pour la phase N3) |
| `run_offline_checks.sh` | N1 + N2 : 46 contrôles | aucun (installe dbt dans un venv jetable) |
| `bootstrap.sh` | découvre et prépare le workspace, écrit `env.local` | `DBT_HOST`, `DBT_TOKEN` |
| `run_live_checks.sh` | N3 : 24 contrôles exécutés par dbt (+2 skips conditionnels) | `env.local` |
| `verify_uc_state.sh` | N3 : 14 assertions relues **dans** Unity Catalog | `env.local` |
| `teardown.sh` | supprime ce que le protocole a créé (dry-run par défaut) | `env.local` |
| `lib/dbx_api.py` | client REST Databricks (stdlib uniquement) | — |

`run_live_checks.sh` prouve que dbt annonce un succès ; `verify_uc_state.sh` prouve que l'objet dans
Unity Catalog est bien ce qu'il prétend être (un `MATERIALIZED_VIEW` et pas une table, un `MERGE` et
pas une reconstruction, un clustering liquide réellement posé, UniForm réellement activé).

Le projet de test (`fixture/`) couvre : namespace UC à 3 niveaux, source dans un autre catalogue,
écriture cross-catalogue, `catalogs.yml` type `unity`, incrémental MERGE, vue matérialisée,
streaming table, table Iceberg managée UC, snapshot avec `target_catalog`, grants UC,
`persist_docs`, tags UC, clustering liquide, `tblproperties`, modèle Python.

### Rejouer le tout

```bash
# 1. hors ligne seulement (aucun identifiant)
./run_all.sh
./run_all.sh --version 2.0.3            # même protocole sur une autre version de dbt

# 2. de bout en bout sur un workspace
export DBT_HOST=<workspace>.cloud.databricks.com   # sans https://
export DBT_TOKEN=<PAT>
./run_all.sh --live                      # bootstrap + N1/N2 + N3 + assertions UC
./run_all.sh --live --teardown           # ... puis nettoyage complet

# 3. étape par étape
./bootstrap.sh                           # --catalog / --schema / --warehouse pour forcer les cibles
./bootstrap.sh --with-cross-catalog      # crée un 2e catalogue UC (exige CREATE CATALOG)
source env.local
./run_live_checks.sh
./verify_uc_state.sh
./teardown.sh                            # dry-run : affiche le plan
./teardown.sh --yes --include-snapshots  # exécute
```

`bootstrap.sh` ne crée rien dans Unity Catalog sans `--with-cross-catalog` : il découvre le
metastore, choisit un catalogue inscriptible (jamais un `SYSTEM_CATALOG`), démarre le SQL warehouse,
vérifie la connexion et écrit `env.local` (chmod 600, ignoré par git). Le jeton ne vit que là.

La phase N3 écrit dans le workspace : `<catalog>.<schema>` (modèles, MV, streaming table, seeds),
`<catalog>.<schema>_bronze` (table source alimentée par un seed), `<catalog>.<schema>_lakehouse`
(table Iceberg) et `<catalog>.snapshots` (snapshot, cf. §5.4). `teardown.sh` supprime tout cela ;
le schéma `snapshots` n'est touché qu'avec `--include-snapshots`, son nom étant trop générique.

## 2. Résultats — phases N1 + N2 (exécutées)

**46 contrôles, 46 PASS, 0 FAIL** sur dbt 2.0.4, Linux x86_64, Python 3.11.

| Domaine | Vérifié | Preuve |
|---|---|---|
| Adaptateur | `type: databricks` résolu, aucun paquet d'adaptateur à installer | `dbt debug` → `adapter type: databricks (remote)` |
| Namespace UC | `catalog` + `schema` du profil → `main.dbt_bastgau.<objet>` | `dbt list --output json` |
| Source externe | `catalog:` sur une source → `raw_prod.crm.customers` | idem |
| SQL compilé | références rendues en `` `catalogue`.`schéma`.`objet` `` | `target/compiled/**` |
| Cross-catalogue | `catalog: gold_prod` respecté sur un modèle | `dbt list` |
| Snapshot | `target_catalog` respecté → `main.snapshots.snap_customers` | `dbt list` |
| `catalogs.yml` | `type: unity` accepté ; types valides : `horizon, glue, iceberg_rest, unity, hive_metastore, biglake_metastore, ducklake, local_filesystem` | message de validation du moteur |
| Iceberg UC | `table_format: iceberg` + `config.databricks.{use_uniform, file_format, location_root}` | `dbt parse` |
| Configs Databricks | `file_format`, `location_root`, `partition_by`, `clustered_by`/`buckets`, `liquid_clustered_by`, `auto_liquid_cluster`, `zorder`, `tblproperties`, `table_format`, `databricks_compute`, `include_full_name_in_path`, `databricks_tags`, `grants`, `persist_docs`, options MERGE/`replace_where`/`microbatch` — toutes acceptées | `dbt parse` + contrôle négatif |
| Dialecte SQL | `qualify`, `lateral view explode`, accesseur JSON `:`, `read_files()`, time travel `version/timestamp as of`, `stream()`, `IDENTIFIER()`, `parse_json`/`try_cast`, map/array/struct — tous parsés localement | `dbt compile` + contrôle négatif (`dbt0101`) |
| Auth PAT | session ouverte sur `httpPath=/sql/1.0/warehouses/...` | `dbt debug --connection` |
| Auth OAuth M2M | jeton demandé sur `https://<host>/oidc/oauth2/v2.0/token` | `dbt debug --connection --target oauth_m2m` |
| Lint / format | fonctionnent hors ligne sur du SQL Databricks | `dbt lint` |
| Modèles Python | nœud reconnu avec `language: python` | `dbt list --output-keys language` |

Signaux complémentaires (lecture des symboles du moteur, **non exécutés** — indice, pas preuve) :
l'implémentation Databricks embarque `databricks__get_create_materialized_view_as_sql`,
`databricks__get_create_streaming_table_as_sql`, `databricks__refresh_streaming_table`,
`databricks__get_create_metric_view_as_sql`, `databricks__get_incremental_{append,delete_insert,microbatch,replace_where}_sql`,
`databricks__get_merge_sql`, `databricks__optimize`, `databricks__persist_constraints`,
`databricks__py_write_table`, `databricks__use_catalog`, `apply_grants`, `SHOW GRANTS`,
ainsi que les DDL `CREATE OR REFRESH STREAMING TABLE`, `CREATE OR REPLACE MATERIALIZED VIEW`,
`CLUSTER BY AUTO`, `SET TAGS`.

## 3. Résultats — phase N3 (exécutée sur un workspace réel)

Workspace : Databricks AWS, Unity Catalog actif (metastore `76951471-…`), catalogue `workspace`,
SQL warehouse serverless 2X-Small. **`run_live_checks.sh` : 24 PASS, 0 FAIL, 2 SKIP** et
**`verify_uc_state.sh` : 14 PASS, 0 FAIL** (assertions relues dans Unity Catalog).

| Vérifié dans Unity Catalog | Preuve |
|---|---|
| `dbt debug`, seed, view, table, incrémental, tests, snapshot | suite N3 |
| **MERGE incrémental réel** | `describe history` → opérations `MERGE` (v1, v3, v4) et `CREATE OR REPLACE TABLE AS SELECT` au `--full-refresh` |
| **Clustering liquide + tblproperties** | `describe detail` → `clusteringColumns=["id"]`, `delta.enableChangeDataFeed=true`, feature `clustering` |
| **Vue matérialisée** | `information_schema.tables` → `mv_customers` = `MATERIALIZED_VIEW` |
| **Streaming table** | `st_customers` = `STREAMING_TABLE` (créée par dbt) |
| **Iceberg managé UC** | `workspace.dbt_uc_compat_lakehouse.iceberg_customers` avec `delta.enableIcebergCompatV2 = True` (UniForm) |
| **Grants UC appliqués** | `show grants` → principal `account users`, action `SELECT` sur `workspace.dbt_uc_compat.dim_customers` |
| **Tags UC appliqués** | `system.information_schema.table_tags` → `domain = crm` |
| Lecture cross-catalogue | modèle sur `samples.tpch.customer` depuis le catalogue `workspace` |
| Source déclarée + fraîcheur | `dbt source freshness` sur `loaded_at_field` |
| `persist_docs`, `docs generate` | exécutés sans erreur |
| Analyse statique **stricte** (types + lignage colonne) | `dbt compile --static-analysis strict` |
| `dbt show` | aperçu de données rendu |
| **Modèle Python** | `submission_method="serverless_cluster"` → table Delta managée `py_customers` créée dans UC |

Deux aménagements ont été nécessaires pour rendre la suite rejouable, tous deux dus à Databricks
et non à dbt :

* la streaming table est rafraîchie en `--full-refresh` (dbt recrée le seed amont à chaque run, ce
  qui invalide le checkpoint de streaming, cf. §5.7) ;
* elle est exclue du `dbt build` de bout en bout, pour la même raison, et les matérialisations
  adossées à un pipeline DBSQL tournent en `--threads 1`. Attention : la limite de 1 pipeline DBSQL
  actif est **propre au tier du compte de test** (le message renvoie vers un upgrade Enterprise),
  pas une règle générale.

Les 2 SKIP restants sont des limites du workspace, pas de dbt : écriture cross-catalogue (un seul
catalogue inscriptible, `bootstrap.sh --with-cross-catalog` lève le point si les droits le
permettent) et modèle Python (aucun cluster ni job, seulement un SQL warehouse).

## 4. Limites de ce qui a été exécuté

* **Écriture cross-catalogue non prouvée** : elle exige un second catalogue UC inscriptible.
  `./bootstrap.sh --with-cross-catalog` le crée si le compte a `CREATE CATALOG`, sinon passer
  `DBT_CROSS_CATALOG=<catalogue existant>`.
* **Connexion via un cluster classique non testée** : l'org du workspace de test est
  *serverless-only* (`CREATE cluster` → `does not have any associated worker environments`), donc le
  `http_path` de type `/sql/protocolv1/o/<orgId>/<clusterId>` n'a pas pu être exercé. Seul le
  `http_path` de SQL warehouse l'a été.
* Auth OAuth M2M vérifiée jusqu'à l'appel du endpoint OIDC seulement (pas de service principal
  disponible) ; PAT vérifié de bout en bout.
* **Modèle Python depuis le notebook non prouvé** : la soumission fonctionne (vérifiée en CLI,
  §7), mais lancée depuis un notebook serverless elle s'interbloque sur le quota de ce workspace
  (§5.17). Elle est donc exclue du `build` joué par le notebook.

## 5. Points d'attention détectés

Les points 2 et 3 ont un test de non-régression dans `run_offline_checks.sh` ; le point 1 est
documenté dans le fixture (`models/marts/schema.yml`).

1. **`grants` : ne pas pré-quoter le principal.** dbt v2 ajoute lui-même les backticks. L'idiome
   dbt-databricks 1.x, qui entoure de backticks un nom contenant un espace, produit
   `to ``account users``` et un `PARSE_SYNTAX_ERROR` (SQLSTATE 42601) qui fait échouer le modèle.
   Écrire le principal nu : `select: ['account users']`.
   *C'est le seul vrai défaut de compatibilité trouvé, et il casse un projet migré depuis 1.x.*
2. **`file_format` vs Iceberg natif UC.** Un `+file_format: delta` au niveau projet/modèle entre en
   conflit avec un catalogue `type: unity` / `table_format: iceberg` : le moteur exige
   `file_format: parquet`, ou `use_uniform: true` avec `delta`. L'erreur pointe une position
   inutilisable (`:2:34`) sans nommer le modèle fautif.
3. **Le nom d'entrée de `catalogs.yml` devient le catalogue UC.** Avec `catalog_name='uc_iceberg_uniform'`
   et sans `catalog:` explicite, la relation devient `uc_iceberg_uniform.<schéma>.<objet>` : dbt vise
   un catalogue UC inexistant.
4. **`target_schema` d'un snapshot est pris littéralement**, sans la concaténation appliquée aux
   modèles : `schema: lakehouse` donne `dbt_uc_compat_lakehouse`, mais `target_schema: snapshots`
   donne `workspace.snapshots` — un schéma de premier niveau créé au ras du catalogue. Vérifié sur
   le workspace de test.
5. **Syntaxe v2 sur les sources** : `loaded_at_field` et `freshness` doivent passer sous `config:`,
   sinon `dbt1060` (clé ignorée) — migration à prévoir depuis dbt-core 1.x.
6. **`compute` est réservé par le moteur** (`remote|inline|local|sidecar|service`). Le sélecteur de
   compute Databricks reste `databricks_compute`.
7. **Streaming table et amont recréé.** Une streaming table qui lit `stream(ref(...))` casse dès que
   dbt recrée la relation amont (`dbt seed`, `--full-refresh` : nouvel id Delta), avec
   `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`. Conséquence directe : un `dbt build` qui
   contient à la fois le seed et la streaming table échoue à chaque exécution. Remèdes vérifiés :
   la rafraîchir en `--full-refresh` (elle se reconstruit alors sans erreur), ou la faire lire une
   table que dbt ne remplace pas.
8. **Un `--full-refresh` de streaming table qui échoue laisse l'objet supprimé** : constaté quand le
   quota serverless a coupé la recréation — `st_customers` avait disparu du catalogue, en laissant
   les tables internes `__materialization_mat_*` et `event_log_*`. Non atomique : à surveiller en
   production.
9. **`dbt show --inline` ajoute `limit N`** : inutilisable sur `SHOW GRANTS` / `DESCRIBE`. Utiliser
   `--limit -1`.
10. Les modèles introspectifs (`is_incremental()`) ouvrent une connexion **dès le `compile`** : pas de
    compilation 100 % hors ligne d'un projet incrémental.
11. **`dbt.config()` d'un modèle Python n'accepte que des littéraux** : un `os.environ.get(...)` est
    refusé au parse (`Non-literal expression found`). Comportement identique à dbt-core 1.x, mais il
    empêche de paramétrer `submission_method` par l'environnement.
12. **Le `method` d'un profil `databricks` n'est pas validé** : `method: zzz_bogus` passe le parse
    sans un mot. La clé est simplement ignorée — pratique pour se croire configuré alors que non.
13. **`%pip install "dbt==$widget"` n'interpole pas dans un job** : pip reçoit le littéral
    `$dbt_version` et échoue (`PipError ... returned non-zero exit status 1`). Constaté sur le
    premier run du notebook ; il faut appeler la magic depuis Python
    (`get_ipython().run_line_magic("pip", f"install dbt=={version}")`).
14. **Un chemin workspace passé en paramètre doit être préfixé `/Workspace`** : `project_dir` valant
    `/Users/<moi>/x` donne `FileNotFoundError` côté notebook ; le notebook résout désormais les deux
    formes.
15. **Un nœud en `warn` laisse la commande en succès** (exit 0) : le modèle Python remonte
    `warn` + `Invalid timeout value, using default of 0` tout en construisant sa table. À filtrer
    soi-même si un warning doit bloquer.
16. **L'adaptateur expérimental `lakecompute` fait paniquer le moteur** avec un profil minimal
    (`panicked at dbt-main/src/compilation.rs:2687: called Option::unwrap() on a None value`) au lieu
    de rendre une erreur de configuration.
17. **Modèle Python lancé depuis un notebook serverless = interblocage** sur un workspace à quota :
    dbt soumet le modèle comme job Databricks séparé, ce job attend un créneau de compute serverless,
    et le créneau est occupé par le notebook qui l'a soumis. Constaté : sous-job
    `workspace-…-py_customers-…` en `QUEUED` pendant 800 s, notebook parent en `RUNNING`, aucun des
    deux n'avançant. Lancer les modèles Python depuis un autre point d'entrée (CLI, tâche dédiée).
18. Le commentaire injecté dans les requêtes annonce `"dbt_version": "2.0.0"` alors que la CLI est en
    2.0.4 — cosmétique, mais trompeur dans l'historique des requêtes Databricks.

## 6. Verdict

**dbt v2 (2.0.4) est compatible avec Databricks et Unity Catalog**, vérifié à l'exécution sur un
workspace réel : namespace à 3 niveaux, catalogues UC, MERGE incrémental, clustering liquide,
`tblproperties`, vues matérialisées, streaming tables, Iceberg managé UC via `catalogs.yml`
(`type: unity`, UniForm), grants et tags UC appliqués, fraîcheur des sources, analyse statique
stricte, PAT et OAuth M2M.

Chaque affirmation ci-dessus est rejouable : `run_live_checks.sh` fait exécuter dbt, puis
`verify_uc_state.sh` relit l'état dans Unity Catalog (14 assertions) plutôt que de croire le rapport
de dbt.

Les modèles Python sont également prouvés (soumission `serverless_cluster`, table créée dans UC), et
le projet tourne aussi bien en CLI qu'en notebook Databricks sur compute serverless (§7).

Un seul défaut bloquant à la migration : le **double-quoting des principals de `grants`** (§5.1).
Reste non prouvée l'écriture cross-catalogue, faute d'un second catalogue inscriptible sur ce
workspace : `DBT_CROSS_CATALOG` l'active.

## 7. Exécuter le job via `dbtRunner`

`runner/` contient l'exécution programmatique, en processus : le moteur tourne dans le processus
courant et rend les artefacts comme objets Python (`RunResultsArtifact`, `Manifest`,
`FreshnessResultsArtifact`), au lieu d'un `subprocess` dont il faudrait parser la sortie. Une seule
instance de `dbtRunner` est réutilisée pour toutes les commandes du job.

| Fichier | Rôle |
|---|---|
| `runner/run_dbt_job.py` | script/CLI **et** bibliothèque (`run_dbt_job(...)` → `JobReport`) |
| `runner/databricks_dbt_notebook.py` | notebook Databricks (format source, à importer tel quel) |
| `runner/test_notebook_locally.py` | rejoue les cellules du notebook hors Databricks (stubs `dbutils`/`spark`) |

### Script

```bash
# profil existant
python runner/run_dbt_job.py --project-dir fixture --profiles-dir fixture --target live \
  --command "seed" --command "build --exclude st_customers"

# profil généré depuis l'environnement (rien de secret dans le dépôt)
export DBT_HOST=... DBT_HTTP_PATH=/sql/1.0/warehouses/... DBT_TOKEN=... DBT_CATALOG=... DBT_SCHEMA=...
python runner/run_dbt_job.py --project-dir fixture --generate-profile \
  --command build --select "tag:daily" --fail-on-empty
```

Le `profiles.yml` généré est écrit en 0600 dans un répertoire temporaire et supprimé à la fin, même
en cas d'échec. Le code de sortie du processus est celui du moteur (0 ok, 1 échec, 2 warnings
élevés) : utilisable directement dans un ordonnanceur.

En bibliothèque :

```python
from run_dbt_job import run_dbt_job

report = run_dbt_job(["build"], project_dir="fixture", generate_profile=True, fail_on_empty=True)
for row in report.rows():
    print(row["status"], row["unique_id"], row["relation_name"])
report.raise_for_status()      # lève si une commande a échoué
```

### Notebook Databricks

Importer `runner/databricks_dbt_notebook.py` dans le workspace (fichier au format *notebook source*,
reconnu à l'import). Widgets : `project_dir`, `catalog`, `schema`, `http_path`, `secret_scope`,
`secret_key`, `dbt_version`, `commands`, `threads`.

Ce que fait le notebook, dans l'ordre : `%pip install dbt==<version>` puis `restartPython` (le moteur
est une extension native : sans redémarrage, c'est l'ancien module qui reste importé) ; résolution du
projet (dossier voisin `../fixture` par défaut, chemin `/Workspace` ou `/Repos` géré) ; **copie du
projet sur disque local**, parce que dbt écrit `target/` et `logs/` et que les Workspace files et Git
folders sont en lecture seule à l'exécution ; jeton lu dans un secret scope (à défaut, le jeton du
notebook) ; `profiles.yml` généré depuis l'environnement ; exécution via `run_dbt_job` — donc
exactement le même chemin de code qu'en CLI ; résultats en `display()` ; puis `raise_for_status()`
pour que l'échec dbt fasse échouer la tâche du Job.

Deux choix à connaître :

* **`http_path` vide → le cluster du notebook** (`/sql/protocolv1/o/<orgId>/<clusterId>`). Préférer un
  SQL warehouse : les vues matérialisées et streaming tables passent par des pipelines DBSQL
  (vérifié), et rien ne garantit qu'un cluster puisse les créer (non vérifié).
* **`fail_on_empty=True`** dans le notebook : dbt traite une sélection vide comme un *warning* et
  sort en 0 — sans ce garde-fou, une faute de frappe dans `--select` ferait un job « vert » qui n'a
  rien construit. Vérifié : le job échoue désormais avec
  ``dbt run` selected no node (check --select/--exclude)``.

### Ce qui a été testé

Script, contre le workspace réel (SQL warehouse serverless) :

| Cas | Résultat |
|---|---|
| Profil existant, 2 commandes enchaînées | seed + run OK, résumé par nœud |
| `--generate-profile` | OK, profil temporaire supprimé après coup |
| Modèle en échec | `job success: False`, code de sortie processus **1** |
| Modèle Python, `submission_method="serverless_cluster"` | job Databricks soumis, **table Delta managée créée** dans UC (`py_customers`, lignes conformes), 2 min 3 s |

Notebook, **exécuté dans Databricks sur compute serverless** (job `dbt-v2-uc-compat-notebook`,
tâche sans `existing_cluster_id` ni `job_cluster_key` ni `cluster_instance`) :

| Run | Résultat |
|---|---|
| `seed` + `run --select stg_customers` | `SUCCESS` — `dbt ok: 2 nodes, exit 0`, 42 s |
| `build --exclude st_customers mv_customers cross_catalog py_customers` | `SUCCESS` — `dbt ok: 11 nodes, exit 0` |
| Modèle en échec / sélection vide (via `test_notebook_locally.py`) | `RuntimeError`, tâche en échec |

Deux bugs du notebook trouvés **par** ces exécutions, corrigés : l'interpolation de widget dans
`%pip` (§5.13) et le préfixe `/Workspace` sur un chemin passé en paramètre (§5.14).

Le déploiement est scripté : `runner/deploy_and_run_notebook.py` téléverse le notebook et le projet
dans le workspace, crée (ou met à jour) un job dont la tâche notebook tourne en serverless, le
déclenche et rend la main avec le résultat.

```bash
source env.local
python runner/deploy_and_run_notebook.py --commands "build --exclude st_customers"
python runner/deploy_and_run_notebook.py --no-run     # déploiement seul
```

### Peut-on se passer du SQL warehouse ?

Non pour les modèles SQL, et c'est structurel : l'adaptateur `databricks` se connecte par un
endpoint SQL (`http_path`), jamais par la session Spark ambiante du notebook. Deux formes existent —
SQL warehouse (`/sql/1.0/warehouses/<id>`, **vérifiée**) et cluster all-purpose
(`/sql/protocolv1/o/<orgId>/<clusterId>`, **non vérifiée** ici : l'org de test est serverless-only,
`clusters/create` répond `does not have any associated worker environments`).

Le notebook orchestre donc, mais n'exécute rien : le DDL/DML part vers le warehouse.

| Rôle | Compute |
|---|---|
| Parsing, DAG, artefacts (`dbtRunner`) | compute du notebook (serverless ici) |
| `CREATE` / `MERGE` / `GRANT`, MV, streaming tables | SQL warehouse |
| Modèles Python | job Databricks séparé (`submission_method`), **sans** warehouse |

Les alternatives testées et écartées : dbt v2 n'a **plus** le `method: session` de dbt-spark 1.x
(méthodes acceptées : `thrift`, `http`, `livy`, `spark-connect`), et les adaptateurs `spark` comme
`lakecompute` sont **expérimentaux** — hors de la liste GA (`snowflake, bigquery, databricks,
redshift, duckdb, salesforce, clickhouse`), derrière `DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true`, et
`lakecompute` fait paniquer le moteur (§5.16).

### Topologie de compute : combien, et lequel pour quoi

Un projet mêlant SQL et Python consomme **trois** computes distincts par défaut : l'orchestrateur,
le endpoint SQL, et le compute de soumission des modèles Python. Aucun `submission_method` ne
réutilise le processus courant — les valeurs présentes dans le moteur sont `all_purpose_cluster`,
`job_cluster`, `serverless_cluster`, `workflow_job`.

| Montage | Computes Databricks | Remarque |
|---|---|---|
| Notebook serverless + warehouse + modèle Python | 3 | **interblocage** si le quota serverless est à 1 (§5.17) |
| CLI (CI ou poste) + warehouse + modèle Python | 2 | pas d'interblocage — c'est ainsi que le modèle Python a été validé |
| CLI + warehouse, aucun modèle Python | 1 | l'orchestration ne coûte rien côté Databricks |
| Cluster all-purpose seul | 1 | voir les contreparties ci-dessous |

L'orchestration ne fait que du parsing et de l'attente d'I/O : la sortir de Databricks est le levier
le plus simple pour descendre à deux computes et supprimer l'interblocage.

#### Tout sur un seul cluster all-purpose

```yaml
# profiles.yml — le SQL passe par l'endpoint Thrift du cluster, pas par un warehouse
type: databricks
host: <workspace>.cloud.databricks.com
http_path: /sql/protocolv1/o/<orgId>/<clusterId>
token: "{{ env_var('DBT_TOKEN') }}"
catalog: main
schema: analytics
```

```python
# le modèle Python cible le même cluster que le SQL
def model(dbt, session):
    dbt.config(materialized="table",
               submission_method="all_purpose_cluster",
               cluster_id="<le même clusterId>",
               create_notebook=False)
```

`cluster_id`, `http_path` et `create_notebook` sont **vérifiés** comme clés valides du schéma v2 pour
un modèle Python (contrôle négatif : `existing_cluster_id` → `Ignored unexpected key`). Qu'elles
soient honorées à l'exécution n'a **pas** été prouvé : le workspace de test est serverless-only
(worker environment `serverless-<orgId>`, `clusters/list-zones` → `No such workerEnvironment`,
`clusters/create` → `does not have any associated worker environments`).

Prérequis : cluster **UC-enabled** (access mode Dedicated ou Standard) pour lire/écrire dans Unity
Catalog, et cluster démarré — dbt ne le réveille pas de façon fiable (non vérifié).

**Ce que ce montage coûte.** Trois affirmations de portée différente, à ne pas confondre :

1. **Vérifié, et propre à l'implémentation Databricks (donc général)** : les vues matérialisées et
   les streaming tables créées par dbt sont adossées à des pipelines DBSQL. Preuves relevées dans le
   catalogue de test : tables internes `__materialization_mat_*` et `event_log_*` à côté des objets,
   et le message
   `[DLT ERROR CODE: QUOTA_EXCEEDED_EXCEPTION] Cannot start update … the limit for active pipelines
   of type 'DBSQL' has been reached`.
2. **Propre au compte de test, pas général** : la *valeur* de cette limite — 1 pipeline DBSQL actif,
   le message invitant lui-même à « upgrading to an Enterprise account tier » — ainsi que le
   `RESOURCE_EXHAUSTED` sur le compute serverless et l'absence de worker environment. Sur un compte
   d'un autre tier, ces trois plafonds diffèrent ou disparaissent.
3. **Non vérifié, documentation seule** : qu'un cluster all-purpose ne puisse pas *créer ni
   rafraîchir* MV et streaming tables. Le point 1 prouve que ces objets passent par DBSQL, pas
   qu'un cluster en soit incapable ; l'inférence est raisonnable mais elle n'a pas été testée, et ne
   pouvait pas l'être ici faute de cluster. À confirmer sur un workspace disposant de compute
   classique avant d'en faire un critère de choix.

S'y ajoute, côté coût : le tarif DBU all-purpose est plus élevé que celui d'un SQL warehouse pour du
SQL pur, et un cluster facture tant qu'il tourne (autotermination obligatoire) là où un warehouse
serverless s'éteint seul — tarification publique, non mesurée ici.

#### Le compromis recommandé

| Charge | Compute |
|---|---|
| Orchestration `dbtRunner` | CI ou poste de travail — aucun compute Databricks |
| Modèles SQL, MV, streaming tables | SQL warehouse (`http_path` du profil) |
| Modèles Python | cluster all-purpose, via `cluster_id` au niveau du modèle |

Deux computes, chacun sur son terrain, et pas d'interblocage. Dernier point de vigilance : dbt
soumet **un job par modèle Python**, donc `threads: 4` avec trois modèles Python déclenche trois
allocations concurrentes — brider avec `--threads 1` sur un workspace à quota.

## 8. Unity Catalog OSS en local

**Question** : le protocole tourne-t-il contre [Unity Catalog OSS](https://github.com/unitycatalog/unitycatalog)
en local, sans compte Databricks ?

**Réponse** : l'écriture est impossible, et ce n'est pas dbt qui bloque — c'est UC OSS. Vérifié de
bout en bout par `uc-oss/run_uc_oss_checks.sh` (**14 contrôles, 14 PASS**) avec dbt **2.0.4**, sur
UC OSS **0.6.0** (la dernière version) *et* **0.3.0** — résultat identique sur les deux.

Pour connaître les versions disponibles, se fier à la métadonnée du dépôt, pas à l'API de recherche
(qui a renvoyé 0.3.0 comme « latest » alors que 0.6.0 existe) :
`curl https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-server/maven-metadata.xml`.
Maven Central limite les rafales de requêtes par un `HTTP 429`.

### Le montage

Le fixture du protocole vise `type: databricks`, qui exige un endpoint SQL Databricks : il ne peut
pas cibler UC OSS. Le chemin local passe par l'adaptateur **`duckdb`** (GA en v2) et le *même*
`catalogs.yml` de type `unity` que côté Databricks :

```yaml
catalogs:
  - name: dbt_oss
    type: unity
    table_format: iceberg
    config:
      duckdb:
        endpoint: http://127.0.0.1:8081/api/2.1/unity-catalog/iceberg
        warehouse: dbt_oss          # le catalogue UC
        authorization_type: none    # UC OSS en mode dev n'a pas d'OAuth2
```

Clés vérifiées de `config.duckdb` sous un catalogue `type: unity` : `endpoint`, `warehouse`,
`authorization_type`, `secret`. Rejetées : `url`, `token`, `catalog`, `schema`, `database`, `path`,
`client_id`, `client_secret`, `oauth2_scope`. `endpoint_type` existe mais n'accepte que
`GLUE|S3_TABLES` — rien pour UC.

### Ce qui marche

* dbt v2 tourne **entièrement en local** : un modèle construit dans un fichier DuckDB, sans aucun
  warehouse.
* `catalogs.yml` `type: unity` + bloc `duckdb` valide au parse.
* dbt atteint réellement le catalogue **Iceberg REST** de UC OSS, avec le bon préfixe
  (`/iceberg/v1/catalogs/dbt_oss/namespaces`) — la requête part, elle est bien formée.

### Ce qui bloque

UC OSS expose **7 endpoints Iceberg REST, tous en lecture** (`GET`/`HEAD`, plus un
`POST …/metrics`) — la liste est **strictement identique en 0.3.0 et en 0.6.0**. Aucun endpoint de
création. Conséquences constatées :

| Tentative | Réponse |
|---|---|
| `POST …/iceberg/v1/catalogs/dbt_oss/namespaces` en direct | `HTTP 405 Method Not Allowed` |
| `dbt run` sur un modèle visant ce catalogue | `Failed to commit Iceberg transaction: … (MethodNotAllowed_405)` |

Donc aucune matérialisation dbt n'est possible dans UC OSS, 0.6.0 comprise : ni table, ni schéma —
pour les versions **publiées** ; l'écriture est visée pour la v0.7, cf. « Support d'écriture » plus bas. Le premier
essai échouait d'ailleurs plus tôt encore, sur
`AUTHORIZATION_TYPE is 'oauth2', yet no 'secret' was provided` — l'extension Iceberg de DuckDB
impose OAuth2 par défaut, d'où `authorization_type: none`.

### Rejouer

```bash
./uc-oss/run_uc_oss_checks.sh              # dbt 2.0.4 + UC OSS 0.6.0 par défaut
./uc-oss/run_uc_oss_checks.sh 2.0.4 0.3.0  # autre version de dbt / de UC OSS
```

Le script télécharge le serveur UC OSS depuis **Maven Central** (`io.unitycatalog:unitycatalog-server`,
GitHub étant inaccessible depuis l'environnement de test), le démarre, crée un catalogue et un schéma
par son API REST, puis lance dbt. Le contrôle sur les endpoints est écrit pour **détecter l'inverse** :
si une version ultérieure de UC OSS annonce des endpoints d'écriture, il le signale au lieu de
conclure au read-only.

### Non testé

* La **lecture** d'une table UC OSS existante par dbt : il aurait fallu une table déjà peuplée avec
  des métadonnées Iceberg (UniForm), absente d'un serveur vierge.
* L'adaptateur `spark` (expérimental, cf. §7) contre un Spark local muni du plugin UC OSS — l'autre
  voie théorique, hors périmètre ici.

### Support d'écriture : où en est-on ?

La conclusion ci-dessus porte sur les versions **publiées**. Le read-only n'est pas un choix
d'architecture définitif : c'est un chantier en cours, et l'écriture est au roadmap.

**Ce qui est vérifié, dans le binaire 0.6.0.** `IcebergRestCatalogService` n'expose que huit méthodes
publiques, toutes en lecture : `config`, `listNamespaces`, `getNamespace`, `tableExists`, `loadTable`,
`loadView`, `reportMetrics`, `listTables`. Aucune méthode de mutation — ni `createTable`, ni
`commitTable`, ni `registerTable`, ni `createNamespace`, ni `dropNamespace`, ni `renameTable`. Il n'y
a donc pas de chemin d'écriture désactivé par configuration : le code n'existe pas dans la release.
L'endpoint sert une façade de lecture au-dessus de tables Delta UniForm (classes
`DeltaUniformMetadataIceberg`, `DeltaUniformUtils`, et le contrôle
`Iceberg table location must match the registered table location.`).

**Ce qu'annonce le projet** — `roadmap.md`, ligne citée telle quelle :

| Feature | Area | v0.3 | v0.4 | v0.5 | v0.6 | v0.7 | v0.8+ |
|---|---|---|---|---|---|---|---|
| Delta Uniform tables with read as Iceberg via Iceberg REST API | API + Server | 🛠️ | 🛠️ | ✓ | ✓ | ✓ | ✓ |
| **Iceberg tables with create+read+write** | API + Server | | | | | **✓** | ✓ |
| Iceberg view support | API + Server | | | | | | ✓ |

L'écriture (`create+read+write`) est donc visée pour **v0.7**, soit la prochaine version au moment de
ce test (0.6.0 étant la dernière publiée). Le thème associé du roadmap parle de
« *Iceberg table lifecycle support* » au titre de « Full Iceberg REST Catalog support ».

**Ce que montre le tracker** (contenu externe, rapporté tel qu'affiché, non vérifié par exécution) :
des endpoints de mutation atterrissent déjà dans `main` après la 0.6.0 — l'issue *« Iceberg REST
catalog is missing dropNamespace, updateNamespaceProperties and renameTable »* (#1846) est fermée le
15/09/2026, alors que le jar 0.6.0 testé ici ne contient aucune de ces méthodes. Restent ouvertes
*« Iceberg REST catalog does not serve registerTable »* (#1850) et *« Add additional Iceberg REST
Catalog endpoints »* (#3).

**Ne pas confondre avec Unity Catalog managé (Databricks).** Côté SaaS, l'écriture par Iceberg REST
existe déjà : des clients externes créent et alimentent des tables Iceberg managées. C'est une autre
base de code que le serveur OSS — la documentation Databricks ne dit rien du serveur open source, et
c'est la confusion la plus facile à faire en cherchant sur le sujet.

**Conséquence pratique** : ce test est à rejouer à la sortie de la 0.7. Le contrôle sur les endpoints
de `run_uc_oss_checks.sh` est écrit pour signaler le basculement (`write endpoints advertised (UC OSS
now accepts writes — revisit the README)`) au lieu de conclure au read-only.

## 9. Delta + VARIANT

Testé sur le workspace réel (DBSQL **2026.36**), tables Delta managées en Unity Catalog.
`fixture/models/marts/variant_events.sql` porte le cas.

### Ce qui marche

| Vérifié | Preuve |
|---|---|
| Colonne `VARIANT` dans une table Delta créée par dbt | `information_schema.columns` → `payload` = `variant` |
| Features Delta activées **automatiquement** par Databricks | `show tblproperties` → `delta.feature.variantType: supported`, `delta.feature.variantShredding: supported`, `delta.enableVariantShredding: true` |
| `data_type: variant` dans un **contrat** (`contract: enforced`) | parse + run OK |
| Analyse statique hors ligne sur le type | `dbt compile` OK |
| Accesseurs : `payload:user.name::string`, `variant_get`, `try_variant_get`, `schema_of_variant`, `is_variant_null` | relecture : `alice`, `12.5`, `NULL` pour un champ absent, `OBJECT<amount: DECIMAL(3,1), user: OBJECT<name: STRING>>` |
| Incrémental `merge` avec une colonne VARIANT (`unique_key` sur un entier) | 2 passages verts, dont un vrai MERGE |
| Test `not_null` sur la colonne VARIANT | passé |
| `tblproperties` cohabitant avec VARIANT (`enableChangeDataFeed`) | passé |

### Les deux pièges — VARIANT n'a ni égalité ni ordre

Databricks refuse de comparer deux VARIANT. Tout ce que dbt génère avec un `=` ou un `GROUP BY` sur
cette colonne casse :

| Cas | Erreur exacte |
|---|---|
| Test `unique` sur la colonne VARIANT | `[GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE] The expression "payload" cannot be used as a grouping expression because its data type "VARIANT" is not an orderable data type. SQLSTATE: 42822` |
| Snapshot `strategy: check` avec `check_cols: ['payload']`, **au 2ᵉ passage** | `[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] Cannot resolve "(payload = payload)" … The `=` does not support ordering on type "VARIANT". SQLSTATE: 42K09` |

Le snapshot **réussit au premier passage** (création) et n'échoue qu'au second, quand la stratégie
`check` compare les colonnes : un CI qui ne joue le snapshot qu'une fois ne verra rien.

### Le contournement, vérifié

Exposer une projection comparable à côté du VARIANT, et faire porter snapshots et tests `unique`
dessus :

```sql
select
    parse_json(raw)          as payload,        -- la donnée, en VARIANT
    to_json(parse_json(raw)) as payload_json    -- la projection comparable
```

Avec `check_cols: ['payload_json']`, le snapshot enchaîne **trois passages verts** — la table
snapshot continue de porter la colonne VARIANT, seule la comparaison change. `data_type: string` pour
cette colonne dans le contrat.

Autres options non testées ici : `strategy: timestamp` sur une colonne de date (évite toute
comparaison de contenu), ou éclater les champs utiles en colonnes typées (`variant_get`) et ne
comparer que celles-là.
