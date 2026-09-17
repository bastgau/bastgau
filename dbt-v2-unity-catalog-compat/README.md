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

| Script | Niveaux | Prérequis |
|---|---|---|
| `run_offline_checks.sh` | N1 + N2 | aucun (installe dbt dans un venv jetable) |
| `run_live_checks.sh` | N3 | `DBT_HOST`, `DBT_HTTP_PATH`, `DBT_TOKEN` |

Le projet de test (`fixture/`) couvre : namespace UC à 3 niveaux, source dans un autre catalogue,
écriture cross-catalogue, `catalogs.yml` type `unity`, incrémental MERGE, vue matérialisée,
streaming table, table Iceberg managée UC, snapshot avec `target_catalog`, grants UC,
`persist_docs`, tags UC, clustering liquide, `tblproperties`.

```bash
./run_offline_checks.sh            # N1 + N2, ~1 min
./run_offline_checks.sh 2.0.3      # même protocole sur une autre version

export DBT_HOST=<workspace>.cloud.databricks.com
export DBT_HTTP_PATH=/sql/1.0/warehouses/<id>     # GET /api/2.0/sql/warehouses
export DBT_TOKEN=<PAT>
export DBT_CATALOG=workspace DBT_SCHEMA=dbt_uc_compat      # schémas jetables
export DBT_SOURCE_CATALOG=workspace DBT_SOURCE_SCHEMA=dbt_uc_compat_bronze \
       DBT_SOURCE_TABLE=seed_bronze_customers DBT_SOURCE_TS_COLUMN=_ingested_at
export DBT_EXT_CATALOG=samples DBT_EXT_SCHEMA=tpch         # lecture cross-catalogue
export DBT_CROSS_CATALOG=<2e catalogue>                    # optionnel, écriture cross-catalogue
./run_live_checks.sh
```

La phase N3 écrit dans le workspace : `<catalog>.<schema>` (modèles, MV, streaming table, seeds),
`<catalog>.<schema>_bronze` (table source), `<catalog>.<schema>_lakehouse` (table Iceberg) et
`<catalog>.snapshots` (snapshot, cf. §5.4). Elle ne supprime rien : nettoyer avec
`DROP SCHEMA ... CASCADE` après coup.

## 2. Résultats — phases N1 + N2 (exécutées)

**45 contrôles, 45 PASS, 0 FAIL** sur dbt 2.0.4, Linux x86_64, Python 3.11.

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
SQL warehouse serverless 2X-Small. **22 contrôles PASS, 2 FAIL, 1 SKIP.**

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

Les 2 FAIL et le SKIP ne sont **pas** des défauts de compatibilité dbt v2 :

* `streaming_table` en ré-exécution → `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` : `dbt seed`
  recrée la table amont (nouvel id Delta), ce qui invalide le checkpoint du streaming. Voir §5.
* `dbt build` de bout en bout → quotas du workspace : `QUOTA_EXCEEDED_EXCEPTION` (1 pipeline DBSQL
  actif maximum hors Enterprise) puis `RESOURCE_EXHAUSTED` sur le compute serverless. Le script
  sérialise désormais ces étapes (`--threads 1`), mais le quota reste une limite du workspace.
* écriture cross-catalogue → SKIP : un seul catalogue inscriptible sur ce workspace.

## 4. Limites de ce qui a été exécuté

* **Écriture cross-catalogue non prouvée** : elle exige un second catalogue UC inscriptible.
  Relancer avec `DBT_CROSS_CATALOG=<catalogue>` pour lever ce point.
* **Rafraîchissement d'une streaming table non prouvé** : bloqué par le quota serverless de ce
  workspace (la *création* l'est).
* **Modèles Python non testés en exécution** : ils exigent un cluster all-purpose ou un job,
  absents de ce workspace (seul un SQL warehouse serverless existe). Le nœud est bien reconnu
  (`language: python`) et le moteur embarque `databricks__py_write_table`, mais rien n'a été exécuté.
* Auth OAuth M2M vérifiée jusqu'à l'appel du endpoint OIDC seulement (pas de service principal
  disponible) ; PAT vérifié de bout en bout.


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
   `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`. La faire lire une table que dbt ne remplace pas,
   ou la rafraîchir en `--full-refresh`.
8. **Un `--full-refresh` de streaming table qui échoue laisse l'objet supprimé** : la relation
   `st_customers` a disparu du catalogue, en laissant derrière les tables internes
   `__materialization_mat_*` et `event_log_*`. Non atomique : à surveiller en production.
9. **`dbt show --inline` ajoute `limit N`** : inutilisable sur `SHOW GRANTS` / `DESCRIBE`. Utiliser
   `--limit -1`.
10. Les modèles introspectifs (`is_incremental()`) ouvrent une connexion **dès le `compile`** : pas de
    compilation 100 % hors ligne d'un projet incrémental.
11. Le commentaire injecté dans les requêtes annonce `"dbt_version": "2.0.0"` alors que la CLI est en
    2.0.4 — cosmétique, mais trompeur dans l'historique des requêtes Databricks.

## 6. Verdict

**dbt v2 (2.0.4) est compatible avec Databricks et Unity Catalog**, vérifié à l'exécution sur un
workspace réel : namespace à 3 niveaux, catalogues UC, MERGE incrémental, clustering liquide,
`tblproperties`, vues matérialisées, streaming tables, Iceberg managé UC via `catalogs.yml`
(`type: unity`, UniForm), grants et tags UC appliqués, fraîcheur des sources, analyse statique
stricte, PAT et OAuth M2M.

Un seul défaut bloquant à la migration : le **double-quoting des principals de `grants`** (§5.1).
Restent non prouvés faute d'environnement adéquat : écriture cross-catalogue, rafraîchissement d'une
streaming table, modèles Python — les trois sont couverts par `run_live_checks.sh` dès qu'un
workspace le permet.
