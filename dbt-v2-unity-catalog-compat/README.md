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

export DBT_HOST=adb-xxx.azuredatabricks.net
export DBT_HTTP_PATH=/sql/1.0/warehouses/xxx
export DBT_TOKEN=dapi...
export DBT_CATALOG=main DBT_SCHEMA=dbt_uc_compat   # schéma jetable
./run_live_checks.sh
```

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

## 3. Limites de ce qui a été exécuté

* Aucun workspace Databricks n'était disponible (pas d'identifiants dans l'environnement), donc
  **la phase N3 n'a pas été exécutée**. Le réseau sortant vers Databricks est ouvert depuis cet
  environnement (`docs.databricks.com` → 301, `accounts.cloud.databricks.com` → 303) : la phase N3
  est lançable ici dès que des identifiants sont fournis.
* Restent donc **non vérifiés** : le DDL réellement émis pour `materialized_view`, `streaming_table`
  et l'Iceberg managé UC ; l'application effective des grants, tags et commentaires dans UC ;
  le MERGE incrémental ; les modèles Python (soumission de job) ; l'analyse statique *stricte*
  (types et lignage colonne par colonne — elle exige une connexion) ; la fraîcheur des sources.
* `materialized` et `incremental_strategy` **ne sont pas validés au parse ni au compile** (une valeur
  fantaisiste passe) : ces valeurs sont résolues à l'exécution. Toute conclusion sur le support d'une
  matérialisation Databricks exige donc la phase N3.

## 4. Points d'attention détectés (garde-fous dans le script)

1. **`file_format` vs Iceberg natif UC.** Un `+file_format: delta` au niveau projet/modèle entre en
   conflit avec un catalogue `type: unity` / `table_format: iceberg` : le moteur exige
   `file_format: parquet` (ou `use_uniform: true` avec `delta`). Le message d'erreur pointe une
   position inutilisable (`:2:34`), sans nommer le modèle fautif — coûteux à diagnostiquer.
2. **Le nom d'entrée de `catalogs.yml` devient le catalogue UC.** Avec `catalog_name='uc_iceberg_uniform'`
   et sans `catalog:` explicite, la relation devient `uc_iceberg_uniform.<schéma>.<objet>` : dbt viserait
   un catalogue UC inexistant. Il faut soit nommer l'entrée exactement comme le catalogue UC, soit
   fixer `catalog:` sur le modèle (c'est ce que fait le fixture).
3. **`compute` est réservé par le moteur** (`remote|inline|local|sidecar|service`). Le sélecteur de
   compute Databricks reste `databricks_compute`.
4. **Syntaxe v2 sur les sources** : `loaded_at_field` et `freshness` doivent passer sous `config:`,
   sinon `dbt1060` (clé ignorée) — migration à prévoir depuis dbt-core 1.x.
5. Les modèles introspectifs (`is_incremental()`) ouvrent une connexion **dès le `compile`** : pas de
   compilation 100 % hors ligne d'un projet incrémental.

## 5. Verdict

Sur la base des phases N1 + N2 : **dbt v2 (2.0.4) est compatible avec Databricks et Unity Catalog**
pour tout ce qui est vérifiable sans warehouse — adaptateur intégré, namespace à 3 niveaux,
catalogues UC (y compris Iceberg managé via `catalogs.yml` `type: unity`), configs Delta/UC,
dialecte SQL Databricks, PAT et OAuth M2M.

La compatibilité **fonctionnelle** (matérialisations Databricks, gouvernance UC appliquée, MERGE,
modèles Python) reste à confirmer par `run_live_checks.sh` sur un workspace. Tant que ce script n'a
pas tourné, la réponse honnête est : compatible sur contrat et connexion, non prouvé à l'exécution.
