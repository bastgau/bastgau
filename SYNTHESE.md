# dbt (v1 / v2) × Databricks Unity Catalog × Unity Catalog OSS — synthèse

Document de synthèse de l'investigation menée les 17 et 18 septembre 2026. Tout ce qui est
marqué **vérifié** a été exécuté : soit sur un workspace Databricks réel (jobs serverless),
soit en local dans un conteneur Linux. Ce qui n'a pas pu être exécuté est listé séparément,
avec la raison.

Convention de vocabulaire :

| terme | ce que c'est exactement |
|---|---|
| **dbt v2** | paquet PyPI `dbt`, version 2.0.4 — moteur *Fusion*, extension native `dbt/_core.abi3.so`, adaptateurs intégrés |
| **dbt v1** | `dbt-core` 1.12.3/1.12.5 + un adaptateur : `dbt-databricks` 1.12.5, `dbt-spark` 1.11.0, `dbt-duckdb` 1.11.0 |
| **UC** | Unity Catalog géré par Databricks |
| **UC OSS** | Unity Catalog open source, serveur `io.unitycatalog:unitycatalog-server` 0.6.0, lancé en local |

---

## 1. Grille de synthèse

| | Databricks + UC | Unity Catalog OSS (local) |
|---|---|---|
| **dbt v2** (`dbt` 2.0.4) | ✅ **vérifié** — 46 contrôles hors ligne + 24 contrôles live + 14 assertions sur l'état réel de UC | ⚠️ **lecture oui, écriture non** — 31 contrôles ; l'API Iceberg REST de UC OSS répond `405` en écriture |
| **dbt v1** (`dbt-core` 1.12.x) | ✅ **vérifié** — 20 contrôles avec `dbt-databricks`, en local *et* dans un notebook serverless | ✅ **écriture vérifiée** — deux chemins : `dbt-spark` `method: session` (ce protocole) et `dbt-duckdb` + plugin via l'API UC native (10 contrôles) |

Réponse courte à la question de départ : **oui, dbt v2 est compatible Unity Catalog et
Databricks**, par un endpoint SQL. Non, dbt v2 ne sait pas écrire dans Unity Catalog OSS.

---

## 2. Les chemins de compute sur Databricks

| chemin | dbt v1 | dbt v2 | ce que ça implique |
|---|---|---|---|
| **SQL warehouse** (`http_path`) | ✅ vérifié | ✅ vérifié | le seul chemin pour `dbt-databricks` et pour v2 ; deux computes si dbt tourne dans un notebook |
| **Notebook / job compute, SQL sur le warehouse** | ✅ vérifié | ✅ vérifié | le compute du notebook n'héberge que le processus dbt |
| **Notebook / job compute, SQL sur ce compute** (`dbt-spark` `method: session`) | ✅ **vérifié** | ❌ impossible | un seul compute, pas de warehouse, pas de token — mais adaptateur `spark` |
| **Cluster all-purpose** (`http_path` de cluster) | ❓ non testé | ❓ non testé | organisation *serverless-only* : `clusters/create` refusé sur le workspace de test |

Point clé, **vérifié** : `dbt-spark` en `method: session` appelle
`SparkSession.builder.getOrCreate()`, qui dans un notebook Databricks renvoie la session
existante — `pyspark.sql.connect.session.SparkSession` sur serverless. dbt exécute donc son
SQL sur le compute du notebook.

---

## 3. Ce qui change dans le projet dbt selon le chemin

La ligne de fracture est **l'adaptateur**, pas la version de dbt : le passage de v1 à v2 sur
un warehouse n'a demandé **aucune modification du projet** (même `profiles.yml`, mêmes
sources, même modèle ; seul le `pip install` change).

| | v1 warehouse | **v2** warehouse | v1 compute du notebook | UC OSS |
|---|---|---|---|---|
| projet | `databricks/dbt_project` | **le même, inchangé** | `databricks/dbt_project_session` | `uc_oss/dbt_project` |
| `profiles.yml` → `type` | `databricks` | `databricks` | `spark` + `method: session` | `spark` + `method: session` |
| connexion | `host` / `http_path` / `token` | idem | `host: localhost` | `host: localhost` |
| catalogue de sortie | clé `catalog:` | clé `catalog:` | `USE CATALOG` (notebook) | `spark.sql.defaultCatalog` (script) |
| `sources.yml` | `catalog:` + `schema:` | idem | catalogue **dans** `schema:` | catalogue **dans** `schema:` |
| matérialisation | `table` | `table` | `table` | `incremental` + `merge` |
| corps du `select` | identique | identique | identique | identique |
| `schema.yml` (tests) | identique | identique | identique | identique |
| `threads` | 4 | 4 | 1 | 1 |

Pourquoi le catalogue ne peut pas être une clé à part avec `dbt-spark` — **vérifié dans le
code** de l'adaptateur :

```python
class SparkIncludePolicy(Policy):      class DatabricksIncludePolicy(Policy):
    database: bool = False                 database: bool = True
```

`dbt-spark` ne rend donc que des noms en deux parties. Conséquence : la clé `catalog:` de
`sources.yml` est ignorée, et la découverte de relations (`show table extended in <schema>`)
se fait dans le catalogue courant.

---

## 4. Performance mesurée

Même projet, même modèle (1 modèle + 4 tests, 100 lignes), notebooks serverless. Temps dbt
chronométrés par le notebook lui-même, temps de bout en bout lus dans l'API Jobs.

| chemin | moteur | `dbt run` | dont le modèle | `dbt test` | exécution notebook | run total |
|---|---|---|---|---|---|---|
| warehouse **froid** | dbt-core 1.12.3 + dbt-databricks 1.12.5 | 34,98 s | 13,55 s | 3,56 s | 112 s | 117,0 s |
| warehouse **chaud** | idem | 10,78 s | 3,11 s | 2,71 s | 53 s | 57,7 s |
| warehouse, **dbt 2.0.4** | Fusion | **5,96 s** | 4,62 s | 2,20 s | 43 s | 48,1 s |
| compute du notebook | dbt-core 1.12.5 + dbt-spark 1.11.0 | 14,15 s | 9,52 s | 5,79 s | 75 s | 80,0 s |

Unity Catalog OSS en local (conteneur 4 vCPU, `local[*]`) : script complet 73,7 s, dont
`dbt run` (merge incrémental) 43,98 s et `dbt test` 10,13 s ; le reste est la construction de
la session Spark et le chargement des jars.

Lectures, par ordre d'importance :

1. **Ce qui sépare v1 de v2, c'est le surcoût propre à dbt, pas l'exécution SQL.** À chaud,
   `dbt run` moins le temps du modèle : ≈ 7,7 s en v1 contre ≈ 1,3 s en v2 (parse, compile,
   introspection du catalogue). Sur le nœud lui-même, même warehouse, même requête : 3,11 s
   (v1) contre 4,62 s (v2) — même ordre de grandeur.
2. **L'installation de dbt écrase tout le reste dans un notebook** : ≈ 73 s pour
   `dbt-databricks`, ≈ 55 s pour `dbt-spark`, ≈ 35 s pour `dbt==2.0.4` (une seule roue avec
   l'extension native). C'est le premier levier sur un pipeline réel — environnement
   serverless pré-construit ou bibliothèque de cluster plutôt qu'un `%pip install` par run
   (piste *non testée*).
3. **Un warehouse froid coûte ≈ 25 s** sur le premier modèle (13,55 s → 3,11 s entre le run
   froid et le run chaud). La première mesure v1 était faussée par ça, d'où la relance.
4. **La première invocation dbt d'un process est chère** : en local, `dbt test` prend 71 s la
   première fois puis 14,3 s — vérifié en inversant l'ordre des essais pour s'assurer que
   c'est bien le démarrage à froid.
5. **`threads > 1` fonctionne sur une session Spark partagée** : 4 tests en 8,0 s à 4 threads
   contre 14,3 s à 1 thread. Les profils livrés restent à `threads: 1` par prudence ;
   `dbt run --threads 4` suffit.

---

## 5. Fonctionnalités par adaptateur

Inventaire **lu dans les paquets installés** (`dbt-databricks` 1.12.5, `dbt-spark` 1.11.0).
La colonne v2 reprend ce qui a été exécuté dans le protocole `dbt-v2-unity-catalog-compat`.

| | `dbt-databricks` (warehouse) | dbt v2 Fusion | `dbt-spark` session |
|---|---|---|---|
| noms à 3 niveaux, `catalog:` | ✅ (`database = True`) | ✅ | ❌ (`database = False`) |
| matérialisations livrées | table, view, incremental, seed, snapshot, clone, **materialized_view, streaming_table, metric_view, functions** | MV + streaming table vérifiées | table, view, incremental, seed, snapshot, clone |
| stratégies incrémentales | append, merge, insert_overwrite, **replace_where**, **delete+insert**, microbatch | merge vérifié (`describe history`) | append, merge, insert_overwrite, microbatch |
| modèles Python | ✅ (`submission_method`) | ✅ vérifié (`serverless_cluster`) | macro `py_write_table` présente, non testée |
| grants / tags UC | plomberie complète | ✅ vérifié | minimal (2 macros DCL) |
| `catalogs.yml` `type: unity`, UniForm / Iceberg | ❌ | ✅ vérifié | ❌ |
| `threads > 1` | ✅ (connexions parallèles) | ✅ | ✅ vérifié sur session partagée |
| écrire dans **UC OSS** | hors sujet | ❌ (Iceberg REST `405`) | ✅ **seul chemin qui écrit** |
| se passer d'un warehouse | ❌ | ❌ | ✅ |

En une phrase : `dbt-databricks` ou v2 pour les fonctions Unity Catalog / Databricks ;
`dbt-spark` en session pour économiser un compute ou atteindre UC OSS, au prix des noms à
deux parties, des matérialisations Databricks et de la partie grants.

---

## 6. VARIANT

### Est-ce du « vrai » VARIANT ?

Oui. Trois choses distinctes, à ne pas confondre — **vérifiées sur les deux piles** :

| question | commande | résultat |
|---|---|---|
| type de la **colonne** | `describe table demo_cities.core.cities` | `metadata  variant` |
| type de la **valeur** | `typeof(metadata)` | `variant` |
| **forme** de la valeur | `schema_of_variant(metadata)` | `OBJECT<coords: OBJECT<lat: DECIMAL(6,4), lon: DECIMAL(5,4)>, population: BIGINT, region: STRING, tags: ARRAY<STRING>>` |

Le `OBJECT<…>` que l'on voit affiché est le résultat de `schema_of_variant()` : la **forme de
la valeur stockée**, pas le type de la colonne. VARIANT est auto-descriptif — chaque valeur
porte son type. Databricks et Spark 4.1 renvoient exactement la même chaîne pour les mêmes
données.

Le `DECIMAL(6,4)` sur une latitude vient de `parse_json`, **vérifié par sonde** :

```sql
schema_of_variant(parse_json('{"a":48.8566,"b":1.5,"c":1e3,"d":3,"e":3.0,"f":1234567890123456789012}'))
-- OBJECT<a: DECIMAL(6,4), b: DECIMAL(2,1), c: DOUBLE, d: BIGINT, e: DECIMAL(1,0), f: DECIMAL(22,0)>
```

Un littéral décimal est conservé en décimal exact ; seule la notation exponentielle donne un
DOUBLE. Le `::double` de l'aplatissement redonne bien `48.8566`.

### Syntaxes de lecture

| | Databricks | Spark OSS |
|---|---|---|
| construire | `parse_json(payload)` | `parse_json(payload)` |
| champ | `metadata:population::bigint` | idem **à partir de Spark 4.1** ; `variant_get(metadata,'$.population','bigint')` en 4.0 |
| imbriqué | `metadata:coords.lat::double` | idem 4.1 ; `variant_get(metadata,'$.coords.lat','double')` en 4.0 |
| élément de tableau | `metadata:tags[0]::string` | idem 4.1 ; `try_variant_get(metadata,'$.tags[0]','string')` en 4.0 |
| brut, comparable | `to_json(metadata)` | `to_json(metadata)` |

**Vérifié** : la syntaxe « deux-points » est une fonctionnalité **Spark 4.1**, pas une
extension propriétaire Databricks. Sur Spark 4.0.1 les quatre formes échouent en
`[PARSE_SYNTAX_ERROR] Syntax error at or near ':' SQLSTATE 42601` ; sur Spark 4.1.3 elles
passent toutes. Contrôle fait avec le même script sur les deux versions.

### Les deux pièges

1. **VARIANT n'a ni égalité ni ordre.** Une colonne VARIANT ne peut pas porter un test
   `unique`, ni servir de clé de comparaison dans un snapshot (`check`). Contournement
   retenu : une projection `to_json(metadata) as metadata_json`, qui redonne quelque chose de
   comparable.
2. **Sans cast, une extraction reste du VARIANT** : `typeof(metadata:population)` vaut
   `variant`. Le `::type` n'est pas cosmétique.

---

## 7. Versions : lesquelles, et pourquoi

| composant | version retenue | pourquoi celle-là |
|---|---|---|
| dbt v2 | `dbt` 2.0.4 | dernière publiée au moment des tests |
| dbt v1 | `dbt-core` 1.12.3 / 1.12.5 | tiré par l'adaptateur |
| adaptateurs | `dbt-databricks` 1.12.5, `dbt-spark` 1.11.0, `dbt-duckdb` 1.11.0 | derniers publiés |
| Unity Catalog OSS | 0.6.0 | dernière publiée — vérifiée sur `maven-metadata.xml`, pas sur l'API de recherche Maven (qui renvoyait encore 0.3.0) |
| pyspark | **4.1.3** | VARIANT existe depuis Spark 4.0, mais la syntaxe `metadata:champ` n'arrive qu'en **4.1** |
| Delta | `io.delta:delta-spark_4.1_2.13:4.4.0` | Delta publie des artefacts **qualifiés par version de Spark** ; `_4.1_` est la ligne Spark 4.1 |
| connecteur UC Spark | `io.unitycatalog:unitycatalog-spark_2.13:0.4.1` | dernier publié ; compilé contre `spark-sql` **4.0.0** (lu dans son POM) |
| client UC | `io.unitycatalog:unitycatalog-client:0.6.0` **épinglé** | Delta 4.4 charge `io.unitycatalog.client.delta.model.*` par réflexion ; le client 0.4.1 imposé par le connecteur ne contient pas ces classes |

---

## 8. Unity Catalog OSS : ce qui marche, ce qui bloque

| | verdict | preuve |
|---|---|---|
| serveur 0.6.0 lancé depuis Maven Central, sans Docker | ✅ | 259 jars, H2 en base, écoute sur 8080 |
| création de catalogues | ✅ **par l'API REST UC** | Spark ne sait pas créer un catalogue |
| tables Delta **externes** écrites *à travers* UC | ✅ | `CREATE TABLE … LOCATION` puis `INSERT` ; enregistrées `EXTERNAL / DELTA` |
| colonne **VARIANT** | ✅ | `describe` → `variant`, avec connecteur 0.4.1 sur Spark 4.1.3 |
| jointure entre **deux catalogues** | ✅ | 100 lignes en sortie, 100 `user_id` distincts |
| dbt en écriture (`dbt-spark` session) | ✅ | `dbt run` 1/1 + `dbt test` 4/4, idempotent au 2ᵉ passage |
| dbt en écriture (`dbt-duckdb` + plugin, API UC native) | ✅ | 10 contrôles, 10 PASS |
| dbt **v2** en écriture | ❌ | Iceberg REST : 7 endpoints en lecture, `405` en écriture ; `plugins:` ignoré silencieusement ; `external format=delta` refusé |
| table **managée** | ❌ | `Managed table creation requires 'delta.feature.catalogManaged'='supported'` |
| matérialisation `table` de dbt | ❌ | `CREATE OR REPLACE … LOCATION … AS SELECT` résout contre `spark_catalog` |
| authentification | ⚠️ non exercée | serveur en mode `dev`, aucun jeton exigé |

### Les cinq contournements nécessaires

1. **Noms à deux parties** : catalogue dans `schema:` pour les sources,
   `spark.sql.defaultCatalog` pour la cible — sinon `show table extended in marts` cherche
   dans `spark_catalog` et dbt ne voit jamais sa propre table.
2. **Pas de matérialisation `table`** : la table externe est pré-créée vide par le script, et
   le modèle est `incremental` + `merge`.
3. **`spark_catalog` doit rester `DeltaCatalog`** : le pointer sur `UCSingleCatalog` échoue
   (`uri must be specified for Unity Catalog 'spark_catalog'`).
4. **Test d'existence par l'API REST**, pas par `spark.catalog.tableExists()`, qui lève
   `DELTA_PATH_DOES_NOT_EXIST` quand la table est encore enregistrée mais ses fichiers
   supprimés.
5. **Client UC épinglé à 0.6.0** (voir §7).

---

## 9. Journal des erreurs rencontrées

Chaque ligne a été observée à l'exécution, puis corrigée.

| symptôme | cause | correctif |
|---|---|---|
| `PARSE_SYNTAX_ERROR 42601` sur un `grant` | principal entre *backticks* dans `catalogs.yml` | forme nue : `['account users']` — portable v1 et v2 |
| schéma de `catalogs.yml` v2 inconnu | non documenté au moment du test | découvert par sondes : `name`, `type: unity`, `table_format`, `config.databricks{use_uniform, file_format, location_root}` |
| UC OSS annoncé en 0.3.0 | l'API de recherche Maven était périmée | lire `maven-metadata.xml` : 0.6.0 disponible ; tout rejoué dessus |
| `%pip install "dbt==$widget"` sans effet dans un job | la magie `%pip` n'interpole pas les widgets | `get_ipython().run_line_magic(...)` |
| projet dbt introuvable dans le notebook | chemin workspace à préfixer | `/Workspace` + copie dans `/tmp` (workspace en lecture seule) |
| `CANNOT_DETERMINE_TYPE` sur `createDataFrame` | une colonne entièrement `None` | schéma explicite |
| `RunResult has no attribute unique_id` | v2 porte `unique_id` sur la ligne, v1 sur `result.node` | accès défensif aux deux |
| `400 CATALOG_ALREADY_EXISTS` (et non 409) | convention UC OSS | détection sur le corps de la réponse |
| `NoClassDefFoundError … DeltaTableUpdate` | Delta 4.4 réflexion sur le client UC ; 0.4.1 épinglé par le connecteur | épingler `unitycatalog-client:0.6.0` |
| `DELTA_PATH_DOES_NOT_EXIST` au démarrage | table encore enregistrée, fichiers supprimés | désenregistrement par l'API REST puis recréation |
| `[SCHEMA_NOT_FOUND] spark_catalog.marts` | CTAS + LOCATION résout contre le catalogue de session | pré-création + `incremental`/`merge` |

Deux affirmations que j'ai dû corriger en cours de route, pour mémoire : le connecteur UC
0.4.1 est compilé contre **spark-sql 4.0.0** (et non 3.5), et la syntaxe VARIANT
« deux-points » est une fonctionnalité **Spark 4.1** (et non une extension Databricks) — dans
les deux cas, c'est la lecture du POM et une sonde sur les deux versions de Spark qui
tranchent.

---

## 10. Ce qui n'a pas pu être vérifié

| point | pourquoi |
|---|---|
| `http_path` d'un cluster all-purpose | organisation *serverless-only* : `clusters/create` refusé |
| MV / streaming tables sur cluster all-purpose | dépend du point ci-dessus |
| écriture cross-catalogue en v2 | un seul catalogue inscriptible sur le workspace de test |
| OAuth M2M de bout en bout | vérifié jusqu'à l'appel du endpoint OIDC, pas de service principal |
| modèle Python depuis un notebook | interblocage sur l'unique créneau serverless (le chemin CLI, lui, est vérifié) |
| dbt v2 sur la chaîne UC OSS en session | dbt v2 n'expose pas d'adaptateur Spark en session |
| modèles Python avec `dbt-spark` | la macro existe, non exercée |
| authentification UC OSS | serveur en mode `dev` |
| UC OSS 0.7 (écriture Iceberg REST annoncée au roadmap) | pas encore publiée |

---

## 11. Contenu de l'archive

```
SYNTHESE.md                        ce document
demo-variant/                      protocole court : 2 catalogues, VARIANT, dbt des 2 côtés
  README.md                        mode d'emploi + résultats mesurés
  setup_venv.sh                    venv local (pyspark 4.1.3, dbt-spark)
  start_uc_oss.sh                  UC OSS 0.6.0 depuis Maven Central, sans Docker
  deploy_databricks.py             upload des notebooks + jobs serverless
  databricks/01_create_tables.py           notebook : 2 tables Delta, 1 colonne VARIANT
  databricks/02_run_dbt.py                 notebook : dbtRunner, SQL sur un warehouse
  databricks/03_run_dbt_no_warehouse.py    notebook : dbtRunner, SQL sur le compute du notebook
  databricks/dbt_project/                  projet dbt pour le warehouse (v1 et v2)
  databricks/dbt_project_session/          projet dbt pour le mode session
  uc_oss/01_create_tables_spark.py         mêmes tables dans UC OSS, via le connecteur Spark
  uc_oss/02_run_dbt.py                     dbtRunner contre UC OSS
  uc_oss/dbt_project/                      projet dbt pour UC OSS
dbt-v2-unity-catalog-compat/       protocole long : compatibilité dbt v2 × UC × Databricks
  README.md                        ~900 lignes : protocole, résultats, limites
  bootstrap.sh, teardown.sh, run_all.sh
  run_offline_checks.sh            46 contrôles sans workspace
  run_live_checks.sh               24 contrôles exécutés par dbt sur le workspace
  verify_uc_state.sh               14 assertions sur l'état réel de Unity Catalog
  run_dbt1x_databricks_checks.sh   20 contrôles dbt v1 × Databricks
  uc-oss/run_uc_oss_checks.sh      31 contrôles UC OSS
  uc-oss/run_dbt1x_uc_write.sh     10 contrôles : écriture UC OSS en dbt v1
  fixture/, dbt1x-databricks/      projets dbt de test (modèles, seeds, snapshots)
  runner/                          dbtRunner en script, en notebook, déploiement
  lib/dbx_api.py                   client REST Databricks minimal
```

L'archive ne contient **aucun identifiant** : `env.local` (hôte + PAT) est exclu, comme dans
le dépôt Git.

---

## 12. Rejouer

```bash
# --- Databricks ---------------------------------------------------------------
export DBT_HOST=adb-xxxx.cloud.databricks.com DBT_TOKEN=dapi…
cd demo-variant
python deploy_databricks.py --http-path /sql/1.0/warehouses/xxxxxxxx   # 01 + 02 + 03
python deploy_databricks.py --http-path … --pip-spec dbt==2.0.4        # 02 en dbt v2
python deploy_databricks.py --only 03                                  # dbt sans warehouse

# --- Unity Catalog OSS en local ----------------------------------------------
./setup_venv.sh                 # java 17+ et maven requis
./start_uc_oss.sh --bg          # http://127.0.0.1:8080
.venv/bin/python uc_oss/01_create_tables_spark.py
.venv/bin/python uc_oss/02_run_dbt.py

# --- Protocole long ----------------------------------------------------------
cd ../dbt-v2-unity-catalog-compat
./run_offline_checks.sh         # aucun workspace requis
./bootstrap.sh                  # crée env.local à partir de DBT_HOST / DBT_TOKEN
./run_live_checks.sh && ./verify_uc_state.sh
```

---

## 13. Traces d'exécution

Jobs serverless sur le workspace de test, le 18/09/2026. URL d'un run :
`https://<workspace>/#job/<job_id>/run/<run_id>`.

| ce qui a tourné | job | run | résultat |
|---|---|---|---|
| `demo-variant-01` création des tables | 311234512504421 | 652701775574429 | SUCCESS — tables Delta + VARIANT |
| `demo-variant-02` dbt v1, warehouse | 360858539908631 | 873263577939206 | SUCCESS |
| `demo-variant-02` dbt 2.0.4, warehouse | 360858539908631 | 1020979258990556 | SUCCESS |
| sonde « sans warehouse » (run one-off) | — | 38935376354432 | SUCCESS — `dbt_success: true`, 100 lignes |
| `demo-variant-03` sans warehouse | 464352203555815 | 346436099122034 | SUCCESS |
| mesures §4 — v1 froid / v2 / v1 chaud | 360858539908631 | 449924032233041 / 667211924897198 / 423965134829197 | SUCCESS |
| mesures §4 — sans warehouse | 464352203555815 | 85700779417425 | SUCCESS |

---

## 14. Hygiène — à faire côté workspace

Ressources créées pendant l'investigation, à supprimer si elles ne servent plus :

* catalogues `demo_users` (schémas `core`, `marts`, `marts_session`) et `demo_cities`
  (schéma `core`) ;
* schémas `dbt_uc_compat*` et `dbt_uc_compat_v1*` dans le catalogue `workspace` ;
* dossiers workspace `/Users/<vous>/demo_variant` (dont un notebook de sonde
  `probe_session`) et `/Users/<vous>/dbt_uc_compat` ;
* jobs `demo-variant-01`, `demo-variant-02`, `demo-variant-03`,
  `dbt-v2-uc-compat-notebook`, `dbt-v1-uc-compat-notebook`.

Et surtout : **le PAT utilisé pendant ces tests a circulé en clair — il faut le révoquer.**
Il n'est stocké que dans `dbt-v2-unity-catalog-compat/env.local` (droits `600`, exclu de Git
et de cette archive).
