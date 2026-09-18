# demo-variant — deux catalogues, une colonne VARIANT, dbt des deux côtés

Protocole minimal et reproductible : deux tables **Delta** dans **deux catalogues
différents**, dont une colonne **VARIANT**, jointes et aplaties par dbt — une fois sur
**Databricks / Unity Catalog**, une fois sur **Unity Catalog OSS 0.6.0** en local.

Les deux chaînes sont volontairement indépendantes (code dupliqué assumé) : on peut jouer
l'une sans l'autre.

```
demo_users.core.users        100 utilisateurs, chacun avec une ville
demo_cities.core.cities      référentiel ville + metadata VARIANT
        │
        └── dbt ──► demo_users.marts.users_enriched   (jointure + VARIANT aplati)
```

## Les scripts

| # | Fichier | Rôle | Où ça tourne |
|---|---------|------|--------------|
| 1 | `databricks/01_create_tables.py` | crée les deux tables (Delta, VARIANT) dans Unity Catalog | notebook Databricks |
| 2 | `uc_oss/01_create_tables_spark.py` | mêmes deux tables dans Unity Catalog OSS, via le connecteur Spark | local (pyspark) |
| 3 | `databricks/02_run_dbt.py` | `dbtRunner` en Python : jointure + aplatissement VARIANT | notebook Databricks, SQL sur un **warehouse** |
| 4 | `uc_oss/02_run_dbt.py` | `dbtRunner` en Python, cible Unity Catalog OSS | local (pyspark) |
| 5 | `databricks/03_run_dbt_no_warehouse.py` | le même dbt **sans warehouse** : `dbt-spark`, `method: session` | notebook Databricks, SQL sur le compute du notebook |

Autour : `deploy_databricks.py` (upload + jobs serverless), `setup_venv.sh`,
`start_uc_oss.sh`, et un projet dbt par cas (`databricks/dbt_project`,
`databricks/dbt_project_session`, `uc_oss/dbt_project`).

---

## A. Databricks / Unity Catalog

### Prérequis

* droit `CREATE CATALOG` sur le metastore (le notebook crée `demo_users` et
  `demo_cities`), ou deux catalogues existants passés en widgets ;
* un **SQL warehouse** pour le notebook 02 uniquement : `dbt-databricks` ne sait parler
  qu'à un endpoint SQL (`http_path`). Le notebook 03 n'en a pas besoin.

### Exécution

Tout depuis un poste local, jobs serverless créés à la volée :

```bash
export DBT_HOST=adb-xxxx.cloud.databricks.com DBT_TOKEN=dapi…
python deploy_databricks.py --http-path /sql/1.0/warehouses/xxxxxxxx   # 01 + 02 + 03
python deploy_databricks.py --http-path … --only 02                    # rejouer dbt (warehouse)
python deploy_databricks.py --http-path … --pip-spec dbt==2.0.4        # 02 avec dbt v2
python deploy_databricks.py --only 03                                  # dbt sans warehouse
```

Ou à la main : importer les fichiers de `databricks/` comme notebooks, copier les
dossiers `dbt_project*` à côté, et lancer le notebook voulu.

Les notebooks 02 et 03 installent dbt (`pip_spec`), redémarrent le Python, recopient le
projet dans `/tmp` (les fichiers workspace sont en lecture seule), puis appellent
`dbtRunner` pour `dbt run` et `dbt test`.

### 02 (warehouse) vs 03 (sans warehouse)

| | `02_run_dbt` | `03_run_dbt_no_warehouse` |
|---|---|---|
| adaptateur | `dbt-databricks` ou `dbt==2.0.4` | `dbt-spark`, `method: session` |
| compute SQL | le warehouse (`http_path` + token) | le compute du notebook |
| nombre de computes | 2 (notebook + warehouse) | 1 |
| nommage | 3 parties, `catalog:` dans `sources.yml` | 2 parties : `USE CATALOG` + catalogue dans `schema:` |
| dbt v2 | oui | non : pas d'adaptateur session en v2 |
| ce qu'on perd | — | les fonctions propres à `dbt-databricks` (MV/streaming tables, `catalog:`, notions Unity Catalog de l'adaptateur) |

`dbt-spark` en `method: session` appelle `SparkSession.builder.getOrCreate()`, qui dans un
notebook renvoie la session existante (Spark Connect sur serverless) : dbt exécute donc
son SQL sur le compute du notebook, sans warehouse ni token.

---

## B. Unity Catalog OSS 0.6.0 (local)

### Prérequis

Java 17+, Maven, Python 3.11+. Pas de Docker : le serveur UC est récupéré depuis Maven
Central et lancé en JVM directe.

```bash
./setup_venv.sh                 # .venv : pyspark 4.1.3, dbt-spark, deltalake
./start_uc_oss.sh --bg          # UC OSS 0.6.0 sur http://127.0.0.1:8080
.venv/bin/python uc_oss/01_create_tables_spark.py
.venv/bin/python uc_oss/02_run_dbt.py
```

Versions de la pile locale (toutes vérifiées à l'exécution) :

| composant | version | pourquoi celle-là |
|---|---|---|
| Unity Catalog OSS | 0.6.0 | dernière publiée (`unitycatalog-server`) |
| pyspark | 4.1.3 | VARIANT existe depuis Spark 4.0, mais la **syntaxe `metadata:champ` n'arrive qu'en 4.1** |
| Delta | `io.delta:delta-spark_4.1_2.13:4.4.0` | artefact Delta qualifié pour la ligne Spark 4.1 |
| connecteur UC Spark | `unitycatalog-spark_2.13:0.4.1` | dernier publié (compilé contre spark-sql 4.0.0) |
| client UC | `unitycatalog-client:0.6.0` épinglé | Delta 4.4 charge `io.unitycatalog.client.delta.model.*` par réflexion ; le client 0.4.1 imposé par le connecteur ne les contient pas → `NoClassDefFoundError` |

### Ce que font les scripts

`01_create_tables_spark.py` crée les deux catalogues via l'**API REST de UC** (Spark ne
sait pas créer un catalogue), puis les deux tables **Delta externes** sous
`./data/<catalog>/<schema>/<table>` — `CREATE TABLE … LOCATION` puis `INSERT`, donc
l'écriture passe **par Unity Catalog**, pas dans le chemin en douce.

`02_run_dbt.py` construit la `SparkSession` **avant** dbt : le profil dbt-spark est en
`method: session`, donc dbt réutilise cette session (connecteur UC déjà branché) au lieu
d'en créer une. Il pré-crée la table cible vide, puis lance `dbt run` + `dbt test`.

### Résultat

`demo_users.marts.users_enriched`, Delta **externe** sous
`./data/demo_users/marts/users_enriched`, 100 lignes, enregistrée `EXTERNAL / DELTA`
dans UC OSS. Rejouer le script est idempotent (merge sur `user_id`).

---

## Ce qui a demandé un contournement côté UC OSS

Tout ce qui suit a été constaté à l'exécution sur cette pile :

1. **dbt-spark ne génère que des noms en deux parties** (`include_policy.database =
   False`). Le catalogue voyage donc dans `schema` pour les sources
   (`schema: demo_cities.core`) et via `spark.sql.defaultCatalog=demo_users` pour la
   cible — sans quoi `show table extended in marts` cherche dans `spark_catalog` et dbt
   ne voit jamais sa propre table.
2. **`CREATE … LOCATION … AS SELECT` résout contre `spark_catalog`**, pas contre le
   catalogue Unity Catalog : `[SCHEMA_NOT_FOUND] spark_catalog.marts`. Donc pas de
   matérialisation `table` possible → la table est pré-créée vide par le script et le
   modèle est `incremental` + `merge`. Une création *managée* est refusée de son côté
   (`Managed table creation requires 'delta.feature.catalogManaged'='supported'`).
3. **`spark_catalog` doit rester `DeltaCatalog`.** Le pointer sur `UCSingleCatalog`
   échoue (`uri must be specified for Unity Catalog 'spark_catalog'`) ; seuls les deux
   catalogues de démo sont des catalogues UC.
4. **`spark.catalog.tableExists()` lève** `DELTA_PATH_DOES_NOT_EXIST` si la table est
   encore enregistrée mais que ses fichiers ont disparu (typiquement après un `rm -rf
   data/`). L'existence est donc testée par l'API REST de UC, et l'entrée orpheline est
   désenregistrée — le script reste rejouable.
5. **Le client UC doit être épinglé** (voir tableau des versions) : sans cela,
   `UCSingleCatalog.tableExists` → `UCTokenBasedRestClientFactory.createDeltaClient` →
   `NoClassDefFoundError: io/unitycatalog/client/delta/model/DeltaTableUpdate`.

## VARIANT : ce qu'il faut savoir

### Est-ce du « vrai » VARIANT ?

Oui. Trois choses distinctes, toutes vérifiées sur les deux piles :

| | résultat |
|---|---|
| type de **colonne** (`describe table …`) | `metadata  variant` |
| `typeof(metadata)` | `variant` |
| `schema_of_variant(metadata)` | `OBJECT<coords: OBJECT<lat: DECIMAL(6,4), lon: DECIMAL(5,4)>, population: BIGINT, region: STRING, tags: ARRAY<STRING>>` |

Le `OBJECT<…>` est la **forme de la valeur stockée**, pas le type de la colonne : VARIANT
est auto-descriptif, chaque valeur porte son type. Databricks et Spark 4.1 renvoient
exactement la même chaîne pour les mêmes données.

Le `DECIMAL(6,4)` sur `lat` vient de `parse_json`, qui garde les littéraux décimaux en
décimal exact — vérifié :
`schema_of_variant(parse_json('{"a":48.8566,"b":1.5,"c":1e3,"d":3,"e":3.0,"f":1234567890123456789012}'))`
→ `OBJECT<a: DECIMAL(6,4), b: DECIMAL(2,1), c: DOUBLE, d: BIGINT, e: DECIMAL(1,0), f: DECIMAL(22,0)>`.
Seule la notation exponentielle donne un DOUBLE. Le `::double` de l'aplatissement redonne
bien `48.8566`.

### Syntaxes de lecture

| | Databricks | Spark OSS |
|---|---|---|
| construire | `parse_json(payload)` | `parse_json(payload)` |
| champ | `metadata:population::bigint` | idem **à partir de Spark 4.1** ; en 4.0 → `variant_get(metadata,'$.population','bigint')` |
| imbriqué | `metadata:coords.lat::double` | idem 4.1 ; `variant_get(metadata,'$.coords.lat','double')` en 4.0 |
| tableau | `metadata:tags[0]::string` | idem 4.1 ; `try_variant_get(metadata,'$.tags[0]','string')` en 4.0 |
| brut | `to_json(metadata)` | `to_json(metadata)` |

Sur Spark 4.0.1, `select metadata:population::bigint …` échoue avec
`[PARSE_SYNTAX_ERROR] Syntax error at or near ':' SQLSTATE 42601` (vérifié) ; sur 4.1.3 les
quatre formes passent. Sans cast, `typeof(metadata:population)` vaut `variant`.

VARIANT n'a **ni égalité ni ordre** : une colonne VARIANT ne peut pas porter un test
`unique`, ni servir de clé de comparaison dans un snapshot. D'où `metadata_json` :
`to_json()` redonne quelque chose de comparable.

---

## Performance mesurée

Même projet, même modèle (1 modèle + 4 tests, 100 lignes), notebooks serverless. Temps dbt
relevés par le notebook lui-même, temps de bout en bout par l'API Jobs.

| chemin | moteur | `dbt run` | dont le modèle | `dbt test` | exécution notebook | run total |
|---|---|---|---|---|---|---|
| warehouse, warehouse **froid** | dbt-core 1.12.3 + dbt-databricks 1.12.5 | 34,98 s | 13,55 s | 3,56 s | 112 s | 117,0 s |
| warehouse, warehouse **chaud** | idem | 10,78 s | 3,11 s | 2,71 s | 53 s | 57,7 s |
| warehouse, **dbt 2.0.4** | dbt 2.0.4 (Fusion) | **5,96 s** | 4,62 s | 2,20 s | 43 s | 48,1 s |
| compute du notebook | dbt-core 1.12.5 + dbt-spark 1.11.0 | 14,15 s | 9,52 s | 5,79 s | 75 s | 80,0 s |

Unity Catalog OSS en local (conteneur 4 vCPU, `local[*]`) : script complet 73,7 s, dont
`dbt run` (merge incrémental) 43,98 s et `dbt test` 10,13 s ; le reste est la construction
de la session Spark et le chargement des jars.

Ce que disent ces chiffres :

1. **Le surcoût propre à dbt est ce qui sépare v1 de v2**, pas l'exécution SQL. À chaud,
   `dbt run` − temps du modèle vaut ≈ 7,7 s en v1 contre ≈ 1,3 s en v2 (parse, compile,
   introspection du catalogue). Sur le nœud lui-même, même warehouse, même requête :
   3,11 s (v1) contre 4,62 s (v2) — du même ordre.
2. **L'installation de dbt dans le notebook domine tout le reste** : ≈ 73 s pour
   `dbt-databricks`, ≈ 55 s pour `dbt-spark`, ≈ 35 s pour `dbt==2.0.4` (une seule roue avec
   l'extension native). Sur un projet réel, c'est là qu'il faut agir (environnement
   serverless pré-construit ou bibliothèque de cluster plutôt qu'un `%pip install` par run —
   non testé ici).
3. **Un warehouse froid coûte ~25 s** sur le premier modèle (13,55 s → 3,11 s entre le run
   froid et le run chaud).
4. **La première invocation dbt d'un process est chère** : en local, `dbt test` prend 71 s la
   première fois puis 14,3 s (threads=1) / 8,0 s (threads=4) — mesuré en inversant l'ordre
   pour vérifier que c'est bien le démarrage à froid, pas le parallélisme.
5. **`threads > 1` fonctionne sur une session Spark partagée** (vérifié : 4 tests, 8,0 s à 4
   threads contre 14,3 s à 1 thread). Les profils du repo restent à `threads: 1` par
   prudence ; `dbt run --threads 4` suffit à l'augmenter.

## Fonctionnalités : ce que chaque chemin sait faire

Inventaire lu dans les paquets installés (`dbt-databricks` 1.12.5, `dbt-spark` 1.11.0) ; la
colonne v2 reprend ce qui a été vérifié à l'exécution dans `../dbt-v2-unity-catalog-compat/`.

| | `dbt-databricks` (warehouse) | dbt v2 Fusion (warehouse) | `dbt-spark` session (notebook / UC OSS) |
|---|---|---|---|
| noms à 3 niveaux, `catalog:` dans `sources.yml` | oui (`DatabricksIncludePolicy.database = True`) | oui | **non** (`SparkIncludePolicy.database = False`) |
| matérialisations livrées | table, view, incremental, seed, snapshot, clone, **materialized_view, streaming_table, metric_view, functions** | MV et streaming table vérifiées | table, view, incremental, seed, snapshot, clone |
| stratégies incrémentales | append, merge, insert_overwrite, replace_where, delete+insert, microbatch | merge vérifié (`describe history`) | append, merge, insert_overwrite, microbatch |
| modèles Python | oui (`submission_method`) | vérifié (`serverless_cluster`) | macro `py_write_table` présente, non testée |
| grants / tags UC | plomberie complète | vérifié | minimal : 2 macros DCL seulement |
| `catalogs.yml` `type: unity`, UniForm/Iceberg | non | vérifié | non |
| `threads > 1` | oui (connexions parallèles au warehouse) | oui | oui, vérifié sur session partagée |
| écrire dans Unity Catalog **OSS** | hors sujet | **impossible** (Iceberg REST en lecture seule, `405`) | **seul chemin qui écrit** |
| se passer d'un warehouse | non | non (pas d'adaptateur session) | **oui** |

En résumé : `dbt-databricks` (ou v2) pour les fonctions Unity Catalog et Databricks,
`dbt-spark` en session pour économiser un compute ou pour atteindre UC OSS — au prix des
noms à deux parties, des matérialisations Databricks et de la partie grants.

## Résultats vérifiés

Exécuté le 2026-09-18 — jobs serverless sur le workspace de test, et en local dans ce
conteneur.

| Chaîne | Versions | Résultat |
|---|---|---|
| Databricks, création des tables | serverless | `demo_users.core.users` 100 lignes ; `demo_cities.core.cities` MANAGED/DELTA, `metadata` de type `variant` |
| Databricks, dbt via warehouse | dbt-core 1.12.3 + dbt-databricks 1.12.5 | `dbt run` + `dbt test` OK → `demo_users.marts.users_enriched` |
| Databricks, dbt via warehouse | **dbt 2.0.4** (moteur Fusion) | même notebook et même projet, `--pip-spec dbt==2.0.4` → OK |
| Databricks, dbt **sans warehouse** | dbt-core 1.12.5 + dbt-spark 1.11.0 `method: session` | session `pyspark.sql.connect.session.SparkSession`, `dbt run` + `dbt test` OK → `demo_users.marts_session.users_enriched`, 100 lignes |
| UC OSS, création des tables | UC OSS 0.6.0, connecteur 0.4.1, client 0.6.0, Spark 4.1.3, Delta 4.4.0 | deux tables Delta **externes**, `metadata` en `variant` |
| UC OSS, dbt | dbt-core 1.12.5 + dbt-spark 1.11.0, `method: session` | `dbt run` 1/1 + `dbt test` 4/4 → 100 lignes, 100 `user_id` distincts ; 2ᵉ passage idempotent |

Non testé ici : `dbt==2.0.4` sur la chaîne UC OSS — dbt v2 n'expose pas d'adaptateur Spark
en session, donc le montage `method: session` ne s'y applique pas. La compatibilité dbt v2
× Databricks × Unity Catalog est couverte plus largement par
`../dbt-v2-unity-catalog-compat/`.
