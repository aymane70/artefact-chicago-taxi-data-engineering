# Architecture — Pipeline Chicago Taxi

## 1. Vue d'ensemble

Le pipeline Chicago Taxi est une solution de data engineering qui récupère les données de trajets depuis le portail officiel de la ville de Chicago, les stocke dans un data lake, puis les transforme en jeux de données analytiques accessibles depuis un dashboard interactif.

L'architecture suit le modèle Medallion, avec trois couches principales : Bronze, Silver et Gold. Apache Airflow orchestre les traitements et RustFS assure le stockage des données via une API compatible S3.

## 2. Schéma de l'architecture

```text id="k3v5xd"
             City of Chicago Data Portal
                     Taxi Trips API
                            |
                            v
                 +---------------------+
                 |  Python Ingestion  |
                 |  Extraction API    |
                 |  Pagination        |
                 |  Validation lots   |
                 +---------------------+
                            |
                            v
                 +---------------------+
                 |    Bronze Layer    |
                 |   RustFS / JSONL    |
                 |   Données brutes    |
                 +---------------------+
                            |
                            v
                 +---------------------+
                 | Traitement PySpark  |
                 | Nettoyage & cast    |
                 | Contrôles qualité   |
                 +---------------------+
                            |
                            v
                 +---------------------+
                 |    Silver Layer    |
                 |  RustFS / Parquet  |
                 |  Données nettoyées |
                 +---------------------+
                            |
                  +---------+---------+
                  |                   |
                  v                   v
          +----------------+  +------------------+
          | Rapport qualité|  | Agrégations Gold |
          | Métriques      |  | PySpark          |
          | mensuelles     |  |                  |
          +----------------+  +------------------+
                  |                   |
                  v                   v
          +----------------+  +------------------+
          | Quality Layer  |  | Gold Layer       |
          | RustFS/Parquet |  | RustFS/Parquet   |
          +----------------+  +------------------+
                                      |
                                      v
                              +------------------+
                              | Streamlit        |
                              | Dashboard        |
                              +------------------+

                 Apache Airflow orchestre
                 les différentes tâches.
```

## 3. Technologies utilisées

| Composant | Technologie | Rôle |
|---|---|---|
| Source de données | Chicago Data Portal API | Fournit les données de trajets. |
| Ingestion | Python, Requests | Récupère les données par lots et les stocke. |
| Stockage objet | RustFS | Stocke les données Bronze, Silver, Quality et Gold. |
| Protocole de stockage | API compatible S3 | Permet d'accéder à RustFS avec des clients S3. |
| Traitement | Apache Spark, PySpark | Nettoie, valide et agrège les données. |
| Orchestration | Apache Airflow | Coordonne les différentes tâches du pipeline. |
| Formats de stockage | JSONL, Parquet | Stockent les données brutes et transformées. |
| Dashboard | Streamlit, Pandas, PyArrow | Lit les données Gold et Quality et affiche les indicateurs. |
| Environnement | Docker, Docker Compose | Exécute les services dans des conteneurs. |

## 4. Flux de données

### 4.1 Extraction — Bronze

Un module Python récupère les trajets depuis l'API du Chicago Data Portal.

L'extraction :

- Récupère les données sur la période définie.
- Utilise la pagination pour traiter les données par lots.
- Stocke les enregistrements bruts au format JSONL.
- Organise les fichiers par année et par mois.
- Génère des manifestes contenant les informations de suivi des lots.

Bronze conserve les données telles qu'elles proviennent de la source, avant toute transformation analytique.

### 4.2 Transformation — Silver

PySpark lit les fichiers JSONL de Bronze et applique les règles de transformation.

Le traitement comprend :

- Le nettoyage des chaînes de caractères et la gestion des valeurs vides.
- La conversion des dates et des champs numériques vers les types attendus.
- La conservation des valeurs numériques brutes pour faciliter les diagnostics.
- L'application des contrôles qualité.
- La séparation des enregistrements qui ne respectent pas les contrôles structurels critiques dans Quarantine.

Les données Silver sont enregistrées au format Parquet et partitionnées par année et par mois.

### 4.3 Rapports qualité

Une tâche dédiée analyse les données Silver et calcule les indicateurs qualité chaque mois.

Elle mesure notamment :

- Le nombre total d'enregistrements et d'identifiants de trajet distincts.
- Le nombre d'identifiants dupliqués.
- Le nombre et le pourcentage d'enregistrements concernés par chaque anomalie détectée.

Les rapports sont enregistrés au format Parquet dans la couche Quality.

### 4.4 Transformation analytique — Gold

PySpark produit trois jeux de données à partir de Silver :

- **Daily Revenue :** volume de trajets, revenus déclarés, revenu moyen, montants des courses et pourboires par jour.
- **Daily Trip Metrics :** durée et distance moyennes des trajets par jour, avec des règles d'éligibilité indépendantes.
- **Pickup Zone Performance :** volume de trajets, revenus, distance moyenne et classement mensuel des zones de prise en charge.

Ces jeux de données sont stockés au format Parquet et servent de source au dashboard.

### 4.5 Dashboard

Le dashboard Streamlit lit directement les données Gold et les rapports Quality stockés dans RustFS, via son interface compatible S3.

Il permet de consulter :

- Les principaux indicateurs de trajets et de revenus.
- L'évolution journalière des revenus et du volume de trajets.
- Les indicateurs de durée et de distance.
- La performance des zones de prise en charge.
- Les métriques qualité mensuelles.
- Les résultats filtrés par mois.

Le dashboard ne relance pas les transformations Spark : il exploite les données déjà produites par le pipeline.

## 5. Orchestration

Apache Airflow coordonne le workflow à travers le DAG `chicago_taxi_pipeline`.

Le DAG comprend quatre tâches exécutées dans l'ordre :

| Ordre | Tâche | Rôle |
|---|---|---|
| 1 | `ingest_data` | Récupère les données de l'API et écrit les lots dans Bronze. |
| 2 | `bronze_to_silver` | Nettoie, convertit et valide les données brutes. |
| 3 | `silver_quality_report` | Calcule les indicateurs qualité mensuels. |
| 4 | `silver_to_gold` | Produit les jeux de données analytiques Gold. |

Chaque tâche dépend de la réussite de la précédente.

Le DAG est configuré pour être lancé manuellement (`schedule=None`).

## 6. Organisation du stockage

Toutes les données persistantes du pipeline sont stockées dans le bucket RustFS `chicago-taxi-lake`.

```text id="c2m4qv"
chicago-taxi-lake/
├── bronze/
│   └── year=2023/
│       ├── month=09/
│       ├── month=10/
│       ├── month=11/
│       └── month=12/
├── silver/
│   └── taxi_trips/
├── quarantine/
│   └── taxi_trips/
├── quality/
│   └── silver/
│       └── monthly_report/
└── gold/
    ├── daily_revenue/
    ├── daily_trip_metrics/
    └── pickup_zone_performance/
```

Bronze utilise le format JSONL. Silver, Quarantine, Quality et Gold utilisent le format Parquet.

## 7. Déploiement et exécution

Docker Compose démarre les principaux services du projet :

- **Airflow :** exécute et supervise le DAG.
- **RustFS :** fournit le stockage objet persistant compatible S3.
- **Streamlit :** héberge le dashboard analytique.

PySpark s'exécute dans l'environnement Airflow pour les tâches de transformation. Les fichiers temporaires utilisés pendant les traitements sont distincts des données persistantes stockées dans RustFS.

Le dashboard accède à RustFS via le réseau interne Docker. Les interfaces Airflow, RustFS et Streamlit sont accessibles localement sur leurs ports respectifs.

## 8. Principes de conception

L'architecture repose sur quelques principes :

- **Séparation des couches :** les données brutes, transformées et analytiques sont stockées séparément.
- **Reproductibilité :** les transformations repartent des données Bronze persistées.
- **Traçabilité :** les indicateurs qualité et les informations de quarantaine permettent de retrouver les anomalies.
- **Idempotence de l'ingestion :** les lots déjà validés peuvent être réutilisés sans être extraits à nouveau inutilement.
- **Découplage du dashboard :** la visualisation consomme les résultats Gold et Quality sans dépendre de l'exécution de Spark.
- **Persistance :** les volumes Docker permettent de conserver les données RustFS après le redémarrage des conteneurs.

---

Pour les définitions des champs, les indicateurs qualité et les schémas des jeux de données analytiques, consulter le [Dictionnaire de données](data_dictionary.md).