# Architecture du pipeline Chicago Taxi

## Vue d'ensemble

Le pipeline récupère les trajets de taxis depuis le portail de la ville de Chicago, les stocke dans un data lake, puis les transforme en jeux de données analytiques affichés dans un dashboard.

Il suit l'architecture Medallion, avec trois couches : Bronze (données brutes), Silver (données nettoyées) et Gold (données analytiques). Airflow orchestre les traitements et RustFS assure le stockage grâce à une API compatible S3.

## Schéma

```text
API Chicago Data Portal
        ↓
Ingestion Python
        ↓
Bronze (JSONL, données brutes)
        ↓
Traitement PySpark
        ↓
Silver (Parquet, données nettoyées)
        ↓
   ┌────┴─────────────┐
   ↓                  ↓
Rapport qualité   Agrégations Gold
(Quality)         (Gold, Parquet)
                      ↓
                Dashboard Streamlit

Airflow orchestre l'ensemble.
```

## Technologies

| Composant | Technologie |
|---|---|
| Ingestion | Python, Requests |
| Stockage | RustFS (API compatible S3) |
| Traitement | Apache Spark, PySpark |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit, Pandas, PyArrow |
| Environnement | Docker, Docker Compose |

## Le parcours des données

### Bronze : l'extraction

Un module Python interroge l'API par lots et enregistre les données brutes en JSONL, classées par année et par mois. Bronze garde les données exactement comme la source les fournit.

### Silver : le nettoyage

PySpark lit les fichiers Bronze, nettoie les textes, convertit les dates et les nombres, puis applique les contrôles qualité. Les enregistrements qui échouent aux contrôles critiques sont écartés dans une zone de quarantaine. Le résultat est enregistré en Parquet, partitionné par année et par mois.

### Quality : les rapports

Une tâche dédiée analyse Silver et calcule chaque mois le nombre d'enregistrements, les doublons et la part de données concernée par chaque anomalie.

### Gold : les données analytiques

PySpark produit trois jeux de données à partir de Silver :

- **Daily Revenue :** trajets, revenus et pourboires par jour.
- **Daily Trip Metrics :** durée et distance moyennes par jour.
- **Pickup Zone Performance :** trajets, revenus et classement mensuel des zones de prise en charge.

### Dashboard

Streamlit lit directement les données Gold et les rapports Quality dans RustFS. Il affiche les revenus, le volume de trajets, la durée et la distance, la performance des zones et les métriques qualité, avec un filtre par mois. Il ne relance aucun traitement Spark.

## Orchestration

Le DAG Airflow `chicago_taxi_pipeline` enchaîne quatre tâches, chacune démarrant seulement si la précédente a réussi :

| Ordre | Tâche | Rôle |
|---|---|---|
| 1 | `ingest_data` | Récupère les données et alimente Bronze |
| 2 | `bronze_to_silver` | Nettoie et valide les données |
| 3 | `silver_quality_report` | Calcule les indicateurs qualité |
| 4 | `silver_to_gold` | Produit les données analytiques |

Le DAG se lance manuellement.

## Stockage

Tout est stocké dans un seul bucket RustFS, `chicago-taxi-lake` :

```text
chicago-taxi-lake/
├── bronze/                       (JSONL)
├── silver/taxi_trips/            (Parquet)
├── quarantine/taxi_trips/        (Parquet)
├── quality/silver/               (Parquet)
└── gold/
    ├── daily_revenue/
    ├── daily_trip_metrics/
    └── pickup_zone_performance/
```

## Déploiement

Docker Compose démarre Airflow, RustFS et Streamlit. PySpark tourne directement dans l'environnement Airflow. Les volumes Docker conservent les données après un redémarrage des conteneurs.

## Principes de conception

- **Couches séparées :** données brutes, nettoyées et analytiques ne se mélangent pas.
- **Reproductibilité :** les transformations repartent toujours des données Bronze.
- **Traçabilité :** les rapports qualité et la quarantaine permettent de retrouver les anomalies.
- **Ingestion idempotente :** les lots déjà validés ne sont pas extraits une seconde fois.
- **Dashboard indépendant :** il lit les résultats sans dépendre de Spark.

Pour le détail des champs et des indicateurs, consultez le [dictionnaire de données](data_dictionary.md).d