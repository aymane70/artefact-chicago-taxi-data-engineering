# Pipeline Data Engineering Chicago Taxi

Un projet de data engineering complet, construit avec Python, Apache Airflow, PySpark, RustFS et Streamlit.

Le pipeline récupère les trajets de taxis de Chicago sur le portail officiel de la ville et les range dans un data lake compatible S3. Les données sont ensuite transformées selon une architecture Medallion. Avant de produire les données analytiques qui alimentent le dashboard, plusieurs contrôles qualité sont effectués.

## Résultats

Le pipeline traite les données de septembre à décembre 2023.

| Indicateur | Résultat |
|---|---:|
| Enregistrements ingérés | 2 158 006 |
| Lots d'ingestion | 217 |
| Enregistrements Silver | 2 158 006 |
| Enregistrements en quarantaine | 0 |
| Revenus journaliers | 122 lignes |
| Indicateurs journaliers | 122 lignes |
| Performance des zones | 308 lignes |

## Documentation

Le dossier `docs/` regroupe les détails techniques du projet :

- [Architecture](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Technical Decisions](docs/technical_decisions.md)

## Installation et démarrage

### Prérequis

- Docker Desktop avec WSL2 sous Windows
- Git
- 8 Go de RAM recommandés
- Une connexion Internet

Pas besoin d'installer Spark séparément, ni d'avoir un compte cloud.

### 1. Récupérer le projet

```powershell
git clone https://github.com/aymane70/artefact-chicago-taxi-data-engineering.git
cd artefact-chicago-taxi-data-engineering
```

### 2. Démarrer les services

Assurez-vous que Docker Desktop est lancé, puis exécutez :

```powershell
docker compose up --build -d
```

Pour vérifier que les conteneurs ont bien démarré :

```powershell
docker compose ps
```

Cette commande lance Airflow, RustFS et Streamlit. Aucune configuration supplémentaire n'est requise et il n'y a pas de fichier `.env` à créer : tous les paramètres se trouvent déjà dans `docker-compose.yml`.

### 3. Accéder aux interfaces

| Service | URL |
|---|---|
| Airflow | [http://localhost:8080](http://localhost:8080) |
| Console RustFS | [http://localhost:9001](http://localhost:9001) |
| Dashboard Streamlit | [http://localhost:8501](http://localhost:8501) |

**Airflow**

Si une authentification vous est demandée, consultez les logs :

```powershell
docker compose logs airflow
```

**RustFS**

Identifiants de démonstration :

```text
Username: CHICAGO_TAXI_ADMIN
Password: ChicagoTaxi_DE_Project_2026_Key
```

Le bucket utilisé est `chicago-taxi-lake`.

**Dashboard**

Le dashboard présente les résultats Gold et les rapports qualité stockés dans RustFS. Il devient utilisable une fois le pipeline exécuté.

## Exécuter le pipeline

1. Ouvrez Airflow et cherchez le DAG `chicago_taxi_pipeline`.
2. Activez-le s'il est en pause.
3. Cliquez sur **Trigger DAG**.
4. Suivez l'avancement des quatre tâches :

| Tâche | Rôle |
|---|---|
| `ingest_data` | Extrait les données vers la couche Bronze |
| `bronze_to_silver` | Nettoie et valide les données |
| `silver_quality_report` | Génère les rapports qualité |
| `silver_to_gold` | Produit les données analytiques |

**Bon à savoir :** l'API de Chicago peut être très sollicitée ou répondre lentement. Il est donc normal de voir des nouvelles tentatives (*retries*) pendant l'ingestion. Si une tâche échoue malgré tout, ouvrez ses logs dans Airflow pour comprendre ce qui s'est passé.

Quand le DAG est terminé, les résultats se trouvent dans le bucket RustFS et s'affichent sur le dashboard Streamlit.

## Durée d'exécution

- **Ingestion :** environ 20 minutes, selon les réponses de l'API et votre réseau.
- **Transformations Spark :** le premier lancement peut prendre plus de temps, car les dépendances doivent d'abord être téléchargées.

La durée totale varie selon les conditions d'exécution et les ressources de votre machine.

## Arrêter les services

Pour arrêter les conteneurs en gardant vos données :

```powershell
docker compose down
```

Pour relancer le projet ensuite :

```powershell
docker compose up -d
```

Pour tout supprimer, volumes et données stockées compris :

```powershell
docker compose down -v
```

## Source des données

Le projet s'appuie sur le jeu de données officiel [Chicago Taxi Trips](https://data.cityofchicago.org/resource/wrvz-psew.json).

- Identifiant : `wrvz-psew`
- Période : de septembre à décembre 2023
- Taille d'un lot : 10 000 enregistrements