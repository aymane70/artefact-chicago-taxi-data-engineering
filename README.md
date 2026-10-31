# Pipeline Data Engineering — Chicago Taxi

Projet de data engineering de bout en bout développé avec Python, Apache Airflow, PySpark, RustFS et Streamlit.

Le pipeline récupère les données de trajets de taxis de Chicago depuis le portail officiel de la ville, les stocke dans un data lake compatible S3, puis les transforme selon une architecture Medallion. Des règles de qualité sont appliquées avant de produire les jeux de données utilisés par le dashboard.

##  Documentation complémentaire

Le dossier `docs/` contient trois documents :

- **[Architecture](docs/architecture.md) :** présente les composants du système et la circulation des données.
- **[Data Dictionary](docs/data_dictionary.md) :** décrit les jeux de données, les champs, les types et les sorties analytiques.
- **[Technical Decisions](docs/technical_decisions.md) :** détaille les choix techniques, les alternatives étudiées et les pistes d'amélioration.

---

##  Présentation du projet

Le pipeline traite quatre mois de données, de septembre à décembre 2023.

**Résultats obtenus :**

| Indicateur | Résultat |
|---|---:|
| Période traitée | Septembre–décembre 2023 |
| Enregistrements ingérés | 2 158 006 |
| Lots d'ingestion | 217 |
| Enregistrements Silver | 2 158 006 |
| Enregistrements en quarantaine | 0 |
| Lignes — revenus journaliers | 122 |
| Lignes — indicateurs journaliers | 122 |
| Lignes — performance des zones | 308 |

### Architecture

```text
              Chicago Data Portal API
                         |
                         v
                Extraction Python
                         |
                         v
                  RustFS - Bronze
                    Données JSONL
                         |
                         v
                    Apache Airflow
                         |
                         v
                      PySpark
                         |
                +--------+--------+
                |                 |
                v                 v
          RustFS - Silver    Quarantine
              Parquet          Parquet
                |
                v
          Rapports qualité
                |
                v
              PySpark
                |
                v
           RustFS - Gold
              Parquet
                |
                v
          Dashboard Streamlit
```

### Technologies utilisées

- **Python :** extraction depuis l'API et logique du pipeline.
- **Apache Airflow :** orchestration et suivi des tâches.
- **PySpark :** nettoyage, validation et transformations.
- **RustFS :** stockage objet compatible S3.
- **JSONL et Parquet :** formats de stockage.
- **Streamlit :** dashboard interactif.
- **PyArrow :** lecture des fichiers Parquet depuis RustFS.

---

##  Installation et démarrage

### Prérequis

Avant de commencer, il faut disposer de :

- Docker Desktop avec WSL2 activé sous Windows.
- Git.
- Au moins 8 Go de RAM disponibles sont recommandés.
- Une connexion Internet pour la construction des images, l'extraction des données et le téléchargement des dépendances Spark.

Aucune installation séparée de Spark ni aucun compte cloud ne sont nécessaires.

### Étape 1 — Récupérer le projet

Ouvrir PowerShell ou un terminal :

```powershell
git clone https://github.com/aymane70/artefact-chicago-taxi-data-engineering.git
cd artefact-chicago-taxi-data-engineering
```

### Étape 2 — Démarrer les services

Vérifier que Docker Desktop est lancé, puis exécuter :

```powershell
docker compose up --build -d
```

Pour vérifier que les conteneurs sont démarrés :

```powershell
docker compose ps
```

Trois services principaux sont lancés :

| Service | Rôle |
|---|---|
| Airflow | Orchestration du pipeline |
| RustFS | Stockage des données |
| Streamlit | Visualisation des résultats |

Le premier démarrage peut prendre quelques minutes, le temps de construire les images et d'initialiser les services.


**Configuration simplifiée**

Pour faciliter le démarrage du projet, les services utilisent directement les paramètres définis dans `docker-compose.yml`, sans nécessiter de fichier `.env` ni de configuration supplémentaire. Ce choix permet de lancer l'ensemble du pipeline avec une seule commande et de simplifier sa reproduction dans un environnement local.

Cette configuration est adaptée à une démonstration et à un projet local. Pour un déploiement en production, les identifiants et paramètres sensibles devraient être externalisés.

### Étape 3 — Accéder aux interfaces

| Interface | URL | Utilisation |
|---|---|---|
| Airflow | [http://localhost:8080](http://localhost:8080) | Exécuter et suivre le pipeline |
| Console RustFS | [http://localhost:9001](http://localhost:9001) | Parcourir le data lake |
| Dashboard Streamlit | [http://localhost:8501](http://localhost:8501) | Consulter les résultats |

**Airflow**

Accéder à [http://localhost:8080](http://localhost:8080).

Si une authentification est demandée, les identifiants générés peuvent être consultés dans les logs :

```powershell
docker compose logs airflow
```

**RustFS**

Accéder à [http://localhost:9001](http://localhost:9001).

Les identifiants de démonstration configurés dans `docker-compose.yml` sont :

```text
Username: CHICAGO_TAXI_ADMIN
Password: ChicagoTaxi_DE_Project_2026_Key
```

Le bucket utilisé est :

```text
chicago-taxi-lake
```

Après l'exécution du pipeline, son contenu est organisé comme suit :

```text
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

**Dashboard Streamlit**

Accéder à [http://localhost:8501](http://localhost:8501).

Le dashboard lit directement les données Gold et les rapports qualité stockés dans RustFS. Il ne relance pas les transformations Spark.

Il permet notamment de consulter :

- Les indicateurs de revenus et leur évolution journalière.
- Le volume et la durée des trajets.
- Les métriques liées aux distances.
- Le classement des zones de prise en charge.
- Les rapports qualité mensuels.
- Les résultats filtrés par mois.

Le dashboard affiche les résultats une fois le pipeline exécuté et les données Gold disponibles.

---

##  Exécution du pipeline

### Étape 1 — Ouvrir Airflow

Accéder à [http://localhost:8080](http://localhost:8080) et rechercher le DAG :

```text
chicago_taxi_pipeline
```

Il contient quatre tâches exécutées dans l'ordre :

| Tâche | Description |
|---|---|
| `ingest_data` | Récupère les données et les stocke dans Bronze |
| `bronze_to_silver` | Nettoie, convertit et valide les données, puis sépare Silver et Quarantine |
| `silver_quality_report` | Calcule et enregistre les indicateurs de qualité |
| `silver_to_gold` | Génère les jeux de données analytiques |

### Étape 2 — Lancer le DAG

1. Rechercher `chicago_taxi_pipeline`.
2. Activer le DAG s'il est en pause.
3. Ouvrir le DAG.
4. Cliquer sur **Trigger DAG**.
5. Suivre l'exécution dans les vues Grid ou Graph.

Chaque tâche démarre uniquement lorsque la précédente s'est terminée correctement.

### Étape 3 — Suivre l'exécution

Les logs de chaque tâche permettent de suivre :

- L'avancement de l'extraction API.
- Le nombre d'enregistrements et de lots traités.
- Les chemins de stockage Bronze, Silver et Quarantine.
- Les transformations Spark.
- Les résultats des contrôles qualité.
- La génération des données Gold.

En cas d'échec, consulter les logs de la tâche concernée avant de la relancer.

### Étape 4 — Vérifier les résultats

Une fois le DAG terminé :

1. Ouvrir RustFS et accéder au bucket `chicago-taxi-lake`.
2. Vérifier les dossiers Bronze, Silver, Quality et Gold.
3. Ouvrir le dashboard Streamlit pour explorer les résultats.

---

##  Durée d'exécution et résultats attendus

La première exécution peut être plus longue que les suivantes.

| Étape | Durée et fonctionnement |
|---|---|
| `ingest_data` | Environ 20 minutes pour récupérer plus de 2 millions d'enregistrements par lots. La durée dépend notamment des réponses de l'API et du réseau. |
| `bronze_to_silver` | Peut prendre plus de temps lors du premier lancement, notamment à cause du téléchargement des dépendances Hadoop et S3A de Spark. |
| `silver_quality_report` | Calcule les indicateurs qualité par mois et enregistre les rapports. |
| `silver_to_gold` | Agrège les données Silver pour produire les trois jeux de données Gold. |

### Résultats attendus

| Indicateur | Résultat |
|---|---:|
| Enregistrements Bronze | 2 158 006 |
| Enregistrements Silver | 2 158 006 |
| Enregistrements en quarantaine | 0 |
| Revenus journaliers | 122 lignes |
| Indicateurs journaliers | 122 lignes |
| Performance des zones | 308 lignes |

Les rapports qualité signalent également des anomalies de durée, des informations financières incomplètes et des écarts de réconciliation. Ces observations sont conservées et prises en compte selon les règles définies dans le pipeline.

### Quelques points à retenir

- Garder les services Docker actifs pendant toute l'exécution.
- Le premier lancement de Spark peut nécessiter le téléchargement de dépendances supplémentaires.
- Les dépendances peuvent être réutilisées lors des exécutions suivantes si le cache est conservé.
- Les manifestes d'ingestion permettent d'identifier les lots déjà traités et de reprendre une extraction interrompue.
- Consulter les logs Airflow si une tâche prend plus de temps que prévu.
- Éviter de supprimer les volumes Docker si les données doivent être conservées.

---

##  Arrêt des services

Pour arrêter les conteneurs :

```powershell
docker compose down
```

Cette commande supprime les conteneurs, mais conserve les volumes Docker nommés, notamment les données stockées dans RustFS.

Pour redémarrer le projet :

```powershell
docker compose up -d
```

**Attention :** la commande suivante supprime également les volumes et toutes les données qu'ils contiennent :

```powershell
docker compose down -v
```

À utiliser uniquement si un environnement vierge est souhaité et que les données Bronze, Silver et Gold peuvent être supprimées.

---

##  Organisation du data lake

Le projet suit une architecture Medallion composée de trois couches :

| Couche | Rôle |
|---|---|
| Bronze | Conserve les données brutes provenant de la source |
| Silver | Nettoie, normalise et valide les enregistrements |
| Gold | Regroupe les données pour les analyses |

Deux espaces complémentaires sont également utilisés :

- **Quality :** stocke les rapports mensuels de qualité.
- **Quarantine :** conserve les enregistrements présentant des problèmes structurels.

### Formats de stockage

| Espace | Format | Utilisation |
|---|---|---|
| Bronze | JSONL | Conserver les données brutes dans un format lisible |
| Silver | Parquet | Faciliter les traitements analytiques |
| Gold | Parquet | Simplifier la consommation par les outils de reporting |
| Quality | Parquet | Stocker les indicateurs qualité |

Les données brutes restent ainsi séparées des données nettoyées et des résultats analytiques.

---

##  Contrôles de qualité

### Mise en quarantaine ou signalement

Deux approches sont possibles :

1. Rejeter tout enregistrement qui ne respecte pas une règle de qualité.
2. Mettre en quarantaine les enregistrements structurellement invalides et conserver les autres avec des indicateurs de qualité.

La deuxième approche est utilisée dans ce projet.

Les enregistrements sans identifiant de trajet exploitable ou sans date de début valide sont mis en quarantaine.

Les autres anomalies, comme les durées négatives, les informations financières incomplètes ou les écarts de réconciliation, sont signalées dans Silver.

Un enregistrement suspect n'est pas forcément inutilisable pour toutes les analyses. Le conserver avec son statut qualité permet aux traitements Gold d'appliquer des règles adaptées à chaque indicateur.

### Réconciliation financière

L'écart financier est calculé de la manière suivante :

```text
trip_total - (fare + tips + tolls + extras)
```

Un écart est signalé lorsque sa valeur absolue dépasse `0.02`.

Cette tolérance permet d'éviter de signaler de très petites différences liées aux arrondis. Les écarts sont conservés et reportés, plutôt que supprimés automatiquement.

### Règles selon les indicateurs

La couche Gold n'applique pas une règle unique à toutes les métriques.

Par exemple :

- Le calcul des revenus nécessite une valeur `trip_total` disponible.
- Les analyses de durée excluent les durées manquantes, négatives, invalides ou ambiguës à cause des changements d'heure (DST).
- Les analyses de distance appliquent leurs propres conditions.

Cette approche permet de conserver les enregistrements utiles sans compromettre la fiabilité des indicateurs.

---

##  Structure du projet

```text
artefact-chicago-taxi-data-engineering/
│
├── dags/
│   └── chicago_taxi_pipeline.py
│
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── technical_decisions.md
│
├── src/
│   ├── ingestion/
│   │   └── extract_taxi_data.py
│   ├── quality/
│   │   └── silver_quality_report.py
│   └── transformations/
│       ├── bronze_to_silver.py
│       ├── schemas.py
│       └── silver_to_gold.py
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---



##  Source des données

Le projet utilise le jeu de données officiel **Chicago Taxi Trips**, disponible sur le Chicago Data Portal.

- Identifiant : `wrvz-psew`
- API : [https://data.cityofchicago.org/resource/wrvz-psew.json](https://data.cityofchicago.org/resource/wrvz-psew.json)
- Période traitée : septembre à décembre 2023.
- Taille d'un lot : 10 000 enregistrements.

L'extraction est réalisée mois par mois, avec une pagination et un ordre stable basé sur `trip_start_timestamp` et `trip_id`.

---

##  Améliorations possibles

Plusieurs évolutions pourraient être ajoutées par la suite :

- Traitement incrémental des couches Silver et Gold.
- Mise à jour ciblée des partitions au lieu d'une reconstruction complète.
- Seuils qualité automatisés avec des conditions d'échec.
- Amélioration du suivi et des alertes.
- Déploiement sur un stockage cloud et un service d'orchestration managé.
- Tests automatisés des transformations et des règles qualité.

Ces éléments sont des pistes d'évolution et ne font pas partie de l'implémentation actuelle.