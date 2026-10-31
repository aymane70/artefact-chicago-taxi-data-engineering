# Choix techniques — Pipeline de données Chicago Taxi

Ce document présente les principaux choix techniques du pipeline Chicago Taxi. Il explique les décisions prises pour l’ingestion, le stockage, les transformations, la qualité des données, l’orchestration et la production des données analytiques.

Pour chaque choix, je présente les alternatives possibles, les raisons de la solution retenue et ses limites.

---

## 1. Source des données et stratégie d’extraction

### 1.1 Source et périmètre de traitement

Le pipeline utilise le jeu de données *Chicago Taxi Trips*, disponible sur le portail open data de la ville de Chicago.

- **ID du dataset :** `wrvz-psew`
- **API :** https://data.cityofchicago.org/resource/wrvz-psew.json
- **Période traitée :** septembre à décembre 2023.

L'implémentation couvre quatre mois consécutifs et complets :

| Mois | Nombre d'enregistrements |
|---|---:|
| Septembre 2023 | 558 682 |
| Octobre 2023 | 606 474 |
| Novembre 2023 | 527 797 |
| Décembre 2023 | 465 053 |
| **Total** | **2 158 006** |

**Pourquoi cette période ?**

Quatre mois de données représentent un volume suffisant pour mettre en place et tester les différentes étapes du pipeline : ingestion, transformation, contrôle qualité et agrégation.

Cette période permet également de comparer les résultats d'un mois à l'autre, sans avoir à extraire et traiter une année entière ou l'historique complet du dataset.

D'autres périmètres étaient possibles :

- **Un mois :** exécution plus rapide et volume réduit, mais moins de possibilités de comparaison.
- **Quatre mois :** plus de 2,15 millions d'enregistrements, avec un volume qui reste raisonnable pour une exécution locale.
- **Une année ou plus :** couverture historique plus large, mais davantage de temps d'extraction, de stockage et de traitement.

Le pipeline traite donc cette période mois par mois.

### 1.2 Stratégie de pagination

**Choix retenu :** utiliser une pagination par offset avec des lots de 10 000 enregistrements.

Les requêtes API utilisent :

- `$limit=10000` pour fixer le nombre maximum d'enregistrements par requête.
- `$offset` pour récupérer les lots suivants.
- Un tri basé sur `trip_start_timestamp` et `trip_id`.

**Pourquoi utiliser la pagination ?**

Récupérer plus de deux millions d'enregistrements dans une seule requête présenterait plusieurs risques :

- Des réponses trop volumineuses et une consommation mémoire importante.
- Des temps de réponse plus longs et davantage de risques de timeout.
- Une récupération plus coûteuse en cas d'échec.
- Une gestion plus compliquée de l'extraction et de la reprise.

La pagination permet de diviser le travail en plusieurs requêtes indépendantes, plus faciles à suivre et à relancer.

**Pourquoi 10 000 enregistrements par lot ?**

Ce volume représente un compromis entre le nombre de requêtes et la taille des réponses.

| Taille du lot | Avantages | Limites |
|---|---|---|
| 1 000 | Réponses plus petites et reprise plus simple | Beaucoup plus de requêtes |
| 5 000 | Taille de réponse modérée | Plus de requêtes qu'avec l'approche retenue |
| **10 000** | Bon équilibre entre nombre de requêtes et taille des réponses | Nécessite toujours une gestion des erreurs et des validations |
| 50 000 ou plus | Moins de requêtes | Réponses plus lourdes, davantage de mémoire et des temps de réponse potentiellement plus longs |

J'ai retenu une taille de 10 000 pour limiter le nombre d'appels API tout en gardant des réponses faciles à gérer.

L'extraction a ainsi produit **217 lots** sur les quatre mois.

### 1.3 Stabilité de l'ordre des données

Avec une pagination par offset, un changement dans l'ordre des résultats entre deux requêtes peut entraîner des doublons ou des enregistrements manquants.

Pour limiter ce risque, les requêtes utilisent le tri suivant :

```sql
ORDER BY trip_start_timestamp ASC, trip_id ASC
```

Le timestamp définit l'ordre chronologique et `trip_id` sert de second critère lorsque plusieurs trajets ont le même timestamp.

Cela rend la pagination plus déterministe.

### 1.4 Extraction mensuelle

**Choix retenu :** extraire les données mois par mois plutôt que de traiter toute la période en une seule extraction.

Cette approche permet de :

- Suivre plus facilement l'avancement de l'extraction.
- Vérifier les volumes récupérés pour chaque mois.
- Reprendre l'extraction à partir d'un mois précis.
- Organiser les données Bronze selon la période concernée.
- Inspecter ou retraiter un mois indépendamment des autres.

Les bornes temporelles utilisées incluent le début du mois et excluent le début du mois suivant. Cela évite les chevauchements entre deux périodes.

---

## 2. Stockage Bronze et gestion des lots

### 2.1 Conservation des données brutes

**Choix retenu :** stocker les réponses de l'API au format JSONL dans la couche Bronze.

Chaque lot est enregistré dans un objet distinct, organisé par année et par mois.

Exemple :

```text
bronze/
└── year=2023/
    ├── month=09/
    │   ├── batch_00000.jsonl
    │   ├── batch_00001.jsonl
    │   └── _manifest.json
    ├── month=10/
    ├── month=11/
    └── month=12/
```

**Pourquoi le format JSONL ?**

Le format JSONL stocke un enregistrement JSON par ligne.

Par rapport à un seul gros fichier JSON pour tout un mois, il offre plusieurs avantages :

- Les données sont organisées par enregistrement.
- Les lots sont plus faciles à inspecter.
- Le format est directement exploitable par Spark.
- La représentation d'origine des données est conservée.
- La reprise et le retraitement sont plus simples.

La couche Bronze conserve les données telles qu'elles proviennent de la source. Le nettoyage et la conversion des types sont réalisés dans les étapes suivantes, sans modifier les données brutes.

### 2.2 Validation des lots et idempotence

**Choix retenu :** conserver un manifeste pour chaque mois avec les informations de chaque lot.

Les manifestes contiennent notamment :

- Les identifiants des lots.
- Le nombre d'enregistrements.
- Les checksums.
- Le statut de l'ingestion.

Avant de télécharger ou d'écrire un lot, l'extracteur vérifie si celui-ci existe déjà et si son contenu est valide.

Si les métadonnées et le contenu sont conformes, le lot est ignoré. S'il est absent ou invalide, il peut être extrait à nouveau.

**Pourquoi cette approche ?**

Sans suivi des lots, une extraction interrompue pourrait entraîner des téléchargements inutiles ou des doublons lors de sa reprise.

Les manifestes permettent donc de :

- Rendre l'ingestion idempotente.
- Reprendre une extraction après une interruption.
- Vérifier l'intégrité des lots.
- Suivre l'avancement de l'extraction.

### 2.3 Gestion des tentatives

**Choix retenu :** prévoir plusieurs tentatives en cas d'erreur temporaire de l'API ou du réseau.

Une extraction paginée de cette taille peut rencontrer des problèmes de connexion, des timeouts ou des réponses API incorrectes.

Les nouvelles tentatives permettent de relancer uniquement les requêtes concernées, sans recommencer toute l'extraction.

Les erreurs persistantes restent visibles afin de pouvoir les analyser.

---

## 3. Technologie de stockage

### 3.1 Utilisation de RustFS

**Choix retenu :** utiliser RustFS comme solution de stockage objet.

RustFS propose une interface compatible avec S3. Les différents composants du pipeline peuvent donc accéder aux données à travers des clients S3 standards et le connecteur Hadoop S3A.

**Alternatives envisagées :**

| Solution | Avantages | Limites |
|---|---|---|
| Système de fichiers local | Simple à configurer | Couplage plus important entre le traitement et le stockage local |
| MinIO | Stockage objet compatible S3 | Nécessite un service et une configuration supplémentaires |
| **RustFS** | Stockage objet compatible S3, déployable localement avec Docker | Nécessite de configurer S3A et son endpoint |
| Stockage cloud | Solution managée et évolutive | Nécessite une configuration cloud et peut entraîner des coûts |

**Pourquoi RustFS ?**

Il permet de mettre en place une architecture de stockage objet sans dépendre d'un compte cloud.

Il fournit également une interface commune pour l'ingestion, Spark et le dashboard.

Le pipeline utilise l'endpoint suivant :

```text
S3A endpoint: http://rustfs:9000
```

Le connecteur S3A est configuré avec la dépendance Hadoop AWS, l'endpoint RustFS, les identifiants d'accès et les paramètres nécessaires au mode *path-style*.

---

## 4. Architecture Medallion et formats de stockage

**Choix retenu :** organiser le Data Lake en plusieurs couches : Bronze, Silver et Gold.

| Couche | Rôle | Format |
|---|---|---|
| Bronze | Conserver les données brutes de l'API | JSONL |
| Silver | Nettoyer, convertir, valider et enrichir les trajets | Parquet |
| Gold | Fournir les données analytiques | Parquet |
| Quality | Stocker les rapports agrégés de qualité | Parquet |
| Quarantine | Conserver les enregistrements structurellement invalides | Parquet |

**Pourquoi séparer les couches ?**

Cette séparation permet de distinguer clairement l'ingestion, le nettoyage et la préparation des données analytiques.

Elle apporte plusieurs avantages :

- Les données brutes restent disponibles pour un nouveau traitement.
- Les transformations peuvent évoluer sans relancer l'extraction API.
- Les contrôles qualité sont clairement identifiés.
- Les données analytiques ne dépendent pas directement de la structure brute de la source.
- Chaque couche a un rôle précis.

### Pourquoi utiliser Parquet pour Silver et Gold ?

JSONL convient bien à la conservation des données brutes, mais il est moins adapté aux requêtes analytiques répétées.

Parquet offre notamment :

- Un stockage en colonnes.
- De la compression.
- La lecture ciblée de certaines colonnes.
- De meilleures performances pour les agrégations.
- Une bonne compatibilité avec Spark et PyArrow.

Le pipeline conserve donc le JSONL dans Bronze et utilise Parquet pour les couches de traitement et d'analyse.

---

## 5. Transformations et traitement des données

### 5.1 Utilisation de PySpark

**Choix retenu :** utiliser PySpark pour les transformations Bronze vers Silver et Silver vers Gold.

**Alternatives envisagées :**

- Pandas pour le traitement en mémoire.
- SQL pour les transformations.
- PySpark DataFrames pour le traitement structuré et distribué.

**Pourquoi PySpark ?**

Le dataset contient plus de deux millions d'enregistrements, avec des champs imbriqués, des valeurs numériques représentées sous forme de chaînes de caractères et plusieurs règles de validation.

PySpark permet de :

- Manipuler les données avec des DataFrames.
- Définir un schéma et convertir les types.
- Appliquer des filtres et des conditions.
- Utiliser des fenêtres et des agrégations.
- Écrire et lire des fichiers Parquet.
- Garder un même environnement de traitement entre Silver et Gold.

Le déploiement actuel utilise Spark en mode local à l'intérieur du conteneur Airflow, sans cluster Spark séparé.

Cela évite d'ajouter de l'infrastructure tout en conservant les capacités de traitement de Spark.

### 5.2 Gestion des types

Dans les données sources, certains nombres et timestamps sont représentés sous forme de chaînes de caractères.

Le pipeline applique un schéma explicite et convertit les champs vers les types attendus.

Exemples :

| Champ | Type cible |
|---|---|
| `trip_start_timestamp` | TIMESTAMP |
| `trip_end_timestamp` | TIMESTAMP |
| `trip_seconds` | INT |
| `trip_miles` | DOUBLE |
| `fare` | DECIMAL(12,2) |
| `trip_total` | DECIMAL(12,2) |

L'utilisation de `try_cast` permet de gérer les conversions invalides sans faire échouer toute la transformation à cause d'une seule valeur mal formée.

Les valeurs numériques d'origine sont temporairement conservées dans des colonnes de diagnostic afin de repérer les erreurs de conversion.

### 5.3 Partitionnement de Silver

**Choix retenu :** partitionner les données Silver par année et par mois.

Exemple :

```text
silver/taxi_trips/
├── year=2023/
│   ├── month=09/
│   ├── month=10/
│   ├── month=11/
│   └── month=12/
```

Ce partitionnement permet de :

- Organiser les données selon la période traitée.
- Faciliter les filtres par mois.
- Rendre la structure de stockage plus lisible.
- Préparer une éventuelle évolution vers un traitement incrémental.

---

## 6. Qualité des données et gestion des anomalies

### 6.1 Erreurs structurelles et indicateurs qualité

**Choix retenu :** distinguer les erreurs structurelles critiques des autres anomalies de qualité.

Deux approches étaient possibles :

1. Mettre en quarantaine chaque enregistrement qui ne respecte pas une règle de qualité.
2. Mettre en quarantaine uniquement les enregistrements structurellement invalides et conserver les autres avec des indicateurs qualité.

J'ai retenu la deuxième approche.

**Pourquoi ?**

Un trajet peut contenir un montant financier suspect ou une durée invalide tout en restant exploitable pour certaines analyses.

Écarter systématiquement tous les enregistrements présentant une anomalie réduirait inutilement le volume de données disponible.

### 6.2 Contrôles critiques

Un enregistrement est envoyé en quarantaine dans les cas suivants :

- `trip_id` est manquant.
- `trip_start_timestamp` est manquant ou ne peut pas être converti en timestamp valide.

Ces deux champs sont nécessaires pour identifier un trajet et le rattacher à une date.

### 6.3 Contrôles non bloquants

Les autres anomalies sont représentées par des indicateurs dans Silver :

- Échec de conversion numérique.
- Valeurs négatives.
- Informations de durée manquantes.
- Durées négatives.
- Durées potentiellement ambiguës à cause du changement d'heure.
- Informations financières incomplètes.
- Écarts dans le calcul du montant total.

Les enregistrements concernés restent dans Silver.

Cette approche distingue la validité structurelle de l'éligibilité aux analyses. Chaque dataset Gold peut ainsi appliquer ses propres règles.

### 6.4 Vérification des montants financiers

Le pipeline compare le montant total du trajet à la somme du tarif, des pourboires, des péages et des frais supplémentaires.

```text
financial_difference =
    trip_total - (fare + tips + tolls + extras)
```

Un écart est signalé lorsque :

```text
ABS(financial_difference) > 0.02
```

**Pourquoi une tolérance de 0,02 $ ?**

De petits écarts d'arrondi ne doivent pas être considérés automatiquement comme des incohérences financières importantes.

Les montants d'origine sont conservés. Les écarts sont signalés dans les indicateurs qualité, sans correction automatique des valeurs.

### 6.5 Durées et changement d'heure

À Chicago, le passage à l'heure d'hiver a eu lieu le 5 novembre 2023. L'heure de 1 h du matin s'est donc répétée.

Dans cette situation, un intervalle négatif entre deux timestamps peut être ambigu.

Le pipeline applique une règle ciblée basée sur les conditions suivantes :

- La date de début du trajet est le 5 novembre 2023.
- Les heures de début et de fin sont toutes les deux à 1 h.
- Le timestamp de fin est antérieur à celui de début.

Les enregistrements concernés reçoivent un indicateur spécifique d'ambiguïté liée au changement d'heure.

Il s'agit d'une règle adaptée à la période traitée, et non d'un mécanisme général de reconstruction des fuseaux horaires.

### 6.6 Règles d'éligibilité selon les indicateurs

La couche Gold n'applique pas une seule règle d'éligibilité à tous les calculs.

Par exemple :

- Les calculs de revenus nécessitent un `trip_total` disponible.
- Les indicateurs de durée excluent les durées invalides, négatives, manquantes ou ambiguës.
- Les indicateurs de distance appliquent leurs propres conditions.

Cela évite d'exclure un trajet de toutes les analyses alors qu'il peut rester exploitable pour certains indicateurs.

---

## 7. Conception des datasets Gold

**Choix retenu :** produire trois datasets analytiques distincts plutôt qu'une seule grande table agrégée.

| Dataset | Objectif |
|---|---|
| `daily_revenue` | Suivre le volume de trajets et les revenus par jour |
| `daily_trip_metrics` | Analyser les durées et les distances par jour |
| `pickup_zone_performance` | Analyser les performances par zone de prise en charge |

**Pourquoi séparer les datasets ?**

Chaque dataset répond à un besoin analytique différent et peut avoir ses propres règles d'éligibilité.

Cette séparation permet de :

- Donner un rôle précis à chaque dataset.
- Éviter de mélanger des indicateurs qui n'ont pas les mêmes règles.
- Faciliter la maintenance des règles métier.
- Simplifier les requêtes et les visualisations du dashboard.
- Faire évoluer chaque dataset indépendamment.

Les résultats obtenus sont les suivants :

| Dataset | Nombre de lignes |
|---|---:|
| `daily_revenue` | 122 |
| `daily_trip_metrics` | 122 |
| `pickup_zone_performance` | 308 |

---

## 8. Orchestration

**Choix retenu :** utiliser Apache Airflow pour orchestrer le pipeline.

**Alternative :** exécuter les scripts Python et Spark manuellement ou les enchaîner avec un mécanisme d'exécution plus léger.

**Pourquoi Airflow ?**

Le pipeline comporte plusieurs étapes dépendantes les unes des autres. Airflow permet de définir clairement leur ordre d'exécution et de suivre leur état depuis une interface centralisée.

Le DAG contient quatre tâches exécutées dans l'ordre suivant :

1. `ingest_data`
2. `bronze_to_silver`
3. `silver_quality_report`
4. `silver_to_gold`

Airflow apporte notamment :

- Le suivi de l'état de chaque tâche.
- La gestion des dépendances.
- Les logs d'exécution.
- Le déclenchement manuel.
- Le suivi indépendant des différentes étapes.

Le DAG utilise `schedule=None`, car il traite un historique de données défini et doit être déclenché manuellement.

---

## 9. Dashboard et consommation des données

**Choix retenu :** utiliser Streamlit pour le dashboard et PyArrow pour lire les fichiers Parquet déjà produits.

Le dashboard accède directement aux données Gold et Quality stockées dans RustFS, à travers son interface compatible S3.

**Pourquoi ne pas lancer Spark depuis le dashboard ?**

Le dashboard sert à consulter les résultats, pas à exécuter les transformations.

Relancer Spark à chaque ouverture ou filtrage du dashboard entraînerait des traitements inutiles et créerait une dépendance entre l'affichage et l'exécution du pipeline.

L'approche retenue est donc la suivante :

- Airflow orchestre les transformations.
- Spark produit les datasets Parquet.
- Les résultats sont stockés dans RustFS.
- Streamlit lit les données existantes.
- PyArrow permet d'accéder directement aux fichiers stockés.

Le traitement et la visualisation restent ainsi indépendants.

---

## 10. Exécution et performances

### 10.1 Première exécution

La première exécution peut prendre plus de temps pour plusieurs raisons :

- L'API doit fournir plus de deux millions d'enregistrements.
- Spark doit résoudre et télécharger ses dépendances Hadoop AWS et AWS SDK.
- Les images Docker et les services doivent être initialisés.

Les exécutions suivantes peuvent réutiliser les dépendances téléchargées, à condition que le cache Ivy/Maven soit conservé.

### 10.2 Gestion de la mémoire Spark

La transformation Bronze vers Silver traite un volume important de fichiers JSONL et applique plusieurs règles de validation.

Pendant les tests, cette étape a rencontré une erreur `Java heap space`.

J'ai donc ajusté la configuration Spark pour réduire la pression sur la mémoire :

| Configuration | Objectif |
|---|---|
| `local[2]` | Limiter le nombre de tâches exécutées en parallèle |
| Driver de 3 GB | Définir la mémoire allouée au driver Spark |
| Partitions d'entrée de 32 MB | Réduire le volume de données traité par partition de fichier |
| 8 partitions de shuffle | Contrôler le nombre de partitions utilisé par défaut pour les opérations de shuffle SQL |
| Adaptive Query Execution | Permettre l'optimisation du plan d'exécution pendant le traitement |
| Persistance `DISK_ONLY` | Réutiliser les données transformées sans conserver tout le dataset en mémoire JVM |
| `coalesce(8)` | Réduire le nombre de partitions avant l'écriture, sans effectuer un repartitionnement complet |

Ces ajustements visent à limiter la consommation mémoire et les calculs répétés, tout en conservant les transformations et les règles qualité.

Cette configuration est adaptée au déploiement local avec Docker. Elle ne constitue pas une configuration Spark universelle pour un environnement de production.

---

## 11. Résultats de l'implémentation

Le pipeline a produit les résultats suivants :

| Indicateur | Résultat |
|---|---:|
| Période d'extraction | Septembre à décembre 2023 |
| Nombre total d'enregistrements ingérés | 2 158 006 |
| Nombre de lots d'ingestion | 217 |
| Enregistrements dans Silver | 2 158 006 |
| Enregistrements en quarantaine | 0 |
| Lignes dans `daily_revenue` | 122 |
| Lignes dans `daily_trip_metrics` | 122 |
| Lignes dans `pickup_zone_performance` | 308 |

L'analyse qualité a identifié des anomalies de durée, des informations financières incomplètes et des écarts dans les montants totaux.

Ces anomalies ont été conservées et signalées conformément aux règles qualité définies dans le pipeline.

L'exécution complète du DAG Airflow a abouti avec succès.

---

## 12. Périmètre et améliorations possibles

L'implémentation actuelle est conçue comme un projet de Data Engineering batch, reproductible en environnement local.

Certaines technologies et fonctionnalités ont volontairement été laissées en dehors du périmètre actuel :

| Technologie ou fonctionnalité | Statut | Raison |
|---|---|---|
| Kafka | Non implémenté | La source est une API REST historique et le pipeline fonctionne en batch |
| Kubernetes | Non implémenté | Docker Compose suffit pour le déploiement local |
| MLflow | Non implémenté | Le projet se concentre sur le Data Engineering, pas sur l'entraînement de modèles |
| Cluster Spark séparé | Non implémenté | L'exécution locale de Spark suffit pour le volume et le périmètre actuels |
| Stockage cloud managé | Non implémenté | RustFS fournit un stockage objet local compatible S3 |

Plusieurs évolutions sont possibles :

- Ajouter un traitement incrémental pour Silver et Gold.
- Mettre à jour uniquement les partitions concernées au lieu de tout recalculer.
- Définir des seuils qualité et des conditions d'échec automatiques.
- Améliorer l'observabilité et les alertes.
- Déployer le stockage et l'orchestration sur des services cloud.
- Ajouter des tests automatisés pour les transformations et les règles qualité.

Ces éléments sont des pistes d'évolution et ne font pas partie de l'implémentation actuelle.

Le choix final privilégie une ingestion batch fiable et reprenable, la conservation des données brutes, des règles qualité explicites, des transformations reproductibles et des données analytiques faciles à consulter.


**Configuration simplifiée**

Pour faciliter le démarrage du projet, les services utilisent directement les paramètres définis dans `docker-compose.yml`, sans nécessiter de fichier `.env` ni de configuration supplémentaire. Ce choix permet de lancer l'ensemble du pipeline avec une seule commande et de simplifier sa reproduction dans un environnement local.

Cette configuration est adaptée à une démonstration et à un projet local. Pour un déploiement en production, les identifiants et paramètres sensibles devraient être externalisés.