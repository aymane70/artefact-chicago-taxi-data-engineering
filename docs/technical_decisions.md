# Choix techniques

Ce document explique les principaux choix du pipeline et les compromis que j'ai faits. Les définitions des champs sont dans le [dictionnaire de données](data_dictionary.md), l'architecture dans [architecture.md](architecture.md).

## Source et périmètre

J'utilise le jeu de données Chicago Taxi Trips (`wrvz-psew`) via l'API du portail, de septembre à décembre 2023, soit **2 158 006 trajets**.

J'ai choisi quatre mois complets parce que c'est assez pour tester toutes les étapes et comparer les mois entre eux, tout en restant raisonnable pour une exécution locale. Un mois seul aurait limité les comparaisons, et une année entière aurait surtout allongé les temps d'extraction.

Je passe par l'API plutôt que par le fichier CSV complet, qui pèse plusieurs dizaines de Go. L'extraction se fait mois par mois, ce qui permet de contrôler les volumes de chaque mois et de reprendre à un mois précis.

## Ingestion

**Pagination par lots de 10 000.** Une seule requête de plus de deux millions de lignes serait lourde et risquée (mémoire, timeouts, reprise impossible). 10 000 est un bon compromis : des réponses faciles à gérer sans multiplier les appels. L'extraction produit 217 lots.

**Tri stable.** La pagination par offset peut créer des doublons ou des trous si l'ordre change entre deux requêtes. Je trie donc par `trip_start_timestamp` puis `trip_id`.

**Reprise sans doublons.** Chaque mois a un manifeste qui liste ses lots avec leur nombre d'enregistrements et leur checksum. Avant d'extraire un lot, le pipeline vérifie s'il existe déjà et s'il est valide. Une extraction interrompue peut ainsi reprendre sans tout retélécharger.

**Nouvelles tentatives.** Les erreurs temporaires de l'API ou du réseau déclenchent des retries. Les erreurs persistantes restent visibles dans les logs Airflow.

## Stockage

**RustFS.** C'est un stockage objet compatible S3 qui tourne en local avec Docker. Il me donne une vraie architecture de data lake sans compte cloud, et le même accès pour l'ingestion, Spark et le dashboard. Le compromis est qu'il faut configurer le connecteur S3A de Spark.

**JSONL pour Bronze, Parquet ensuite.** Le JSONL conserve les données exactement comme la source les fournit. Parquet, avec son stockage en colonnes et sa compression, est bien plus adapté aux traitements et aux agrégations des couches suivantes.

**Architecture Medallion.** Séparer Bronze, Silver et Gold permet de refaire les transformations sans relancer l'extraction, et de repérer clairement où se font le nettoyage et les contrôles qualité. Deux zones complètent les trois couches : Quality pour les rapports et Quarantine pour les enregistrements inutilisables.

## Transformations

**PySpark en mode local**, dans le conteneur Airflow. Le volume (plus de 2 millions de lignes, champs imbriqués, nombres stockés en texte) justifie Spark, et le mode local évite un cluster supplémentaire. Pandas aurait été plus simple mais moins adapté à ce volume et à une logique de couches.

**Schéma explicite et conversions tolérantes.** Les types sont convertis avec `try_cast` : une valeur mal formée est signalée par un indicateur au lieu de faire échouer tout le traitement.

**Partitionnement de Silver par année et par mois**, pour faciliter les filtres et préparer un traitement incrémental.

**Mémoire.** Pendant les tests, l'étape Bronze vers Silver a provoqué une erreur `Java heap space`. J'ai réglé Spark pour limiter le parallélisme et la mémoire utilisée (2 threads, driver de 3 Go, petites partitions, persistance sur disque). Cette configuration est adaptée à un poste local, pas à la production.

## Qualité des données

Je distingue deux niveaux d'anomalies.

- **Erreurs bloquantes** : un `trip_id` manquant ou une date de début invalide. Le trajet part en quarantaine, car il est impossible de l'identifier ou de le dater.
- **Anomalies non bloquantes** : valeurs négatives, durées incohérentes, montants incomplets ou écart sur le total. L'enregistrement reste dans Silver avec un indicateur qualité.

Écarter tous les trajets imparfaits ferait perdre des données encore utiles. Chaque jeu de données Gold applique ensuite ses propres règles d'éligibilité : un trajet sans durée valide peut quand même compter dans les revenus.

Deux règles méritent une explication :

- **Tolérance de 0,02 $** sur le total d'un trajet, pour ne pas signaler de simples écarts d'arrondi. Les montants d'origine ne sont jamais corrigés.
- **Changement d'heure du 5 novembre 2023.** L'heure de 1 h se répète à Chicago, ce qui peut produire des durées négatives. Je les signale avec une règle ciblée sur cette date, sans reconstruire les fuseaux horaires de façon générale.

Résultat sur les quatre mois : aucun enregistrement en quarantaine, mais des anomalies de durée, des montants incomplets et des écarts de total, tous signalés dans le rapport qualité.

## Données Gold

J'ai préféré **trois jeux de données séparés** à une seule grande table, car chacun répond à un besoin différent avec ses propres règles :

| Jeu de données | Lignes | Usage |
|---|---:|---|
| `daily_revenue` | 122 | Trajets et revenus par jour |
| `daily_trip_metrics` | 122 | Durée et distance moyennes par jour |
| `pickup_zone_performance` | 308 | Performance des zones de prise en charge |

C'est plus simple à maintenir et à utiliser dans le dashboard.

## Orchestration et dashboard

**Airflow.** Les quatre étapes dépendent les unes des autres. Airflow gère l'ordre, les logs et le suivi de chaque tâche. Le DAG se lance manuellement, car il traite un historique fixe.

**Streamlit et PyArrow.** Le dashboard lit directement les fichiers Parquet de Gold et Quality dans RustFS, sans relancer Spark. L'affichage reste ainsi indépendant du traitement.

## Résultats

| Indicateur | Résultat |
|---|---:|
| Enregistrements ingérés | 2 158 006 |
| Lots d'ingestion | 217 |
| Enregistrements Silver | 2 158 006 |
| Enregistrements en quarantaine | 0 |

Le DAG s'exécute de bout en bout avec `docker compose up` suivi d'un déclenchement manuel. La première exécution est plus longue, car Spark télécharge ses dépendances.

## Limites volontaires

Pour rester dans le périmètre d'un pipeline batch local, je n'ai pas mis en place :

- **Kafka**, car la source est une API historique et le traitement se fait en batch ;
- **Kubernetes ou un cluster Spark séparé**, car Docker Compose et Spark local suffisent à ce volume ;
- **un stockage cloud managé**, car RustFS joue ce rôle en local ;
- **MLflow**, car le projet porte sur le data engineering, pas sur des modèles.

Pour simplifier le démarrage, la configuration est directement dans `docker-compose.yml`, sans fichier `.env`. C'est pratique pour une démonstration, mais en production les identifiants devraient être externalisés.

## Avec plus de temps

- Passer à un **traitement incrémental**, en ne recalculant que les partitions concernées.
- Ajouter des **tests automatisés** sur les transformations et les règles qualité.
- Définir des **seuils qualité** qui font échouer le DAG automatiquement.
- Mettre en place des **alertes** et un meilleur suivi des exécutions.
- Déployer le stockage et l'orchestration sur le **cloud**.
- **Planifier** le DAG et l'étendre à d'autres périodes.