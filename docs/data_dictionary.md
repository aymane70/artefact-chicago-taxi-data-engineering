# Dictionnaire de données — Pipeline Chicago Taxi

Ce document présente les jeux de données produits par le pipeline Chicago Taxi, depuis les données brutes (Bronze) jusqu'aux données analytiques (Gold), en passant par Silver, Quality et Quarantine.

## 1. Couche Bronze

**Format :** JSONL  
**Emplacement :** `s3://chicago-taxi-lake/bronze/year=2023/`

La couche Bronze conserve les données brutes récupérées depuis l'API Chicago Taxi Trips.

Les données sont organisées par mois. Chaque lot extrait est enregistré dans un fichier JSONL distinct.

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

Chaque ligne JSONL représente un trajet et conserve les champs provenant de la source : identifiants, dates, informations de paiement, durée, distance, montants financiers et localisation des points de prise en charge et de dépose.

Les manifestes contiennent les informations de suivi des lots. Ils permettent notamment de retrouver les lots déjà traités et de reprendre une extraction interrompue.

## 2. Couche Silver

**Format :** Parquet  
**Emplacement :** `s3://chicago-taxi-lake/silver/taxi_trips/`

Silver contient les trajets après transformation : les types de données sont harmonisés, les chaînes de caractères nettoyées et les anomalies identifiées à l'aide d'indicateurs qualité.

### Schéma

| Champ | Type | Description |
|---|---|---|
| `trip_id` | String | Identifiant unique du trajet. |
| `taxi_id` | String | Identifiant du taxi. |
| `trip_start_timestamp` | Timestamp | Date et heure de début du trajet. |
| `trip_end_timestamp` | Timestamp | Date et heure de fin du trajet. |
| `trip_seconds` | Integer | Durée du trajet en secondes. |
| `trip_miles` | Double | Distance parcourue en miles. |
| `pickup_census_tract` | String | Secteur de recensement du point de départ. |
| `dropoff_census_tract` | String | Secteur de recensement du point d'arrivée. |
| `pickup_community_area` | Integer | Identifiant de la zone de prise en charge. |
| `dropoff_community_area` | Integer | Identifiant de la zone de dépose. |
| `fare` | Decimal(12,2) | Montant de la course. |
| `tips` | Decimal(12,2) | Pourboire. |
| `tolls` | Decimal(12,2) | Frais de péage. |
| `extras` | Decimal(12,2) | Frais supplémentaires. |
| `trip_total` | Decimal(12,2) | Montant total déclaré pour le trajet. |
| `payment_type` | String | Mode de paiement. |
| `company` | String | Société de taxi. |
| `pickup_centroid_latitude` | Double | Latitude du point de départ. |
| `pickup_centroid_longitude` | Double | Longitude du point de départ. |
| `pickup_centroid_location` | Struct | Localisation GeoJSON du départ, contenant `type` et `coordinates`. |
| `dropoff_centroid_latitude` | Double | Latitude du point d'arrivée. |
| `dropoff_centroid_longitude` | Double | Longitude du point d'arrivée. |
| `dropoff_centroid_location` | Struct | Localisation GeoJSON de l'arrivée, contenant `type` et `coordinates`. |
| `year` | Integer | Année de partitionnement. |
| `month` | String | Mois extrait du chemin de stockage Bronze. |

### Indicateurs de qualité

Ces champs booléens permettent d'identifier les enregistrements concernés par les contrôles qualité.

| Champ | Description |
|---|---|
| `has_invalid_numeric` | Au moins une valeur numérique non vide n'a pas pu être convertie vers le type attendu. |
| `has_negative_values` | Au moins une durée, distance ou valeur financière contrôlée est négative. |
| `has_dst_ambiguous_duration` | L'intervalle négatif correspond à l'heuristique utilisée pour détecter l'heure ambiguë du 5 novembre 2023 à Chicago, pendant le changement d'heure. |
| `has_negative_duration` | L'heure de fin est antérieure à l'heure de début, hors cas signalés comme ambigus à cause du changement d'heure. |
| `has_missing_duration_data` | L'heure de fin ou la durée en secondes est manquante. |
| `has_invalid_duration` | Le trajet présente une durée négative. |
| `has_incomplete_financial_data` | Au moins un des champs `fare`, `tips`, `tolls`, `extras` ou `trip_total` est manquant. |
| `financial_difference` | Différence entre le montant total déclaré et la somme de `fare`, `tips`, `tolls` et `extras`. |
| `has_total_mismatch` | L'écart financier absolu dépasse 0,02 pour un enregistrement dont les données financières sont complètes. |

### Règles de transformation

- Les chaînes vides sont remplacées par des valeurs nulles dans les champs nettoyés.
- Les champs numériques et temporels sont convertis vers les types attendus.
- `try_cast` permet de gérer les erreurs de conversion sans faire échouer toute la transformation.
- Les montants financiers d'origine sont conservés. Le pipeline ne modifie pas automatiquement les totaux déclarés.
- La réconciliation financière utilise une tolérance de 0,02.
- La règle DST cible spécifiquement les intervalles potentiellement ambigus lors du changement d'heure à Chicago, le 5 novembre 2023.

### Règle de mise en quarantaine

Un enregistrement est mis en quarantaine dans deux cas :

- `trip_id` est nul.
- `trip_start_timestamp` est nul après conversion.

**Emplacement :** `s3://chicago-taxi-lake/quarantine/taxi_trips/`

Les enregistrements mis en quarantaine conservent des informations utiles au diagnostic, notamment :

- `dq_reason` : indique la raison de la mise en quarantaine.
- Les champs numériques bruts, préfixés par `raw_`.
- Les champs convertis et les autres indicateurs qualité.

Les autres anomalies ne provoquent pas automatiquement l'exclusion de Silver. Les enregistrements restent disponibles pour les traitements suivants, avec leurs indicateurs qualité.

Sur les quatre mois traités, aucun enregistrement n'a été mis en quarantaine.

## 3. Couche Quality

**Format :** Parquet  
**Emplacement :** `s3://chicago-taxi-lake/quality/silver/monthly_report/`

Cette couche contient les indicateurs qualité calculés à partir des données Silver. Les résultats sont regroupés par `year` et `month`.

### Schéma du rapport

| Champ | Description |
|---|---|
| `year` | Année concernée. |
| `month` | Mois concerné. |
| `total_records` | Nombre total d'enregistrements Silver pour le mois. |
| `unique_trip_ids` | Nombre d'identifiants de trajet distincts. |
| `duplicate_trip_ids` | Différence entre le nombre total d'enregistrements et le nombre d'identifiants distincts. |

### Indicateurs qualité

Pour chaque anomalie, le rapport contient le nombre d'enregistrements concernés et leur pourcentage par rapport au total mensuel.

| Anomalie | Champ de comptage | Champ de pourcentage |
|---|---|---|
| Valeurs numériques invalides | `has_invalid_numeric_count` | `has_invalid_numeric_percentage` |
| Valeurs négatives | `has_negative_values_count` | `has_negative_values_percentage` |
| Durées ambiguës à cause du DST | `has_dst_ambiguous_duration_count` | `has_dst_ambiguous_duration_percentage` |
| Durées négatives | `has_negative_duration_count` | `has_negative_duration_percentage` |
| Données de durée manquantes | `has_missing_duration_data_count` | `has_missing_duration_data_percentage` |
| Durées invalides | `has_invalid_duration_count` | `has_invalid_duration_percentage` |
| Données financières incomplètes | `has_incomplete_financial_data_count` | `has_incomplete_financial_data_percentage` |
| Écarts sur le montant total | `has_total_mismatch_count` | `has_total_mismatch_percentage` |

Le pourcentage est calculé ainsi :

```text
Nombre d'enregistrements concernés / Nombre total d'enregistrements × 100
```

Les résultats sont arrondis à deux décimales.

À chaque exécution de la tâche de rapport qualité, le rapport existant est remplacé par les résultats calculés à partir des dernières données Silver.

## 4. Couche Gold

**Format :** Parquet  
**Emplacement principal :** `s3://chicago-taxi-lake/gold/`

Gold contient trois jeux de données analytiques construits à partir de Silver. Ils sont destinés au reporting et au dashboard.

### 4.1 Revenus journaliers — Daily Revenue

**Emplacement :** `s3://chicago-taxi-lake/gold/daily_revenue/`  
**Granularité :** une ligne par date de trajet.

| Champ | Description |
|---|---|
| `year` | Année concernée. |
| `month` | Mois concerné. |
| `trip_date` | Date extraite de l'heure de début du trajet. |
| `total_trips` | Nombre de trajets ayant une date de début valide. |
| `revenue_eligible_trips` | Nombre de trajets dont le champ `trip_total` est renseigné. |
| `total_revenue` | Somme des montants totaux déclarés. |
| `average_trip_revenue` | Moyenne des montants totaux pour les trajets éligibles au calcul des revenus. |
| `total_fares` | Somme des montants des courses. |
| `total_tips` | Somme des pourboires. |

**Règles de calcul :**

- Les trajets sans date de début sont exclus.
- Un trajet est éligible au calcul des revenus si `trip_total` est renseigné.
- Les montants financiers sont agrégés tels qu'ils sont enregistrés dans Silver.

### 4.2 Indicateurs journaliers — Daily Trip Metrics

**Emplacement :** `s3://chicago-taxi-lake/gold/daily_trip_metrics/`  
**Granularité :** une ligne par date de trajet.

| Champ | Description |
|---|---|
| `year` | Année concernée. |
| `month` | Mois concerné. |
| `trip_date` | Date extraite de l'heure de début du trajet. |
| `valid_duration_trips` | Nombre de trajets retenus pour le calcul des durées. |
| `average_trip_duration_minutes` | Durée moyenne des trajets valides, en minutes, arrondie à deux décimales. |
| `valid_distance_trips` | Nombre de trajets retenus pour le calcul des distances. |
| `average_trip_distance_miles` | Distance moyenne des trajets valides, en miles, arrondie à deux décimales. |
| `total_distance_miles` | Somme des distances valides, en miles, arrondie à deux décimales. |

**Conditions de calcul des durées :**

- La date de début doit être renseignée.
- La durée doit être disponible et strictement positive.
- Les trajets signalés comme ayant une durée invalide, négative, manquante ou ambiguë à cause du DST sont exclus.

**Conditions de calcul des distances :**

- La date de début doit être renseignée.
- `trip_miles` doit être renseigné et supérieur ou égal à zéro.
- Les conditions de calcul des distances sont indépendantes de celles des durées.

Les métriques de durée et de distance sont calculées séparément, puis réunies avec une jointure externe complète (*full outer join*). Leurs nombres de trajets peuvent donc être différents.

### 4.3 Performance des zones de prise en charge — Pickup Zone Performance

**Emplacement :** `s3://chicago-taxi-lake/gold/pickup_zone_performance/`  
**Granularité :** une ligne par zone de prise en charge et par mois.

| Champ | Description |
|---|---|
| `year` | Année concernée. |
| `month` | Mois concerné. |
| `pickup_community_area` | Identifiant de la zone de prise en charge. |
| `total_trips` | Nombre de trajets éligibles au départ de cette zone. |
| `total_revenue` | Somme des montants totaux déclarés pour cette zone. |
| `average_trip_distance_miles` | Distance moyenne des trajets dont la distance est non négative, arrondie à deux décimales. |
| `monthly_rank` | Classement dense des zones selon le volume de trajets pour chaque mois. |

**Règles de calcul :**

- La zone de prise en charge et la date de début doivent être renseignées.
- Le classement est calculé séparément pour chaque année et chaque mois.
- Les zones sont classées par ordre décroissant du nombre de trajets.
- Les zones ayant le même volume de trajets obtiennent le même rang.

## 5. Récapitulatif des couches

| Couche | Format | Utilisation |
|---|---|---|
| Bronze | JSONL | Données brutes de l'API et manifestes d'ingestion. |
| Silver | Parquet | Données de trajets nettoyées et transformées. |
| Quarantine | Parquet | Enregistrements qui ne respectent pas les contrôles structurels critiques. |
| Quality | Parquet | Indicateurs qualité calculés chaque mois. |
| Gold | Parquet | Jeux de données agrégés pour les analyses. |

**Remarque :** les descriptions des champs correspondent à l'implémentation actuelle du pipeline. Pour les définitions originales des champs de la source, le Chicago Data Portal reste la référence.