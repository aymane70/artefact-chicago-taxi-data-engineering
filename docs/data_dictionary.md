# Dictionnaire de données du pipeline Chicago Taxi

Ce document décrit les jeux de données produits à chaque étape du pipeline, des données brutes (Bronze) aux données analytiques (Gold). Tout est stocké dans le bucket `chicago-taxi-lake`.

| Couche | Format | Contenu |
|---|---|---|
| Bronze | JSONL | Données brutes de l'API |
| Silver | Parquet | Trajets nettoyés et typés |
| Quarantine | Parquet | Enregistrements inutilisables |
| Quality | Parquet | Rapports qualité mensuels |
| Gold | Parquet | Données agrégées pour le dashboard |

## Bronze

**Emplacement :** `bronze/year=2023/`

Les données de l'API sont enregistrées telles quelles, un fichier JSONL par lot, classées par mois. Chaque ligne correspond à un trajet.

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

Le fichier `_manifest.json` garde la trace des lots déjà traités, ce qui permet de reprendre une extraction interrompue.

## Silver

**Emplacement :** `silver/taxi_trips/`, partitionné par année et par mois.

Les types sont harmonisés, les textes nettoyés et les anomalies signalées par des indicateurs qualité.

### Champs principaux

| Champ | Type | Description |
|---|---|---|
| `trip_id` | String | Identifiant unique du trajet |
| `taxi_id` | String | Identifiant du taxi |
| `trip_start_timestamp` | Timestamp | Début du trajet |
| `trip_end_timestamp` | Timestamp | Fin du trajet |
| `trip_seconds` | Integer | Durée en secondes |
| `trip_miles` | Double | Distance en miles |
| `pickup_community_area` | Integer | Zone de prise en charge |
| `dropoff_community_area` | Integer | Zone de dépose |
| `pickup_census_tract`, `dropoff_census_tract` | String | Secteurs de recensement |
| `fare`, `tips`, `tolls`, `extras` | Decimal(12,2) | Course, pourboire, péage, frais supplémentaires |
| `trip_total` | Decimal(12,2) | Montant total déclaré |
| `payment_type` | String | Mode de paiement |
| `company` | String | Société de taxi |
| `pickup_centroid_*`, `dropoff_centroid_*` | Double / Struct | Latitude, longitude et localisation GeoJSON |
| `year`, `month` | Integer / String | Partitionnement |

### Indicateurs qualité

Ces champs booléens signalent les enregistrements à surveiller, sans les exclure de Silver.

| Champ | Signification |
|---|---|
| `has_invalid_numeric` | Une valeur numérique n'a pas pu être convertie |
| `has_negative_values` | Une durée, une distance ou un montant est négatif |
| `has_negative_duration` | L'heure de fin précède l'heure de début |
| `has_dst_ambiguous_duration` | Durée négative expliquée par le changement d'heure du 5 novembre 2023 |
| `has_missing_duration_data` | Heure de fin ou durée manquante |
| `has_invalid_duration` | Durée négative |
| `has_incomplete_financial_data` | Un des montants financiers est manquant |
| `has_total_mismatch` | Le total diffère de plus de 0,02 de la somme des composantes |

Le champ `financial_difference` contient l'écart entre le total déclaré et la somme de `fare`, `tips`, `tolls` et `extras`.

### Règles de transformation

- Les chaînes vides deviennent des valeurs nulles.
- Les montants d'origine sont conservés, jamais corrigés automatiquement.
- Une erreur de conversion n'interrompt pas le traitement : elle est signalée par un indicateur.

### Quarantaine

**Emplacement :** `quarantine/taxi_trips/`

Un enregistrement est mis en quarantaine si son `trip_id` ou sa date de début est nul. Le champ `dq_reason` explique pourquoi, et les valeurs brutes sont conservées dans des champs préfixés par `raw_`.

Sur les quatre mois traités, aucun enregistrement n'a été mis en quarantaine.

## Quality

**Emplacement :** `quality/silver/monthly_report/`, une ligne par mois.

| Champ | Description |
|---|---|
| `year`, `month` | Période concernée |
| `total_records` | Nombre d'enregistrements Silver |
| `unique_trip_ids` | Nombre d'identifiants distincts |
| `duplicate_trip_ids` | Nombre de doublons |

Pour chaque anomalie listée dans les indicateurs qualité, le rapport donne le nombre d'enregistrements concernés (`<indicateur>_count`) et leur part en pourcentage du total mensuel (`<indicateur>_percentage`, arrondi à deux décimales).

Le rapport est recalculé et remplacé à chaque exécution.

## Gold

Trois jeux de données construits à partir de Silver pour alimenter le dashboard.

### Revenus journaliers (Daily Revenue)

**Emplacement :** `gold/daily_revenue/`, une ligne par jour.

| Champ | Description |
|---|---|
| `trip_date` | Date du trajet |
| `total_trips` | Nombre de trajets du jour |
| `revenue_eligible_trips` | Trajets dont le total est renseigné |
| `total_revenue` | Somme des montants totaux |
| `average_trip_revenue` | Montant moyen par trajet éligible |
| `total_fares` | Somme des courses |
| `total_tips` | Somme des pourboires |

Les trajets sans date de début sont exclus.

### Indicateurs journaliers (Daily Trip Metrics)

**Emplacement :** `gold/daily_trip_metrics/`, une ligne par jour.

| Champ | Description |
|---|---|
| `trip_date` | Date du trajet |
| `valid_duration_trips` | Trajets retenus pour la durée |
| `average_trip_duration_minutes` | Durée moyenne en minutes |
| `valid_distance_trips` | Trajets retenus pour la distance |
| `average_trip_distance_miles` | Distance moyenne en miles |
| `total_distance_miles` | Distance totale en miles |

La durée et la distance sont calculées séparément, donc leurs nombres de trajets peuvent différer. Les trajets à durée invalide, négative, manquante ou ambiguë sont exclus du calcul des durées. Pour les distances, seules les valeurs renseignées et positives ou nulles sont retenues.

### Performance des zones (Pickup Zone Performance)

**Emplacement :** `gold/pickup_zone_performance/`, une ligne par zone et par mois.

| Champ | Description |
|---|---|
| `pickup_community_area` | Zone de prise en charge |
| `total_trips` | Nombre de trajets au départ de la zone |
| `total_revenue` | Revenus de la zone |
| `average_trip_distance_miles` | Distance moyenne |
| `monthly_rank` | Rang de la zone selon le nombre de trajets du mois |

Les zones sont classées chaque mois par nombre de trajets décroissant, et deux zones à égalité obtiennent le même rang.

---

Les descriptions correspondent à l'implémentation actuelle du pipeline. Pour les définitions d'origine des champs, la référence reste le Chicago Data Portal.