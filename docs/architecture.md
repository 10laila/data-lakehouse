# Architecture du Data Lakehouse

## Flux des données

```text
CSV e-commerce (customers, products, orders)
   │
   ▼
spark/bronze/ingest_bronze.py  → tables Iceberg bronze.*
   │                              ↘ spark/quality/profiling/ (diagnostic, lecture seule)
   ▼
spark/silver/run_silver.py     → silver.products, silver.customers,
   │                              silver.customer_devices, silver.orders
   │                              ↘ spark/quality/checks/report_silver.py
   ▼
spark/gold/                   → futurs jeux de données analytiques
   │
   ▼
Restitution / BI (à concevoir)
```

Stockage des tables via Apache Iceberg sur MinIO ; catalogue REST Iceberg et services Docker. Airflow sera ajouté quand les jobs seront stabilisés.

## Responsabilités

| Dossier | Rôle |
| --- | --- |
| `spark/common` | Connexions et paramètres communs à tous les jobs |
| `spark/bronze` | Ingestion de sources, traçabilité, pas de nettoyage métier |
| `spark/silver` | Normalisation, conversions, déduplication et écriture Silver |
| `spark/quality/profiling` | Profilage exploratoire ; n'écrit aucune table |
| `spark/quality/checks` | Rapports / validations après transformation |
| `spark/gold` | Agrégations et modèles métier à implémenter |
| `airflow/dags` | Orchestration future |

## Exécution Silver

`run_silver.py` lance : `clean_products`, `clean_customers`, `clean_orders`, puis le rapport `report_silver`. Tous les traitements utilisent une SparkSession commune, fermée à la fin même en cas d'erreur.

Les trois modules Silver proviennent d'un découpage sans changement volontaire des règles de nettoyage. L'appel de `clean_orders` a été ajouté : la fonction existait mais n'était pas invoquée dans l'ancien point d'entrée.

## Précautions

- Les commandes `git` ne changent **pas** les données déjà présentes dans les volumes Docker/MinIO. Seuls les scripts versionnés sont réorganisés.
- Les jobs Bronze et Silver appellent `createOrReplace()`. Une **exécution** de ces jobs peut remplacer des tables ; sauvegarder les données avant un nouveau lancement.
- Le profilage Bronze existe indépendamment de Silver. Le script `profile_orders.py` utilise aussi `silver.customers` et `silver.products` pour certaines vérifications croisées : exécuter celles-ci seulement après la génération des deux tables.
- Le rapport `report_silver.py` affiche des statistiques mais ne constitue **pas** une suite de tests d'assertions.
- Le dossier `docs/diagnostics/` archive un ancien journal PowerShell déplacé hors du répertoire Docker.
- L'ingestion conserve `inferSchema=True` à ce stade. Le contrat `docs/schema_bronze.md` doit encore être aligné avec la stratégie réelle de typage ; c'est un chantier distinct de ce refactoring.
