# Data Lakehouse e-commerce

Projet Data Engineering construit autour de **PySpark, Apache Iceberg, MinIO, PostgreSQL et Docker**. Airflow est prévu pour l'orchestration.

Le dépôt est organisé selon l'architecture **médaillon** : Bronze (ingestion), Silver (nettoyage) puis Gold (modèles analytiques).

## Avancement

| Étape | État dans le dépôt |
| --- | --- |
| Infrastructure Docker / Spark / Iceberg / MinIO | Configurations présentes ; disponibilité à vérifier localement |
| Bronze (customers, products, orders) | Code d'ingestion disponible |
| Profilage Bronze | Scripts génériques et spécialisés disponibles |
| Silver products / customers / orders | Modules disponibles ; exécution et résultats à valider |
| Gold | Non implémenté |
| Airflow | Squelette de DAG seulement, non opérationnel |
| Tests automatisés des traitements | À implémenter |

## Organisation des scripts

- `spark/common/` : construction de la SparkSession (catalogue Iceberg, MinIO).
- `spark/bronze/ingest_bronze.py` : lecture des CSV et écriture des tables Bronze.
- `spark/silver/clean_*.py` : transformations par entité ; `run_silver.py` est le point d'entrée.
- `spark/quality/profiling/` : inspection des données et recherche d'anomalies, principalement dans Bronze.
- `spark/quality/checks/report_silver.py` : rapport de contrôle **informatif** après traitement Silver (ne fait pas échouer le pipeline).
- `spark/gold/` et `airflow/dags/` : prochaines étapes non encore développées.
- `docs/architecture.md` : architecture, flux, décisions et précautions.

## Exécuter les scripts

Pré-requis : Docker Desktop démarré, fichier local `docker/.env` configuré, fichiers CSV placés dans `data/sample/` et buckets/catalogue disponibles. Les commandes suivantes s'exécutent depuis la racine du dépôt sous PowerShell :

```powershell
cd docker
docker compose up -d postgres minio iceberg-rest

# Ingestion Bronze : ATTENTION remplace les tables Bronze existantes
docker compose run --rm spark spark-submit /opt/app/spark/bronze/ingest_bronze.py

# Explorer la table des commandes (lecture seule)
docker compose run --rm spark spark-submit /opt/app/spark/quality/profiling/profile_orders.py

# Transformer Bronze -> Silver : ATTENTION remplace les tables Silver existantes
docker compose run --rm spark spark-submit /opt/app/spark/silver/run_silver.py

# Rapport Silver seulement (lecture seule)
docker compose run --rm spark spark-submit /opt/app/spark/quality/checks/report_silver.py
```

**Important :** le dépôt GitHub ne contient pas les volumes MinIO ni les données locales. Les scripts utilisent `createOrReplace()` : ne relance pas l'ingestion ou Silver sur des données que tu veux conserver sans sauvegarde préalable.

## À améliorer ensuite

- Vérifier les schémas Bronze : l'ingestion actuelle utilise `inferSchema=True`, malgré un contrat documentaire prévoyant du texte brut pour les colonnes sales.
- Ajouter des tests automatiques des règles de qualité et des transformations.
- Construire la couche Gold et son DAG Airflow.
- Remplacer les identifiants de développement codés en dur dans `spark/common/spark_session.py` par des variables d'environnement.

Voir `docs/architecture.md` et `docs/schema_bronze.md` pour plus de détails.
