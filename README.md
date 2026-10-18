# Data Lakehouse from Scratch

Projet en cours de construction, étape par étape (voir `docs/` pour le détail
de chaque décision).

## Où on en est

- [x] Cadrage : cas d'usage e-commerce, tables MVP = customers / products / orders
      (voir `docs/schema_bronze.md`)
- [x] Squelette du projet
- [x] `docker-compose.yml` pour MinIO (buckets bronze/silver/gold)
- [ ] Spark + Iceberg (prochaine étape)
- [ ] Jobs Bronze
- [ ] Jobs Silver
- [ ] Jobs Gold
- [ ] DAG Airflow
- [ ] Qualité, partitionnement, schema evolution, time travel
- [ ] Documentation finale + extensions

## Démarrer MinIO (première brique à valider seule)

```bash
cd docker
docker compose up -d
```

Ensuite, vérifie que tout fonctionne **avant de passer à la suite** :

1. Ouvre la console web : http://localhost:9001
   - Identifiant : `minioadmin`
   - Mot de passe : `minioadmin123`
2. Tu dois voir 3 buckets déjà créés : `bronze`, `silver`, `gold`
   (créés automatiquement par le service `minio-init`).
3. Si les buckets n'apparaissent pas, regarde les logs :
   ```bash
   docker compose logs minio-init
   ```

Pourquoi valider MinIO seul avant d'ajouter Spark ? Parce que si un problème
de connexion apparaît plus tard, tu sauras immédiatement que ça ne vient pas
du stockage — il aura déjà été testé indépendamment. C'est une règle générale
utile pour tout le projet : ne jamais ajouter un composant sans avoir validé
le précédent isolément.

## Arrêter / nettoyer

```bash
docker compose down          # arrête les conteneurs, garde les données
docker compose down -v       # arrête et supprime aussi le volume MinIO
```

## Prochaine étape

Une fois les 3 buckets confirmés visibles dans la console MinIO : mise en
place de Spark avec les jars Iceberg + S3A (config déjà pré-remplie dans
`config/settings.yaml`, versions à figer précisément à ce moment-là).
