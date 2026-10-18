# Schéma Bronze — MVP (customers, products, orders)

Ce document fige le schéma d'entrée avant d'écrire le moindre job Spark.
Il sert de contrat entre l'ingestion et tout le reste du pipeline.

Règle Bronze : on ingère TOUT tel quel, y compris les valeurs sales.
Aucune correction ici — seulement le typage strict minimum pour pouvoir stocker
en Parquet/Iceberg, et l'ajout de métadonnées d'ingestion.

---

## 1. bronze_customers (source : CRM Customers, ~50 000 lignes)

| Colonne          | Type source | Type Bronze | Sale ? |
|------------------|-------------|-------------|--------|
| customer_id      | UUID        | string      | non (clé) |
| first_name       | string      | string      | oui (casse, symboles) |
| last_name        | string      | string      | oui (casse, symboles) |
| email            | string      | string      | oui (manquants) |
| phone_number     | string      | string      | oui (formats incohérents) |
| gender           | string      | string      | non précisé |
| dob               | date       | string      | à valider en Silver |
| signup_date      | date        | string      | à valider en Silver |
| address          | string      | string      | oui (non standardisé) |
| city             | string      | string      | à normaliser en Silver |
| state            | string      | string      | à normaliser en Silver |
| country          | string      | string      | à normaliser en Silver |
| device_id(s)     | string      | string      | plusieurs IDs par ligne, à séparer en Silver |
| source           | string      | string      | non précisé |

**Métadonnées d'ingestion à ajouter (toutes les tables Bronze) :**
`_ingested_at` (timestamp), `_source_file` (string), `_batch_id` (string)

**Clé primaire attendue** : `customer_id` (à vérifier — le dataset annonce des doublons de profils, donc l'unicité n'est PAS garantie en Bronze, seulement en Silver).

---

## 2. bronze_products (source : Product Catalog, 500 lignes)

| Colonne       | Type source | Type Bronze | Sale ? |
|---------------|-------------|-------------|--------|
| product_id    | string      | string      | non (clé, garantie propre) |
| product_name  | string      | string      | oui (30%) |
| category      | string      | string      | oui (30%) |
| price         | float       | string*     | oui (30%) — typé en string en Bronze pour ne pas planter sur des valeurs non numériques |

*En Bronze, tout ce qui peut contenir une valeur non conforme (ex. "N/A", "12,50€") est stocké en `string`. Le cast en `double` se fait en Silver, après nettoyage, pour ne pas perdre silencieusement des lignes.

---

## 3. bronze_orders (source : Orders/Transactions, ~300 000 lignes)

| Colonne         | Type source | Type Bronze | Sale ? |
|-----------------|-------------|-------------|--------|
| order_id        | string      | string      | non (clé) |
| customer_id     | string      | string      | clé étrangère vers customers |
| product_id      | string      | string      | clé étrangère vers products |
| order_amount    | float       | string*     | oui (30%) |
| order_date      | date        | string      | oui (30%) |
| payment_method  | string      | string      | oui (25%) |
| status          | string      | string      | oui (30%) |
| quantity        | int         | string*     | oui (25%) |

*Même logique que `price` : stocké en string en Bronze, typé/validé en Silver.

---

## Ce que cette table de schéma nous donne pour la suite

- **Étape Silver** : la liste exacte des colonnes à nettoyer/normaliser/valider est déjà connue (dates, casse, séparation des device_id multiples, cast numérique avec gestion des valeurs invalides).
- **Étape qualité** : on peut déjà écrire les règles (`order_id IS NOT NULL`, `price >= 0`, `quantity > 0`, `order_date` parseable) car les colonnes concernées sont identifiées.
- **Étape Gold** : `gold_sales_by_region` dépendra de `city/state/country` nettoyés — donc leur qualité en Silver est un point critique à surveiller en premier.

## Tables volontairement exclues du MVP (à réintroduire à l'étape extension)

- Support Tickets (30 000 lignes)
- Clickstream Events (2 000 000 lignes)

Elles suivront exactement le même principe Bronze (ingestion brute + métadonnées), une fois le socle customers/products/orders stabilisé.
