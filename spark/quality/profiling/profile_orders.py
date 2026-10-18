"""
Profilage spécifique de bronze.orders : ce que profile_table.py ne peut pas voir.
Lecture seule : aucune table n'est modifiée.
"""

import sys

sys.path.append("/opt/app/spark")
from common.spark_session import get_spark_session


def profile_order_amount(spark):
    print("\n--- order_amount : répartition par type de valeur ---")
    # CAST(... AS DOUBLE) renvoie NULL quand le texte n'est pas un nombre.
    # On classe chaque ligne dans une catégorie (CASE WHEN), puis on compte par catégorie.
    # Les WHEN sont lus dans l'ordre : le premier qui est vrai gagne.
    spark.sql("""
        SELECT
            CASE
                WHEN order_amount IS NULL OR TRIM(order_amount) = '' THEN 'vide'
                WHEN CAST(order_amount AS DOUBLE) IS NULL THEN 'texte_non_numerique'
                WHEN isnan(CAST(order_amount AS DOUBLE)) THEN 'NaN'
                WHEN abs(CAST(order_amount AS DOUBLE)) = CAST('Infinity' AS DOUBLE) THEN 'infini'
                WHEN CAST(order_amount AS DOUBLE) < 0 THEN 'negatif'
                WHEN CAST(order_amount AS DOUBLE) = 0 THEN 'zero'
                ELSE 'positif'
            END AS categorie,
            COUNT(*) AS n
        FROM bronze.orders
        GROUP BY 1
        ORDER BY n DESC
    """).show(truncate=False)

    print("\n--- order_amount : les textes non numériques les plus fréquents ---")
    spark.sql("""
        SELECT order_amount, COUNT(*) AS n
        FROM bronze.orders
        WHERE CAST(order_amount AS DOUBLE) IS NULL
        GROUP BY order_amount
        ORDER BY n DESC
        LIMIT 20
    """).show(truncate=False)

    print("\n--- order_amount : les montants négatifs ou nuls les plus fréquents ---")
    spark.sql("""
        SELECT order_amount, COUNT(*) AS n
        FROM bronze.orders
        WHERE CAST(order_amount AS DOUBLE) <= 0
        GROUP BY order_amount
        ORDER BY n DESC
        LIMIT 10
    """).show(truncate=False)

    print("\n--- order_amount : statut des commandes à montant négatif ---")
    # Si les montants négatifs sont des remboursements légitimes, ils seront surtout
    # sur le statut "refunded". S'ils sont répartis partout : anomalies injectées.
    # lower(trim(...)) regroupe les variantes de casse et d'espaces du statut.
    spark.sql("""
        SELECT lower(trim(status)) AS statut, COUNT(*) AS nb_montants_negatifs
        FROM bronze.orders
        WHERE CAST(order_amount AS DOUBLE) < 0
        GROUP BY 1
        ORDER BY nb_montants_negatifs DESC
    """).show(truncate=False)

    print("\n--- order_amount : distribution des montants strictement positifs ---")
    # percentile_approx(v, array(...)) renvoie la valeur sous laquelle se trouve
    # 50 %, 90 %, 99 %, 99,9 % des montants : utile pour repérer des valeurs aberrantes.
    spark.sql("""
        SELECT MIN(v) AS min_val,
               percentile_approx(v, array(0.5, 0.9, 0.99, 0.999)) AS percentiles,
               MAX(v) AS max_val
        FROM (SELECT CAST(order_amount AS DOUBLE) AS v FROM bronze.orders)
        WHERE v > 0 AND NOT isnan(v)
    """).show(truncate=False)


def profile_order_date(spark):
    print("\n--- order_date : format jour-mois-année ou mois-jour-année ? (99-99-9999) ---")
    # substr(texte, position_de_départ, longueur) : la position commence à 1.
    # Si le premier bloc dépasse 12 quelque part, c'est forcément un jour (JJ-MM-AAAA).
    # Si c'est le second qui dépasse 12, c'est le format MM-JJ-AAAA.
    # Si les deux dépassent 12 : les deux formats sont mélangés (ambigu).
    spark.sql("""
        SELECT
            SUM(CASE WHEN CAST(substr(order_date, 1, 2) AS INT) > 12 THEN 1 ELSE 0 END) AS premier_bloc_sup_12,
            SUM(CASE WHEN CAST(substr(order_date, 4, 2) AS INT) > 12 THEN 1 ELSE 0 END) AS second_bloc_sup_12,
            COUNT(*) AS total
        FROM bronze.orders
        WHERE order_date RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
    """).show(truncate=False)

    print("\n--- order_date : formats qui commencent par l'année (AAAA-MM-JJ ou AAAA/MM/JJ) ---")
    # Même logique : si le mois dépasse 12, le format est AAAA-JJ-MM, pas AAAA-MM-JJ.
    spark.sql("""
        SELECT
            SUM(CASE WHEN CAST(substr(order_date, 6, 2) AS INT) > 12 THEN 1 ELSE 0 END) AS mois_sup_12,
            SUM(CASE WHEN CAST(substr(order_date, 9, 2) AS INT) > 12 THEN 1 ELSE 0 END) AS jour_sup_12,
            MIN(substr(order_date, 1, 4)) AS annee_min,
            MAX(substr(order_date, 1, 4)) AS annee_max,
            COUNT(*) AS total
        FROM bronze.orders
        WHERE order_date RLIKE '^[0-9]{4}[-/][0-9]{2}[-/][0-9]{2}'
    """).show(truncate=False)


def profile_references(spark):
    print("\n--- Commandes dont le client est absent de silver.customers ---")
    # LEFT JOIN garde toutes les commandes ; si le client n'existe pas, c.customer_id est NULL.
    spark.sql("""
        SELECT COUNT(*) AS commandes_client_inconnu,
               COUNT(DISTINCT o.customer_id) AS clients_inconnus_distincts
        FROM bronze.orders o
        LEFT JOIN silver.customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """).show(truncate=False)

    print("\n--- Commandes dont le produit est absent de silver.products ---")
    spark.sql("""
        SELECT COUNT(*) AS commandes_produit_inconnu,
               COUNT(DISTINCT o.product_id) AS produits_inconnus_distincts
        FROM bronze.orders o
        LEFT JOIN silver.products p ON o.product_id = p.product_id
        WHERE p.product_id IS NULL
    """).show(truncate=False)

    print("\n--- Identifiants clients avec des majuscules (UUID attendus en minuscules) ---")
    spark.sql("""
        SELECT COUNT(*) AS ids_avec_majuscules
        FROM bronze.orders
        WHERE customer_id <> lower(customer_id)
    """).show(truncate=False)


def main():
    spark = get_spark_session("profile-orders")
    profile_order_amount(spark)
    profile_order_date(spark)
    profile_references(spark)
    spark.stop()


if __name__ == "__main__":
    main()