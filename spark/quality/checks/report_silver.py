"""Rapport d'inspection des tables Silver (lecture seule, sans assertions)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.spark_session import get_spark_session


def report_silver(spark):
    print("=== Vérification customers ===")

    # Exemples de noms réparés : valeur brute à gauche, valeur propre à droite.
    spark.sql("""
        SELECT first_name_raw, first_name, last_name_raw, last_name
        FROM silver.customers
        WHERE first_name <> first_name_raw OR last_name <> last_name_raw
        LIMIT 15
    """).show(truncate=False)

    # Noms contenant un tiret ou une apostrophe (les guillemets doubles "'" évitent
    # le piège de l'apostrophe dans une chaîne SQL). On regarde si la majuscule
    # après le signe est correcte : "O'Connor" et non "O'connor".
    spark.sql("""
        SELECT first_name_raw, first_name, last_name_raw, last_name
        FROM silver.customers
        WHERE last_name LIKE '%-%' OR last_name LIKE "%'%"
           OR first_name LIKE '%-%' OR first_name LIKE "%'%"
        LIMIT 15
    """).show(truncate=False)

    # Combien de clients ont eu leur prénom modifié, leur nom modifié, ou les deux ?
    # GROUP BY 1, 2 = on groupe sur les deux premières colonnes du SELECT.
    spark.sql("""
        SELECT (first_name <> first_name_raw) AS prenom_modifie,
               (last_name <> last_name_raw) AS nom_modifie,
               COUNT(*) AS nb_clients
        FROM silver.customers
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).show()

    # Téléphones : brut, normalisé, extension.
    spark.sql("""
        SELECT phone_raw, phone, phone_extension
        FROM silver.customers
        LIMIT 12
    """).show(truncate=False)

    # Doit afficher 0 : il ne reste aucun customer_id en double.
    spark.sql("""
        SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS doublons_restants
        FROM silver.customers
    """).show()
    print("=== Vérification orders ===")

    # 1) Répartition des montants par tranche : y a-t-il un vide entre 50 000 et 100 000 ?
    #    Un vide confirmerait que le seuil de 75 000 sépare bien deux populations.
    spark.sql("""
        SELECT CASE WHEN order_amount <= 50000  THEN 'a. jusqu a 50 000'
                    WHEN order_amount <= 100000 THEN 'b. 50 000 a 100 000'
                    WHEN order_amount <= 500000 THEN 'c. 100 000 a 500 000'
                    ELSE 'd. 500 000 a 1 million' END AS tranche,
               COUNT(*) AS nb_commandes
        FROM silver.orders
        WHERE order_amount IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).show(truncate=False)

    # 2) Répartition par année : les commandes de 2026 ou 2027 sont-elles nombreuses ?
    spark.sql("""
        SELECT year(order_date) AS annee, COUNT(*) AS nb_commandes
        FROM silver.orders
        GROUP BY 1
        ORDER BY 1
    """).show()

    # 3) Trois exemples par format de date : valeur brute à côté de la date obtenue.
    #    forme = chiffre -> 9, lettre -> A (même astuce que dans profile_table.py).
    #    ROW_NUMBER() OVER (PARTITION BY forme ...) numérote les lignes dans chaque format.
    spark.sql("""
        WITH base AS (
            SELECT regexp_replace(regexp_replace(order_date_raw, '[0-9]', '9'), '[A-Za-z]', 'A') AS forme,
                   order_date_raw, order_date, order_id
            FROM silver.orders
            WHERE order_date_raw IS NOT NULL
        )
        SELECT forme, order_date_raw, order_date
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY forme ORDER BY order_id) AS rang FROM base)
        WHERE rang <= 3
        ORDER BY forme, rang
    """).show(truncate=False)

    # 4) Commandes passées AVANT l'inscription du client : incohérence logique à repérer.
    spark.sql("""
        SELECT COUNT(*) AS commandes_avant_inscription
        FROM silver.orders o
        JOIN silver.customers c ON o.customer_id = c.customer_id
        WHERE o.order_date < c.signup_date
    """).show()

    # 5) Moyens de paiement et statuts après nettoyage : il ne doit rester que les valeurs officielles.
    spark.sql("""
        SELECT payment_method, COUNT(*) AS nb FROM silver.orders GROUP BY 1 ORDER BY nb DESC
    """).show()
    spark.sql("""
        SELECT status, COUNT(*) AS nb FROM silver.orders GROUP BY 1 ORDER BY nb DESC
    """).show()


def main():
    spark = get_spark_session("silver-report")
    try:
        report_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
