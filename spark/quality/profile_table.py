"""
Outil de profilage réutilisable pour n'importe quelle table du lakehouse.
À lancer AVANT d'écrire le nettoyage Silver d'une table, pour connaître les vraies
données plutôt que de deviner à partir d'un échantillon.

Exemple :
  spark-submit profile_table.py bronze.customers --distinct gender source --patterns phone_number
"""

import argparse
import sys

sys.path.append("/opt/app/spark")
from common.spark_session import get_spark_session


def profile_nulls(spark, table_name: str):
    df = spark.table(table_name)
    columns = df.columns
    # df.dtypes = liste de couples (nom de colonne, type), ex : ("dob", "date").
    # dict(...) la transforme en dictionnaire pour retrouver le type d'une colonne par son nom.
    types = dict(df.dtypes)
    total = spark.sql(f"SELECT COUNT(*) AS n FROM {table_name}").collect()[0]["n"]
    print(f"\n--- {table_name} : {total} lignes, valeurs manquantes par colonne ---")
    for column in columns:
        # Une requête par colonne : chaque calcul reste petit.
        # "Manquant" = NULL, chaîne vide, ou chaîne faite uniquement d'espaces.
        # CAST(... AS STRING) permet de traiter aussi les dates et les timestamps.
        n = spark.sql(f"""
            SELECT COUNT(CASE WHEN `{column}` IS NULL
                                OR TRIM(CAST(`{column}` AS STRING)) = '' THEN 1 END) AS n
            FROM {table_name}
        """).collect()[0]["n"]
        pourcentage = 100 * n / total if total else 0
        print(f"{column:20} {types[column]:12} {n:>8}  ({pourcentage:.1f}%)")


def profile_column(spark, table_name: str, column: str, limit: int = 50):
    # Valeurs distinctes et leur fréquence (repère les variantes : "M", "male", "Male "...).
    print(f"\n--- {table_name}.{column} : valeurs distinctes (top {limit}) ---")
    spark.sql(f"""
        SELECT `{column}`, COUNT(*) as n
        FROM {table_name}
        GROUP BY `{column}`
        ORDER BY n DESC
        LIMIT {limit}
    """).show(limit, truncate=False)


def profile_numeric(spark, table_name: str, column: str):
    print(f"\n--- {table_name}.{column} : statistiques numériques ---")
    spark.sql(f"""
        SELECT
            MIN(CAST(`{column}` AS DOUBLE)) as min_val,
            MAX(CAST(`{column}` AS DOUBLE)) as max_val,
            COUNT(CASE WHEN CAST(`{column}` AS DOUBLE) <= 0 THEN 1 END) as valeurs_zero_ou_negatives,
            COUNT(CASE WHEN `{column}` IS NULL THEN 1 END) as valeurs_nulles,
            COUNT(CASE WHEN CAST(`{column}` AS DOUBLE) IS NULL AND `{column}` IS NOT NULL THEN 1 END) as non_castables,
            COUNT(*) as total
        FROM {table_name}
    """).show(truncate=False)


def profile_patterns(spark, table_name: str, column: str, limit: int = 15):
    # Transforme chaque valeur en "forme" : chiffre -> 9, lettre -> A.
    # Exemple : "(522)382-8559" devient "(999)999-9999".
    # On compte ensuite les formes, des plus fréquentes aux plus rares.
    # GROUP BY 1 = grouper sur la première colonne du SELECT (la forme).
    print(f"\n--- {table_name}.{column} : formes des valeurs (top {limit}) ---")
    spark.sql(f"""
        SELECT
            regexp_replace(regexp_replace(CAST(`{column}` AS STRING), '[0-9]', '9'), '[A-Za-z]', 'A') AS forme,
            COUNT(*) AS n
        FROM {table_name}
        GROUP BY 1
        ORDER BY n DESC
        LIMIT {limit}
    """).show(limit, truncate=False)


def profile_duplicates(spark, table_name: str, expression: str):
    # expression peut être une colonne ("customer_id") ou une expression SQL
    # ("lower(trim(email))") pour détecter les doublons malgré la casse ou les espaces.
    # HAVING COUNT(*) > 1 ne garde que les valeurs qui apparaissent plusieurs fois.
    print(f"\n--- {table_name} : doublons sur {expression} ---")
    doublons = spark.sql(f"""
        SELECT {expression} AS cle, COUNT(*) AS n
        FROM {table_name}
        WHERE {expression} IS NOT NULL
        GROUP BY {expression}
        HAVING COUNT(*) > 1
        ORDER BY n DESC
    """)
    print(f"{doublons.count()} valeurs apparaissent plusieurs fois")
    doublons.show(10, truncate=False)


def main():
    # argparse lit les arguments tapés après le nom du script.
    # nargs="*" = zéro, une ou plusieurs valeurs après l'option.
    parser = argparse.ArgumentParser(description="Profilage d'une table du lakehouse")
    parser.add_argument("table", help="nom complet de la table, ex : bronze.customers")
    parser.add_argument("--distinct", nargs="*", default=[], help="colonnes : valeurs distinctes")
    parser.add_argument("--patterns", nargs="*", default=[], help="colonnes : formes des valeurs")
    parser.add_argument("--numeric", nargs="*", default=[], help="colonnes : statistiques numériques")
    parser.add_argument("--duplicates", nargs="*", default=[], help="colonnes ou expressions SQL")
    args = parser.parse_args()

    spark = get_spark_session("profiling")

    # Les valeurs manquantes sont toujours affichées : c'est le premier contrôle à faire.
    profile_nulls(spark, args.table)
    for column in args.distinct:
        profile_column(spark, args.table, column)
    for column in args.patterns:
        profile_patterns(spark, args.table, column)
    for column in args.numeric:
        profile_numeric(spark, args.table, column)
    for expression in args.duplicates:
        profile_duplicates(spark, args.table, expression)

    spark.stop()


if __name__ == "__main__":
    main()