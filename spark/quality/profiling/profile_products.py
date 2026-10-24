"""
Profilage spécifique de bronze.products : détecter les anomalies avant le nettoyage.
Lecture seule : aucune table n'est modifiée.
La comparaison Bronze/Silver doit être effectuée dans un contrôle post-nettoyage
séparé, car ce profilage doit pouvoir tourner avant la création de silver.products.
"""

import sys

sys.path.append("/opt/app/spark")

from common.spark_session import get_spark_session
from pyspark.sql import functions as F


PRODUCT_ID_PATTERN = r"^PROD-\d{4}$"


def profile_products_overview(df):
    print("\n--- Vue d'ensemble de bronze.products ---")
    print(f"Nombre total de lignes : {df.count()}")
    df.printSchema()
    df.show(20, truncate=False)


def profile_missing_values(df):
    print("\n--- Valeurs manquantes ---")
    for column_name in ["product_id", "product_name", "category", "price"]:
        if column_name not in df.columns:
            print(f"{column_name} : colonne absente")
            continue

        text = F.trim(F.col(column_name).cast("string"))
        missing = df.filter(text.isNull() | (text == "")).count()
        print(f"{column_name} : {missing} valeurs manquantes")


def profile_product_duplicates(df):
    """Distingue les copies exactes des lignes qui ont le même ID mais diffèrent."""
    print("\n--- Doublons de product_id ---")
    business_columns = [
        name for name in ["product_name", "category", "price"] if name in df.columns
    ]
    fingerprint = F.to_json(
        F.struct(*[F.col(name).alias(name) for name in business_columns]),
        {"ignoreNullFields": "false"},
    )

    id_text = F.trim(F.col("product_id").cast("string"))
    base = df.withColumn("_fingerprint", fingerprint).filter(
        id_text.isNotNull() & (id_text != "")
    )

    duplicates = (
        base.groupBy("product_id")
        .agg(
            F.count(F.lit(1)).alias("nb_lignes"),
            F.countDistinct("_fingerprint").alias("nb_versions"),
        )
        .filter(F.col("nb_lignes") > 1)
    )

    print(f"Identifiants product_id dupliqués : {duplicates.count()}")
    print(
        "Groupes de copies exactes : "
        f"{duplicates.filter(F.col('nb_versions') == 1).count()}"
    )
    print(
        "Groupes avec données contradictoires : "
        f"{duplicates.filter(F.col('nb_versions') > 1).count()}"
    )
    duplicates.orderBy(F.desc("nb_lignes")).show(30, truncate=False)


def profile_categories(df):
    print("\n--- Catégories brutes et fréquence après trim/lower ---")
    category_norm = F.lower(F.trim(F.col("category").cast("string")))
    categories = df.groupBy(
        F.when(
            category_norm.isNull() | (category_norm == ""),
            "<NULL/VIDE>",
        ).otherwise(category_norm).alias("category_norm")
    ).count().orderBy(F.desc("count"))

    print(f"Nombre de catégories brutes distinctes : {df.select('category').distinct().count()}")
    print(f"Nombre de catégories après trim/lower : {categories.count()}")
    categories.show(100, truncate=False)


def _profiled_prices(df):
    price_text = F.trim(F.col("price").cast("string"))
    price_num = F.expr(
        "try_cast(trim(CAST(price AS STRING)) AS DOUBLE)"
    )

    return (
        df.withColumn("_price_text", price_text)
        .withColumn("_price_num", price_num)
        .withColumn(
            "_price_status",
            F.when(
                F.col("_price_text").isNull() | (F.col("_price_text") == ""),
                "missing",
            )
            .when(F.col("_price_num").isNull(), "non_numeric")
            .when(F.isnan(F.col("_price_num")), "nan")
            .when(F.abs(F.col("_price_num")) == F.lit(float("inf")), "infinite")
            .when(F.col("_price_num") < 0, "negative")
            .when(F.col("_price_num") == 0, "zero")
            .otherwise("positive"),
        )
    )


def profile_prices(df):
    print("\n--- Répartition des prix par type de valeur ---")
    profiled = _profiled_prices(df)
    profiled.groupBy("_price_status").count().orderBy(
        F.desc("count")
    ).show(truncate=False)

    print("\n--- Textes de prix non numériques les plus fréquents ---")
    (
        profiled.filter(
            F.col("_price_text").isNotNull()
            & (F.col("_price_text") != "")
            & F.col("_price_num").isNull()
        )
        .groupBy("price")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )

    print("\n--- Prix négatifs ou nuls observés ---")
    (
        profiled.filter(F.col("_price_status").isin("negative", "zero"))
        .groupBy("price", "_price_status")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )


def profile_price_statistics(df):
    print("\n--- Statistiques des prix positifs et finis ---")
    profiled = _profiled_prices(df)
    valid = profiled.filter(F.col("_price_status") == "positive")

    valid.agg(
        F.count("*").alias("nb_prix_positifs"),
        F.min("_price_num").alias("prix_min"),
        F.round(F.avg("_price_num"), 2).alias("prix_moyen"),
        F.percentile_approx(
            "_price_num", [0.5, 0.9, 0.99], 10000
        ).alias("percentiles_50_90_99"),
        F.max("_price_num").alias("prix_max"),
    ).show(truncate=False, vertical=True)


def profile_price_outliers(df):
    """Repère les prix aberrants par la méthode IQR ; ne modifie aucune donnée."""
    profiled = _profiled_prices(df)
    valid = profiled.filter(F.col("_price_status") == "positive")

    quartiles = valid.approxQuantile("_price_num", [0.25, 0.75], 0.01)
    print("\n--- Détection de valeurs aberrantes des prix (IQR) ---")

    if len(quartiles) != 2:
        print("Pas assez de prix valides pour calculer les quartiles.")
        return

    q1, q3 = quartiles
    iqr = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    print(f"Q1 : {q1}")
    print(f"Q3 : {q3}")
    print(f"IQR : {iqr}")
    print(f"Seuil inférieur : {lower_fence}")
    print(f"Seuil supérieur : {upper_fence}")

    outliers = valid.filter(
        (F.col("_price_num") < F.lit(lower_fence))
        | (F.col("_price_num") > F.lit(upper_fence))
    )
    print(f"Nombre de prix signalés par IQR : {outliers.count()}")

    outliers.select(
        "product_id", "product_name", "category", "price", "_price_num"
    ).orderBy("_price_num").show(30, truncate=False)


def profile_product_names(df):
    print("\n--- Indicateurs de forme des noms de produits ---")
    name = F.col("product_name").cast("string")

    stats = df.agg(
        F.sum(
            F.when(
                name.isNotNull() & (name != F.trim(name)),
                1,
            ).otherwise(0)
        ).alias("espaces_autour"),
        F.sum(
            F.when(
                name.isNotNull() & name.rlike(r"(?i)([a-z])\1"),
                1,
            ).otherwise(0)
        ).alias("noms_avec_lettres_adjacentes_repetees_candidats"),
        F.sum(
            F.when(
                name.isNotNull()
                & name.rlike(r"[A-Za-z]")
                & (name == F.upper(name)),
                1,
            ).otherwise(0)
        ).alias("noms_entierement_en_majuscules"),
    )
    stats.show(truncate=False)

    print(
        "Exemples avec lettres adjacentes répétées "
        "(candidats à vérifier, pas nécessairement des anomalies) :"
    )
    (
        df.filter(name.rlike(r"(?i)([a-z])\1"))
        .select("product_id", "product_name")
        .show(20, truncate=False)
    )


def profile_suspicious_names(df):
    suspicious = df.filter(
        F.col("product_name").rlike(r"(?i)([a-z])\1{2,}")
    )
    print(f"\nNoms avec au moins trois lettres identiques consécutives : {suspicious.count()}")
    suspicious.select("product_id", "product_name").show(30, truncate=False)


def profile_unusual_vowels(df):
    suspicious = df.filter(
        F.col("product_name").rlike(r"(?i)(aa|ii|uu)")
    )
    print(
        "\nNoms contenant aa, ii ou uu "
        f"(candidats à vérifier, pas nécessairement des anomalies) : {suspicious.count()}"
    )
    suspicious.select("product_id", "product_name").show(30, truncate=False)


def profile_product_id_format(df):
    invalid_ids = df.filter(
        F.col("product_id").isNull()
        | (F.trim(F.col("product_id").cast("string")) == "")
        | (~F.col("product_id").cast("string").rlike(PRODUCT_ID_PATTERN))
    )
    print(f"\nIdentifiants absents ou mal formatés : {invalid_ids.count()}")
    invalid_ids.select("product_id", "product_name").show(20, truncate=False)


def main():
    spark = get_spark_session("profile-products")
    try:
        df = spark.table("bronze.products")
        profile_products_overview(df)
        profile_missing_values(df)
        profile_product_duplicates(df)
        profile_categories(df)
        profile_prices(df)
        profile_price_statistics(df)
        profile_price_outliers(df)
        profile_product_names(df)
        profile_suspicious_names(df)
        profile_unusual_vowels(df)
        profile_product_id_format(df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
