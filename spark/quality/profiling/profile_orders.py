"""
Profilage spécifique de bronze.orders : détecter les anomalies avant Silver.
Lecture seule : aucune table n'est modifiée.
Les références sont comparées à bronze.customers et bronze.products, afin que ce
profilage puisse être exécuté avant la création ou le remplacement des tables Silver.
"""

import sys

sys.path.append("/opt/app/spark")

from common.spark_session import get_spark_session
from pyspark.sql import functions as F


def _safe_to_date(value, date_format):
    return F.try_to_timestamp(value, F.lit(date_format)).cast("date")


def profile_order_amount(spark):
    df = spark.table("bronze.orders")
    amount_text = F.trim(F.col("order_amount").cast("string"))
    amount_num = F.expr(
        "try_cast(trim(CAST(order_amount AS STRING)) AS DOUBLE)"
    )

    profiled = (
        df.withColumn("_amount_text", amount_text)
        .withColumn("_amount_num", amount_num)
        .withColumn(
            "_amount_category",
            F.when(
                F.col("_amount_text").isNull() | (F.col("_amount_text") == ""),
                "vide",
            )
            .when(F.col("_amount_num").isNull(), "texte_non_numerique")
            .when(F.isnan(F.col("_amount_num")), "NaN")
            .when(F.abs(F.col("_amount_num")) == F.lit(float("inf")), "infini")
            .when(F.col("_amount_num") < 0, "negatif")
            .when(F.col("_amount_num") == 0, "zero")
            .otherwise("positif"),
        )
    )

    print("\n--- order_amount : répartition par type de valeur ---")
    profiled.groupBy("_amount_category").count().orderBy(
        F.desc("count")
    ).show(truncate=False)

    print("\n--- order_amount : textes non numériques les plus fréquents ---")
    (
        profiled.filter(
            F.col("_amount_text").isNotNull()
            & (F.col("_amount_text") != "")
            & F.col("_amount_num").isNull()
        )
        .groupBy("order_amount")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )

    finite_amount = (
        F.col("_amount_num").isNotNull()
        & (~F.isnan(F.col("_amount_num")))
        & (F.abs(F.col("_amount_num")) != F.lit(float("inf")))
    )

    print("\n--- order_amount : montants négatifs et nuls observés ---")
    (
        profiled.filter(finite_amount & (F.col("_amount_num") <= 0))
        .groupBy("order_amount", "_amount_num")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )

    print("\n--- order_amount négatifs par statut ---")
    (
        profiled.filter(finite_amount & (F.col("_amount_num") < 0))
        .groupBy(F.lower(F.trim(F.col("status").cast("string"))).alias("statut"))
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    print("\n--- Statistiques des montants strictement positifs et finis ---")
    (
        profiled.filter(finite_amount & (F.col("_amount_num") > 0))
        .agg(
            F.count("*").alias("nb_montants_positifs"),
            F.min("_amount_num").alias("minimum"),
            F.percentile_approx(
                "_amount_num", [0.5, 0.9, 0.99, 0.999], 10000
            ).alias("percentiles_50_90_99_999"),
            F.max("_amount_num").alias("maximum"),
        )
        .show(truncate=False, vertical=True)
    )


def profile_quantity(spark):
    df = spark.table("bronze.orders")
    quantity_text = F.lower(F.trim(F.col("quantity").cast("string")))
    quantity_num = F.expr(
        "try_cast(trim(CAST(quantity AS STRING)) AS DOUBLE)"
    )

    profiled = (
        df.withColumn("_quantity_text", quantity_text)
        .withColumn("_quantity_num", quantity_num)
        .withColumn(
            "_quantity_category",
            F.when(
                F.col("_quantity_text").isNull()
                | (F.col("_quantity_text") == ""),
                "vide",
            )
            .when(F.col("_quantity_text") == "five", "nombre_ecrit_en_lettres")
            .when(F.col("_quantity_num").isNull(), "texte_non_numerique")
            .when(
                F.col("_quantity_num") != F.floor(F.col("_quantity_num")),
                "non_entier",
            )
            .when(F.col("_quantity_num") < 0, "negatif")
            .when(F.col("_quantity_num") == 0, "zero")
            .otherwise("entier_positif"),
        )
    )

    print("\n--- quantity : répartition par type de valeur ---")
    profiled.groupBy("_quantity_category").count().orderBy(
        F.desc("count")
    ).show(truncate=False)

    print("\n--- quantity : valeurs non numériques les plus fréquentes ---")
    (
        profiled.filter(
            (F.col("_quantity_category") == "texte_non_numerique")
            | (F.col("_quantity_category") == "nombre_ecrit_en_lettres")
        )
        .groupBy("quantity")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )

    print("\n--- quantity : exemples de quantités négatives, nulles ou non entières ---")
    (
        profiled.filter(
            F.col("_quantity_category").isin("negatif", "zero", "non_entier")
        )
        .select("order_id", "quantity", "_quantity_num", "_quantity_category")
        .show(20, truncate=False)
    )


def profile_order_date(spark):
    df = spark.table("bronze.orders")
    date_text = F.trim(F.col("order_date").cast("string"))
    date_part = F.substring(date_text, 1, 10)

    year_dash = date_text.rlike(r"^\d{4}-\d{2}-\d{2}([ T].*)?$")
    year_slash = date_text.rlike(r"^\d{4}/\d{2}/\d{2}([ T].*)?$")
    two_dash = date_text.rlike(r"^\d{2}-\d{2}-\d{4}$")
    two_slash = date_text.rlike(r"^\d{2}/\d{2}/\d{4}$")

    year_middle = F.substring(date_part, 6, 2).cast("int")
    year_last = F.substring(date_part, 9, 2).cast("int")
    first_block = F.substring(date_part, 1, 2).cast("int")
    second_block = F.substring(date_part, 4, 2).cast("int")

    format_category = (
        F.when(date_text.isNull() | (date_text == ""), "vide")
        .when(year_dash, "annee-mois-jour_avec_tirets")
        .when(year_slash, "annee-blocs_avec_slashs")
        .when(two_dash, "deux_blocs_annee_avec_tirets")
        .when(two_slash, "deux_blocs_annee_avec_slashs")
        .otherwise("autre_format")
    )

    year_slash_parsed = F.when(
        year_middle > 12,
        _safe_to_date(date_part, "yyyy/dd/MM"),
    ).otherwise(
        _safe_to_date(date_part, "yyyy/MM/dd"),
    )

    dash_parsed = (
        F.when(first_block > 12, _safe_to_date(date_part, "dd-MM-yyyy"))
        .when(second_block > 12, _safe_to_date(date_part, "MM-dd-yyyy"))
        .otherwise(_safe_to_date(date_part, "dd-MM-yyyy"))
    )
    slash_parsed = (
        F.when(first_block > 12, _safe_to_date(date_part, "dd/MM/yyyy"))
        .when(second_block > 12, _safe_to_date(date_part, "MM/dd/yyyy"))
        .otherwise(_safe_to_date(date_part, "dd/MM/yyyy"))
    )

    parsed_date = (
        F.when(year_dash, _safe_to_date(date_part, "yyyy-MM-dd"))
        .when(year_slash, year_slash_parsed)
        .when(two_dash, dash_parsed)
        .when(two_slash, slash_parsed)
    )

    profiled = (
        df.withColumn("_date_text", date_text)
        .withColumn("_date_format", format_category)
        .withColumn("_parsed_date", parsed_date)
    )

    print("\n--- order_date : répartition des formats détectés ---")
    profiled.groupBy("_date_format").count().orderBy(
        F.desc("count")
    ).show(truncate=False)

    ambiguous_two_blocks = (
        (two_dash | two_slash)
        & (first_block <= 12)
        & (second_block <= 12)
        & (first_block > 0)
        & (second_block > 0)
    )
    ambiguous_year_slash = (
        year_slash
        & (year_middle <= 12)
        & (year_last <= 12)
        & (year_middle > 0)
        & (year_last > 0)
    )

    print("\n--- Dates ambiguës : interprétation par défaut ---")
    print(
        "JJ-MM-AAAA par défaut pour les formats à deux blocs initiaux <= 12 ; "
        "AAAA/MM/JJ par défaut pour les dates qui commencent par l'année."
    )
    print(
        "Dates ambiguës avec deux blocs initiaux : "
        f"{profiled.filter(ambiguous_two_blocks).count()}"
    )
    print(
        "Dates ambiguës au format AAAA/MM/JJ : "
        f"{profiled.filter(ambiguous_year_slash).count()}"
    )

    recognized = year_dash | year_slash | two_dash | two_slash
    invalid_calendar = profiled.filter(
        date_text.isNotNull()
        & (date_text != "")
        & recognized
        & F.col("_parsed_date").isNull()
    )
    print(f"\nDates avec format reconnu mais calendrier invalide : {invalid_calendar.count()}")
    invalid_calendar.select("order_date").show(20, truncate=False)

    print("\n--- Exemples de formats non reconnus ---")
    (
        profiled.filter(
            date_text.isNotNull()
            & (date_text != "")
            & (~recognized)
        )
        .groupBy("order_date")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )


def profile_order_categories(spark):
    df = spark.table("bronze.orders")
    for column_name in ["payment_method", "status"]:
        value = F.lower(F.trim(F.col(column_name).cast("string")))
        profiled = df.withColumn(
            "_value_norm",
            F.when(value.isNull() | (value == ""), "<NULL/VIDE>").otherwise(value),
        )
        print(f"\n--- Valeurs observées pour {column_name} ---")
        (
            profiled.groupBy("_value_norm")
            .count()
            .orderBy(F.desc("count"))
            .show(100, truncate=False)
        )


def profile_references(spark):
    orders = spark.table("bronze.orders")
    customers = spark.table("bronze.customers")
    products = spark.table("bronze.products")

    known_customer_ids = (
        customers.select(
            F.lower(F.trim(F.col("customer_id").cast("string"))).alias("_ref_id")
        )
        .filter(F.col("_ref_id").isNotNull() & (F.col("_ref_id") != ""))
        .distinct()
    )
    order_customer_ids = (
        orders.select("customer_id")
        .withColumn(
            "_ref_id",
            F.lower(F.trim(F.col("customer_id").cast("string"))),
        )
        .filter(F.col("_ref_id").isNotNull() & (F.col("_ref_id") != ""))
    )
    unknown_customers = order_customer_ids.join(
        known_customer_ids, on="_ref_id", how="left_anti"
    )

    print("\n--- Références client absentes de bronze.customers ---")
    print(f"Lignes de commande concernées : {unknown_customers.count()}")
    print(
        "Identifiants clients inconnus distincts : "
        f"{unknown_customers.select('_ref_id').distinct().count()}"
    )
    unknown_customers.groupBy("_ref_id").count().orderBy(
        F.desc("count")
    ).show(20, truncate=False)

    known_product_ids = (
        products.select(
            F.lower(F.trim(F.col("product_id").cast("string"))).alias("_ref_id")
        )
        .filter(F.col("_ref_id").isNotNull() & (F.col("_ref_id") != ""))
        .distinct()
    )
    order_product_ids = (
        orders.select("product_id")
        .withColumn(
            "_ref_id",
            F.lower(F.trim(F.col("product_id").cast("string"))),
        )
        .filter(F.col("_ref_id").isNotNull() & (F.col("_ref_id") != ""))
    )
    unknown_products = order_product_ids.join(
        known_product_ids, on="_ref_id", how="left_anti"
    )

    print("\n--- Références produit absentes de bronze.products ---")
    print(f"Lignes de commande concernées : {unknown_products.count()}")
    print(
        "Identifiants produits inconnus distincts : "
        f"{unknown_products.select('_ref_id').distinct().count()}"
    )
    unknown_products.groupBy("_ref_id").count().orderBy(
        F.desc("count")
    ).show(20, truncate=False)

    print("\n--- Identifiants client avec majuscules ou espaces ---")
    customer_id_text = F.trim(F.col("customer_id").cast("string"))
    print(
        orders.filter(
            customer_id_text.isNotNull()
            & (F.col("customer_id").cast("string") != F.lower(customer_id_text))
        ).count()
    )

    print("\n--- Identifiants produit avec majuscules ou espaces ---")
    product_id_text = F.trim(F.col("product_id").cast("string"))
    print(
        orders.filter(
            product_id_text.isNotNull()
            & (F.col("product_id").cast("string") != F.lower(product_id_text))
        ).count()
    )


def main():
    spark = get_spark_session("profile-orders")
    try:
        print("\n--- Schéma de bronze.orders ---")
        spark.table("bronze.orders").printSchema()

        profile_order_amount(spark)
        profile_quantity(spark)
        profile_order_date(spark)
        profile_order_categories(spark)
        profile_references(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
