"""Nettoyage déterministe de bronze.orders vers silver.orders.

Le profilage des anomalies est réalisé dans spark/quality/profiling/profile_orders.py.
Ce module applique les règles de transformation et affiche uniquement des contrôles
synthétiques du résultat Silver.
"""
import sys

sys.path.append("/opt/app/spark")

from common.spark_session import get_spark_session
from pyspark.sql import functions as F


# Variantes observées au profilage et dont la correspondance est suffisamment claire.
# Les valeurs non reconnues deviennent NULL dans la colonne nettoyée ; leur valeur
# originale reste disponible dans la colonne *_raw.
PAYMENT_METHOD_MAP = {
    "cash": "cash",
    "card": "card",
    "crad": "card",
    "c@rd": "card",
    "wallet": "wallet",
    "wall-et": "wallet",
    "upi": "upi",
}

STATUS_MAP = {
    "success": "success",
    "suc": "success",
    "refunded": "refunded",
    "ref": "refunded",
    "failed": "failed",
    "fail": "failed",
}


def map_values(col_name, mapping):
    """Retourne une expression Spark qui normalise une valeur via un dictionnaire."""
    mapping_expr = F.create_map(
        *[F.lit(item) for pair in mapping.items() for item in pair]
    )
    normalized = F.lower(F.trim(F.col(col_name).cast("string")))
    return mapping_expr[normalized]


def parse_amount(col_name):
    """Convertit un montant en DECIMAL sans faire échouer le job sur un texte invalide.

    Les montants négatifs et nuls sont conservés : leur interprétation métier
    doit être décidée à partir du profilage et des règles métier.
    """
    return F.expr(
        f"try_cast(trim(CAST(`{col_name}` AS STRING)) AS DECIMAL(18,2))"
    )


def parse_quantity(col_name):
    """Convertit une quantité en entier ; les valeurs négatives et zéro sont conservées.

    La valeur textuelle 'five', observée dans les données, est normalisée en 5.
    Les textes non numériques et les nombres non entiers deviennent NULL.
    """
    raw = F.lower(F.trim(F.col(col_name).cast("string")))
    numeric = F.when(raw == "five", F.lit(5.0)).otherwise(
        F.expr(
            f"try_cast(trim(CAST(`{col_name}` AS STRING)) AS DOUBLE)"
        )
    )

    is_integer = numeric == F.floor(numeric)
    in_int_range = numeric.between(-2147483648, 2147483647)

    return F.when(
        is_integer & in_int_range,
        numeric.cast("int"),
    )


def _safe_to_date(value, date_format):
    """Parse une date sans faire échouer le job si le calendrier est invalide."""
    return F.try_to_timestamp(value, F.lit(date_format)).cast("date")


def parse_order_date(col_name):
    """Convertit les formats de date reconnus en DATE.

    Règles :
    - AAAA-MM-JJ : année/mois/jour ;
    - AAAA/MM/JJ : année/mois/jour par défaut ;
    - AAAA/JJ/MM : retenu si le bloc du milieu est supérieur à 12 ;
    - JJ-MM-AAAA ou JJ/MM/AAAA : jour en premier si le premier bloc > 12 ;
    - MM-JJ-AAAA ou MM/JJ/AAAA : mois en premier si le second bloc > 12 ;
    - dates ambiguës à deux blocs initiaux <= 12 : JJ-MM-AAAA par défaut.
    Les formats non reconnus ou dates calendaires impossibles deviennent NULL.
    """
    raw = F.trim(F.col(col_name).cast("string"))
    date_part = F.substring(raw, 1, 10)

    year_dash = raw.rlike(r"^\d{4}-\d{2}-\d{2}([ T].*)?$")
    year_slash = raw.rlike(r"^\d{4}/\d{2}/\d{2}([ T].*)?$")
    day_first_dash = raw.rlike(r"^\d{2}-\d{2}-\d{4}$")
    day_first_slash = raw.rlike(r"^\d{2}/\d{2}/\d{4}$")

    year_middle = F.substring(date_part, 6, 2).cast("int")
    first_block = F.substring(date_part, 1, 2).cast("int")
    second_dash = F.substring(date_part, 4, 2).cast("int")
    second_slash = F.substring(date_part, 4, 2).cast("int")

    year_slash_date = F.when(
        year_middle > 12,
        _safe_to_date(date_part, "yyyy/dd/MM"),
    ).otherwise(
        _safe_to_date(date_part, "yyyy/MM/dd"),
    )

    dash_date = (
        F.when(
            first_block > 12,
            _safe_to_date(date_part, "dd-MM-yyyy"),
        )
        .when(
            second_dash > 12,
            _safe_to_date(date_part, "MM-dd-yyyy"),
        )
        .otherwise(
            _safe_to_date(date_part, "dd-MM-yyyy"),
        )
    )

    slash_date = (
        F.when(
            first_block > 12,
            _safe_to_date(date_part, "dd/MM/yyyy"),
        )
        .when(
            second_slash > 12,
            _safe_to_date(date_part, "MM/dd/yyyy"),
        )
        .otherwise(
            _safe_to_date(date_part, "dd/MM/yyyy"),
        )
    )

    return (
        F.when(year_dash, _safe_to_date(date_part, "yyyy-MM-dd"))
        .when(year_slash, year_slash_date)
        .when(day_first_dash, dash_date)
        .when(day_first_slash, slash_date)
    )


def clean_orders(spark):
    """Nettoie bronze.orders puis remplace silver.orders."""
    print("\n--- Nettoyage de orders (Bronze -> Silver) ---")

    df = spark.table("bronze.orders")
    total_bronze = df.count()

    cleaned = (
        df
        # Conserver les valeurs source pour assurer la traçabilité.
        .withColumnRenamed("order_amount", "order_amount_raw")
        .withColumnRenamed("order_date", "order_date_raw")
        .withColumnRenamed("payment_method", "payment_method_raw")
        .withColumnRenamed("status", "status_raw")
        .withColumnRenamed("quantity", "quantity_raw")
        # Appliquer les conversions et normalisations.
        .withColumn("order_amount", parse_amount("order_amount_raw"))
        .withColumn("order_date", parse_order_date("order_date_raw"))
        .withColumn(
            "payment_method",
            map_values("payment_method_raw", PAYMENT_METHOD_MAP),
        )
        .withColumn("status", map_values("status_raw", STATUS_MAP))
        .withColumn("quantity", parse_quantity("quantity_raw"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Contrôles synthétiques du résultat : ils ne cherchent pas à expliquer
    # les anomalies source ; ce travail appartient au profilage Bronze.
    summary = cleaned.agg(
        F.count(F.lit(1)).alias("lignes_silver"),
        F.sum(F.col("order_amount").isNull().cast("int")).alias(
            "montants_null_apres_conversion"
        ),
        F.sum(F.col("order_date").isNull().cast("int")).alias(
            "dates_null_apres_conversion"
        ),
        F.sum(F.col("payment_method").isNull().cast("int")).alias(
            "paiements_null_apres_normalisation"
        ),
        F.sum(F.col("status").isNull().cast("int")).alias(
            "statuts_null_apres_normalisation"
        ),
        F.sum(F.col("quantity").isNull().cast("int")).alias(
            "quantites_null_apres_conversion"
        ),
    ).first()

    print(f"Lignes Bronze : {total_bronze}")
    for name, value in summary.asDict().items():
        print(f"{name:42} : {value}")

    print("\n--- Aperçu des colonnes nettoyées ---")
    preview_columns = [
        column
        for column in [
            "order_id",
            "customer_id",
            "product_id",
            "order_amount_raw",
            "order_amount",
            "order_date_raw",
            "order_date",
            "payment_method_raw",
            "payment_method",
            "status_raw",
            "status",
            "quantity_raw",
            "quantity",
        ]
        if column in cleaned.columns
    ]
    cleaned.select(*preview_columns).show(10, truncate=False)

    print("\n--- Schéma de silver.orders avant écriture ---")
    cleaned.printSchema()

    cleaned.writeTo("silver.orders") \
    .using("iceberg") \
    .createOrReplace()
    
    print("Table silver.orders créée ou remplacée avec succès.\n")

if __name__ == "__main__":
    spark = get_spark_session("clean_orders")

    try:
        clean_orders(spark)
    finally:
        spark.stop()
