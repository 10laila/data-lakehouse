
import sys

sys.path.append("/opt/app/spark")

from common.spark_session import get_spark_session
from pyspark.sql import functions as F


# ========== Nettoyage de product_name ==========

def clean_product_name(df):
    return (
        df.withColumn("product_name_raw", F.col("product_name"))
          .withColumn(
              "product_name",
              F.lower(F.trim(F.col("product_name")))
          )
    )


def remove_trailing_digits_punctuation_name(df):
    return df.withColumn(
        "product_name",
        F.trim(
            F.regexp_replace(
                F.col("product_name"),
                r"[\d?!.]+$",
                ""
            )
        )
    )


def correct_product_name_typos(df):
    corrections = {
        "beaautiful": "beautiful",
        "speaak": "speak",
        "thaat": "that",
        "republicaan": "republican",
        "reaal": "real",
        "especiaally": "especially",
        "maany": "many",
        "raadio": "radio",
        "paast": "past",
        "reaach": "reach",
        "raange": "range",
        "daark": "dark",
        "personaal": "personal"
    }

    name = F.col("product_name")

    for incorrect, correct in corrections.items():
        name = F.regexp_replace(
            name,
            rf"\b{incorrect}\b",
            correct
        )

    return df.withColumn("product_name", name)


# ========== Nettoyage de category ==========

def clean_category(df):
    return (
        df.withColumn("category_raw", F.col("category"))
          .withColumn(
              "category",
              F.lower(F.trim(F.col("category")))
          )
    )


def remove_trailing_digits_punctuation_category(df):
    return df.withColumn(
        "category",
        F.trim(
            F.regexp_replace(
                F.col("category"),
                r"[\d_!-]+$",
                ""
            )
        )
    )


def correct_category_variants(df):
    corrections = {
        "3l3ctronics": "electronics",
        "ele": "electronics",
        "aut": "automotive",
        "automotiv": "automotive",
        "b3auty": "beauty",
        "bea": "beauty",
        "clo": "clothing",
        "hom": "home",
        "kit": "kitchen",
        "kitch3n": "kitchen",
        "toy": "toys"
    }

    return df.replace(
        to_replace=corrections,
        subset=["category"]
    )


# ========== Nettoyage de price ==========

SPECIAL_PRICE_VALUES = [
    "nan",
    "inf",
    "+inf",
    "-inf",
    "infinity",
    "+infinity",
    "-infinity"
]


def clean_price_format(df):
    return (
        df.withColumn(
            "price_raw",
            F.col("price").cast("string")
        )
        .withColumn(
            "price_normalized",
            F.regexp_replace(
                F.trim(F.col("price_raw")),
                ",",
                ""
            )
        )
    )


def convert_price_to_numeric(df):
    normalized = F.lower(
        F.trim(F.col("price_normalized"))
    )

    return df.withColumn(
        "price",
        F.when(
            F.col("price_normalized").isNull()
            | (F.trim(F.col("price_normalized")) == "")
            | normalized.isin(SPECIAL_PRICE_VALUES),
            F.lit(None).cast("double")
        ).otherwise(
            F.expr("try_cast(price_normalized AS DOUBLE)")
        )
    )


def classify_price_quality(df):
    raw = F.lower(
        F.trim(F.col("price_raw").cast("string"))
    )

    return df.withColumn(
        "price_status",
        F.when(
            F.col("price_raw").isNull()
            | (F.trim(F.col("price_raw")) == ""),
            "missing"
        )
        .when(
            raw.isin(SPECIAL_PRICE_VALUES),
            "non_finite"
        )
        .when(
            F.col("price").isNull(),
            "invalid_format"
        )
        .when(
            F.col("price") < 0,
            "negative"
        )
        .when(
            F.col("price") == 0,
            "zero"
        )
        .otherwise("valid")
    )


def invalidate_nonpositive_prices(df):
    return df.withColumn(
        "price",
        F.when(
            F.col("price_status").isin("negative", "zero"),
            F.lit(None).cast("double")
        ).otherwise(
            F.col("price")
        )
    )


def flag_price_outliers(df):
    valid_prices = df.filter(
        F.col("price_status") == "valid"
    )

    quantiles = valid_prices.approxQuantile(
        "price",
        [0.25, 0.75],
        0.01
    )

    if len(quantiles) < 2:
        return df.withColumn(
            "price_outlier",
            F.lit(None).cast("boolean")
        )

    q1, q3 = quantiles
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    return df.withColumn(
        "price_outlier",
        F.when(
            F.col("price").isNull(),
            F.lit(None).cast("boolean")
        )
        .when(
            (F.col("price") < lower_bound)
            | (F.col("price") > upper_bound),
            F.lit(True)
        )
        .otherwise(F.lit(False))
    )


# ========== Traitement principal ==========

def main():
    spark = get_spark_session("clean_products")

    try:
        df = spark.table("bronze.products")

        # Nettoyage des noms
        df_clean = clean_product_name(df)
        df_clean = remove_trailing_digits_punctuation_name(df_clean)
        df_clean = correct_product_name_typos(df_clean)

        print("Noms après nettoyage :")
        df_clean.select(
            "product_id",
            "product_name_raw",
            "product_name"
        ).show(30, truncate=False)

        # Nettoyage des catégories
        df_clean = clean_category(df_clean)
        df_clean = remove_trailing_digits_punctuation_category(df_clean)
        df_clean = correct_category_variants(df_clean)

        print("Catégories après nettoyage :")
        df_clean.groupBy("category") \
            .count() \
            .orderBy("category") \
            .show(30, truncate=False)

        # Nettoyage des prix
        df_clean = clean_price_format(df_clean)
        df_clean = convert_price_to_numeric(df_clean)
        df_clean = classify_price_quality(df_clean)
        df_clean = invalidate_nonpositive_prices(df_clean)
        df_clean = flag_price_outliers(df_clean)

        print("Répartition de la qualité des prix :")
        df_clean.groupBy("price_status") \
            .count() \
            .orderBy(F.desc("count")) \
            .show(truncate=False)

        print("Prix après nettoyage :")
        df_clean.select(
            "product_id",
            "price_raw",
            "price_normalized",
            "price",
            "price_status",
            "price_outlier"
        ).show(300, truncate=False)

        print("Prix extrêmes à vérifier :")
        df_clean.filter(
            F.col("price_outlier") == True
        ).select(
            "product_id",
            "price_raw",
            "price"
        ).show(300, truncate=False)

        # Vérifications de qualité
        total = df_clean.count()

        distinct_ids = (
            df_clean
            .filter(
                F.col("product_id").isNotNull()
                & (F.trim(F.col("product_id")) != "")
            )
            .select("product_id")
            .distinct()
            .count()
        )

        empty_ids = df_clean.filter(
            F.col("product_id").isNull()
            | (F.trim(F.col("product_id")) == "")
        ).count()

        empty_names = df_clean.filter(
            F.col("product_name").isNull()
            | (F.trim(F.col("product_name")) == "")
        ).count()

        print("Contrôles de qualité :")
        print(f"Nombre total de produits : {total}")
        print(f"Nombre d'identifiants distincts non vides : {distinct_ids}")
        print(f"Nombre d'identifiants nuls ou vides : {empty_ids}")
        print(f"Nombre de noms nuls ou vides : {empty_names}")

        print("Identifiants dupliqués :")
        df_clean.filter(
            F.col("product_id").isNotNull()
            & (F.trim(F.col("product_id")) != "")
        ).groupBy("product_id") \
            .count() \
            .filter(F.col("count") > 1) \
            .show(truncate=False)

        # Métadonnée de traitement Silver
        df_clean = df_clean.withColumn(
            "_silver_processed_at",
            F.current_timestamp()
        )

        print("Schéma final :")
        df_clean.printSchema()

        # Écriture dans la couche Silver avec Iceberg
        df_clean.writeTo("silver.products") \
            .using("iceberg") \
            .createOrReplace()

        print("Table silver.products créée ou remplacée avec succès.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
