"""Normalisation de bronze.products vers silver.products."""
from difflib import get_close_matches
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


CANONICAL_CATEGORIES = [
    "clothing", "electronics", "home", "toys", "automotive",
    "sports", "kitchen", "beauty",
]

def normalize_leetspeak(s: str) -> str:
    table = str.maketrans("034157", "oeaist")
    return s.translate(table)


def match_category(raw_value):
    if raw_value is None:
        return None
    cleaned = normalize_leetspeak(raw_value.strip().lower().rstrip("_-"))
    if not cleaned:
        return None
    if len(cleaned) >= 3:
        candidates = [c for c in CANONICAL_CATEGORIES if c.startswith(cleaned)]
        if len(candidates) == 1:
            return candidates[0]
    close = get_close_matches(cleaned, CANONICAL_CATEGORIES, n=1, cutoff=0.8)
    return close[0] if close else None


match_category_udf = F.udf(match_category, StringType())


def clean_products(spark):
    print("--- Nettoyage de products (Bronze -> Silver) ---")
    df = spark.table("bronze.products")

    df_clean = (
        df.withColumn("product_name", F.trim(F.col("product_name")))
        .withColumnRenamed("category", "category_raw")
        .withColumn("category", match_category_udf(F.col("category_raw")))
        .withColumnRenamed("price", "price_raw")
        .withColumn("price_cast", F.col("price_raw").cast("double"))
        .withColumn(
            "price",
            F.when(
                (~F.isnan("price_cast"))
                & (F.col("price_cast") > 0)
                & (F.col("price_cast") < float("inf")),
                F.col("price_cast"),
            ),
        )
        .drop("price_cast")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    total = df_clean.count()
    invalid_price = df_clean.filter(F.col("price").isNull()).count()
    invalid_category = df_clean.filter(F.col("category").isNull()).count()
    print(f"Total lignes: {total}")
    print(f"Prix invalides (mis à NULL): {invalid_price} ({100*invalid_price/total:.1f}%)")
    print(f"Catégories non reconnues (mises à NULL): {invalid_category} ({100*invalid_category/total:.1f}%)")

    (
        df_clean.writeTo("silver.products")
        .using("iceberg")
        .createOrReplace()
    )
    print("✅ Table silver.products créée\n")
