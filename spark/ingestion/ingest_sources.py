"""
Lit les fichiers CSV bruts et les écrit tels quels (sans nettoyage) en tables
Iceberg Bronze. Aucune transformation métier ici : Bronze doit rester le
miroir fidèle de la source, avec juste des métadonnées de traçabilité en plus.
"""

import sys
from datetime import datetime, timezone

sys.path.append("/opt/app/spark")
from common.spark_session import get_spark_session
from pyspark.sql.functions import lit, current_timestamp

DATA_DIR = "/opt/app/data/sample"

# Déclaration des sources à ingérer pour le MVP.
# On garde volontairement clickstream et support_tickets de côté (cf. plan en 8 étapes).
SOURCES = {
    "customers": f"{DATA_DIR}/crm_50000_customers_dirty_v3.csv",
    "orders": f"{DATA_DIR}/orders_300k_dirty.csv",
    "products": f"{DATA_DIR}/product_catalog_dirty_30pct.csv",
}


def ingest_source(spark, table_name: str, csv_path: str):
    print(f"--- Ingestion de {table_name} depuis {csv_path} ---")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )

    row_count = df.count()
    print(f"{row_count} lignes lues depuis {csv_path}")

    # Métadonnées de traçabilité : d'où vient la ligne, quand a-t-elle été ingérée
    df_with_meta = df.withColumn("_ingested_at", current_timestamp()).withColumn(
        "_source_file", lit(csv_path)
    )

    df_with_meta.printSchema()

    full_table_name = f"bronze.{table_name}"

    (
        df_with_meta.writeTo(full_table_name)
        .using("iceberg")
        .createOrReplace()
    )

    print(f"✅ Table {full_table_name} créée avec {row_count} lignes\n")


def main():
    spark = get_spark_session("ingestion-sources")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

    for table_name, csv_path in SOURCES.items():
        ingest_source(spark, table_name, csv_path)

    print("=== Vérification finale ===")
    spark.sql("SHOW TABLES IN bronze").show()

    spark.stop()


if __name__ == "__main__":
    main()