
"""Point d'entrée du pipeline de transformation Bronze vers Silver."""

import sys
from pathlib import Path

# Permet de retrouver les modules du dossier spark.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.spark_session import get_spark_session
from silver.clean_products import clean_products
from silver.clean_customers import clean_customers
from silver.clean_orders import clean_orders
from quality.checks.report_silver import report_silver


def validate_bronze_tables(spark):
    """Vérifie la présence des tables Bronze nécessaires."""

    required_tables = [
        "bronze.products",
        "bronze.customers",
        "bronze.orders",
    ]

    missing_tables = [
        table
        for table in required_tables
        if not spark.catalog.tableExists(table)
    ]

    if missing_tables:
        raise RuntimeError(
            "Tables Bronze manquantes : "
            + ", ".join(missing_tables)
            + ". Vérifie l'ingestion avant de lancer Silver."
        )


def main():
    spark = get_spark_session("silver-jobs")

    try:
        # Vérifier les entrées avant de modifier les tables Silver.
        validate_bronze_tables(spark)

        # S'assurer que le namespace Silver existe.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

        print("\n=== Début du pipeline Bronze -> Silver ===")

        print("\n[1/4] Nettoyage des produits")
        clean_products(spark)

        print("\n[2/4] Nettoyage des clients")
        clean_customers(spark)

        print("\n[3/4] Nettoyage des commandes")
        clean_orders(spark)

        print("\n[4/4] Rapport de vérification Silver")
        report_silver(spark)

        print("\n=== Pipeline Silver terminé ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
