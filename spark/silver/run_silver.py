"""Point d'entrée du pipeline de transformation Bronze vers Silver."""
import sys
from pathlib import Path

# Fonctionne avec spark-submit, quel que soit le répertoire courant.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.spark_session import get_spark_session
from silver.clean_products import clean_products
from silver.clean_customers import clean_customers
from silver.clean_orders import clean_orders
from quality.checks.report_silver import report_silver


def main():
    spark = get_spark_session("silver-jobs")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
        clean_products(spark)
        clean_customers(spark)
        clean_orders(spark)
        report_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
