from common.spark_session import get_spark_session

spark = get_spark_session("inspect")
#spark.sql("SELECT * FROM bronze.customers LIMIT 10").show(truncate=False)
spark.sql("SELECT * FROM silver.products LIMIT 10").show(truncate=False)  
#spark.sql("SELECT * FROM bronze.orders LIMIT 10").show(truncate=False)
spark.stop()