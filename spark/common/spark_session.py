"""
Configuration centralisée de la SparkSession pour tout le projet.
Tous les scripts (ingestion, bronze, silver, gold) importent get_spark_session()
au lieu de recréer leur propre configuration Spark<->MinIO<->Iceberg.
"""
#SparkSesson et la classe fournie par PySpark pour piloter Spark
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "lakehouse") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        # --- Extensions Iceberg ---
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # --- Déclaration du catalogue "lakehouse" pointant vers le REST catalog ---
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/") #Elle indique à Spark d'utiliser le bucket warehouse comme emplacement de stockage par défaut du catalogue.
        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        # --- config du client S3FileIO d'Iceberg (SDK AWS v2) ---
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", "minioadmin")
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", "minioadmin123")
        .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")

        # --- Connexion S3A vers MinIO (nécessaire pour lire/écrire les fichiers) ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # --- Catalogue par défaut pour ne pas préfixer "lakehouse." partout ---
        .config("spark.sql.defaultCatalog", "lakehouse")
                # Désactive le lecteur Parquet "vectorisé" (Arrow) d'Iceberg : plus lent, mais
        # évite les plantages de la JVM (SIGSEGV) observés sur cette machine.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        # Désactive la compilation "whole-stage" de Spark, qui génère de gros blocs de code Java.
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark