
"""Pipeline Gold : création des tables analytiques à partir de Silver."""

import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.spark_session import get_spark_session


# ============================================================
# 1. VALIDATION DES SOURCES SILVER
# ============================================================

REQUIRED_COLUMNS = {
    "silver.orders": [
        "order_id",
        "customer_id",
        "product_id",
        "order_amount",
        "order_date",
        "payment_method",
        "status",
        "quantity",
    ],
    "silver.customers": [
        "customer_id",
        "country",
        "state",
        "city",
    ],
    "silver.products": [
        "product_id",
        "product_name",
        "category",
        "price",
    ],
}


def validate_silver_tables(spark):
    """Vérifie l'existence des tables et de leurs colonnes nécessaires."""

    for table_name, required_columns in REQUIRED_COLUMNS.items():
        if not spark.catalog.tableExists(table_name):
            raise RuntimeError(
                f"Table Silver manquante : {table_name}"
            )

        available_columns = spark.table(table_name).columns
        missing_columns = [
            column
            for column in required_columns
            if column not in available_columns
        ]

        if missing_columns:
            raise RuntimeError(
                f"Colonnes manquantes dans {table_name} : "
                + ", ".join(missing_columns)
            )

    print("Validation des tables Silver réussie.")


def validate_unique_key(df, key, table_name):
    """Empêche les clés dupliquées de fausser les agrégations Gold."""

    valid_key = (
        F.col(key).isNotNull()
        & (F.trim(F.col(key).cast("string")) != "")
    )

    duplicate_groups = (
        df.filter(valid_key)
        .groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(
        f"Groupes de clés dupliquées dans {table_name} "
        f"({key}) : {duplicate_groups}"
    )

    if duplicate_groups > 0:
        raise RuntimeError(
            f"Clé {key} non unique dans {table_name}. "
            "Corrige cette anomalie avant de construire Gold."
        )


def label_or_unknown(column_name):
    """Remplace les valeurs textuelles absentes par 'Unknown'."""

    value = F.trim(F.col(column_name).cast("string"))

    return F.when(
        value.isNull() | (value == ""),
        F.lit("Unknown"),
    ).otherwise(value)


# ============================================================
# 2. CONTRÔLE DES DONNÉES SOURCES
# ============================================================

def inspect_orders(orders):
    """Affiche les principaux indicateurs de qualité des commandes."""

    print("\n=== Qualité des commandes Silver ===")

    orders.groupBy("status").agg(
        F.count("*").alias("nb_commandes"),
        F.sum(
            F.when(F.col("order_amount").isNull(), 1).otherwise(0)
        ).alias("montants_absents"),
        F.sum(
            F.when(F.col("order_amount") < 0, 1).otherwise(0)
        ).alias("montants_negatifs"),
        F.sum(
            F.when(F.col("order_amount") == 0, 1).otherwise(0)
        ).alias("montants_nuls"),
        F.sum(
            F.when(F.col("order_amount") > 0, 1).otherwise(0)
        ).alias("montants_positifs"),
    ).orderBy("status").show(truncate=False)


# ============================================================
# 3. CONSTRUCTION DU JEU DE VENTES
# ============================================================

def build_sales_dataset(orders, customers, products):
    """Sélectionne les ventes retenues et les enrichit avec les dimensions."""

    # Règle métier :
    # seules les commandes réussies, avec un montant positif
    # et renseigné, sont retenues dans les indicateurs de ventes.
    eligible_orders = orders.filter(
        (F.col("status") == "success")
        & F.col("order_amount").isNotNull()
        & (F.col("order_amount") > 0)
    )

    # Préparer les informations géographiques des clients.
    valid_customer_id = (
        F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
    )

    customer_locations = (
        customers.filter(valid_customer_id)
        .select(
            "customer_id",
            "country",
            "state",
            "city",
            F.lit(True).alias("_customer_matched"),
        )
    )

    # Préparer les informations des produits.
    valid_product_id = (
        F.col("product_id").isNotNull()
        & (F.trim(F.col("product_id")) != "")
    )

    product_info = (
        products.filter(valid_product_id)
        .select(
            "product_id",
            "product_name",
            "category",
            F.col("price").alias("catalog_price"),
            F.lit(True).alias("_product_matched"),
        )
    )

    # Relier les commandes aux clients, puis aux produits.
    sales = (
        eligible_orders
        .join(
            customer_locations,
            on="customer_id",
            how="left",
        )
        .join(
            product_info,
            on="product_id",
            how="left",
        )
    )

    # Rendre les dimensions textuelles exploitables dans les rapports.
    for column_name in [
        "country",
        "state",
        "city",
        "payment_method",
        "product_id",
        "product_name",
        "category",
    ]:
        sales = sales.withColumn(
            column_name,
            label_or_unknown(column_name),
        )

    return sales


# ============================================================
# 4. INDICATEURS GLOBAUX
# ============================================================

def build_sales_overview(sales):
    """Construit une vue globale des ventes retenues."""

    return sales.agg(
        F.count("*").alias("nb_commandes_reussies_retenues"),

        F.sum("order_amount").alias("montant_total_ventes"),

        F.round(
            F.avg("order_amount"), 2
        ).alias("montant_moyen_commande"),

        F.countDistinct(
            "customer_id"
        ).alias("nb_clients_distincts"),

        F.sum(
            F.when(F.col("order_date").isNull(), 1).otherwise(0)
        ).alias("nb_commandes_sans_date"),

        F.sum(
            F.when(
                F.col("quantity").isNull()
                | (F.col("quantity") <= 0),
                1,
            ).otherwise(0)
        ).alias("nb_commandes_quantite_invalide"),

        F.sum(
            F.when(
                F.col("quantity") > 0,
                F.col("quantity"),
            ).otherwise(0)
        ).alias("unites_vendues_quantite_positive"),

        F.sum(
            F.when(
                F.col("_customer_matched").isNull(),
                1,
            ).otherwise(0)
        ).alias("nb_commandes_sans_client_correspondant"),

        F.sum(
            F.when(
                F.col("_product_matched").isNull(),
                1,
            ).otherwise(0)
        ).alias("nb_commandes_sans_produit_correspondant"),

        F.sum(
            F.when(
                F.col("payment_method") == "Unknown",
                1,
            ).otherwise(0)
        ).alias("nb_commandes_sans_moyen_paiement"),
    )


# ============================================================
# 5. VENTES PAR LOCALISATION
# ============================================================

def build_sales_by_region(sales):
    """Agrège les ventes par pays, État et ville."""

    return sales.groupBy(
        "country",
        "state",
        "city",
    ).agg(
        F.count("*").alias("nb_commandes"),

        F.sum(
            "order_amount"
        ).alias("montant_total_ventes"),

        F.round(
            F.avg("order_amount"), 2
        ).alias("montant_moyen_commande"),

        F.countDistinct(
            "customer_id"
        ).alias("nb_clients_distincts"),

        F.sum(
            F.when(
                F.col("quantity") > 0,
                F.col("quantity"),
            ).otherwise(0)
        ).alias("unites_vendues_quantite_positive"),

        F.sum(
            F.when(
                F.col("order_date").isNull(),
                1,
            ).otherwise(0)
        ).alias("nb_commandes_sans_date"),

        F.sum(
            F.when(
                F.col("_customer_matched").isNull(),
                1,
            ).otherwise(0)
        ).alias("nb_commandes_sans_client_correspondant"),
    )


# ============================================================
# 6. VENTES PAR MOIS
# ============================================================

def build_sales_by_month(sales):
    """Calcule les indicateurs mensuels sur les commandes datées."""

    dated_sales = (
        sales.filter(F.col("order_date").isNotNull())
        .withColumn(
            "mois",
            F.trunc(F.col("order_date"), "month"),
        )
    )

    return (
        dated_sales.groupBy("mois")
        .agg(
            F.count("*").alias("nb_commandes"),

            F.sum(
                "order_amount"
            ).alias("montant_total_ventes"),

            F.round(
                F.avg("order_amount"), 2
            ).alias("montant_moyen_commande"),

            F.countDistinct(
                "customer_id"
            ).alias("nb_clients_distincts"),

            F.sum(
                F.when(
                    F.col("quantity") > 0,
                    F.col("quantity"),
                ).otherwise(0)
            ).alias("unites_vendues_quantite_positive"),
        )
        .withColumn("annee", F.year("mois"))
        .withColumn("numero_mois", F.month("mois"))
        .select(
            "mois",
            "annee",
            "numero_mois",
            "nb_commandes",
            "montant_total_ventes",
            "montant_moyen_commande",
            "nb_clients_distincts",
            "unites_vendues_quantite_positive",
        )
    )


# ============================================================
# 7. VENTES PAR PRODUIT
# ============================================================

def build_sales_by_product(sales):
    """Calcule les indicateurs commerciaux par produit et catégorie."""

    return sales.groupBy(
        "product_id",
        "product_name",
        "category",
    ).agg(
        F.first(
            "catalog_price",
            ignorenulls=True,
        ).alias("prix_catalogue"),

        F.count("*").alias("nb_commandes"),

        F.sum(
            "order_amount"
        ).alias("montant_total_ventes"),

        F.round(
            F.avg("order_amount"), 2
        ).alias("montant_moyen_commande"),

        F.countDistinct(
            "customer_id"
        ).alias("nb_clients_distincts"),

        F.sum(
            F.when(
                F.col("quantity") > 0,
                F.col("quantity"),
            ).otherwise(0)
        ).alias("unites_vendues_quantite_positive"),

        F.sum(
            F.when(
                F.col("_product_matched").isNull(),
                1,
            ).otherwise(0)
        ).alias("nb_commandes_sans_produit_correspondant"),
    )


# ============================================================
# 8. VENTES PAR MOYEN DE PAIEMENT
# ============================================================

def build_sales_by_payment_method(sales):
    """Compare les ventes selon le moyen de paiement."""

    return sales.groupBy("payment_method").agg(
        F.count("*").alias("nb_commandes"),

        F.sum(
            "order_amount"
        ).alias("montant_total_ventes"),

        F.round(
            F.avg("order_amount"), 2
        ).alias("montant_moyen_commande"),

        F.countDistinct(
            "customer_id"
        ).alias("nb_clients_distincts"),
    )


# ============================================================
# 9. SUIVI DE TOUS LES STATUTS DE COMMANDE
# ============================================================

def build_orders_by_status(orders):
    """Suit les commandes, y compris échouées et remboursées."""

    orders_with_status = orders.withColumn(
        "_status_for_gold",
        label_or_unknown("status"),
    )

    return (
        orders_with_status.groupBy("_status_for_gold")
        .agg(
            F.count("*").alias("nb_commandes"),

            F.sum(
                F.when(
                    F.col("order_amount").isNull(),
                    1,
                ).otherwise(0)
            ).alias("nb_montants_absents"),

            F.sum(
                F.when(
                    F.col("order_amount") < 0,
                    1,
                ).otherwise(0)
            ).alias("nb_montants_negatifs"),

            F.sum(
                F.when(
                    F.col("order_amount") == 0,
                    1,
                ).otherwise(0)
            ).alias("nb_montants_nuls"),

            F.sum(
                F.when(
                    F.col("order_amount") > 0,
                    1,
                ).otherwise(0)
            ).alias("nb_montants_positifs"),

            # Cette somme inclut tous les statuts :
            # ce n'est pas le montant des ventes réussies.
            F.sum(
                "order_amount"
            ).alias("somme_montants_renseignes"),
        )
        .withColumnRenamed(
            "_status_for_gold",
            "status",
        )
    )


# ============================================================
# 10. ÉCRITURE ET VÉRIFICATION DES TABLES GOLD
# ============================================================

def write_gold_table(spark, table_name, dataframe, preview_order=None):
    """Écrit une table Iceberg Gold et affiche un aperçu."""

    output = dataframe.withColumn(
        "_gold_processed_at",
        F.current_timestamp(),
    )

    (
        output.writeTo(table_name)
        .using("iceberg")
        .createOrReplace()
    )

    saved_table = spark.table(table_name)

    print(f"\n=== Table créée : {table_name} ===")
    print(f"Nombre de lignes écrites : {saved_table.count()}")

    if preview_order:
        saved_table.orderBy(*preview_order).show(
            10,
            truncate=False,
        )
    else:
        saved_table.show(10, truncate=False)


# ============================================================
# 11. PIPELINE PRINCIPAL
# ============================================================

def main():
    spark = get_spark_session("gold-pipeline")
    sales = None

    try:
        print("\n========== DÉMARRAGE DU PIPELINE GOLD ==========")

        # A. Vérifier les sources et leurs clés.
        validate_silver_tables(spark)

        orders = spark.table("silver.orders")
        customers = spark.table("silver.customers")
        products = spark.table("silver.products")

        validate_unique_key(
            orders, "order_id", "silver.orders"
        )
        validate_unique_key(
            customers, "customer_id", "silver.customers"
        )
        validate_unique_key(
            products, "product_id", "silver.products"
        )

        # B. Examiner les anomalies des commandes.
        inspect_orders(orders)

        print("\n=== Volumes des tables sources ===")
        print("silver.orders    :", orders.count())
        print("silver.customers :", customers.count())
        print("silver.products  :", products.count())

        # C. Préparer le namespace Gold.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

        # D. Construire le jeu commun de ventes et le mettre en cache.
        sales = build_sales_dataset(
            orders,
            customers,
            products,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        nb_sales = sales.count()

        print("\n=== Jeu de ventes préparé ===")
        print("Nombre de commandes retenues :", nb_sales)

        sales.select(
            "order_id",
            "order_amount",
            "order_date",
            "country",
            "state",
            "city",
            "product_name",
            "category",
            "payment_method",
        ).show(10, truncate=False)

        # E. Construire toutes les tables analytiques.
        sales_overview = build_sales_overview(sales)
        sales_by_region = build_sales_by_region(sales)
        sales_by_month = build_sales_by_month(sales)
        sales_by_product = build_sales_by_product(sales)
        sales_by_payment = build_sales_by_payment_method(sales)
        orders_by_status = build_orders_by_status(orders)

        # F. Écrire les six tables Gold.
        write_gold_table(
            spark,
            "gold.sales_overview",
            sales_overview,
        )

        write_gold_table(
            spark,
            "gold.sales_by_region",
            sales_by_region,
            [F.desc("montant_total_ventes")],
        )

        write_gold_table(
            spark,
            "gold.sales_by_month",
            sales_by_month,
            ["mois"],
        )

        write_gold_table(
            spark,
            "gold.sales_by_product",
            sales_by_product,
            [F.desc("montant_total_ventes")],
        )

        write_gold_table(
            spark,
            "gold.sales_by_payment_method",
            sales_by_payment,
            [F.desc("montant_total_ventes")],
        )

        write_gold_table(
            spark,
            "gold.orders_by_status",
            orders_by_status,
            [F.desc("nb_commandes")],
        )

        # G. Vérification finale.
        print("\n=== Tables présentes dans le namespace Gold ===")
        spark.sql("SHOW TABLES IN gold").show(truncate=False)

        print("\n========== PIPELINE GOLD TERMINÉ ==========")

    finally:
        if sales is not None:
            sales.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
