
"""Nettoyage et déduplication de bronze.customers vers Silver."""

import sys

sys.path.append("/opt/app/spark")

from common.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def repair_name(col_name):
    """Nettoie et répare un prénom ou un nom."""

    c = F.trim(F.col(col_name).cast("string"))

    # Supprimer les caractères non autorisés.
    c = F.regexp_replace(c, r"[^\p{L}\s'\-]", "")

    # Uniformiser la casse.
    c = F.lower(c)

    # Exemple : kkeevvin -> kevin.
    c = F.regexp_replace(
        c,
        r"^(\p{L})\1(\p{L})\2(\p{L})\3",
        "$1$2$3"
    )

    # Exemple : LLii -> Li.
    c = F.regexp_replace(
        c,
        r"^(\p{L})\1(\p{L})\2$",
        "$1$2"
    )

    # Réduire les espaces multiples.
    c = F.trim(F.regexp_replace(c, r"\s+", " "))

    # Mettre une majuscule au début de chaque mot.
    c = F.initcap(c)

    return F.when(F.length(c) > 0, c)


def normalize_phone(col_name):
    """Normalise les formats téléphoniques reconnus."""

    raw = F.col(col_name).cast("string")

    # Retirer l'extension avant de normaliser le numéro principal.
    principal = F.regexp_replace(raw, r"[xX].*$", "")

    # Garder uniquement les chiffres.
    digits = F.regexp_replace(principal, r"\D", "")
    n = F.length(digits)

    return (
        F.when(n == 10, digits)
        .when(
            (n == 11) & digits.startswith("1"),
            digits.substr(2, 10)
        )
        .when(
            (n == 13) & digits.startswith("001"),
            digits.substr(4, 10)
        )
    )


def extract_phone_extension(col_name):
    """Extraire une extension finale comme x26814."""

    ext = F.regexp_extract(
        F.col(col_name).cast("string"),
        r"[xX](\d+)$",
        1
    )

    return F.when(F.length(ext) > 0, ext)


def clean_customers(spark):

    print("\n--- Nettoyage de customers : Bronze -> Silver ---")

    df = spark.table("bronze.customers")
    total_bronze = df.count()

    # --------------------------------------------------------
    # 1. Nettoyage et normalisation
    # --------------------------------------------------------

    email_clean = F.trim(F.col("email").cast("string"))

    cleaned = (
        df
        .withColumnRenamed("device_id(s)", "device_ids_raw")
        .withColumnRenamed("first_name", "first_name_raw")
        .withColumnRenamed("last_name", "last_name_raw")
        .withColumnRenamed("phone_number", "phone_raw")

        .withColumn(
            "first_name",
            repair_name("first_name_raw")
        )
        .withColumn(
            "last_name",
            repair_name("last_name_raw")
        )
        .withColumn(
            "phone",
            normalize_phone("phone_raw")
        )
        .withColumn(
            "phone_extension",
            extract_phone_extension("phone_raw")
        )

        # Normaliser les emails et convertir les chaînes vides en NULL.
        .withColumn(
            "email",
            F.when(
                F.length(email_clean) > 0,
                F.lower(email_clean)
            )
        )

        # Nettoyer les espaces des autres colonnes textuelles.
        .withColumn("address", F.trim(F.col("address")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn("country", F.trim(F.col("country")))
        .withColumn("gender", F.trim(F.col("gender")))
        .withColumn("source", F.trim(F.col("source")))

        # Convertir la liste des appareils en tableau.
        .withColumn(
            "device_ids",
            F.split(F.col("device_ids_raw").cast("string"), ";")
        )
        .drop("device_ids_raw")

        # Calculer l'âge en années au moment de l'inscription.
        .withColumn(
            "age_at_signup",
            F.floor(
                F.months_between(
                    F.to_date(F.col("signup_date")),
                    F.to_date(F.col("dob"))
                ) / 12
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # 2. Déduplication
    # --------------------------------------------------------

    # Un identifiant valide doit être non NULL et non vide.
    valid_id = (
        F.col("customer_id").isNotNull()
        & (
            F.trim(F.col("customer_id").cast("string")) != ""
        )
    )

    # Préférer les lignes nécessitant le moins de corrections de noms.
    corrections = (
        (~F.col("first_name").eqNullSafe(
            F.col("first_name_raw")
        )).cast("int")
        +
        (~F.col("last_name").eqNullSafe(
            F.col("last_name_raw")
        )).cast("int")
    )

    ranked = cleaned.withColumn(
        "_name_corrections",
        corrections
    )

    # Les critères secondaires rendent le choix reproductible.
    window = Window.partitionBy("customer_id").orderBy(
        F.col("_name_corrections").asc(),
        F.col("first_name_raw").asc_nulls_last(),
        F.col("last_name_raw").asc_nulls_last(),
        F.col("phone_raw").asc_nulls_last(),
        F.col("email").asc_nulls_last(),
        F.col("dob").asc_nulls_last(),
        F.col("signup_date").asc_nulls_last(),
        F.col("address").asc_nulls_last(),
        F.col("city").asc_nulls_last(),
        F.col("state").asc_nulls_last(),
        F.col("country").asc_nulls_last(),
        F.col("source").asc_nulls_last(),
        F.concat_ws(";", F.col("device_ids")).asc_nulls_last()
    )

    # Dédupliquer les clients ayant un identifiant valide.
    deduped_valid = (
        ranked
        .filter(valid_id)
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "_name_corrections")
    )

    # Ne pas fusionner toutes les lignes sans identifiant en un seul client.
    customers_without_id = (
        ranked
        .filter(~valid_id)
        .drop("_name_corrections")
    )

    deduped = deduped_valid.unionByName(
        customers_without_id
    )

    # --------------------------------------------------------
    # 3. Enrichissements Silver
    # --------------------------------------------------------

    email_window = Window.partitionBy("email")

    final = (
        deduped
        .withColumn(
            "_nb_clients_email",
            F.count(F.lit(1)).over(email_window)
        )
        .withColumn(
            "email_is_shared",
            F.when(
                F.col("email").isNotNull(),
                F.col("_nb_clients_email") > 1
            )
        )
        .drop("_nb_clients_email")
        .withColumn(
            "_silver_processed_at",
            F.current_timestamp()
        )
    )

    # --------------------------------------------------------
    # 4. Vérifications après transformation
    # --------------------------------------------------------

    total_silver = final.count()

    ids_valides = final.filter(valid_id)
    nb_ids_valides = ids_valides.count()
    nb_ids_distincts = ids_valides.select(
        "customer_id"
    ).distinct().count()

    ids_absents = final.filter(~valid_id).count()

    phones_absents = final.filter(
        F.col("phone_raw").isNull()
        | (F.trim(F.col("phone_raw")) == "")
    ).count()

    phones_non_normalisables = final.filter(
        F.col("phone").isNull()
        & F.col("phone_raw").isNotNull()
        & (F.trim(F.col("phone_raw")) != "")
    ).count()

    prenoms_modifies = final.filter(
        ~F.col("first_name").eqNullSafe(
            F.col("first_name_raw")
        )
    ).count()

    noms_modifies = final.filter(
        ~F.col("last_name").eqNullSafe(
            F.col("last_name_raw")
        )
    ).count()

    prenoms_absents = final.filter(
        F.col("first_name").isNull()
    ).count()

    noms_absents = final.filter(
        F.col("last_name").isNull()
    ).count()

    clients_email_partage = final.filter(
        F.col("email_is_shared") == True
    ).count()

    print("\n--- Contrôles du résultat Silver ---")
    print(f"Lignes Bronze                     : {total_bronze}")
    print(f"Lignes Silver préparées           : {total_silver}")
    print(f"Lignes retirées par déduplication : {total_bronze - total_silver}")
    print(f"Identifiants clients absents      : {ids_absents}")
    print(f"Prénoms modifiés                  : {prenoms_modifies}")
    print(f"Noms modifiés                     : {noms_modifies}")
    print(f"Prénoms absents après nettoyage   : {prenoms_absents}")
    print(f"Noms absents après nettoyage      : {noms_absents}")
    print(f"Téléphones absents                : {phones_absents}")
    print(f"Téléphones non normalisables      : {phones_non_normalisables}")
    print(f"Clients avec email partagé        : {clients_email_partage}")

    # --------------------------------------------------------
    # 5. Écriture de silver.customers
    # --------------------------------------------------------

    final.writeTo(
        "silver.customers"
    ).using(
        "iceberg"
    ).createOrReplace()

    silver_customers = spark.table("silver.customers")
    nb_lignes_ecrites = silver_customers.count()

    ids_apres_ecriture = silver_customers.filter(
        valid_id
    )

    nb_ids_apres_ecriture = ids_apres_ecriture.count()
    nb_ids_distincts_apres_ecriture = ids_apres_ecriture.select(
        "customer_id"
    ).distinct().count()

    doublons_apres_ecriture = (
        ids_apres_ecriture
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("\n--- Vérification de silver.customers ---")
    print(f"Lignes attendues : {total_silver}")
    print(f"Lignes écrites   : {nb_lignes_ecrites}")
    print(f"Identifiants valides : {nb_ids_apres_ecriture}")
    print(f"Identifiants distincts : {nb_ids_distincts_apres_ecriture}")
    print(f"Groupes d'identifiants dupliqués après déduplication : {doublons_apres_ecriture}")

    if (
        nb_lignes_ecrites == total_silver
        and nb_ids_apres_ecriture == nb_ids_distincts_apres_ecriture
    ):
        print("Vérification OK : écriture et unicité des identifiants valides.")
    else:
        print("ATTENTION : vérifier le nombre de lignes et l'unicité des identifiants.")

    print("\n--- Aperçu métier de silver.customers ---")

    silver_customers.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "country",
        "age_at_signup",
        "email_is_shared"
    ).show(5, truncate=25, vertical=True)

    # --------------------------------------------------------
    # 6. Création de silver.customer_devices
    # --------------------------------------------------------

    devices = (
        final
        .filter(valid_id)
        .select(
            "customer_id",
            F.explode("device_ids").alias("device_id")
        )
        .withColumn(
            "device_id",
            F.trim(F.col("device_id"))
        )
        .filter(
            F.col("device_id").isNotNull()
            & (F.col("device_id") != "")
        )
        .dropDuplicates(["customer_id", "device_id"])
    )

    devices.writeTo(
        "silver.customer_devices"
    ).using(
        "iceberg"
    ).createOrReplace()

    silver_devices = spark.table("silver.customer_devices")

    print("\n--- Vérification de silver.customer_devices ---")
    print(f"Relations client-appareil préparées : {devices.count()}")
    print(f"Relations client-appareil écrites   : {silver_devices.count()}")

    silver_devices.show(5, truncate=25)

    print("\nNettoyage de customers terminé.")


if __name__ == "__main__":

    spark = get_spark_session("clean-customers")

    try:
        clean_customers(spark)
    finally:
        spark.stop()
