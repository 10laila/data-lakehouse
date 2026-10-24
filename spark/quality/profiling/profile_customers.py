"""
Profilage de bronze.customers : détecter les anomalies avant le nettoyage Silver.
Lecture seule : aucune table n'est modifiée.
Le profilage générique (valeurs manquantes, formes, doublons simples) reste dans
profile_table.py ; ce fichier contient les contrôles spécifiques aux clients.
"""

import sys

sys.path.append("/opt/app/spark")

from common.spark_session import get_spark_session
from pyspark.sql import functions as F


UUID_PATTERN = (
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _col(name):
    """Permet de référencer aussi les colonnes dont le nom contient des caractères spéciaux."""
    return F.col("`" + name.replace("`", "``") + "`")


def profile_names(df):
    """Mesure les espaces, la casse et les répétitions de lettres candidates."""
    for column_name in ["first_name", "last_name"]:
        c = _col(column_name)
        repeated_pair_pattern = r"(\p{L})\1(\p{L})\2"

        stats = df.agg(
            F.sum(
                F.when(c.isNotNull() & (c != F.trim(c)), 1).otherwise(0)
            ).alias("espaces_autour"),
            F.sum(
                F.when(
                    c.isNotNull()
                    & (c != F.initcap(F.lower(F.trim(c)))),
                    1,
                ).otherwise(0)
            ).alias("casse_non_standard"),
            F.sum(
                F.when(
                    F.lower(c).rlike(repeated_pair_pattern),
                    1,
                ).otherwise(0)
            ).alias("candidats_lettres_doublees"),
        )

        print(f"\n--- Profilage de {column_name} ---")
        stats.show(truncate=False)

        print(f"Exemples de {column_name} avec deux paires de lettres répétées :")
        (
            df.filter(F.lower(c).rlike(repeated_pair_pattern))
            .select(_col(column_name), _col("email"))
            .show(10, truncate=False)
        )


def profile_emails(df):
    """Repère les emails mal formés et ceux dont la casse/les espaces varient."""
    email = F.trim(_col("email").cast("string"))
    email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

    invalides = df.filter(
        email.isNotNull()
        & (email != "")
        & (~email.rlike(email_pattern))
    )

    print(f"\n--- Emails non vides au format invalide : {invalides.count()} ---")
    invalides.select("email").show(20, truncate=False)

    non_normalises = df.filter(
        _col("email").isNotNull()
        & (_col("email").cast("string") != F.lower(email))
    )
    print(
        "Emails contenant des majuscules ou des espaces autour : "
        f"{non_normalises.count()}"
    )
    non_normalises.select("email").show(10, truncate=False)


def profile_dates(df):
    """Contrôle les dates et l'âge calculé à la date d'inscription."""
    dob = F.to_date(_col("dob"))
    signup = F.to_date(_col("signup_date"))
    age_at_signup = F.datediff(signup, dob) / F.lit(365.25)

    print("\n--- Profilage des dates client ---")
    df.select(
        F.min(dob).alias("dob_min"),
        F.max(dob).alias("dob_max"),
        F.min(signup).alias("signup_min"),
        F.max(signup).alias("signup_max"),
        F.sum(
            F.when(
                dob.isNotNull() & signup.isNotNull() & (signup < dob),
                1,
            ).otherwise(0)
        ).alias("signup_avant_naissance"),
        F.sum(
            F.when(signup > F.current_date(), 1).otherwise(0)
        ).alias("signup_dans_le_futur"),
        F.round(F.min(age_at_signup), 1).alias("age_inscription_min"),
        F.round(F.max(age_at_signup), 1).alias("age_inscription_max"),
        F.sum(
            F.when(age_at_signup < 18, 1).otherwise(0)
        ).alias("mineurs_a_l_inscription"),
        F.sum(
            F.when(age_at_signup < 0, 1).otherwise(0)
        ).alias("ages_negatifs_a_l_inscription"),
    ).show(truncate=False, vertical=True)


def profile_devices(df):
    """Compte les appareils par client sans compter les segments vides."""
    if "device_id(s)" in df.columns:
        device_column = "device_id(s)"
    else:
        device_column = next(
            (column for column in df.columns if column.startswith("device")),
            None,
        )

    if device_column is None:
        print("\nAucune colonne d'appareils trouvée ; profilage ignoré.")
        return

    device_text = F.trim(_col(device_column).cast("string"))
    split_values = F.split(device_text, ";")
    nonempty_values = F.filter(split_values, lambda value: F.trim(value) != "")
    device_count = (
        F.when(device_text.isNull() | (device_text == ""), 0)
        .otherwise(F.size(nonempty_values))
    )

    print(f"\n--- Nombre d'appareils par client ({device_column}) ---")
    (
        df.groupBy(device_count.alias("nb_appareils"))
        .count()
        .orderBy("nb_appareils")
        .show(truncate=False)
    )

    empty_devices = df.filter(device_text.isNull() | (device_text == ""))
    print(f"Clients sans identifiant d'appareil renseigné : {empty_devices.count()}")


def profile_customer_duplicates(spark):
    """Distingue les copies exactes des lignes contradictoires pour un même customer_id."""
    df = spark.table("bronze.customers")

    print("\n--- 1. Format des customer_id (UUID en minuscules attendu) ---")
    customer_id = F.trim(_col("customer_id").cast("string"))
    invalid_ids = df.filter(
        customer_id.isNull()
        | (customer_id == "")
        | (~customer_id.rlike(UUID_PATTERN))
    )
    print(f"Identifiants absents ou ne respectant pas le format UUID minuscule : {invalid_ids.count()}")
    invalid_ids.select("customer_id").show(10, truncate=False)

    uppercase_ids = df.filter(
        customer_id.isNotNull()
        & (customer_id != F.lower(customer_id))
    )
    print(f"Identifiants contenant des majuscules : {uppercase_ids.count()}")

    # Les colonnes techniques d'ingestion sont exclues de l'empreinte.
    business_columns = [
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "gender",
        "dob",
        "signup_date",
        "address",
        "city",
        "state",
        "country",
        "device_id(s)",
        "source",
    ]
    available_columns = [name for name in business_columns if name in df.columns]
    missing_columns = [name for name in business_columns if name not in df.columns]
    if missing_columns:
        print(f"Colonnes absentes de Bronze, exclues de l'empreinte : {missing_columns}")

    # JSON structuré plutôt qu'une concaténation avec séparateur : les valeurs NULL
    # et les caractères "|" dans les données ne créent pas de fausses égalités.
    fingerprint = F.to_json(
        F.struct(*[_col(name).alias(name) for name in available_columns]),
        {"ignoreNullFields": "false"},
    )

    with_fingerprint = df.withColumn("_fingerprint", fingerprint)
    nonempty_ids = with_fingerprint.filter(
        _col("customer_id").isNotNull()
        & (F.trim(_col("customer_id").cast("string")) != "")
    )

    duplicate_groups = (
        nonempty_ids.groupBy("customer_id")
        .agg(
            F.count(F.lit(1)).alias("nb_lignes"),
            F.countDistinct("_fingerprint").alias("nb_versions"),
        )
        .filter(F.col("nb_lignes") > 1)
    )

    print("\n--- 2. Répartition des groupes de customer_id dupliqués ---")
    duplicate_groups.groupBy("nb_lignes", "nb_versions").count().orderBy(
        "nb_lignes", "nb_versions"
    ).show(truncate=False)

    exact_copies = duplicate_groups.filter(F.col("nb_versions") == 1).count()
    conflicts = duplicate_groups.filter(F.col("nb_versions") > 1)
    conflict_count = conflicts.count()
    print(f"Groupes de copies exactes : {exact_copies}")
    print(f"Groupes avec versions contradictoires : {conflict_count}")

    print("\n--- 3. Exemples de clients dont les lignes se contredisent ---")
    conflict_ids = conflicts.select("customer_id").limit(5)
    display_columns = [
        name for name in [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "dob",
            "signup_date",
            "city",
            "source",
        ] if name in df.columns
    ]
    (
        df.join(conflict_ids, on="customer_id", how="inner")
        .select(*display_columns)
        .orderBy("customer_id")
        .show(30, truncate=False)
    )

    print("\n--- 4. Emails utilisés par plusieurs customer_id distincts ---")
    email_norm = F.lower(F.trim(_col("email").cast("string")))
    repeated_emails = (
        df.withColumn("_email_norm", email_norm)
        .filter(
            F.col("_email_norm").isNotNull()
            & (F.col("_email_norm") != "")
            & _col("customer_id").isNotNull()
        )
        .groupBy("_email_norm")
        .agg(F.countDistinct("customer_id").alias("nb_clients"))
        .filter(F.col("nb_clients") > 1)
        .withColumn(
            "email_type_shared",
            F.col("_email_norm").startswith("shared"),
        )
    )

    repeated_emails.groupBy("email_type_shared").count().show(truncate=False)
    repeated_emails.orderBy(F.desc("nb_clients")).show(20, truncate=False)


def main():
    spark = get_spark_session("profile-customers")
    try:
        df = spark.table("bronze.customers")
        print("\n--- Schéma de bronze.customers ---")
        df.printSchema()

        profile_customer_duplicates(spark)
        profile_names(df)
        profile_emails(df)
        profile_dates(df)
        profile_devices(df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
