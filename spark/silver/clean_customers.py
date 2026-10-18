"""Nettoyage et déduplication de bronze.customers vers Silver."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def repair_name(col_name):
    # Réparation d'un prénom ou d'un nom, d'après les anomalies observées au profilage.
    # Retourne une expression Spark (pas une valeur Python) : elle s'applique à toute la colonne.
    c = F.trim(F.col(col_name))
    # 1) Retire tout ce qui n'est pas une lettre, un espace, une apostrophe ou un tiret.
    #    "We5ndy" -> "Wendy", "Larson!" -> "Larson".
    #    \p{L} = n'importe quelle lettre, accents compris.
    c = F.regexp_replace(c, r"[^\p{L}\s'\-]", "")
    # 2) Tout en minuscules pour comparer les lettres sans se soucier de la casse.
    c = F.lower(c)
    # 3) Défait le doublement des 3 premières lettres : "kkeevvin" -> "kevin".
    #    (\p{L}) capture une lettre ; \1 exige la même lettre juste après (idem \2, \3).
    #    "$1$2$3" remplace les 6 caractères par les 3 lettres capturées.
    #    Trois paires doublées d'affilée au début n'existent pas dans de vrais noms.
    c = F.regexp_replace(c, r"^(\p{L})\1(\p{L})\2(\p{L})\3", "$1$2$3")
    # 3 bis) Noms très courts : "Li" abîmé donne "LLii" (deux paires, et rien d'autre).
    c = F.regexp_replace(c, r"^(\p{L})\1(\p{L})\2$", "$1$2")
    # 4) Espaces multiples -> un seul, puis retire les espaces aux extrémités.
    c = F.trim(F.regexp_replace(c, r"\s+", " "))
    # 5) Majuscule initiale sur chaque mot : "christian" -> "Christian".
    c = F.initcap(c)
    # F.when sans otherwise : une chaîne devenue vide donne NULL.
    return F.when(F.length(c) > 0, c)


def normalize_phone(col_name):
    raw = F.col(col_name)
    # Sépare l'extension du numéro principal : "449.977.1729x282" -> "449.977.1729"
    principal = F.regexp_replace(raw, r"[xX].*$", "")
    # Ne garde que les chiffres : "(522)382-8559" -> "5223828559"
    digits = F.regexp_replace(principal, r"\D", "")
    n = F.length(digits)
    # Cas reconnus ; tout le reste reste NULL (aucun otherwise).
    return (
        F.when(n == 10, digits)
        # 11 chiffres qui commencent par 1 : indicatif +1, on l'enlève
        # substr(2, 10) = 10 caractères à partir de la 2e position (comptage dès 1)
        .when((n == 11) & digits.startswith("1"), digits.substr(2, 10))
        # 13 chiffres qui commencent par 001 : indicatif 001, on enlève les 3 premiers
        .when((n == 13) & digits.startswith("001"), digits.substr(4, 10))
    )


def extract_phone_extension(col_name):
    # Récupère les chiffres après le "x" final : "...x26814" -> "26814"
    ext = F.regexp_extract(F.col(col_name), r"[xX](\d+)$", 1)
    # regexp_extract renvoie "" s'il n'y a pas d'extension -> on met NULL
    return F.when(F.length(ext) > 0, ext)


def clean_customers(spark):
    print("--- Nettoyage de customers (Bronze -> Silver) ---")
    df = spark.table("bronze.customers")
    total_bronze = df.count()

    cleaned = (
        df
        # Le nom "device_id(s)" contient des parenthèses : on le renomme tout de suite.
        .withColumnRenamed("device_id(s)", "device_ids_raw")
        # On garde les valeurs d'origine avant réparation (traçabilité).
        .withColumnRenamed("first_name", "first_name_raw")
        .withColumnRenamed("last_name", "last_name_raw")
        .withColumnRenamed("phone_number", "phone_raw")
        .withColumn("first_name", repair_name("first_name_raw"))
        .withColumn("last_name", repair_name("last_name_raw"))
        .withColumn("phone", normalize_phone("phone_raw"))
        .withColumn("phone_extension", extract_phone_extension("phone_raw"))
        # Les emails n'ont aucune anomalie de format ; on les normalise par précaution.
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        # Simple nettoyage des espaces sur les colonnes texte restantes.
        .withColumn("address", F.trim(F.col("address")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.trim(F.col("state")))
        .withColumn("country", F.trim(F.col("country")))
        .withColumn("gender", F.trim(F.col("gender")))
        .withColumn("source", F.trim(F.col("source")))
        # "uuid1;uuid2" -> tableau [uuid1, uuid2]. Sans perte : la table devices viendra de là.
        .withColumn("device_ids", F.split(F.col("device_ids_raw"), ";"))
        .drop("device_ids_raw")
        # Âge en années entières au moment de l'inscription (months_between / 12).
        .withColumn(
            "age_at_signup",
            F.floor(F.months_between(F.col("signup_date"), F.col("dob")) / 12).cast("int"),
        )
    )

    # --- Contrôle : y a-t-il encore de vrais conflits entre lignes d'un même client ? ---
    # Empreinte = toutes les colonnes métier assemblées dans une chaîne.
    empreinte = F.concat_ws(
        "|",
        F.col("first_name"), F.col("last_name"), F.col("email"), F.col("phone"),
        F.col("gender"), F.col("dob").cast("string"), F.col("signup_date").cast("string"),
        F.col("address"), F.col("city"), F.col("state"), F.col("country"),
        F.concat_ws(";", F.col("device_ids")), F.col("source"),
    )
    conflits = (
        cleaned.groupBy("customer_id")
        .agg(F.countDistinct(empreinte).alias("nb_versions"))
        .filter(F.col("nb_versions") > 1)
    )
    nb_conflits = conflits.count()
    print(f"Clients dont les lignes se contredisent encore après réparation : {nb_conflits}")
    if nb_conflits > 0:
        # Montre 3 clients en conflit pour comprendre ce qui diffère.
        (
            cleaned.join(conflits.select("customer_id").limit(3), "customer_id")
            .orderBy("customer_id")
            .show(20, truncate=False)
        )

    # --- Déduplication : une seule ligne par customer_id ---
    # nb_corrections = combien de noms ont dû être réparés sur cette ligne.
    # eqNullSafe compare en traitant NULL = NULL comme "égal".
    nb_corrections = (
        (~F.col("first_name").eqNullSafe(F.col("first_name_raw"))).cast("int")
        + (~F.col("last_name").eqNullSafe(F.col("last_name_raw"))).cast("int")
    )
    # On garde, pour chaque client, la ligne qui a eu le moins de corrections
    # (donc la plus proche de l'original). Les colonnes suivantes servent à départager
    # de façon déterministe : le résultat est identique à chaque exécution.
    fenetre = Window.partitionBy("customer_id").orderBy(
        nb_corrections.asc(),
        F.col("first_name_raw").asc(),
        F.col("last_name_raw").asc(),
        F.col("phone_raw").asc(),
    )
    deduped = (
        cleaned.withColumn("_rang", F.row_number().over(fenetre))
        .filter(F.col("_rang") == 1)
        .drop("_rang")
    )

    # --- Email partagé : plusieurs clients (déjà dédupliqués) avec le même email ---
    # Après déduplication, une ligne = un client : compter les lignes par email
    # revient à compter les clients. On ne marque rien quand l'email est NULL.
    meme_email = Window.partitionBy("email")
    final = (
        deduped.withColumn("_nb_clients_email", F.count(F.lit(1)).over(meme_email))
        .withColumn(
            "email_is_shared",
            F.when(F.col("email").isNotNull(), F.col("_nb_clients_email") > 1),
        )
        .drop("_nb_clients_email")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    total_silver = final.count()
    phones_null = final.filter(F.col("phone").isNull() & F.col("phone_raw").isNotNull()).count()
    prenoms_modifies = final.filter(F.col("first_name") != F.col("first_name_raw")).count()
    noms_modifies = final.filter(F.col("last_name") != F.col("last_name_raw")).count()
    partages = final.filter(F.col("email_is_shared")).count()
    print(f"Lignes Bronze : {total_bronze}")
    print(f"Clients uniques (lignes Silver) : {total_silver}")
    print(f"Téléphones non normalisables (mis à NULL) : {phones_null}")
    print(f"Prénoms modifiés : {prenoms_modifies} | noms modifiés : {noms_modifies}")
    print(f"Clients avec un email partagé : {partages}")

    final.writeTo("silver.customers").using("iceberg").createOrReplace()
    print("✅ Table silver.customers créée")

    # --- Table des appareils : une ligne par (client, appareil) ---
    # explode transforme un tableau de 3 éléments en 3 lignes.
    devices = (
        final.select("customer_id", F.explode("device_ids").alias("device_id"))
        .withColumn("device_id", F.trim(F.col("device_id")))
        .filter(F.col("device_id") != "")
        .dropDuplicates()
    )
    devices.writeTo("silver.customer_devices").using("iceberg").createOrReplace()
    print("✅ Table silver.customer_devices créée\n")
