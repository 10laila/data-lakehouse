import sys
from difflib import get_close_matches

sys.path.append("/opt/app/spark")
from common.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

CANONICAL_CATEGORIES = [
    "clothing", "electronics", "home", "toys", "automotive",
    "sports", "kitchen", "beauty",
]

# Tables de correspondance : valeur nettoyée (minuscules, sans espaces autour) -> valeur officielle.
# Elles viennent des valeurs réellement observées au profilage de bronze.orders.
PAYMENT_METHOD_MAP = {
    "cash": "cash",
    "card": "card", "crad": "card", "c@rd": "card",
    # À CONFIRMER : "cd" ne peut pas venir de "cash" (pas de "d"), mais s'obtient en
    # retirant "ar" de "card". Mets None à la place de "card" pour l'exclure.
    "cd": "card",
    "wallet": "wallet", "wall-et": "wallet",
    "upi": "upi",
}
STATUS_MAP = {
    "success": "success", "suc": "success",
    "refunded": "refunded", "ref": "refunded",
    "failed": "failed", "fail": "failed",
}
# Les montants au-dessus de ce seuil sont marqués "aberrants" (colonne is_amount_outlier)
# mais conservés : ce sont des valeurs possibles, simplement suspectes.
AMOUNT_OUTLIER_THRESHOLD = 75000

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

def map_values(col_name, mapping):
    # Transforme le dictionnaire Python en "table de correspondance" Spark (type map).
    # F.create_map attend une liste à plat : clé1, valeur1, clé2, valeur2, ...
    mapping_expr = F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])
    # lower(trim(...)) : on ignore la casse et les espaces autour avant de chercher.
    # Si la valeur n'existe pas dans le dictionnaire, le résultat est NULL.
    return mapping_expr[F.lower(F.trim(F.col(col_name)))]


def clean_amount(col_name):
    # decimal(12,2) = jusqu'à 12 chiffres dont 2 après la virgule. Pour de l'argent, c'est
    # plus fiable que "double", qui peut produire de petites erreurs d'arrondi.
    # Un texte non numérique ("1,200", "NaN") donne NULL lors de la conversion.
    valeur = F.trim(F.col(col_name)).cast("decimal(12,2)")
    # On garde seulement les montants strictement positifs : 0 et -50 sont des valeurs bouchons.
    return F.when(valeur > 0, valeur)


def clean_quantity(col_name):
    texte = F.lower(F.trim(F.col(col_name)))
    # Seul mot observé : "five". Sinon on convertit en nombre ("2.0" -> 2.0, "" -> NULL).
    valeur = F.when(texte == "five", F.lit(5.0)).otherwise(texte.cast("double"))
    # Une quantité valide est un entier d'au moins 1 : 0 et -3 sont des valeurs bouchons.
    return F.when((valeur >= 1) & (valeur == F.floor(valeur)), valeur.cast("int"))


def parse_order_date(col_name):
    d = F.trim(F.col(col_name))
    # \d = un chiffre, {4} = exactement 4 fois. ^ et $ = début et fin du texte.
    # Les formats sont testés dans l'ordre ; le premier qui correspond est utilisé.
    return (
        # 2024-05-01 ou 2024-05-01T10:20:30.123456 : on garde les 10 premiers caractères
        F.when(d.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(d.substr(1, 10), "yyyy-MM-dd"))
        # 2024/05/01 10:20 : année/mois/jour suivi de l'heure
        .when(d.rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$"), F.to_date(d.substr(1, 10), "yyyy/MM/dd"))
        # 2024/15/03 : année/JOUR/MOIS (le bloc du milieu dépasse 12, c'est donc le jour)
        .when(d.rlike(r"^\d{4}/\d{2}/\d{2}$"), F.to_date(d, "yyyy/dd/MM"))
        # 15-03-2024 : JOUR-MOIS-ANNÉE (le premier bloc dépasse 12, c'est donc le jour)
        .when(d.rlike(r"^\d{2}-\d{2}-\d{4}$"), F.to_date(d, "dd-MM-yyyy"))
        # Autre forme ou date impossible (30 février) : NULL
    )


def clean_orders(spark):
    print("--- Nettoyage de orders (Bronze -> Silver) ---")
    df = spark.table("bronze.orders")

    cleaned = (
        df
        # On garde les valeurs d'origine avant nettoyage (traçabilité).
        .withColumnRenamed("order_amount", "order_amount_raw")
        .withColumnRenamed("order_date", "order_date_raw")
        .withColumnRenamed("payment_method", "payment_method_raw")
        .withColumnRenamed("status", "status_raw")
        .withColumnRenamed("quantity", "quantity_raw")
        .withColumn("order_amount", clean_amount("order_amount_raw"))
        # Booléen : True si le montant dépasse le seuil, NULL si le montant est NULL.
        .withColumn(
            "is_amount_outlier",
            F.when(F.col("order_amount").isNotNull(), F.col("order_amount") > AMOUNT_OUTLIER_THRESHOLD),
        )
        .withColumn("order_date", parse_order_date("order_date_raw"))
        .withColumn("payment_method", map_values("payment_method_raw", PAYMENT_METHOD_MAP))
        .withColumn("status", map_values("status_raw", STATUS_MAP))
        .withColumn("quantity", clean_quantity("quantity_raw"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Tous les compteurs en un seul passage sur les données.
    # .cast("int") transforme True/False en 1/0, ce qui permet de les additionner.
    stats = cleaned.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.col("order_amount").isNull().cast("int")).alias("montant_mis_a_null"),
        F.sum(F.coalesce(F.col("is_amount_outlier").cast("int"), F.lit(0))).alias("montant_aberrant"),
        F.sum(F.col("order_date_raw").isNull().cast("int")).alias("date_absente_dans_bronze"),
        F.sum((F.col("order_date").isNull() & F.col("order_date_raw").isNotNull()).cast("int")).alias("date_illisible"),
        F.sum((F.col("order_date") > F.current_date()).cast("int")).alias("date_dans_le_futur"),
        F.sum(F.col("quantity").isNull().cast("int")).alias("quantite_mise_a_null"),
        F.sum(F.col("payment_method").isNull().cast("int")).alias("paiement_non_reconnu"),
        F.sum(F.col("status").isNull().cast("int")).alias("statut_non_reconnu"),
    ).collect()[0]
    for nom, valeur in stats.asDict().items():
        print(f"{nom:28} {valeur}")

    cleaned.writeTo("silver.orders").using("iceberg").createOrReplace()
    print("✅ Table silver.orders créée\n")

def main():
    spark = get_spark_session("silver-jobs")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    clean_products(spark)
    clean_customers(spark)

    print("=== Vérification customers ===")

    # Exemples de noms réparés : valeur brute à gauche, valeur propre à droite.
    spark.sql("""
        SELECT first_name_raw, first_name, last_name_raw, last_name
        FROM silver.customers
        WHERE first_name <> first_name_raw OR last_name <> last_name_raw
        LIMIT 15
    """).show(truncate=False)

    # Noms contenant un tiret ou une apostrophe (les guillemets doubles "'" évitent
    # le piège de l'apostrophe dans une chaîne SQL). On regarde si la majuscule
    # après le signe est correcte : "O'Connor" et non "O'connor".
    spark.sql("""
        SELECT first_name_raw, first_name, last_name_raw, last_name
        FROM silver.customers
        WHERE last_name LIKE '%-%' OR last_name LIKE "%'%"
           OR first_name LIKE '%-%' OR first_name LIKE "%'%"
        LIMIT 15
    """).show(truncate=False)

    # Combien de clients ont eu leur prénom modifié, leur nom modifié, ou les deux ?
    # GROUP BY 1, 2 = on groupe sur les deux premières colonnes du SELECT.
    spark.sql("""
        SELECT (first_name <> first_name_raw) AS prenom_modifie,
               (last_name <> last_name_raw) AS nom_modifie,
               COUNT(*) AS nb_clients
        FROM silver.customers
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).show()

    # Téléphones : brut, normalisé, extension.
    spark.sql("""
        SELECT phone_raw, phone, phone_extension
        FROM silver.customers
        LIMIT 12
    """).show(truncate=False)

    # Doit afficher 0 : il ne reste aucun customer_id en double.
    spark.sql("""
        SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS doublons_restants
        FROM silver.customers
    """).show()
    print("=== Vérification orders ===")

    # 1) Répartition des montants par tranche : y a-t-il un vide entre 50 000 et 100 000 ?
    #    Un vide confirmerait que le seuil de 75 000 sépare bien deux populations.
    spark.sql("""
        SELECT CASE WHEN order_amount <= 50000  THEN 'a. jusqu a 50 000'
                    WHEN order_amount <= 100000 THEN 'b. 50 000 a 100 000'
                    WHEN order_amount <= 500000 THEN 'c. 100 000 a 500 000'
                    ELSE 'd. 500 000 a 1 million' END AS tranche,
               COUNT(*) AS nb_commandes
        FROM silver.orders
        WHERE order_amount IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).show(truncate=False)

    # 2) Répartition par année : les commandes de 2026 ou 2027 sont-elles nombreuses ?
    spark.sql("""
        SELECT year(order_date) AS annee, COUNT(*) AS nb_commandes
        FROM silver.orders
        GROUP BY 1
        ORDER BY 1
    """).show()

    # 3) Trois exemples par format de date : valeur brute à côté de la date obtenue.
    #    forme = chiffre -> 9, lettre -> A (même astuce que dans profile_table.py).
    #    ROW_NUMBER() OVER (PARTITION BY forme ...) numérote les lignes dans chaque format.
    spark.sql("""
        WITH base AS (
            SELECT regexp_replace(regexp_replace(order_date_raw, '[0-9]', '9'), '[A-Za-z]', 'A') AS forme,
                   order_date_raw, order_date, order_id
            FROM silver.orders
            WHERE order_date_raw IS NOT NULL
        )
        SELECT forme, order_date_raw, order_date
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY forme ORDER BY order_id) AS rang FROM base)
        WHERE rang <= 3
        ORDER BY forme, rang
    """).show(truncate=False)

    # 4) Commandes passées AVANT l'inscription du client : incohérence logique à repérer.
    spark.sql("""
        SELECT COUNT(*) AS commandes_avant_inscription
        FROM silver.orders o
        JOIN silver.customers c ON o.customer_id = c.customer_id
        WHERE o.order_date < c.signup_date
    """).show()

    # 5) Moyens de paiement et statuts après nettoyage : il ne doit rester que les valeurs officielles.
    spark.sql("""
        SELECT payment_method, COUNT(*) AS nb FROM silver.orders GROUP BY 1 ORDER BY nb DESC
    """).show()
    spark.sql("""
        SELECT status, COUNT(*) AS nb FROM silver.orders GROUP BY 1 ORDER BY nb DESC
    """).show()

    spark.stop()

if __name__ == "__main__":
    main()