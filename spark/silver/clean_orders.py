"""Nettoyage de bronze.orders et normalisation des montants, dates et statuts."""
from pyspark.sql import functions as F


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
