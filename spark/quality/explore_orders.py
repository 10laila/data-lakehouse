"""
explore_orders.py : explorer la table bronze.orders (données brutes) pour repérer
ce qui doit être corrigé, mis à NULL, supprimé ou simplement signalé.
LECTURE SEULE : ce script ne modifie aucune table.
"""

# ----------------------------------------------------------------------------
# IMPORTS : chaque ligne "import" charge des outils écrits par d'autres.
# ----------------------------------------------------------------------------

# "sys" : module standard de Python (installé avec Python, rien à télécharger).
# Il donne accès à "sys.path", la liste des dossiers où Python cherche les modules
# qu'on lui demande d'importer.
import sys

# On ajoute le dossier du projet tel qu'il est DANS LE CONTENEUR Docker.
# Le "/" du début est indispensable : sans lui, le chemin est relatif au dossier courant
# et Python ne trouve rien.
sys.path.append("/opt/app/spark")

# "common.spark_session" désigne TON fichier spark/common/spark_session.py.
# Le chemin commence à "common" parce qu'on vient d'ajouter /opt/app/spark à sys.path.
# get_spark_session() crée la session Spark déjà configurée pour parler à MinIO et à
# Iceberg. Une session Spark est le point d'entrée pour lire des données.
from common.spark_session import get_spark_session

# "pyspark" : bibliothèque qui permet de piloter Spark depuis Python (déjà installée
# dans le conteneur Spark). Son module "functions" contient les outils qui travaillent
# sur les colonnes : F.col (désigner une colonne), F.trim (enlever les espaces),
# F.lower (minuscules), F.desc (tri décroissant), F.isnan (détecter NaN)...
# "as F" lui donne un nom court pour ne pas écrire "functions" à chaque fois.
from pyspark.sql import functions as F


# ----------------------------------------------------------------------------
# FONCTIONS D'EXPLORATION
# "df" est un DataFrame : l'objet Spark qui représente une table (lignes + colonnes).
# ----------------------------------------------------------------------------

def show_size(df):
    # df.count() compte les lignes ; df.columns est la liste des noms de colonnes.
    print(f"Nombre de lignes   : {df.count()}")
    print(f"Nombre de colonnes : {len(df.columns)}")


def show_schema(df):
    # Affiche chaque colonne avec son TYPE (string = texte, date, timestamp, int...).
    # Une colonne de type "string" peut contenir n'importe quoi, même des chiffres.
    df.printSchema()


def show_sample(df, n=20):
    # Affiche les n premières lignes. truncate=False empêche Spark de couper
    # les textes longs avec des "...", pour voir les valeurs en entier.
    df.show(n, truncate=False)


def show_missing(df):
    # Pour chaque colonne, compte les lignes "vides" : valeur NULL, texte vide,
    # ou texte composé uniquement d'espaces.
    for column in df.columns:
        # .isNull() est vrai quand la valeur est NULL (absente).
        # .cast("string") convertit en texte, pour que trim fonctionne aussi sur les dates.
        # F.trim(...) enlève les espaces au début et à la fin.
        # "|" signifie OU ; les parenthèses autour de chaque condition sont obligatoires.
        condition_vide = F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")
        n = df.filter(condition_vide).count()
        # {column:20} réserve 20 caractères pour le nom, afin d'aligner les nombres.
        print(f"{column:20}{n}")


def show_top_values(df, column, n=30):
    # Affiche chaque valeur distincte d'une colonne et son nombre d'apparitions,
    # de la plus fréquente à la plus rare. Outil principal pour repérer les variantes
    # (majuscules, fautes), les valeurs étranges et les valeurs répétées anormalement.
    # groupBy(column).count() : regroupe les lignes identiques et les compte
    #                           (le résultat a une colonne nommée "count").
    # orderBy(F.desc("count")) : trie du plus grand "count" au plus petit.
    print(f"\n--- Valeurs de {column} (top {n}) ---")
    df.groupBy(column).count().orderBy(F.desc("count")).show(n, truncate=False)


def show_rows_where(df, condition, n=20):
    # Affiche les lignes qui respectent une condition. La condition peut être une
    # expression Spark (F.col("x") == "y") ou un texte SQL ("x = 'y'").
    sous_ensemble = df.filter(condition)
    print(f"\n----- condition : {condition} ---> {sous_ensemble.count()} lignes -----")
    sous_ensemble.show(n, truncate=False)


def show_extremes(df, column, n=10):
    # Affiche les plus petites et les plus grandes valeurs d'une colonne de montants.
    # .cast("double") convertit le texte en nombre ; un texte qui n'est pas un nombre
    # (par exemple "1,200") devient NULL à la conversion.
    nombre = F.col(column).cast("double")
    # On écarte NULL et NaN ("Not a Number", valeur non calculable), sinon ils faussent
    # le tri : Spark classe NaN plus grand que tous les nombres.
    # "~" signifie NON ; "&" signifie ET.
    valides = df.filter(nombre.isNotNull() & ~F.isnan(nombre))
    print(f"\n--- {column} : les {n} plus petites valeurs numériques ---")
    valides.select(column).orderBy(nombre.asc()).show(n, truncate=False)
    print(f"\n--- {column} : les {n} plus grandes valeurs numériques ---")
    valides.select(column).orderBy(nombre.desc()).show(n, truncate=False)


# ----------------------------------------------------------------------------
# PROGRAMME PRINCIPAL
# ----------------------------------------------------------------------------

def main():
    spark = get_spark_session("explore_orders")

    # spark.table(...) crée un DataFrame qui représente la table.
    # Spark est "paresseux" : il ne lit rien tant qu'on ne demande pas un résultat
    # (count, show...).
    df = spark.table("bronze.orders")

    show_size(df)
    show_schema(df)
    show_sample(df, n=20)

    print("\n--- Valeurs manquantes par colonne ---")
    show_missing(df)

    # Valeurs distinctes des colonnes à problèmes. Pour order_amount, cherche les valeurs
    # qui reviennent des milliers de fois : de vrais montants ne se répètent pas ainsi.
    colonnes_a_analyser = ["payment_method", "status", "quantity", "order_date", "order_amount"]
    for column in colonnes_a_analyser:
        show_top_values(df, column, n=50)

    show_extremes(df, "order_amount", n=10)

    # Moyens de paiement qui ne sont PAS dans la liste officielle.
    # .isin(...) est vrai si la valeur est dans la liste ; "~" l'inverse.
    # lower(trim(...)) ignore la casse et les espaces autour avant de comparer.
    show_rows_where(
        df, ~F.lower(F.trim(F.col("payment_method"))).isin("cash", "wallet", "upi", "card"), n=20
    )
    # Même contrôle pour les statuts.
    show_rows_where(
        df, ~F.lower(F.trim(F.col("status"))).isin("success", "refunded", "failed"), n=20
    )

    # Quantités vides (texte vide). NULL et texte vide sont deux choses différentes :
    # == "" ne trouve que les textes vides ; .isNull() trouve les vraies valeurs absentes.
    show_rows_where(df, F.trim(F.col("quantity")) == "", n=20)

    # Dates : on repère chaque format avec LIKE.
    # LIKE compare à un modèle : % = n'importe quels caractères (même aucun),
    # _ = exactement un caractère.
    show_rows_where(df, F.col("order_date").isNull(), n=10)
    show_rows_where(df, F.trim(F.col("order_date")).like("%/%"), n=20)         # contient des "/"
    show_rows_where(df, F.trim(F.col("order_date")).like("__-__-____"), n=20)  # 15-03-2024
    show_rows_where(df, F.trim(F.col("order_date")).like("%:%"), n=20)         # contient une heure

    # Doublons : même commande enregistrée deux fois avec des identifiants différents.
    # On regroupe sur toutes les colonnes SAUF l'identifiant (unique par construction,
    # il empêcherait d'en trouver) et les colonnes techniques d'ingestion.
    # [c for c in df.columns if ...] construit une liste en gardant les colonnes qui
    # ne sont pas dans le tuple d'exclusion.
    colonnes_metier = [c for c in df.columns if c not in ("order_id", "_ingested_at", "_source_file")]
    # .count() ajoute une colonne "count" : le nombre de lignes de chaque groupe.
    doublons = df.groupBy(colonnes_metier).count()
    show_rows_where(doublons, F.col("count") > 1, n=20)

    show_rows_where(df,"order_amount= '-50'", n = 50)

    spark.stop()


# Cette ligne signifie : "lance main() seulement si on exécute ce fichier directement"
# (et pas s'il est importé par un autre fichier).
if __name__ == "__main__":
    main()