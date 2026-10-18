"""
Profilage de bronze.customers : cherche les anomalies AVANT d'écrire le nettoyage Silver.
Lecture seule : aucune table n'est modifiée.
Le profilage générique (manquants, formes, doublons) passe par profile_table.py ;
ici, seulement les contrôles propres aux clients.
"""

import sys

sys.path.append("/opt/app/spark")
from common.spark_session import get_spark_session
from pyspark.sql import functions as F


def profile_names(df):
    for col in ["first_name", "last_name"]:
        c = F.col(col)
        # espaces_autour : la valeur change quand on enlève les espaces
        # casse_irreguliere : la valeur diffère de sa version "Majuscule Initiale"
        #   (peut donner de faux positifs sur "Jean-Pierre" ou "O'Neil")
        # lettres_doublees : deux paires de lettres doublées de suite, comme "KKee"
        #   (\1 et \2 rappellent les lettres capturées par les deux (.) du motif)
        stats = df.select(
            F.sum((c != F.trim(c)).cast("int")).alias("espaces_autour"),
            F.sum((c != F.initcap(F.lower(c))).cast("int")).alias("casse_irreguliere"),
            F.sum(F.lower(c).rlike(r"(.)\1(.)\2").cast("int")).alias("lettres_doublees"),
        )
        print(f"--- {col} ---")
        stats.show(truncate=False)
        print(f"Exemples de {col} avec lettres doublées :")
        # On affiche l'email à côté : il sert à vérifier le vrai prénom/nom.
        df.filter(F.lower(c).rlike(r"(.)\1(.)\2")).select(col, "email").show(
            8, truncate=False
        )


def profile_emails(df):
    # Regex simple de validité : texte@domaine.extension
    valide = F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    # On ne teste que les emails non NULL : les manquants sont comptés par profile_table.py.
    invalides = df.filter(F.col("email").isNotNull() & ~valide)
    print(f"--- Emails invalides : {invalides.count()} ---")
    invalides.select("email").show(10, truncate=False)
    # Emails qui changent quand on les met en minuscules et sans espaces
    n_maj = df.filter(F.col("email") != F.lower(F.trim(F.col("email")))).count()
    print(f"Emails avec majuscules ou espaces : {n_maj}")


def profile_dates(df):
    # dob et signup_date sont déjà de type "date" dans Bronze : pas besoin de les convertir.
    # Les valeurs illisibles du CSV ont été transformées en NULL à l'ingestion ;
    # leur nombre est visible dans les valeurs manquantes de profile_table.py.
    dob = F.col("dob")
    signup = F.col("signup_date")
    # Âge du client le jour de son inscription, en années (datediff donne des jours).
    age_inscription = F.datediff(signup, dob) / 365.25
    print("--- Dates ---")
    df.select(
        F.min(dob).alias("dob_min"),
        F.max(dob).alias("dob_max"),
        F.min(signup).alias("signup_min"),
        F.max(signup).alias("signup_max"),
        # Inscription avant la naissance : forcément une erreur
        F.sum((signup < dob).cast("int")).alias("signup_avant_naissance"),
        # Inscription dans le futur : erreur aussi
        F.sum((signup > F.current_date()).cast("int")).alias("signup_dans_le_futur"),
        # Âge le jour de l'inscription : les valeurs extrêmes signalent des anomalies
        F.round(F.min(age_inscription), 1).alias("age_inscription_min"),
        F.round(F.max(age_inscription), 1).alias("age_inscription_max"),
        # Nombre de clients qui avaient moins de 18 ans à l'inscription
        F.sum((age_inscription < 18).cast("int")).alias("mineurs_a_l_inscription"),
    ).show(truncate=False, vertical=True)


def profile_devices(df):
    # Le nom exact de la colonne (device_id(s)) peut varier : on la retrouve par son début.
    device_col = next(c for c in df.columns if c.startswith("device"))
    # split sépare sur ";" et size compte les éléments (-1 si la valeur est NULL)
    nb = F.size(F.split(F.col(device_col), ";"))
    print(f"--- Nombre d'appareils par client ({device_col}) ---")
    df.groupBy(nb.alias("nb_appareils")).count().orderBy("nb_appareils").show()

def profile_customer_duplicates(spark):
    # Colonnes "métier" uniquement : on exclut _ingested_at et _source_file, qui peuvent
    # différer entre deux lignes sans que le client ait changé.
    cols = ["first_name", "last_name", "email", "phone_number", "gender", "dob",
            "signup_date", "address", "city", "state", "country", "`device_id(s)`", "source"]
    # On convertit chaque colonne en texte puis on les assemble avec "|" : cette chaîne
    # sert d'"empreinte". Deux lignes identiques ont la même empreinte.
    empreinte = "concat_ws('|', " + ", ".join(f"CAST({c} AS STRING)" for c in cols) + ")"

    print("\n--- 1. Format des customer_id (UUID attendu) ---")
    # {8}, {4}, {12} = nombre exact de caractères hexadécimaux dans chaque bloc
    spark.sql("""
        SELECT COUNT(*) AS id_invalides
        FROM bronze.customers
        WHERE NOT (customer_id RLIKE
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    """).show()

    print("\n--- 2. Les customer_id en doublon : copies exactes ou versions différentes ? ---")
    # nb_lignes = combien de lignes pour cet id ; nb_versions = combien d'empreintes différentes.
    # nb_versions = 1 -> copies exactes. nb_versions > 1 -> les lignes se contredisent.
    spark.sql(f"""
        WITH groupes AS (
            SELECT customer_id,
                   COUNT(*) AS nb_lignes,
                   COUNT(DISTINCT {empreinte}) AS nb_versions
            FROM bronze.customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
        SELECT nb_lignes, nb_versions, COUNT(*) AS nb_clients
        FROM groupes
        GROUP BY nb_lignes, nb_versions
        ORDER BY nb_lignes, nb_versions
    """).show(truncate=False)

    print("\n--- 3. Exemples de clients dont les lignes se contredisent ---")
    spark.sql(f"""
        WITH conflits AS (
            SELECT customer_id
            FROM bronze.customers
            GROUP BY customer_id
            HAVING COUNT(DISTINCT {empreinte}) > 1
            LIMIT 5
        )
        SELECT customer_id, first_name, last_name, email, phone_number,
               dob, signup_date, city, source
        FROM bronze.customers
        WHERE customer_id IN (SELECT customer_id FROM conflits)
        ORDER BY customer_id
    """).show(30, truncate=False)

    print("\n--- 4. Emails répétés : partagés ou simples copies ? ---")
    # email_type_shared : l'email commence par "shared"
    # plusieurs_clients : l'email est utilisé par plusieurs customer_id différents
    # Lecture : (shared, plusieurs clients) = emails partagés ;
    #           (non shared, un seul client)  = simple effet des lignes dupliquées ;
    #           (non shared, plusieurs clients) = cas à regarder de plus près.
    spark.sql("""
        WITH emails_repetes AS (
            SELECT lower(trim(email)) AS email_norm,
                   COUNT(DISTINCT customer_id) AS nb_clients
            FROM bronze.customers
            WHERE email IS NOT NULL
            GROUP BY lower(trim(email))
            HAVING COUNT(*) > 1
        )
        SELECT (email_norm LIKE 'shared%') AS email_type_shared,
               (nb_clients > 1) AS plusieurs_clients,
               COUNT(*) AS nb_emails
        FROM emails_repetes
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).show(truncate=False)

def main():
    spark = get_spark_session("profile-customers")
    df = spark.table("bronze.customers")

    # Les doublons d'abord : c'est ce qui décide de la suite du nettoyage.
    profile_customer_duplicates(spark)

    profile_names(df)
    profile_emails(df)
    profile_dates(df)
    profile_devices(df)

    spark.stop()


if __name__ == "__main__":
    main()