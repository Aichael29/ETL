import csv
import datetime

import mysql.connector

# Informations de connexion à la base de données
host = 'localhost'
port = '3306'
username = 'root'
password = 'Mynewp@ssw0rd'
database = 'traffic_database'

# Chemin du fichier CSV
csv_file = 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\traffic.csv'

# Fonction pour créer les tables B2B et B2C dans la base de données
def create_tables():
    conn = mysql.connector.connect(host=host, port=port, user=username, password=password, database=database)
    cursor = conn.cursor()

    # Création de la table B2B
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS B2B (
            id_date VARCHAR(255),
            date_debut DATETIME,
            type_even VARCHAR(255),
            nombre_even INT,
            even_minutes FLOAT,
            direction_appel VARCHAR(255),
            termination_type VARCHAR(255),
            type_reseau VARCHAR(255),
            type_destination VARCHAR(255),
            operator VARCHAR(255),
            country VARCHAR(255),
            city VARCHAR(255),
            gamme VARCHAR(255),
            marche VARCHAR(255),
            segment VARCHAR(255),
            billing_type VARCHAR(255),
            contract_id VARCHAR(255),
            date_fin DATETIME,
            annee INT,
            mois INT,
            jour INT,
            annee_fin INT,
            mois_fin INT,
            jour_fin INT,
            traffic_in FLOAT,
            traffic_out FLOAT
        )
    """)

    # Création de la table B2C
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS B2C (
            id_date VARCHAR(255),
            date_debut DATETIME,
            type_even VARCHAR(255),
            nombre_even INT,
            even_minutes FLOAT,
            direction_appel VARCHAR(255),
            termination_type VARCHAR(255),
            type_reseau VARCHAR(255),
            type_destination VARCHAR(255),
            operator VARCHAR(255),
            country VARCHAR(255),
            city VARCHAR(255),
            gamme VARCHAR(255),
            marche VARCHAR(255),
            segment VARCHAR(255),
            billing_type VARCHAR(255),
            contract_id VARCHAR(255),
            date_fin DATETIME,
            annee INT,
            mois INT,
            jour INT,
            annee_fin INT,
            mois_fin INT,
            jour_fin INT,
            traffic_in FLOAT,
            traffic_out FLOAT
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()

# Fonction pour insérer les données du fichier CSV dans les tables B2B et B2C
def insert_data():
    conn = mysql.connector.connect(host=host, port=port, user=username, password=password, database=database)
    cursor = conn.cursor()

    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        next(csv_data)  # Skip header row

        for row in csv_data:
            # Suppression des colonnes dn et profile_id
            row.pop(1)  # Suppression de dn
            row.pop(12)  # Suppression de profile_id

            id_date, date_debut, type_even, nombre_even, even_minutes, direction_appel, termination_type, \
            type_reseau, type_destination, operator, country, city, gamme, marche, segment, billing_type, \
            contract_id, date_fin = row

            # Conversion des dates de début et de fin au format '%Y-%m-%d %H:%M:%S'
            date_debut = datetime.datetime.strptime(date_debut, '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            date_fin = datetime.datetime.strptime(date_fin, '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

            # Vérification de la valeur de even_minutes
            if even_minutes:
                even_minutes = float(even_minutes)
            else:
                even_minutes = None

            # Division des dates de début et de fin en différentes colonnes
            annee, mois, jour = date_debut[:4], date_debut[5:7], date_debut[8:10]
            annee_fin, mois_fin, jour_fin = date_fin[:4], date_fin[5:7], date_fin[8:10]

            # Renseignement des colonnes traffic_in et traffic_out en fonction de la valeur de direction_appel
            traffic_in = even_minutes if direction_appel == 'IN' else None
            traffic_out = even_minutes if direction_appel == 'OUT' else None

            # Insertion des données dans la table B2B ou B2C en fonction de la valeur de segment
            if segment == 'B2B':
                table = 'B2B'
            else:
                table = 'B2C'

            # Requête d'insertion
            query = f"""
                INSERT INTO {table} (id_date, date_debut, type_even, nombre_even, even_minutes, direction_appel,
                termination_type, type_reseau, type_destination, operator, country, city, gamme, marche, segment,
                billing_type, contract_id, date_fin, annee, mois, jour, annee_fin, mois_fin, jour_fin, traffic_in,
                traffic_out)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s)
            """
            values = (
                id_date, date_debut, type_even, nombre_even, even_minutes, direction_appel, termination_type,
                type_reseau, type_destination, operator, country, city, gamme, marche, segment, billing_type,
                contract_id, date_fin, annee, mois, jour, annee_fin, mois_fin, jour_fin, traffic_in, traffic_out
            )
            cursor.execute(query, values)

    conn.commit()
    cursor.close()
    conn.close()

# Appels des fonctions pour créer les tables et insérer les données
create_tables()
insert_data()
