import csv
import mysql.connector
import datetime

# Informations de connexion à la base de données MySQL
host = 'localhost'
port = '3306'
username = 'root'
password = 'Mynewp@ssw0rd'
database = 'traffic'

# Établir la connexion à la base de données
conn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)
cursor = conn.cursor()

# Chemin vers le fichier CSV à insérer
csv_file = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\traffic.csv"

# Nom de la table cible dans la base de données
table_name = 'call_data'

# Ouvrir le fichier CSV et insérer les lignes dans la table
with open(csv_file, 'r') as file:
    reader = csv.reader(file, delimiter=',')
    next(reader)  # Ignorer l'en-tête du fichier CSV
    for row in reader:
        # Convert date format for date_debut
        date_debut = datetime.datetime.strptime(row[2], '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

        # Convert date format for date_fin
        date_fin = datetime.datetime.strptime(row[19], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')

        # Vérifier si la valeur de even_minutes est vide ou non
        if row[5] != '':
            try:
                even_minutes = float(row[5])
            except ValueError:
                even_minutes = None
        else:
            even_minutes = None

        # Formuler la requête d'insertion SQL
        sql = f"INSERT INTO {table_name} (id_date, dn, date_debut, type_even, nombre_even, even_minutes, direction_appel, termination_type, type_reseau, type_destination, operator, country, profile_id, city, gamme, marche, segment, billing_type, contract_id, date_fin) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # Exécuter la requête d'insertion avec les valeurs de la ligne actuelle du fichier CSV
        cursor.execute(sql, (
            row[0], row[1], date_debut, row[3], row[4], even_minutes, row[6], row[7], row[8], row[9], row[10], row[11],
            row[12], row[13], row[14], row[15], row[16], row[17], row[18], date_fin))

# Valider les changements et fermer la connexion
conn.commit()
cursor.close()
conn.close()
