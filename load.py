import csv
import logging
import mysql.connector
import tempfile

class Loader:
    def __init__(self):
        # connexion à la base de données MySQL
        self.mysql_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Mynewp@ssw0rd",
            database="mydatabase"
        )

    def load_mysql(self, data, table):
        # Enregistrer les données transformées en tant que fichier CSV temporaire
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            # Écrire les données dans le fichier CSV temporaire
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            csv_file = f.name

        # Créer un curseur pour exécuter les requêtes MySQL
        cursor = self.mysql_conn.cursor()
        try:
            # Créer la table avec les bonnes colonnes si elle n'existe pas encore
            columns = data[0].keys()
            types = self.get_column_types(data)
            query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join([f'{col} {types[i]}' for i, col in enumerate(columns)])})"
            cursor.execute(query)
            logging.info(f"Created table {table} with columns: {', '.join(columns)}")

            # Charger les données dans la table à partir du fichier CSV temporaire
            with open(csv_file, "r") as f:
                reader = csv.DictReader(f)
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                data_values = [[row[col] for col in columns] for row in reader]
                cursor.executemany(query, data_values)
                self.mysql_conn.commit()
                logging.info(f"Loaded {len(data)} rows into {table}")
        except mysql.connector.Error as err:
            # Afficher un message d'erreur si une erreur MySQL se produit lors du chargement des données
            logging.error(f"Error loading data into {table}: {err}")

    def get_column_types(self, data):
        # Déterminer les types de colonnes appropriés pour la table MySQL en fonction des types de données dans les données
        types = []
        for row in data:
            for val in row.values():
                if isinstance(val, int):
                    types.append("INT")
                elif isinstance(val, float):
                    types.append("FLOAT")
                else:
                    types.append("VARCHAR(255)")
            break  # Ne considérer que la première ligne pour déterminer les types
        return types
