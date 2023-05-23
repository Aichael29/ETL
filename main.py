import pandas as pd
import sqlite3

# Étape d'extraction : lecture du fichier CSV
df = pd.read_csv('modified_file2.csv')

# Étape de transformation (si nécessaire)
# Vous pouvez effectuer des modifications sur le dataframe ici

# Étape de chargement : création de la connexion à la base de données SQLite
conn = sqlite3.connect('base_de_donnees.db')

# Chargement du dataframe dans une table SQLite
df.to_sql('nom_de_la_table', conn, if_exists='replace')

# Fermeture de la connexion à la base de données
conn.close()
