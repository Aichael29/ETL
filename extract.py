import csv

class Extractor:
    def __init__(self):
        pass

    def extract_csv(self, filepath):
        # Ouvrir le fichier CSV en mode lecture
        with open(filepath, 'r') as file:
            # DictReader pour lire les données sous forme de dictionnaires
            csv_reader = csv.DictReader(file)
        # Retourner les données lues sous forme de liste de dictionnaires
        return [row for row in csv_reader]
