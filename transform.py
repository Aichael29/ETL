import pandas as pd

class Transformer:
    def __init__(self):
        pass

    def transform(self, data):
        # Création d'un objet DataFrame à partir des données en entrée
        df = pd.DataFrame(data)
        # Conversion du DataFrame en un dictionnaire en utilisant la méthode to_dict avec l'argument orient='records'
        return df.to_dict(orient='records')

