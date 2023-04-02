# fichier etl.py
from extract import Extractor
from transform import Transformer
from load import Loader

extractor = Extractor()
transformer = Transformer()
loader = Loader()

# Extraction des données
csv_data = extractor.extract_csv('C:\\Users\\Lenovo\\Pictures\\Camera Roll\\input.csv')

# Transformation des données
transformed_data = transformer.transform(csv_data)

# Chargement des données
loader.load_mysql(transformed_data, 'mytable')
