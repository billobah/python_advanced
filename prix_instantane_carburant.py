import requests
import duckdb
import json


url = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={limit}&offset={offset}'
toutes_les_data = []
total_count = 1
step = 100
offset = 0


print("Télécharger les données")
while True:
    r = requests.get(url.format(limit=step, offset=offset))
    r.raise_for_status()
    data = r.json()
    toutes_les_data += data['results']
    total_count = data['total_count']
    offset += len(toutes_les_data) + 1
    if total_count - len(toutes_les_data) <= 0:
        break
    if offset + step > 10000:
        break


print("Stockage dans le fichier")
with open("prix_instantane_carburant.json", "w") as f:
    for line in toutes_les_data:
        json.dump(line, f)

print("Chargement dans la BDD")
connection = duckdb.connect("bdd_cours_python_avance")
sql_creation = """
CREATE TABLE IF NOT EXISTS prix_instante_raw (
    id INT,
    latitude FLOAT,
    longitude FLOAT,
    cp STRING,
    adresse STRING,
    ville STRING,
    services STRING,
    gazole_prix FLOAT,
    gazole_maj TIMESTAMP,
    horaires STRING,
    sp95_maj TIMESTAMP,
    sp95_prix FLOAT,
    sp98_maj TIMESTAMP,
    sp98_prix FLOAT
)
"""
connection.sql(sql_creation)
connection.sql('INSERT INTO prix_instante_raw '
               'SELECT * FROM read_json_auto("prix_instantane_carburant.json")')