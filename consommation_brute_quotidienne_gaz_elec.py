import requests
import duckdb
import json

url = 'https://tabular-api.data.gouv.fr/api/resources/cfc27ff9-1871-4ee8-be64-b9a290c06935/data/?Date__exact="2024-10-31"'
toutes_les_data = []

print("Télécharger les données")

while url:
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    toutes_les_data += data['data']
    url = data['links'].get("next")

print("Stockage dans le fichier")

with open("consommation_brute_quotidienne_gaz_elec.json", "w") as f:
    for line in toutes_les_data:
        json.dump(line, f)

print("Chargement dans la BDD")

connection = duckdb.connect("bdd_cours_python_avance")
sql_creation = """
CREATE TABLE IF NOT EXISTS consommation_brute_quotidienne_gaz_elec_raw (
    "__id" INT,
    "Date - Heure" TIMESTAMP,
    "Date" DATE,
    "Heure" STRING,
    "Consommation brute gaz (MW PCS 0°C) - GRTgaz" INT,
    "Statut - GRTgaz" STRING,
    "Consommation brute gaz (MW PCS 0°C) - Teréga" INT,
    "Statut - Teréga" STRING,
    "Consommation brute gaz totale (MW PCS 0°C)" INT,
    "Consommation brute électricité (MW) - RTE" INT,
    "Statut - RTE" STRING,
    "Consommation brute totale (MW)" INT 
)
"""
connection.sql(sql_creation)
connection.sql('INSERT INTO consommation_brute_quotidienne_gaz_elec_raw '
               'SELECT * FROM read_json_auto("consommation_brute_quotidienne_gaz_elec.json")')
