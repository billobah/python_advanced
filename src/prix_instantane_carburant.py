import requests

from utils.stocker_fichier import stocker_fichier
from bdd.db import stocker_dans_bdd

url_api = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={limit}&offset={offset}'
fichier_cible = "../data/prix_instantane_carburant.json"
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
fichier_base_de_donnees = "../data/bdd_cours_python_avance"


def telecharger_donnes_prix_carburant(url):
    toutes_les_data = []
    step = 100
    offset = 0

    print("Télécharger les données")
    while True:
        r = requests.get(url.format(limit=step, offset=offset))
        r.raise_for_status()
        data = r.json()
        toutes_les_data += data['results']
        total_count = data['total_count']
        offset += step
        if total_count - offset <= 0:
            break
        if offset + step > 10000:
            break
    return toutes_les_data


resultat = telecharger_donnes_prix_carburant(url_api)
stocker_fichier(resultat, fichier_cible)
nom_table = "prix_instante_raw"
stocker_dans_bdd(sql_creation, fichier_cible, fichier_base_de_donnees, nom_table)