import requests
import duckdb
import json


url_api = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={limit}&offset={offset}'
fichier_cible = "prix_instantane_carburant.json"
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

fichier_base_de_donnees = "bdd_cours_python_avance"


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


def stocker_fichier(donnes, nom_fichier):
    print("Stockage dans le fichier")
    with open(nom_fichier, "w") as f:
        for line in donnes:
            json.dump(line, f)


def stocker_dans_bdd(sql, fichier, bdd):
    print("Chargement dans la BDD")
    connection = duckdb.connect(bdd)
    connection.sql(sql)
    connection.sql('INSERT INTO consommation_brute_quotidienne_gaz_elec_raw '
                   f'SELECT * FROM read_json_auto("{fichier}")')


resultat = telecharger_donnes_prix_carburant(url_api)
stocker_fichier(resultat, fichier_cible)
stocker_dans_bdd(sql_creation, fichier_cible, fichier_base_de_donnees)