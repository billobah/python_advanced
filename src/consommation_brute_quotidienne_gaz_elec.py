import requests
from utils.stocker_fichier import stocker_fichier
from bdd.db import stocker_dans_bdd

url_api = 'https://tabular-api.data.gouv.fr/api/resources/cfc27ff9-1871-4ee8-be64-b9a290c06935/data/?Date__exact="2024-10-31"'
fichier_cible = "../data/consommation_brute_quotidienne_gaz_elec.json"
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
fichier_base_de_donnees = "../data/bdd_cours_python_avance"


def telecharger_donnes_conso_gaz_elec(url):
    toutes_les_data = []

    print("Télécharger les données")
    while url:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        toutes_les_data += data['data']
        url = data['links'].get("next")
    return toutes_les_data


resultat = telecharger_donnes_conso_gaz_elec(url_api)
stocker_fichier(resultat, fichier_cible)
nom_table = "consommation_brute_quotidienne_gaz_elec_raw"
stocker_dans_bdd(sql_creation, fichier_cible, fichier_base_de_donnees, nom_table)