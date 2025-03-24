from utils.stocker_fichier import stocker_fichier
from utils.lecteur_configuration import lire_configuration
from bdd.db import stocker_dans_bdd
from prix_instantane_carburant import telecharger_donnees_economie_gouv
from consommation_brute_quotidienne_gaz_elec import telecharger_data_gouv

fichier_base_de_donnees = "../data/bdd_cours_python_avance"

configuration = lire_configuration()


for config in configuration:
    if config["type_api"] == "economie_gouv":
        resultat = telecharger_donnees_economie_gouv(config["dataset"])
    elif config["type_api"] == "data_gouv":
        resultat = telecharger_data_gouv(config["dataset"])
    else:
        raise ValueError(f"Le type d'API {config["type_api"]} est inconnu.")

    stocker_fichier(resultat, config["fichier_cible"])
    stocker_dans_bdd(config["sql_creation"], config["fichier_cible"], fichier_base_de_donnees)
