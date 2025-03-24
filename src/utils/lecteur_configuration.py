import json

def lire_configuration():
    with open("config.json", "r") as f:
        configuration = json.load(f)

    for config in configuration:
        config["sql_creation"] = retrouver_sql(config["fichier_sql"])

    return configuration


def retrouver_sql(nom_fichier):
    with open(f"sql/{nom_fichier}", "r") as f:
        return f.read()
