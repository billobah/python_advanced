import json

def stocker_fichier(donnes, nom_fichier):
    print("Stockage dans le fichier")
    with open(nom_fichier, "w") as f:
        for line in donnes:
            json.dump(line, f)