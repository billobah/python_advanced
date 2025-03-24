import requests


def telecharger_data_gouv(url):
    toutes_les_data = []

    print("Télécharger les données")
    while url:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        toutes_les_data += data['data']
        url = data['links'].get("next")
    return toutes_les_data
