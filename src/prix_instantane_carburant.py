import requests


def telecharger_donnees_economie_gouv(dataset):
    url = f"https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/records?select=id%2Clatitude%2Clongitude%2Ccp%2Cadresse%2Cville%2Cservices%2Cgazole_prix%2Cgazole_maj%2Choraires%2Csp95_maj%2Csp95_prix%2Csp98_maj%2Csp98_prix&limit={{step}}&offset={{offset}}"
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
