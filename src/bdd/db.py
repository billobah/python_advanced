import duckdb

def stocker_dans_bdd(sql, fichier, bdd, nom_table) -> None:
    print("Chargement dans la BDD")
    connection = duckdb.connect(bdd)
    connection.sql(sql)
    connection.sql(f'INSERT INTO {nom_table} '
                   f'SELECT * FROM read_json_auto("{fichier}")')