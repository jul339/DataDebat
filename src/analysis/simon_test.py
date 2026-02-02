

from notebooks.get_raw_data import response
from src.db.es_connection import ESConnection


def main():
    # initialisation de la connexion à la base de données
    es_conn = ESConnection()

    # création de la requête personnalisée

    personnalise_query = {
        "query":{
        
        }
    }
    # exécution de la requête
    response = es_conn.es.search(index="debats_assemblee_nationale", body=personnalise_query)

    # comptage des mots pour l'année 2022
    count =es_conn.get_word_count_for_year(date_seance="2022-01-03", field="texte")
