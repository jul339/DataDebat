

from notebooks.get_raw_data import response
from src.db.es_connection import ESConnection


def main():d

    es_conn = ESConnection()



    personnalise_query = {
        "query":{
        
        }
    }
    response = es_conn.es.search(index="debats_assemblee_nationale", body=personnalise_query)

    es_conn.get_word_count_for_year(date_seance="2022-01-03", field="texte")
    