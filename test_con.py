from src.db.es_connection import ESConnection

es_conn = ESConnection()

stats = es_conn.get_stats_by_year()
for s in sorted(stats, key=lambda x: x["annee"]):
    print(f"{s['annee']}: {s['nb_interventions']} interventions, {s['nb_para_id_uniques']} para_id uniques")
