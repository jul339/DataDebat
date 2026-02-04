from pathlib import Path
import sys
from typing import Any, Dict

# Ajouter le répertoire src au path pour que les imports fonctionnent lorsqu'on
# exécute ce fichier depuis la racine du projet.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.es_connection import ESConnection



def pretty_total(hits: Dict[str, Any]) -> int: #gère le champ hits selon les version de ES
	
	total = hits.get("total")
	if isinstance(total, dict):
		return int(total.get("value", 0))
	try:
		return int(total)
	except Exception:
		return 0


def main():
	"""Exemple simple d'utilisation d'ESConnection et d'une requête Elasticsearch.

	- Se connecte sur `http://localhost:9200` (valeur par défaut)
	- Lance une requête `match_all` limitée à 5 documents
	- Affiche le nombre total de hits et un aperçu des documents
	- Montre comment appeler `get_word_count_for_year()` (commenté)
	"""

	es_conn = ESConnection()  # utilise l'hôte par défaut si nécessaire

	personnalise_query = {
		"query": {"match_all": {}},
		"size": 5,
	}

	# Exécuter la requête
	try:
		response = es_conn.es.search(index=es_conn.index_name, body=personnalise_query)
	except Exception as e:
		print(f"Erreur lors de la requête ES: {e}")
		return

	hits = response.get("hits", {})
	total = pretty_total(hits)
	print(f"Nombre de résultats (approx): {total}")

	docs = hits.get("hits", [])
	if not docs:
		print("Aucun document retourné par la requête.")
	else:
		print("Aperçu des documents (max 5):")
		for h in docs:
			src = h.get("_source", {})
			print("-", {k: src.get(k) for k in ("date_seance", "document_id", "orateur_nom") if k in src})

	# Exemple d'utilisation d'une méthode utilitaire:
	# total_mots = es_conn.get_word_count_for_year(date_seance="2022-01-03", field="texte")
	# print(f"Nombre total de mots pour la séance 2022-01-03: {total_mots}")


if __name__ == "__main__":
	main()
    