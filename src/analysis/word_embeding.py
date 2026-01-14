# Exemple de vectorisation de discours par Word2Vec pour une année donnée dans Elasticsearch
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # pointe sur .../src
sys.path.insert(0, str(ROOT))

from db.es_connection import ESConnection  # Ce module doit gérer la connexion ES
from gensim.models import Word2Vec
from elasticsearch.helpers import scan
import re

ANNEE = 2018
WORD = "migrant"


def tokenizer(texte):
    # Simple tokenizer: enlève ponctuation, met en minuscule, split sur espaces
    texte = texte.lower()
    texte = re.sub(r"[^\w\s]", "", texte)
    return texte.split()


def entrainer_word2vec_pour_annee(
    annee: int, text_field: str = "texte", taille_vecteur: int = 100, min_count: int = 5
):
    """
    Entraîne un modèle Word2Vec sur tous les discours d'une année dans Elasticsearch.

    Args:
        annee (int): L'année cible
         (str): URL ES
        index_name (str): Nom de l'index
        text_field (str): Champ texte à vectoriser (ex: 'texte')
        taille_vecteur (int): Dimension du vecteur Word2Vec
        min_count (int): Nombre min d'occurences pour retention
    Returns:
        model: Modèle Word2Vec entraîné
    """
    es_conn = ESConnection()

    # Scan pour streamer tous les documents de l'année
    scan_query = {"query": {"term": {"annee": annee}}}

    discours = []
    docs = scan(es_conn.es, query=scan_query, index="debats_assemblee_nationale")
    for doc in docs:
        texte = doc.get("_source", {}).get(text_field, "")
        if texte and texte.strip():
            discours.append(tokenizer(texte))

    if not discours:
        raise ValueError(f"Aucun discours trouvé pour l'année {annee}")

    print(f"Nombre de discours récupérés: {len(discours)}")

    # Entraînement du modèle Word2Vec
    # workers=1 pour éviter les problèmes de threading sous WSL2
    model = Word2Vec(
        sentences=discours,
        vector_size=300,
        window=6,
        min_count=3,
        sg=1,  # Skip-gram
        negative=10,
        epochs=10,
        workers=1,
    )

    print(f"Modèle Word2Vec entraîné sur l'année {annee} !")
    return model


# Exemple d'utilisation :
model = entrainer_word2vec_pour_annee(ANNEE)
print(model.wv.most_similar(WORD))

import json
import os


def trouver_paragraphes_proximite(
    es_conn, mot_principal: str, mot_similaire: str, annee: int, distance_max: int = 20
):
    """
    Trouve les paragraphes où deux mots apparaissent à moins de distance_max mots l'un de l'autre.

    Args:
        es_conn: Connexion Elasticsearch
        mot_principal: Le mot principal (ex: "migration")
        mot_similaire: Le mot similaire à rechercher
        annee: L'année de recherche
        distance_max: Distance maximale en nombre de mots (défaut: 20)

    Returns:
        Liste des IDs de paragraphes (para_id) où les deux mots sont proches
    """
    # Requête avec match_phrase et slop pour trouver les deux mots à proximité
    # On cherche les deux ordres possibles (mot1 puis mot2, et mot2 puis mot1)
    # slop = nombre de mots supplémentaires autorisés entre les deux termes
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"annee": annee}},
                    {
                        "bool": {
                            "should": [
                                {
                                    "match_phrase": {
                                        "texte": {
                                            "query": f"{mot_principal} {mot_similaire}",
                                            "slop": distance_max,
                                        }
                                    }
                                },
                                {
                                    "match_phrase": {
                                        "texte": {
                                            "query": f"{mot_similaire} {mot_principal}",
                                            "slop": distance_max,
                                        }
                                    }
                                },
                            ],
                            "minimum_should_match": 1,
                        }
                    },
                ]
            }
        },
        "_source": ["para_id", "texte"],  # Récupérer seulement para_id et texte
        "size": 10000,  # Limite élevée pour récupérer tous les résultats
    }

    para_ids = []
    try:
        response = es_conn.es.search(index="debats_assemblee_nationale", body=query)
        for hit in response["hits"]["hits"]:
            para_id = hit["_source"].get("para_id")
            if para_id:
                para_ids.append(para_id)
    except Exception as e:
        print(f"Erreur lors de la recherche pour '{mot_similaire}': {e}")

    return para_ids


def analyser_proximite_mots_similaires(
    es_conn,
    model,
    mot_principal: str,
    annee: int,
    topn: int = 20,
    distance_max: int = 20,
):
    """
    Pour chaque mot dans le top N des mots similaires, trouve les paragraphes
    où ce mot apparaît à moins de distance_max mots du mot principal.

    Args:
        es_conn: Connexion Elasticsearch
        model: Modèle Word2Vec entraîné
        mot_principal: Le mot principal (ex: "migration")
        annee: L'année de recherche
        topn: Nombre de mots similaires à analyser (défaut: 20)
        distance_max: Distance maximale en nombre de mots (défaut: 20)

    Returns:
        Dictionnaire {mot_similaire: [liste des para_id]}
    """
    # Récupérer les mots similaires
    mots_similaires = model.wv.most_similar(mot_principal, topn=topn)

    resultats = {}
    print(
        f"\nRecherche des paragraphes pour {len(mots_similaires)} mots similaires à '{mot_principal}'..."
    )

    for i, (mot, score) in enumerate(mots_similaires, 1):
        print(
            f"  [{i}/{len(mots_similaires)}] Recherche de '{mot}' (score: {score:.4f})...",
            end=" ",
        )
        para_ids = trouver_paragraphes_proximite(
            es_conn, mot_principal, mot, annee, distance_max
        )
        resultats[mot] = {
            "score_similarite": score,
            "para_ids": para_ids,
            "nombre_paragraphes": len(para_ids),
        }
        print(f"{len(para_ids)} paragraphe(s) trouvé(s)")

    return resultats


# Exemple : enregistrer les 20 mots les plus similaires à "migration" dans un fichier JSON
resultats = model.wv.most_similar(WORD, topn=20)

# Créer le dossier data/result s'il n'existe pas
os.makedirs("data/result", exist_ok=True)
name_base = f"data/result/similaires_{WORD}_{ANNEE}.json"
name = name_base
count = 1
while os.path.exists(name):
    name = f"data/result/similaires_{WORD}_{ANNEE}_{count}.json"
    count += 1
# Enregistrer les résultats dans un fichier JSON
with open(name, "w", encoding="utf-8") as f:
    json.dump(resultats, f, ensure_ascii=False, indent=2)

print("Résultats enregistrés dans data/result/similaires_{WORD}_2018.json")

# Recherche des paragraphes où chaque mot similaire apparaît à moins de 20 mots de "migration"
print("\n" + "=" * 60)
print("Recherche des paragraphes avec proximité des mots similaires")
print("=" * 60)

es_conn = ESConnection()
resultats_proximite = analyser_proximite_mots_similaires(
    es_conn, model, WORD, ANNEE, topn=20, distance_max=20
)

# Enregistrer les résultats de proximité
name_proximite_base = f"data/result/proximite_{WORD}_{ANNEE}.json"
name_proximite = name_proximite_base
count = 1
while os.path.exists(name_proximite):
    name_proximite = f"data/result/proximite_{WORD}_{ANNEE}_{count}.json"
    count += 1

with open(name_proximite, "w", encoding="utf-8") as f:
    json.dump(resultats_proximite, f, ensure_ascii=False, indent=2)

print(f"\nRésultats de proximité enregistrés dans {name_proximite}")

# Afficher un résumé
print("\n" + "=" * 60)
print("Résumé des résultats de proximité")
print("=" * 60)
for mot, data in resultats_proximite.items():
    print(
        f"  {mot:25s} : {data['nombre_paragraphes']:4d} paragraphe(s) (score: {data['score_similarite']:.4f})"
    )
