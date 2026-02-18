"""
Module d'analyse et de vectorisation des débats (Word2Vec)
Permet d'entraîner des modèles et d'analyser la proximité sémantique.
"""

import sys
import os
import json
import re
from pathlib import Path
from typing import List, Dict, Any

from gensim.models import Word2Vec
from elasticsearch.helpers import scan

# Ajouter le répertoire src au path pour les imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.es_connection import ESConnection


class DiscourseVectorizer:
    """Classe pour gérer l'entraînement et l'utilisation de modèles Word2Vec sur les débats"""

    def __init__(self, es_conn: ESConnection = None):
        self.es_conn = es_conn or ESConnection()
        self.models = {}  # Cache pour les modèles chargés

    def tokenizer(self, texte: str) -> List[str]:
        """
        Tokenize le texte.
        NOTE: Pour une implémentation recherche fidèle, envisager d'utiliser Spacy
        pour la lemmatisation et le retrait des stop-words spécifiques à l'AN.
        """
        if not texte:
            return []
        # Nettoyage basique
        texte = texte.lower()
        texte = re.sub(r"[^\w\s]", "", texte)
        return texte.split()

    def entrainer_modele_annuel(
        self,
        annee: int,
        text_field: str = "texte",
        vector_size: int = 300,
        window: int = 6,
        min_count: int = 5,
        save_dir: str = "./data/models"
    ) -> Word2Vec:
        """
        Entraîne un modèle Word2Vec sur tous les discours d'une année.
        """
        print(f"--- Démarrage de l'entraînement pour {annee} ---")
        
        # Scan pour streamer tous les documents de l'année
        scan_query = {"query": {"term": {"annee": annee}}}
        
        discours = []
        try:
            docs = scan(self.es_conn.es, query=scan_query, index=self.es_conn.index_name)
            for doc in docs:
                texte = doc.get("_source", {}).get(text_field, "")
                if texte and texte.strip():
                    discours.append(self.tokenizer(texte))
        except Exception as e:
            print(f"Erreur lors de la récupération des données: {e}")
            return None

        if not discours:
            print(f"Aucun discours trouvé pour l'année {annee}")
            return None

        print(f"Nombre de discours récupérés: {len(discours)}")

        # Entraînement
        model = Word2Vec(
            sentences=discours,
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            sg=1,  # Skip-gram (souvent meilleur pour la sémantique)
            negative=10,
            epochs=10,
            workers=4,  # Ajuster selon votre CPU
        )

        # Sauvegarde
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            path = os.path.join(save_dir, f"word2vec_{annee}.model")
            model.save(path)
            print(f"Modèle sauvegardé : {path}")

        self.models[annee] = model
        return model

    def charger_modele(self, annee: int, model_dir: str = "./data/models") -> Word2Vec:
        """Charge un modèle existant"""
        path = os.path.join(model_dir, f"word2vec_{annee}.model")
        if os.path.exists(path):
            print(f"Chargement du modèle {annee}...")
            model = Word2Vec.load(path)
            self.models[annee] = model
            return model
        return None

    def trouver_paragraphes_proximite(
        self, mot_principal: str, mot_similaire: str, annee: int, distance_max: int = 20
    ) -> List[str]:
        """Trouve les IDs des paragraphes où deux mots sont proches"""
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
            "_source": ["para_id"],
            "size": 10000,
        }

        para_ids = []
        try:
            response = self.es_conn.es.search(index=self.es_conn.index_name, body=query)
            for hit in response["hits"]["hits"]:
                para_id = hit["_source"].get("para_id")
                if para_id:
                    para_ids.append(para_id)
        except Exception as e:
            print(f"Erreur recherche proximité '{mot_principal}'-'{mot_similaire}': {e}")

        return para_ids

    def analyser_proximite(
        self,
        model: Word2Vec,
        mot_principal: str,
        annee: int,
        topn: int = 20,
        distance_max: int = 20,
    ) -> Dict:
        """Analyse les mots similaires et leur proximité dans le texte"""
        try:
            mots_similaires = model.wv.most_similar(mot_principal, topn=topn)
        except KeyError:
            print(f"Le mot '{mot_principal}' n'est pas dans le vocabulaire du modèle {annee}")
            return {}

        resultats = {}
        print(f"\nAnalyse de proximité pour '{mot_principal}' ({annee})...")

        for mot, score in mots_similaires:
            para_ids = self.trouver_paragraphes_proximite(
                mot_principal, mot, annee, distance_max
            )
            resultats[mot] = {
                "score_similarite": score,
                "nombre_cooccurrences": len(para_ids),
                "para_ids": para_ids,
            }
            print(f"  - {mot}: {len(para_ids)} co-occurrences (sim: {score:.2f})")

        return resultats


if __name__ == "__main__":
    # Configuration
    ANNEE = 2018
    WORD = "migrant"
    
    vectorizer = DiscourseVectorizer()
    
    # 1. Entraîner ou charger le modèle
    # model = vectorizer.charger_modele(ANNEE)
    # if not model:
    model = vectorizer.entrainer_modele_annuel(ANNEE)
    
    if model:
        # 2. Sauvegarder les mots similaires
        try:
            similaires = model.wv.most_similar(WORD, topn=20)
            
            os.makedirs("data/result", exist_ok=True)
            output_file = f"data/result/similaires_{WORD}_{ANNEE}.json"
            
            with open(output_file, "w", encoding="utf-8") as f:
                # Conversion des tuples en liste de dicts pour JSON
                json_data = [{"mot": m, "score": s} for m, s in similaires]
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            print(f"\nRésultats sauvegardés dans {output_file}")
            
            # 3. Analyser la proximité dans les textes
            resultats_proximite = vectorizer.analyser_proximite(
                model, WORD, ANNEE, topn=20
            )
            
            prox_file = f"data/result/proximite_{WORD}_{ANNEE}.json"
            with open(prox_file, "w", encoding="utf-8") as f:
                json.dump(resultats_proximite, f, ensure_ascii=False, indent=2)
                
        except KeyError:
            print(f"Le mot '{WORD}' n'a pas été trouvé dans le modèle.")
