"""
Module de génération et gestion des embeddings pour Elasticsearch
Optimisé pour l'analyse de l'évolution du discours parlementaire
"""

from typing import List, Dict, Optional, Tuple
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
import numpy as np
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import logging
from src.db.es_connection import ESConnection


logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    """Générateur d'embeddings pour les débats parlementaires"""

    # Modèles recommandés pour le français
    MODELS = {
        "multilingual": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    }

    def __init__(
        self,
        model_name: str = "multilingual",
        es_connection: Optional[ESConnection] = None,
    ):
        logger.info(f"Chargement du modèle: {model_name}")
        self.model = SentenceTransformer(self.MODELS[model_name])
        self.model_name = model_name
        self.es_connection = es_connection
        logger.info(
            f"Modèle chargé - Dimension: {self.model.get_sentence_embedding_dimension()}"
        )

    def generate_embedding(self, text: str, normalize: bool = True) -> List[float]:
        """Génère un embedding pour un texte"""
        embedding = self.model.encode(
            text,
            convert_to_numpy=True,
            normalize_embeddings=normalize,  # Important pour cosine similarity
        )
        return embedding.tolist()

    def generate_batch(
        self, texts: List[str], batch_size: int = 32, normalize: bool = True
    ) -> List[List[float]]:
        """Génère des embeddings en batch"""
        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=True,
            normalize_embeddings=normalize,
            show_progress_bar=True,
        )
        return embeddings.tolist()

    # ==================== ELASTICSEARCH ====================

    def update_index_mapping(self, index_name: str):
        """Met à jour le mapping Elasticsearch pour inclure les vecteurs"""
        mapping = {
            "properties": {
                "texte_embedding": {
                    "type": "dense_vector",
                    "dims": self.model.get_sentence_embedding_dimension(),
                    "index": True,
                    "similarity": "cosine",
                }
            }
        }
        self.es_connection.es.indices.put_mapping(index=index_name, body=mapping)
        logger.info(f"Mapping mis à jour pour {index_name} (dims={self.dimension})")

    def enrich_documents_with_embeddings(
        self,
        index_name: str,
        text_field: str = "texte",
        batch_size: int = 100,
        query: dict = None,
    ) -> Tuple[int, List]:
        """
        Enrichit les documents existants avec leurs embeddings

        Args:
            index_name: Nom de l'index Elasticsearch
            text_field: Champ contenant le texte à vectoriser
            batch_size: Taille des batches
            query: Filtre optionnel (ex: {"term": {"annee": 2018}})

        Returns:
            Tuple (nombre de succès, liste des erreurs)
        """
        if not self.es_connection.es:
            raise ValueError("Client Elasticsearch non configuré")

        # Requête pour les documents sans embedding
        search_query = {"bool": {"must_not": {"exists": {"field": "texte_embedding"}}}}
        if query:
            search_query["bool"]["filter"] = query

        # Scan des documents
        docs = list(
            scan(
                self.es_connection.es,
                index=index_name,
                query={"query": search_query},
                scroll="5m",
            )
        )

        logger.info(f"Documents à enrichir: {len(docs)}")

        # Traitement par batch
        actions = []
        for i in tqdm(range(0, len(docs), batch_size), desc="Génération embeddings"):
            batch_docs = docs[i : i + batch_size]
            texts = [doc["_source"].get(text_field, "") for doc in batch_docs]

            # Filtrer les textes vides
            valid_indices = [j for j, t in enumerate(texts) if t.strip()]
            valid_texts = [texts[j] for j in valid_indices]

            if valid_texts:
                embeddings = self.generate_batch(
                    valid_texts, batch_size=len(valid_texts)
                )

                for idx, emb in zip(valid_indices, embeddings):
                    doc = batch_docs[idx]
                    actions.append(
                        {
                            "_op_type": "update",
                            "_index": index_name,
                            "_id": doc["_id"],
                            "doc": {"texte_embedding": emb},
                        }
                    )

        # Bulk update
        if actions:
            success, errors = bulk(self.es_connection.es, actions, raise_on_error=False)
            logger.info(f"Documents enrichis: {success}, Erreurs: {len(errors)}")
            return success, errors

        return 0, []

    # ==================== RECHERCHE SÉMANTIQUE ====================

    def semantic_search(
        self,
        query: str,
        index_name: str,
        k: int = 10,
        num_candidates: int = 100,
        filters: dict = None,
    ) -> List[Dict]:
        """
        Recherche sémantique dans Elasticsearch

        Args:
            query: Texte de recherche
            index_name: Index Elasticsearch
            k: Nombre de résultats
            num_candidates: Candidats pour approximation kNN
            filters: Filtres additionnels (ex: {"term": {"annee": 2020}})

        Returns:
            Liste des documents avec leur score de similarité
        """
        query_embedding = self.generate_embedding(query)

        knn_query = {
            "field": "texte_embedding",
            "query_vector": query_embedding,
            "k": k,
            "num_candidates": num_candidates,
        }

        if filters:
            knn_query["filter"] = filters

        response = self.es_connection.es.search(
            index=index_name,
            knn=knn_query,
            source_excludes=["texte_embedding"],  # Exclure le vecteur du résultat
        )

        return [
            {"score": hit["_score"], **hit["_source"]}
            for hit in response["hits"]["hits"]
        ]

    # ==================== ANALYSE ÉVOLUTION DISCOURS ====================

    def compute_centroid(self, embeddings: List[List[float]]) -> np.ndarray:
        """Calcule le centroïde d'un ensemble d'embeddings"""
        return np.mean(np.array(embeddings), axis=0)

    def get_yearly_centroids(
        self, index_name: str, year_field: str = "annee", topic_filter: dict = None
    ) -> Dict[int, np.ndarray]:
        """
        Calcule les centroïdes par année pour analyser l'évolution du discours

        Args:
            index_name: Index Elasticsearch
            year_field: Champ contenant l'année
            topic_filter: Filtre sur un thème (ex: {"match": {"texte": "immigration"}})

        Returns:
            Dictionnaire {année: centroïde numpy array}
        """
        if not self.es_connection.es:
            raise ValueError("Client Elasticsearch non configuré")

        # Récupérer les années disponibles
        agg_query = {
            "size": 0,
            "aggs": {"years": {"terms": {"field": year_field, "size": 100}}},
        }

        if topic_filter:
            agg_query["query"] = topic_filter

        response = self.es_connection.es.search(index=index_name, body=agg_query)
        years = [
            bucket["key"] for bucket in response["aggregations"]["years"]["buckets"]
        ]

        centroids = {}
        for year in tqdm(years, desc="Calcul centroïdes par année"):
            query = {"bool": {"filter": {"term": {year_field: year}}}}
            if topic_filter:
                query["bool"]["must"] = topic_filter

            # Récupérer les embeddings de l'année
            docs = list(
                scan(
                    self.es_connection.es,
                    index=index_name,
                    query={"query": query},
                    _source=["texte_embedding"],
                )
            )

            embeddings = [
                doc["_source"]["texte_embedding"]
                for doc in docs
                if "texte_embedding" in doc["_source"]
            ]

            if embeddings:
                centroids[year] = self.compute_centroid(embeddings)
                logger.debug(f"Année {year}: {len(embeddings)} embeddings")

        return centroids

    def analyze_discourse_drift(self, centroids: Dict[int, np.ndarray]) -> Dict:
        """
        Analyse la dérive du discours au cours du temps (fenêtre d'Overton)

        Args:
            centroids: Dictionnaire {année: centroïde}

        Returns:
            Dictionnaire avec métriques d'évolution:
            - years: liste des années
            - year_to_year_drift: dérive d'une année à l'autre
            - cumulative_drift: dérive depuis la première année
            - similarity_matrix: matrice de similarité entre années
        """
        from sklearn.metrics.pairwise import cosine_similarity

        years = sorted(centroids.keys())
        results = {
            "years": years,
            "year_to_year_drift": [],
            "cumulative_drift": [],
            "similarity_matrix": None,
        }

        if len(years) < 2:
            logger.warning("Moins de 2 années disponibles pour l'analyse")
            return results

        # Dérive année à année
        for i in range(1, len(years)):
            sim = cosine_similarity([centroids[years[i - 1]]], [centroids[years[i]]])[
                0
            ][0]
            results["year_to_year_drift"].append(
                {
                    "from": years[i - 1],
                    "to": years[i],
                    "similarity": float(sim),
                    "drift": float(1 - sim),
                }
            )

        # Dérive cumulative depuis la première année
        baseline = centroids[years[0]]
        for year in years:
            sim = cosine_similarity([baseline], [centroids[year]])[0][0]
            results["cumulative_drift"].append(
                {
                    "year": year,
                    "similarity_to_baseline": float(sim),
                    "drift_from_baseline": float(1 - sim),
                }
            )

        # Matrice de similarité complète
        vectors = [centroids[y] for y in years]
        results["similarity_matrix"] = cosine_similarity(vectors).tolist()

        return results

    def find_similar_across_time(
        self,
        reference_text: str,
        index_name: str,
        year_field: str = "annee",
        k_per_year: int = 5,
    ) -> Dict[int, List[Dict]]:
        """
        Trouve les interventions similaires à un texte de référence, par année.
        Utile pour tracker comment un sujet est discuté au fil du temps.

        Args:
            reference_text: Texte de référence
            index_name: Index Elasticsearch
            year_field: Champ contenant l'année
            k_per_year: Nombre de résultats par année

        Returns:
            Dictionnaire {année: [interventions similaires]}
        """
        if not self.es_connection.es:
            raise ValueError("Client Elasticsearch non configuré")

        # Récupérer les années
        agg_response = self.es_connection.es.search(
            index=index_name,
            size=0,
            aggs={"years": {"terms": {"field": year_field, "size": 100}}},
        )
        years = [b["key"] for b in agg_response["aggregations"]["years"]["buckets"]]

        results = {}
        for year in tqdm(years, desc="Recherche par année"):
            results[year] = self.semantic_search(
                query=reference_text,
                index_name=index_name,
                k=k_per_year,
                filters={"term": {year_field: year}},
            )

        return results

    def compare_groups(
        self,
        index_name: str,
        group_field: str,
        groups: List[str],
        topic_filter: dict = None,
    ) -> Dict[str, Dict]:
        """
        Compare les positions de différents groupes (ex: partis politiques)

        Args:
            index_name: Index Elasticsearch
            group_field: Champ de regroupement (ex: "groupe_politique")
            groups: Liste des groupes à comparer
            topic_filter: Filtre optionnel sur un thème

        Returns:
            Dictionnaire avec centroïdes et matrice de similarité entre groupes
        """
        from sklearn.metrics.pairwise import cosine_similarity

        if not self.es_connection.es:
            raise ValueError("Client Elasticsearch non configuré")

        centroids = {}
        counts = {}

        for group in tqdm(groups, desc="Calcul centroïdes par groupe"):
            query = {"bool": {"filter": {"term": {group_field: group}}}}
            if topic_filter:
                query["bool"]["must"] = topic_filter

            docs = list(
                scan(
                    self.es_connection.es,
                    index=index_name,
                    query={"query": query},
                    _source=["texte_embedding"],
                )
            )

            embeddings = [
                doc["_source"]["texte_embedding"]
                for doc in docs
                if "texte_embedding" in doc["_source"]
            ]

            if embeddings:
                centroids[group] = self.compute_centroid(embeddings)
                counts[group] = len(embeddings)

        # Matrice de similarité entre groupes
        available_groups = list(centroids.keys())
        vectors = [centroids[g] for g in available_groups]
        similarity_matrix = cosine_similarity(vectors)

        return {
            "groups": available_groups,
            "counts": counts,
            "similarity_matrix": similarity_matrix.tolist(),
            "centroids": {g: centroids[g].tolist() for g in available_groups},
        }
