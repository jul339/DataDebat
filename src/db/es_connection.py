"""
Module de connexion et d'indexation Elasticsearch
Pour l'analyse des débats de l'Assemblée Nationale
"""

import re
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError
from typing import Any, Dict, List, Optional


def _clean_orateur_nom(s: str) -> str:
    """Enlève le préfixe M. / Mme, le point ou la virgule finale du nom d'orateur."""
    s = re.sub(r"^(M\.|Mme)\s*", "", s.strip())
    while s and s[-1] in ".,":
        s = s[:-1].strip()
    return s


class ESConnection:
    """Gestion de la connexion et des opérations Elasticsearch"""
    
    def __init__(self, es_host: str = "http://localhost:9200"):
        """
        Initialise la connexion Elasticsearch
        
        Args:
            es_host: URL du serveur Elasticsearch
        """
        # #region agent log
        _log = lambda **kw: open("/home/jules/DataDebat/.cursor/debug.log", "a").write(__import__("json").dumps({"sessionId": "debug-session", "runId": "run1", "timestamp": __import__("time").time(), "location": "es_connection.py:__init__", **kw}) + "\n") or None
        _log(message="ESConnection __init__ entry", data={"es_host": es_host}, hypothesisId="B")
        # #endregion
        self.es = Elasticsearch(es_host)
        self.index_name = "debats_assemblee_nationale"
        
        # Vérifier la connexion
        try:
            # #region agent log
            _log(message="before ping", data={"es_host": es_host}, hypothesisId="A")
            # #endregion
            if self.es.ping():
                print(f"✓ Connexion établie avec Elasticsearch")
            else:
                # #region agent log
                _log(message="ping returned False", data={"es_host": es_host}, hypothesisId="A")
                # #endregion
                detail = ""
                try:
                    r = self.es.info()
                    detail = f" (réponse: {r})"
                except Exception as info_err:
                    detail = f" (info erreur: {info_err})"
                raise ConnectionError(
                    "Impossible de se connecter à Elasticsearch. "
                    "Vérifiez que le service est démarré (ex: docker compose up -d)."
                    f"{detail}"
                )
        except Exception as e:
            # #region agent log
            _log(message="exception in ping", data={"es_host": es_host, "exception_type": type(e).__name__, "exception_msg": str(e), "exception_args": getattr(e, "args", ())}, hypothesisId="C")
            # #endregion
            raise ConnectionError(f"Erreur de connexion à Elasticsearch: {e}")
    
    def create_index(self):
        """Crée l'index Elasticsearch avec le mapping optimisé pour l'analyse linguistique"""
        
        mapping = {
            "mappings": {
                "properties": {
                    # Métadonnées temporelles
                    "date_seance": {"type": "date", "format": "yyyy-MM-dd"},
                    "date_parution": {"type": "date", "format": "yyyy-MM-dd"},
                    "annee": {"type": "integer"},
                    "mois": {"type": "integer"},
                    
                    # Métadonnées de session
                    "legislature": {"type": "integer"},
                    "session_nom": {"type": "keyword"},
                    "session_parlementaire": {"type": "keyword"},
                    "seance_numero": {"type": "keyword"},
                    "publication_numero": {"type": "integer"},
                    
                    # Identification du document
                    "document_id": {"type": "keyword"},
                    "section_id": {"type": "keyword"},
                    "para_id": {"type": "keyword"},
                    
                    # Contenu textuel
                    "texte": {
                        "type": "text",
                        "analyzer": "french",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    },
                    
                    # Orateur
                    "orateur_nom": {
                        "type": "text",
                        "analyzer": "french",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "orateur_fonction": {"type": "keyword"},
                    
                    # Structure du débat
                    "section_titre": {
                        "type": "text",
                        "analyzer": "french",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "sous_section_titre": {"type": "text", "analyzer": "french"},
                    "niveau_section": {"type": "integer"},
                    
                    
                    # Analyse sémantique (à remplir ultérieurement)
                    "mots_cles_insecurite": {"type": "keyword"},
                    "sentiment": {"type": "keyword"},
                    "polarite": {"type": "float"},
                    
                    # Métadonnées techniques
                    "folio": {"type": "integer"},
                    "numero_premiere_page": {"type": "integer"},
                    "extraction_timestamp": {"type": "date"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "french": {
                            "type": "french"
                        }
                    }
                }
            }
        }
        
        # Supprimer l'index s'il existe déjà
        if self.es.indices.exists(index=self.index_name):
            print(f"⚠ L'index '{self.index_name}' existe déjà. Suppression...")
            self.es.indices.delete(index=self.index_name)
        
        # Créer le nouvel index
        self.es.indices.create(index=self.index_name, body=mapping)
        print(f"✓ Index '{self.index_name}' créé avec succès")
    
    def bulk_index(self, documents: List[Dict], batch_size: int = 500, replace_existing: bool = True):
        """
        Indexe les documents en masse dans Elasticsearch
        Utilise para_id comme identifiant unique pour éviter les doublons
        
        Args:
            documents: Liste des documents à indexer
            batch_size: Taille des lots pour l'indexation
            replace_existing: Si True, remplace les documents existants avec le même ID
                              Si False, ignore les documents dont l'ID existe déjà
        """
        def generate_actions():
            for doc in documents:
                action = {
                    "_index": self.index_name,
                    "_source": doc
                }
                # Utiliser para_id comme _id unique si disponible
                if doc.get('para_id'):
                    action["_id"] = doc['para_id']
                # Si replace_existing=False, utiliser "create" pour ignorer les existants
                if not replace_existing:
                    action["_op_type"] = "create"
                yield action
        
        # Indexation en masse
        success, errors = helpers.bulk(
            self.es,
            generate_actions(),
            chunk_size=batch_size,
            raise_on_error=False
        )

        self.es.indices.refresh(index=self.index_name)

        print(f"✓ {success} documents indexés avec succès")
        if errors:
            if not replace_existing:
                # Filtrer les erreurs "document already exists"
                real_errors = [e for e in errors if 'version_conflict_engine_exception' not in str(e)]
                skipped = len(errors) - len(real_errors)
                if skipped > 0:
                    print(f"ℹ {skipped} documents ignorés (déjà existants)")
                if real_errors:
                    print(f"⚠ {len(real_errors)} erreurs d'indexation")
            else:
                print(f"⚠ {len(errors)} erreurs d'indexation")
    
    def get_document_count(self) -> int:
        """
        Retourne le nombre de documents dans l'index
        
        Returns:
            Nombre de documents indexés
        """
        count = self.es.count(index=self.index_name)
        return count['count']

    def get_stats_by_year(self) -> List[Dict]:
        """
        Retourne le nombre d'interventions et le nombre de para_id uniques par année.

        Returns:
            Liste de dicts avec annee, nb_interventions, nb_para_id_uniques.
        """
        response = self.es.search(
            index=self.index_name,
            size=0,
            query={"match_all": {}},
            aggs={
                "by_year": {
                    "terms": {"field": "annee", "size": 100},
                    "aggs": {
                        "unique_para_id": {"cardinality": {"field": "para_id.keyword"}}
                    },
                }
            },
        )
        buckets = response.get("aggregations", {}).get("by_year", {}).get("buckets", [])
        return [
            {
                "annee": int(b["key"]),
                "nb_interventions": b["doc_count"],
                "nb_para_id_uniques": b["unique_para_id"]["value"],
            }
            for b in buckets
        ]
    
    def get_word_count(self, doc_id: str, field: str = "texte") -> int:
        """
        Compte le nombre de mots pour un document identifié.
        
        Args:
            doc_id: Identifiant du document (ex: para_id).
            field: Nom du champ texte à analyser.
        
        Returns:
            Nombre de mots dans le champ texte. Retourne 0 si le champ est vide ou absent.
        """
        try:
            doc = self.es.get(index=self.index_name, id=doc_id)
        except Exception as e:
            raise RuntimeError(f"Impossible de récupérer le document {doc_id}: {e}") from e
        
        source = doc.get("_source", {})
        if field not in source or not source[field]:
            return 0
        
        # Comptage simple sur découpage par espaces
        return len(str(source[field]).split())
    
    def get_word_count_for_year(self, date_seance: str, field: str = "texte") -> int:
        """
        Calcule le nombre total de mots pour tous les documents d'une année.
        
        Args:
            year: Année à analyser (champ 'annee' dans l'index).
            field: Champ texte sur lequel compter les mots.
        
        Returns:
            Nombre total de mots pour l'année demandée.
        """
        try:
            response = self.es.search(
                index=self.index_name,
                size=0,
                query={"term": {"date_seance": date_seance}},
                aggs={
                    "word_count": {
                        "sum": {
                            "script": {
                                "source": """
                                    def src = params._source;
                                    if (src == null || !src.containsKey(params.field)) return 0;
                                    def txt = src[params.field];
                                    if (txt == null) return 0;
                                    return txt.toString().splitOnToken(' ').length;
                                """,
                                "params": {"field": field},
                            }
                        }
                    }
                },
            )
        except Exception as e:
            raise RuntimeError(f"Erreur lors du comptage des mots pour {date_seance}: {e}") from e
        
        return int(response.get("aggregations", {}).get("word_count", {}).get("value", 0))

    def get_dates_for_para_ids(
        self, para_ids: List[str], date_field: str = "date_seance"
    ) -> Dict[str, Optional[str]]:
        """
        Récupère la date (ex. date_seance) pour une liste de para_id via mget.

        Args:
            para_ids: Liste d'identifiants de paragraphes (strings).
            date_field: Champ date à retourner (défaut: "date_seance").

        Returns:
            Dict para_id -> date (string ou None si absent).
        """
        if not para_ids:
            return {}
        ids = [str(pid) for pid in para_ids]
        try:
            response = self.es.mget(
                index=self.index_name,
                body={"ids": ids},
                _source=[date_field],
            )
        except Exception as e:
            raise RuntimeError(
                f"Erreur mget pour les para_id: {e}"
            ) from e
        out = {}
        for doc in response.get("docs", []):
            pid = doc.get("_id", "")
            if doc.get("found") and doc.get("_source"):
                out[pid] = doc["_source"].get(date_field)
            else:
                out[pid] = None
        return out

    def get_field_for_para_ids(
        self, para_ids: List[str], field_name: str
    ) -> Dict[str, Optional[Any]]:
        """
        Récupère un champ (ex. orateur_nom) pour une liste de para_id via mget.
        Pour orateur_nom : supprime le préfixe "M." / "Mme" et le point final.

        Args:
            para_ids: Liste d'identifiants de paragraphes (strings).
            field_name: Nom du champ à retourner (ex. "orateur_nom").

        Returns:
            Dict para_id -> valeur du champ (ou None si absent).
        """
        if not para_ids:
            return {}
        ids = [str(pid) for pid in para_ids]
        try:
            response = self.es.mget(
                index=self.index_name,
                body={"ids": ids},
                _source=[field_name],
            )
        except Exception as e:
            raise RuntimeError(
                f"Erreur mget pour les para_id: {e}"
            ) from e
        out = {}
        for doc in response.get("docs", []):
            pid = doc.get("_id", "")
            if doc.get("found") and doc.get("_source"):
                val = doc["_source"].get(field_name)
                if field_name == "orateur_nom" and isinstance(val, str):
                    val = _clean_orateur_nom(val)
                out[pid] = val
            else:
                out[pid] = None
        return out

    def count_documents_without_text(self, field: str = "texte") -> int:
        """
        Compte le nombre de documents qui n'ont pas le champ texte renseigné
        ou qui ont un champ texte vide (chaîne vide, null, ou liste vide).
        
        Args:
            field: Nom du champ texte à vérifier (par défaut 'texte').
        
        Returns:
            Nombre de documents sans ce champ ou avec un champ vide.
        """
        try:
            response = self.es.count(
                index=self.index_name,
                query={
                    "bool": {
                        "should": [
                            # Documents où le champ n'existe pas
                            {
                                "bool": {
                                    "must_not": [
                                        {"exists": {"field": field}}
                                    ]
                                }
                            },
                            # Documents où le champ existe mais est vide
                            {
                                "script": {
                                    "script": {
                                        "source": """
                                            def src = params._source;
                                            if (src == null || !src.containsKey(params.field)) {
                                                return true;
                                            }
                                            def txt = src[params.field];
                                            if (txt == null) {
                                                return true;
                                            }
                                            if (txt instanceof List) {
                                                return txt.isEmpty() || txt.stream().allMatch(x -> x == null || x.toString().trim().isEmpty());
                                            }
                                            return !(txt.toString().trim().isEmpty());
                                        """,
                                        "params": {"field": field}
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
            )
        except Exception as e:
            raise RuntimeError(
                f"Erreur lors du comptage des documents sans champ {field}: {e}"
            ) from e
        
        return int(response.get("count", 0))
    
    def count_documents_with_text_list(self, field: str = "texte") -> int:
        """
        Compte le nombre de documents où le champ texte est une liste (array)
        plutôt qu'une chaîne simple.
        
        Args:
            field: Nom du champ texte à vérifier (par défaut 'texte').
        
        Returns:
            Nombre de documents avec ce champ sous forme de liste.
        """
        try:
            response = self.es.count(
                index=self.index_name,
                query={
                    "script": {
                        "script": {
                            "source": """
                                def src = params._source;
                                if (src == null || !src.containsKey(params.field)) {
                                    return false;
                                }
                                def txt = src[params.field];
                                if (txt == null) {
                                    return false;
                                }
                                return txt instanceof List;
                            """,
                            "params": {"field": field}
                        }
                    }
                },
            )
        except Exception as e:
            raise RuntimeError(
                f"Erreur lors du comptage des documents avec liste pour {field}: {e}"
            ) from e
        
        return int(response.get("count", 0))

    def get_interventions_containing_word(
        self, word: Optional[str] = None, field: str = "texte", scroll_size: int = 1000
    ) -> List[Dict]:
        """
        Récupère les interventions. Si word est fourni, filtre par match analysé sur le champ;
        sinon retourne toutes les interventions (scroll).

        Args:
            word: Mot à chercher (analyseur french). None = toutes les interventions.
            field: Champ texte à interroger (défaut: "texte").
            scroll_size: Nombre de hits par requête de scroll.

        Returns:
            Liste des _source des documents trouvés.
        """
        query = {"match": {field: word}} if word else {"match_all": {}}
        results = []
        try:
            response = self.es.search(
                index=self.index_name,
                scroll="2m",
                size=scroll_size,
                query=query,
                _source=True,
            )
            scroll_id = response.get("_scroll_id")
            hits = response.get("hits", {}).get("hits", [])
            results.extend(hit["_source"] for hit in hits)
            while len(hits) == scroll_size:
                response = self.es.scroll(scroll_id=scroll_id, scroll="2m")
                scroll_id = response.get("_scroll_id")
                hits = response.get("hits", {}).get("hits", [])
                results.extend(hit["_source"] for hit in hits)
            if scroll_id:
                self.es.clear_scroll(scroll_id=scroll_id, ignore=(404,))
        except Exception as e:
            raise RuntimeError(
                f"Erreur lors de la récupération des interventions: {e}"
            ) from e
        return results