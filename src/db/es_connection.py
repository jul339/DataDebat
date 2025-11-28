"""
Module de connexion et d'indexation Elasticsearch
Pour l'analyse des débats de l'Assemblée Nationale
"""

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError
from typing import Dict, List


class ESConnection:
    """Gestion de la connexion et des opérations Elasticsearch"""
    
    def __init__(self, es_host: str = "http://localhost:9200"):
        """
        Initialise la connexion Elasticsearch
        
        Args:
            es_host: URL du serveur Elasticsearch
        """
        self.es = Elasticsearch(es_host)
        self.index_name = "debats_assemblee_nationale"
        
        # Vérifier la connexion
        try:
            if self.es.ping():
                print(f"✓ Connexion établie avec Elasticsearch")
            else:
                raise ConnectionError("Impossible de se connecter à Elasticsearch")
        except Exception as e:
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

