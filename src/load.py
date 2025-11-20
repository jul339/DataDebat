"""
Extracteur de débats de l'Assemblée Nationale vers Elasticsearch
Analyse de l'évolution du discours politique sur l'insécurité (2009-2025)
"""

import os
import tarfile
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import re

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError


class ANDebatsExtractor:
    """Extracteur et indexeur de débats de l'Assemblée Nationale"""
    
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
                    
                    # Vote (si présent)
                    "vote_present": {"type": "boolean"},
                    "nombre_votants": {"type": "integer"},
                    "nombre_suffrages_exprimes": {"type": "integer"},
                    "votes_pour": {"type": "integer"},
                    "votes_contre": {"type": "integer"},
                    
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
    
    def extract_xml_from_taz(self, taz_path: str) -> Tuple[Optional[ET.Element], str]:
        """
        Extrait et parse le fichier CRI XML depuis un fichier TAZ
        La structure est: .taz contient .tar qui contient .xml
        
        Args:
            taz_path: Chemin vers le fichier .taz
            
        Returns:
            Tuple (root XML Element, nom du fichier XML) ou (None, "") si erreur
        """
        try:
            print(f"Ouverture de {os.path.basename(taz_path)}...")
            
            # Ouvrir le fichier .taz (qui contient un .tar)
            with tarfile.open(taz_path, "r:*") as taz:
                # Trouver le fichier .tar à l'intérieur
                membre_tar = None
                for m in taz.getmembers():
                    if m.name.endswith(".tar"):
                        membre_tar = m
                        break
                
                if not membre_tar:
                    print("⚠ Aucun fichier .tar trouvé dans le .taz")
                    return None, ""
                
                print(f"✓ Fichier TAR trouvé: {membre_tar.name}")
                
                # Extraire le .tar en mémoire
                tar_bytes = taz.extractfile(membre_tar).read()
                tar_buffer = io.BytesIO(tar_bytes)
                
                # Ouvrir le .tar depuis la mémoire
                with tarfile.open(fileobj=tar_buffer, mode="r:") as tar:
                    # Chercher le fichier CRI XML
                    for membre in tar.getmembers():
                        if membre.name.startswith('CRI_') and membre.name.endswith('.xml'):
                            print(f"✓ Fichier XML trouvé: {membre.name}")
                            
                            # Extraire et parser le XML directement en mémoire
                            xml_file = tar.extractfile(membre)
                            xml_content = xml_file.read()
                            
                            # Parser le XML
                            root = ET.fromstring(xml_content)
                            
                            return root, membre.name
                    
                    print("⚠ Aucun fichier CRI XML trouvé dans le TAR")
                    return None, ""
                    
        except Exception as e:
            print(f"✗ Erreur lors de l'extraction: {e}")
            import traceback
            traceback.print_exc()
            return None, ""
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse les dates au format 'Mercredi-22-05-Mai-2013' vers 'YYYY-MM-DD'
        
        Args:
            date_str: Date au format de l'AN
            
        Returns:
            Date au format ISO
        """
        try:
            # Format: Jour-JJ-MM-Mois-AAAA
            parts = date_str.split('-')
            if len(parts) >= 3:
                jour = parts[1].zfill(2)
                mois = parts[2].zfill(2)
                annee = parts[-1]
                return f"{annee}-{mois}-{jour}"
        except:
            pass
        return None
    
    def extract_metadata(self, root: ET.Element) -> Dict:
        """
        Extrait les métadonnées du document XML
        
        Args:
            root: Élément racine du XML
            
        Returns:
            Dictionnaire des métadonnées
        """
        metadata = {}
        
        meta_elem = root.find('.//Metadonnees')
        if meta_elem is not None:
            # Publication
            pub_num = meta_elem.find('PublicationNumero')
            if pub_num is not None:
                metadata['publication_numero'] = int(pub_num.text)
            
            # Dates
            date_parution = meta_elem.find('DateParution')
            if date_parution is not None:
                metadata['date_parution'] = self.parse_date(date_parution.text)
            
            date_seance = meta_elem.find('DateSeance')
            if date_seance is not None:
                metadata['date_seance'] = self.parse_date(date_seance.text)
                # Extraire année et mois
                if metadata.get('date_seance'):
                    parts = metadata['date_seance'].split('-')
                    metadata['annee'] = int(parts[0])
                    metadata['mois'] = int(parts[1])
            
            # Session
            session_nom = meta_elem.find('SessionNom')
            if session_nom is not None:
                metadata['session_nom'] = session_nom.text
            
            session_parl = meta_elem.find('SessionParlementaire')
            if session_parl is not None:
                metadata['session_parlementaire'] = session_parl.text
            
            # Législature
            legislature = meta_elem.find('LegislatureNumero')
            if legislature is not None:
                metadata['legislature'] = int(legislature.text)
            
            # Numéro de première page
            premiere_page = meta_elem.find('NumeroPremierePage')
            if premiere_page is not None:
                metadata['numero_premiere_page'] = int(premiere_page.text)
        
        return metadata
    
    def clean_text(self, text: str) -> str:
        """Nettoie le texte en supprimant les espaces multiples et caractères parasites"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def extract_orateur(self, para_elem: ET.Element) -> Dict:
        """
        Extrait les informations sur l'orateur
        
        Args:
            para_elem: Élément Para contenant l'intervention
            
        Returns:
            Dictionnaire avec nom et fonction de l'orateur
        """
        orateur_info = {}
        
        orateur_elem = para_elem.find('.//Orateur')
        if orateur_elem is not None:
            nom_elem = orateur_elem.find('Nom')
            if nom_elem is not None:
                orateur_info['nom'] = self.clean_text(nom_elem.text)
            
            # Fonction peut être déduite du nom (ex: "M. le président.")
            if orateur_info.get('nom'):
                nom_lower = orateur_info['nom'].lower()
                if 'président' in nom_lower:
                    orateur_info['fonction'] = 'Président'
                elif 'ministre' in nom_lower:
                    orateur_info['fonction'] = 'Ministre'
                elif 'secrétaire' in nom_lower:
                    orateur_info['fonction'] = 'Secrétaire'
                else:
                    orateur_info['fonction'] = 'Député'
        
        return orateur_info
    
    def extract_vote(self, section_elem: ET.Element) -> Optional[Dict]:
        """
        Extrait les résultats de vote s'ils sont présents
        
        Args:
            section_elem: Élément Section contenant potentiellement un vote
            
        Returns:
            Dictionnaire avec les résultats du vote ou None
        """
        vote_elem = section_elem.find('.//ResultatVote')
        if vote_elem is None:
            return None
        
        vote_data = {'vote_present': True}
        
        # Nombre de votants
        votants = vote_elem.find('.//NombreVotants/Valeur')
        if votants is not None:
            vote_data['nombre_votants'] = int(votants.text)
        
        # Suffrages exprimés
        suffrages = vote_elem.find('.//NombreSuffrageExprime/Valeur')
        if suffrages is not None:
            vote_data['nombre_suffrages_exprimes'] = int(suffrages.text)
        
        # Pour
        pour = vote_elem.find('.//Pour/Valeur')
        if pour is not None:
            vote_data['votes_pour'] = int(pour.text)
        
        # Contre
        contre = vote_elem.find('.//Contre/Valeur')
        if contre is not None:
            vote_data['votes_contre'] = int(contre.text)
        
        return vote_data
    
    def extract_text_recursive(self, elem: ET.Element) -> str:
        """Extrait récursivement tout le texte d'un élément et ses enfants"""
        texts = []
        if elem.text:
            texts.append(elem.text)
        for child in elem:
            texts.append(self.extract_text_recursive(child))
            if child.tail:
                texts.append(child.tail)
        return ' '.join(texts)
    
    def extract_sections(self, root: ET.Element, metadata: Dict) -> List[Dict]:
        """
        Extrait toutes les sections et interventions du débat
        
        Args:
            root: Élément racine du XML
            metadata: Métadonnées du document
            
        Returns:
            Liste de documents à indexer
        """
        documents = []
        
        # Chercher toutes les sections
        for section in root.findall('.//Section'):
            section_data = metadata.copy()
            
            # Titre de section
            titre_struct = section.find('.//TitreStruct')
            if titre_struct is not None:
                section_id = titre_struct.get('Ident', '')
                section_data['section_id'] = section_id
                
                intitule = titre_struct.find('.//Intitule')
                if intitule is not None:
                    section_data['section_titre'] = self.clean_text(
                        self.extract_text_recursive(intitule)
                    )
            
            # Extraire tous les paragraphes de cette section
            for para in section.findall('.//Para'):
                para_data = section_data.copy()
                para_data['para_id'] = para.get('Ident', '')
                
                # Extraire l'orateur
                orateur_info = self.extract_orateur(para)
                if orateur_info.get('nom'):
                    para_data['orateur_nom'] = orateur_info['nom']
                if orateur_info.get('fonction'):
                    para_data['orateur_fonction'] = orateur_info['fonction']
                
                # Extraire le texte complet du paragraphe
                texte = self.extract_text_recursive(para)
                para_data['texte'] = self.clean_text(texte)
                
                # Ne garder que les paragraphes avec du contenu
                if para_data['texte'] and len(para_data['texte']) > 10:
                    para_data['extraction_timestamp'] = datetime.now().isoformat()
                    para_data['vote_present'] = False
                    documents.append(para_data)
            
            # Extraire les résultats de vote s'ils existent
            vote_data = self.extract_vote(section)
            if vote_data:
                # Ajouter les données de vote au dernier paragraphe de la section
                if documents and documents[-1].get('section_id') == section_data.get('section_id'):
                    documents[-1].update(vote_data)
        
        return documents
    
    def bulk_index(self, documents: List[Dict], batch_size: int = 500):
        """
        Indexe les documents en masse dans Elasticsearch
        
        Args:
            documents: Liste des documents à indexer
            batch_size: Taille des lots pour l'indexation
        """
        def generate_actions():
            for doc in documents:
                yield {
                    "_index": self.index_name,
                    "_source": doc
                }
        
        # Indexation en masse
        success, errors = helpers.bulk(
            self.es,
            generate_actions(),
            chunk_size=batch_size,
            raise_on_error=False
        )
        
        print(f"✓ {success} documents indexés avec succès")
        if errors:
            print(f"⚠ {len(errors)} erreurs d'indexation")
    
    def process_taz_file(self, taz_path: str):
        """
        Traite un fichier TAZ complet: extraction, parsing et indexation
        
        Args:
            taz_path: Chemin vers le fichier .taz
        """
        print(f"\n{'='*60}")
        print(f"Traitement de: {os.path.basename(taz_path)}")
        print(f"{'='*60}")
        
        try:
            # Étape 1: Extraire et parser le XML directement depuis le .taz
            root, xml_filename = self.extract_xml_from_taz(taz_path)
            
            if root is None:
                print("✗ Impossible d'extraire le XML")
                return
            
            print(f"✓ XML parsé avec succès: {xml_filename}")
            
            # Étape 2: Extraire les métadonnées
            metadata = self.extract_metadata(root)
            print(f"✓ Métadonnées extraites: Séance du {metadata.get('date_seance', 'N/A')}")
            
            # Étape 3: Extraire les sections et interventions
            documents = self.extract_sections(root, metadata)
            print(f"✓ {len(documents)} interventions extraites")
            
            # Étape 4: Indexer dans Elasticsearch
            if documents:
                self.bulk_index(documents)
            
            print(f"✓ Traitement terminé avec succès")
            
        except Exception as e:
            print(f"✗ Erreur lors du traitement: {e}")
            import traceback
            traceback.print_exc()
    
    def process_directory(self, directory: str):
        """
        Traite tous les fichiers TAZ d'un répertoire
        
        Args:
            directory: Chemin vers le répertoire contenant les fichiers TAZ
        """
        taz_files = list(Path(directory).glob("*.taz"))
        
        if not taz_files:
            print(f"⚠ Aucun fichier .taz trouvé dans {directory}")
            return
        
        print(f"\n{'='*60}")
        print(f"Traitement de {len(taz_files)} fichier(s) TAZ")
        print(f"{'='*60}")
        
        for i, taz_file in enumerate(taz_files, 1):
            print(f"\n[{i}/{len(taz_files)}]")
            self.process_taz_file(str(taz_file))
        
        print(f"\n{'='*60}")
        print(f"Traitement global terminé")
        print(f"{'='*60}")


def main():
    """Fonction principale"""
    
    # Configuration
    ES_HOST = "http://localhost:9200"
    DATA_DIR = "./data/raw"  # Répertoire contenant les fichiers TAZ
    
    # Créer l'extracteur
    extractor = ANDebatsExtractor(es_host=ES_HOST)
    
    # Créer l'index
    extractor.create_index()
    
    # Traiter les fichiers
    # Option 1: Traiter un seul fichier
    # extractor.process_taz_file("./AN_2022001.taz")
    
    # Option 2: Traiter tous les fichiers d'un répertoire
    extractor.process_directory(DATA_DIR)
    
    # Vérifier l'indexation
    count = extractor.es.count(index=extractor.index_name)
    print(f"\n📊 Total de documents indexés: {count['count']}")


if __name__ == "__main__":
    main()