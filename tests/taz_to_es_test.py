"""
Tests PyTest pour valider la cohérence entre fichiers TAZ et données Elasticsearch
Projet: Analyse du discours politique sur l'insécurité (2009-2025)
"""

import pytest
import tarfile
import io
import xml.etree.ElementTree as ET
from elasticsearch import Elasticsearch
from datetime import datetime
import re
from typing import Dict, List, Tuple


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def es_client():
    """Fixture pour la connexion Elasticsearch"""
    es = Elasticsearch("http://localhost:9200")
    
    # Vérifier la connexion
    if not es.ping():
        pytest.fail("Impossible de se connecter à Elasticsearch")
    
    yield es
    
    # Cleanup si nécessaire
    es.close()


@pytest.fixture(scope="session")
def index_name():
    """Nom de l'index Elasticsearch"""
    return "debats_assemblee_nationale"


@pytest.fixture(scope="module")
def sample_taz_file():
    """
    Chemin vers un fichier TAZ de test
    Modifier ce chemin selon votre structure
    """
    return "./data/raw/AN_2022001.taz"


@pytest.fixture(scope="module")
def parsed_xml_data(sample_taz_file):
    """
    Parse le fichier TAZ et retourne les données XML structurées
    """
    def parse_date(date_str: str) -> str:
        """Parse les dates au format 'Mercredi-22-05-Mai-2013' vers 'YYYY-MM-DD'"""
        try:
            parts = date_str.split('-')
            if len(parts) >= 3:
                jour = parts[1].zfill(2)
                mois = parts[2].zfill(2)
                annee = parts[-1]
                return f"{annee}-{mois}-{jour}"
        except:
            pass
        return None
    
    def clean_text(text: str) -> str:
        """Nettoie le texte"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def extract_text_recursive(elem: ET.Element) -> str:
        """Extrait récursivement le texte"""
        texts = []
        if elem.text:
            texts.append(elem.text)
        for child in elem:
            texts.append(extract_text_recursive(child))
            if child.tail:
                texts.append(child.tail)
        return ' '.join(texts)
    
    # Ouvrir et parser le TAZ
    with tarfile.open(sample_taz_file, "r:*") as taz:
        membre_tar = next(m for m in taz.getmembers() if m.name.endswith(".tar"))
        tar_bytes = taz.extractfile(membre_tar).read()
        tar_buffer = io.BytesIO(tar_bytes)
        
        with tarfile.open(fileobj=tar_buffer, mode="r:") as tar:
            for membre in tar.getmembers():
                if membre.name.startswith('CRI_') and membre.name.endswith('.xml'):
                    xml_file = tar.extractfile(membre)
                    xml_content = xml_file.read()
                    root = ET.fromstring(xml_content)
                    
                    # Extraire les métadonnées
                    metadata = {}
                    meta_elem = root.find('.//Metadonnees')
                    if meta_elem is not None:
                        pub_num = meta_elem.find('PublicationNumero')
                        if pub_num is not None:
                            metadata['publication_numero'] = int(pub_num.text)
                        
                        date_seance = meta_elem.find('DateSeance')
                        if date_seance is not None:
                            metadata['date_seance'] = parse_date(date_seance.text)
                        
                        legislature = meta_elem.find('LegislatureNumero')
                        if legislature is not None:
                            metadata['legislature'] = int(legislature.text)
                    
                    # Extraire les sections et paragraphes
                    paragraphs = []
                    for section in root.findall('.//Section'):
                        section_data = {}
                        
                        # Titre de section
                        titre_struct = section.find('.//TitreStruct')
                        if titre_struct is not None:
                            section_data['section_id'] = titre_struct.get('Ident', '')
                            intitule = titre_struct.find('.//Intitule')
                            if intitule is not None:
                                section_data['section_titre'] = clean_text(
                                    extract_text_recursive(intitule)
                                )
                        
                        # Paragraphes
                        for para in section.findall('.//Para'):
                            para_data = section_data.copy()
                            para_data['para_id'] = para.get('Ident', '')
                            
                            # Orateur
                            orateur_elem = para.find('.//Orateur')
                            if orateur_elem is not None:
                                nom_elem = orateur_elem.find('Nom')
                                if nom_elem is not None:
                                    para_data['orateur_nom'] = clean_text(nom_elem.text)
                            
                            # Texte
                            texte = extract_text_recursive(para)
                            para_data['texte'] = clean_text(texte)
                            
                            if para_data['texte'] and len(para_data['texte']) > 10:
                                paragraphs.append(para_data)
                    
                    return {
                        'metadata': metadata,
                        'paragraphs': paragraphs,
                        'xml_filename': membre.name
                    }
    
    pytest.fail("Aucun fichier XML CRI trouvé dans le TAZ")


# ============================================================================
# TESTS DE CONNEXION ET EXISTENCE
# ============================================================================

class TestElasticsearchConnection:
    """Tests de connexion et configuration Elasticsearch"""
    
    def test_elasticsearch_is_running(self, es_client):
        """Vérifie que Elasticsearch est accessible"""
        assert es_client.ping(), "Elasticsearch n'est pas accessible"
    
    def test_index_exists(self, es_client, index_name):
        """Vérifie que l'index existe"""
        assert es_client.indices.exists(index=index_name), \
            f"L'index '{index_name}' n'existe pas"
    
    def test_index_has_documents(self, es_client, index_name):
        """Vérifie que l'index contient des documents"""
        count = es_client.count(index=index_name)
        assert count['count'] > 0, "L'index ne contient aucun document"


# ============================================================================
# TESTS DE COHÉRENCE DES MÉTADONNÉES
# ============================================================================

class TestMetadataConsistency:
    """Tests de cohérence des métadonnées entre TAZ et Elasticsearch"""
    
    def test_publication_numero_matches(self, es_client, index_name, parsed_xml_data):
        """Vérifie que le numéro de publication correspond"""
        expected_pub_num = parsed_xml_data['metadata'].get('publication_numero')
        
        if expected_pub_num is None:
            pytest.skip("Pas de numéro de publication dans le XML")
        
        # Rechercher dans ES
        
        response = es_client.search(index=index_name, query = {"term": {"publication_numero": expected_pub_num}} , size=1)
        assert response['hits']['total']['value'] > 0, \
            f"Aucun document avec publication_numero={expected_pub_num} trouvé dans ES"
    
    def test_date_seance_matches(self, es_client, index_name, parsed_xml_data):
        """Vérifie que la date de séance correspond"""
        expected_date = parsed_xml_data['metadata'].get('date_seance')
        
        if expected_date is None:
            pytest.skip("Pas de date de séance dans le XML")
        
        # Rechercher dans ES
        query = {
            "query": {
                "term": {"date_seance": expected_date}
            },
            "size": 1
        }
        
        response = es_client.search(index=index_name, query = query['query'], size=1)
        assert response['hits']['total']['value'] > 0, \
            f"Aucun document avec date_seance={expected_date} trouvé dans ES"
    
    def test_legislature_matches(self, es_client, index_name, parsed_xml_data):
        """Vérifie que le numéro de législature correspond"""
        expected_legislature = parsed_xml_data['metadata'].get('legislature')
        
        if expected_legislature is None:
            pytest.skip("Pas de législature dans le XML")
        
        # Rechercher dans ES
        
        response = es_client.search(index=index_name, query = {"term": {"legislature": expected_legislature}} , size=1)
        assert response['hits']['total']['value'] > 0, \
            f"Aucun document avec legislature={expected_legislature} trouvé dans ES"


# ============================================================================
# TESTS DE COHÉRENCE DU CONTENU
# ============================================================================

class TestContentConsistency:
    """Tests de cohérence du contenu entre TAZ et Elasticsearch"""
    
    def test_number_of_paragraphs_matches(self, es_client, index_name, parsed_xml_data):
        """Vérifie que le nombre de paragraphes correspond"""
        expected_count = len(parsed_xml_data['paragraphs'])
        date_seance = parsed_xml_data['metadata'].get('date_seance')
        
        if date_seance is None:
            pytest.skip("Pas de date de séance pour filtrer")
        
        # Compter dans ES
        
        response = es_client.count(index=index_name, query ={"term": {"date_seance": date_seance}} )
        actual_count = response['count']
        
        # Tolérance de 5% pour les paragraphes très courts qui peuvent être filtrés
        tolerance = max(1, int(expected_count * 0.05))
        
        assert abs(actual_count - expected_count) <= tolerance, \
            f"Nombre de paragraphes différent: XML={expected_count}, ES={actual_count}"
    
    def test_section_ids_exist(self, es_client, index_name, parsed_xml_data):
        """Vérifie que les IDs de section existent dans ES"""
        # Prendre les 5 premiers IDs de section uniques
        section_ids = list(set([
            p['section_id'] for p in parsed_xml_data['paragraphs']
            if p.get('section_id')
        ]))[:5]
        
        if not section_ids:
            pytest.skip("Pas d'ID de section dans le XML")
        
        for section_id in section_ids:
            response = es_client.search(index=index_name, query={"term": {"section_id": section_id}}, size=1)
            assert response['hits']['total']['value'] > 0, \
                f"Section ID '{section_id}' non trouvé dans ES"
    
    def test_paragraph_ids_exist(self, es_client, index_name, parsed_xml_data):
        """Vérifie que les IDs de paragraphe existent dans ES"""
        # Prendre les 10 premiers IDs de paragraphe
        para_ids = [
            p['para_id'] for p in parsed_xml_data['paragraphs'][:10]
            if p.get('para_id')
        ]
        
        if not para_ids:
            pytest.skip("Pas d'ID de paragraphe dans le XML")
        
        for para_id in para_ids:
            response = es_client.search(index=index_name, query = {"term": {"para_id": para_id}} , size=1)
            assert response['hits']['total']['value'] > 0, \
                f"Paragraph ID '{para_id}' non trouvé dans ES"
    
    def test_text_content_matches(self, es_client, index_name, parsed_xml_data):
        """Vérifie que le contenu textuel correspond (échantillon)"""
        # Prendre 3 paragraphes aléatoires
        sample_paragraphs = parsed_xml_data['paragraphs'][:3]
        
        for para in sample_paragraphs:
            para_id = para.get('para_id')
            expected_text = para.get('texte', '')
            
            if not para_id or not expected_text:
                continue
            
            # Rechercher dans ES
            response = es_client.search(index=index_name, query = {"term": {"para_id": para_id}} , size=1)
            
            if response['hits']['total']['value'] == 0:
                pytest.fail(f"Paragraphe {para_id} non trouvé dans ES")
            
            actual_text = response['hits']['hits'][0]['_source'].get('texte', '')
            
            # Normaliser pour comparaison (espaces, etc.)
            expected_normalized = re.sub(r'\s+', ' ', expected_text).strip()
            actual_normalized = re.sub(r'\s+', ' ', actual_text).strip()
            
            assert expected_normalized == actual_normalized, \
                f"Texte différent pour para_id={para_id}\nAttendu: {expected_normalized[:100]}...\nObtenu: {actual_normalized[:100]}..."


# ============================================================================
# TESTS DE QUALITÉ DES DONNÉES
# ============================================================================

class TestDataQuality:
    """Tests de qualité des données indexées"""
    
    def test_no_empty_texts(self, es_client, index_name):
        """Vérifie qu'il n'y a pas de textes vides"""
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"texte.keyword": ""}},
                        {"bool": {"must_not": {"exists": {"field": "texte"}}}}
                    ]
                }
            }
        }
        
        response = es_client.count(index=index_name, query= query['query'])
        assert response['count'] == 0, \
            f"Il y a {response['count']} documents avec texte vide"
    
    def test_dates_are_valid(self, es_client, index_name):
        """Vérifie que toutes les dates sont valides"""
        response = es_client.search(index=index_name, query = {"match_all": {}}, size = 100, _source = ["date_seance"] )
        
        for hit in response['hits']['hits']:
            date_str = hit['_source'].get('date_seance')
            if date_str:
                try:
                    datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError:
                    pytest.fail(f"Date invalide trouvée: {date_str}")
    
    def test_orateurs_have_names(self, es_client, index_name):
        """Vérifie que les documents avec orateur ont un nom"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "orateur_nom"}},
                        {"term": {"orateur_nom.keyword": ""}}
                    ]
                }
            }
        }
        
        response = es_client.count(index=index_name, 
                                   query = query['query'])
        assert response['count'] == 0, \
            f"Il y a {response['count']} documents avec orateur_nom vide"
    
    def test_text_minimum_length(self, es_client, index_name):
        """Vérifie que les textes ont une longueur minimale raisonnable"""
        query = {
            "query": {"match_all": {}},
            "size": 100,
            "_source": ["texte"]
        }
        
        response = es_client.search(index=index_name, query={"match_all": {}}, size=100, _source=["texte"])
        
        short_texts = 0
        for hit in response['hits']['hits']:
            texte = hit['_source'].get('texte', '')
            if len(texte) < 10:
                short_texts += 1
        
        # Maximum 5% de textes trop courts
        max_short = int(len(response['hits']['hits']) * 0.05)
        assert short_texts <= max_short, \
            f"Trop de textes courts: {short_texts}/{len(response['hits']['hits'])}"


# ============================================================================
# TESTS DE STRUCTURE
# ============================================================================

class TestIndexStructure:
    """Tests de la structure de l'index Elasticsearch"""
    
    def test_required_fields_exist(self, es_client, index_name):
        """Vérifie que les champs requis existent dans le mapping"""
        required_fields = [
            'date_seance', 'texte', 'section_id', 'para_id',
            'legislature', 'publication_numero', 'orateur_nom'
        ]
        
        mapping = es_client.indices.get_mapping(index=index_name)
        properties = mapping[index_name]['mappings']['properties']
        
        for field in required_fields:
            assert field in properties, f"Champ requis '{field}' manquant dans le mapping"
    
    def test_text_field_has_french_analyzer(self, es_client, index_name):
        """Vérifie que le champ texte utilise l'analyseur français"""
        mapping = es_client.indices.get_mapping(index=index_name)
        text_field = mapping[index_name]['mappings']['properties']['texte']
        
        assert 'analyzer' in text_field, "Le champ 'texte' n'a pas d'analyseur défini"
        assert text_field['analyzer'] == 'french', \
            f"L'analyseur devrait être 'french', pas '{text_field['analyzer']}'"
    
    def test_sample_documents_have_all_core_fields(self, es_client, index_name):
        """Vérifie qu'un échantillon de documents a tous les champs essentiels"""
        query = {
            "query": {"match_all": {}},
            "size": 20
        }
        
        response = es_client.search(index=index_name, query = {"match_all": {}}, size=20)
        
        core_fields = ['date_seance', 'texte', 'para_id']
        
        for hit in response['hits']['hits']:
            doc = hit['_source']
            for field in core_fields:
                assert field in doc, \
                    f"Champ essentiel '{field}' manquant dans le document {hit['_id']}"


# ============================================================================
# TESTS DE PERFORMANCE ET STATS
# ============================================================================

class TestStatistics:
    """Tests statistiques sur les données"""
    
    def test_reasonable_number_of_documents(self, es_client, index_name):
        """Vérifie qu'il y a un nombre raisonnable de documents"""
        count = es_client.count(index=index_name)
        total = count['count']
        
        # Un fichier TAZ devrait contenir au moins 100 interventions
        assert total >= 100, \
            f"Trop peu de documents: {total}. Vérifier l'indexation."
    
    def test_orateurs_distribution(self, es_client, index_name):
        """Vérifie qu'il y a plusieurs orateurs différents"""
        query = {
            "aggs": {
                "unique_orateurs": {
                    "cardinality": {
                        "field": "orateur_nom.keyword"
                    }
                }
            },
            "size": 0
        }
        
        response = es_client.search(index=index_name, query = {"match_all": {}}, size=0, aggs = query['aggs'])
        unique_orateurs = response['aggregations']['unique_orateurs']['value']
        
        # Au moins 10 orateurs différents dans une séance
        assert unique_orateurs >= 10, \
            f"Trop peu d'orateurs uniques: {unique_orateurs}"
    
    def test_text_length_distribution(self, es_client, index_name):
        """Vérifie que la distribution des longueurs de texte est raisonnable"""

        response = es_client.search(index=index_name, query ={"match_all": {}}, size=100, _source=["texte"])
        
        lengths = [len(hit['_source'].get('texte', '')) for hit in response['hits']['hits']]
        avg_length = sum(lengths) / len(lengths)
        
        # Longueur moyenne devrait être entre 50 et 5000 caractères
        assert 50 <= avg_length <= 5000, \
            f"Longueur moyenne anormale: {avg_length:.0f} caractères"


# ============================================================================
# TEST SUITE PRINCIPAL
# ============================================================================

@pytest.mark.order(1)
class TestFullIntegration:
    """Test d'intégration complet TAZ -> Elasticsearch"""
    
    def test_full_taz_to_es_pipeline(self, es_client, index_name, parsed_xml_data):
        """Test d'intégration complet vérifiant toute la chaîne"""
        
        # 1. Vérifier les métadonnées
        metadata = parsed_xml_data['metadata']
        assert metadata.get('date_seance') is not None, "Date de séance manquante"
        assert metadata.get('legislature') is not None, "Législature manquante"
        
        # 2. Vérifier qu'on a des paragraphes
        paragraphs = parsed_xml_data['paragraphs']
        assert len(paragraphs) > 0, "Aucun paragraphe extrait du XML"
        
        # 3. Vérifier que ces paragraphes sont dans ES
        date_seance = metadata['date_seance']
        response = es_client.count(index=index_name, query = {"term": {"date_seance": date_seance}} )
        es_count = response['count']
        
        assert es_count > 0, \
            f"Aucun document trouvé dans ES pour la date {date_seance}"
        
        # 4. Vérifier la cohérence du nombre (avec tolérance)
        tolerance = max(1, int(len(paragraphs) * 0.1))  # 10% de tolérance
        assert abs(es_count - len(paragraphs)) <= tolerance, \
            f"Différence importante: XML={len(paragraphs)}, ES={es_count}"
        
        print(f"\n✅ Test d'intégration réussi:")
        print(f"   • XML: {len(paragraphs)} paragraphes")
        print(f"   • ES: {es_count} documents")
        print(f"   • Cohérence: {(es_count/len(paragraphs)*100):.1f}%")


# ============================================================================
# CONFIGURATION PYTEST
# ============================================================================

def pytest_configure(config):
    """Configuration des markers pytest"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )