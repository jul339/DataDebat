"""
Test rapide : compter les interventions dans un XML et les comparer aux stats Elasticsearch
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from load2 import ANDebatsExtractor


def count_interventions_in_xml(xml_path: str) -> int:
    """
    Compte le nombre d'éléments Para (interventions) dans un fichier XML
    
    Args:
        xml_path: Chemin vers le fichier XML
        
    Returns:
        Nombre total de Para trouvés
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # Compter tous les Para (interventions) dans le document
    para_count = len(root.findall('.//Para'))
    
    return para_count


def test_intervention_count(taz_path: str):
    """
    Compare le nombre d'interventions dans le XML avec les stats Elasticsearch
    
    Args:
        taz_path: Chemin vers le fichier TAZ
    """
    print(f"\n{'='*70}")
    print(f"TEST : Comparaison du nombre d'interventions")
    print(f"{'='*70}\n")
    
    extractor = ANDebatsExtractor()
    
    # Extraire le XML depuis le TAZ
    print(f"📂 Extraction du XML depuis: {Path(taz_path).name}")
    root, xml_filename = extractor.extract_xml_from_taz(taz_path)
    
    if root is None:
        print("✗ Erreur lors de l'extraction du XML")
        return
    
    # Compter les Para directement
    xml_para_count = len(root.findall('.//Para'))
    print(f"✓ Nombre total de <Para> dans le XML: {xml_para_count}")
    
    # Extraire les métadonnées
    pub_metadata = extractor.extract_publication_metadata(root)
    compte_rendu_elem = root.find('.//CompteRendu')
    cr_metadata = extractor.extract_compte_rendu_metadata(compte_rendu_elem)
    
    contenu_elem = compte_rendu_elem.find('Contenu')
    quantiemes = extractor.extract_quantiemes(contenu_elem)
    president = extractor.extract_president_seance(contenu_elem)
    signature = extractor.extract_signature(contenu_elem)
    
    # Extraire tous les documents
    print(f"\n📊 Extraction des documents...")
    documents = extractor.extract_hierarchical_sections(
        root=root,
        pub_metadata=pub_metadata,
        cr_metadata=cr_metadata,
        quantiemes=quantiemes,
        president=president,
        signature=signature
    )
    
    # Compter les interventions extraites
    intervention_count = len([d for d in documents if d.get('type_document') == 'intervention'])
    
    print(f"✓ Nombre d'interventions extraites: {intervention_count}")
    
    # Comparaison
    print(f"\n{'='*70}")
    print(f"RÉSULTATS DE LA COMPARAISON")
    print(f"{'='*70}")
    print(f"<Para> dans le XML:        {xml_para_count}")
    print(f"Interventions extraites:   {intervention_count}")
    print(f"Différence:                {xml_para_count - intervention_count}")
    print(f"Ratio extraction:          {(intervention_count/xml_para_count*100):.1f}%")
    
    # Détails des documents extraits
    print(f"\n📋 Répartition des {len(documents)} documents extraits:")
    type_counts = {}
    for doc in documents:
        doc_type = doc.get('type_document', 'unknown')
        type_counts[doc_type] = type_counts.get(doc_type, 0) + 1
    
    for doc_type, count in sorted(type_counts.items()):
        print(f"  - {doc_type}: {count}")
    
    # Indexer dans ES
    print(f"\n🔄 Indexation dans Elasticsearch...")
    extractor.create_index()
    extractor.bulk_index(documents)
    
    # Vérifier les stats ES
    print(f"\n{'='*70}")
    print(f"VÉRIFICATION DES STATS ELASTICSEARCH")
    print(f"{'='*70}")
    extractor.print_index_stats()
    
    # Vérifier les interventions en ES
    es_interventions = extractor.es.count(
        index=extractor.index_name,
        body={"query": {"term": {"type_document": "intervention"}}}
    )['count']
    
    print(f"\n✅ Interventions indexées dans ES: {es_interventions}")
    
    if es_interventions == intervention_count:
        print(f"✅ SUCCÈS : Les nombres correspondent parfaitement!")
    else:
        print(f"⚠ ÉCART : {intervention_count - es_interventions} intervention(s) manquante(s)")


if __name__ == "__main__":
    # Utiliser le premier TAZ disponible
    taz_file = "./data/raw/2021/AN_2021001.taz"  # À adapter selon votre structure
    test_intervention_count(taz_file)