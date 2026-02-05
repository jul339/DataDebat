"""
Script de comparaison des idsyceron entre fichiers bruts et transformés
Compare les identifiants idsyceron des Para du XML brut avec les para_id du JSON transformé
"""

import sys
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import Counter

# Ajouter le répertoire src au path pour importer transform
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl.transform import ANDebatsTransformer


def extraire_idsyceron_root(root: ET.Element):
    """
    Extrait tous les idsyceron du XML (élément racine).

    Args:
        root: Élément racine du XML (ET.Element)

    Returns:
        Liste des idsyceron trouvés
    """
    idsyceron_list = []
    for element in root.iter():
        if "idsyceron" in element.attrib:
            idsyceron = element.attrib["idsyceron"]
            idsyceron_list.append(
                {
                    "id": idsyceron,
                    "tag": element.tag,
                    "ident": element.attrib.get("Ident", ""),
                }
            )
    return idsyceron_list


def extraire_idsyceron_xml(xml_path):
    """
    Extrait tous les idsyceron des éléments du fichier XML brut.

    Args:
        xml_path: Chemin vers le fichier XML

    Returns:
        Liste des idsyceron trouvés
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
    return extraire_idsyceron_root(root)


def extraire_idsyceron_depuis_taz(taz_path):
    """
    Extrait tous les idsyceron depuis un fichier TAZ (XML extrait du .taz).

    Args:
        taz_path: Chemin vers le fichier .taz

    Returns:
        Liste des idsyceron trouvés, ou liste vide si erreur
    """
    transformer = ANDebatsTransformer()
    root, _ = transformer.extract_xml_from_taz(str(taz_path))
    if root is None:
        return []
    return extraire_idsyceron_root(root)


def extraire_para_id_json(json_path):
    """
    Extrait tous les para_id du fichier JSON transformé

    Args:
        json_path: Chemin vers le fichier JSON

    Returns:
        Liste des para_id trouvés
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    para_id_list = []

    for item in data:
        if "para_id" in item:
            para_id_list.append(
                {
                    "id": item["para_id"],
                    "orateur": item.get("orateur_nom", ""),
                    "fonction": item.get("fonction", ""),
                }
            )

    return para_id_list


def comparer_ids(xml_ids, json_ids):
    """
    Compare les deux listes d'identifiants

    Args:
        xml_ids: Liste des idsyceron du XML
        json_ids: Liste des para_id du JSON

    Returns:
        Dictionnaire avec les résultats de la comparaison
    """
    # Extraire juste les IDs
    xml_id_set = set(item["id"] for item in xml_ids)
    json_id_set = set(item["id"] for item in json_ids)

    # Calculer les différences
    manquants_json = xml_id_set - json_id_set  # IDs dans XML mais pas dans JSON
    en_trop_json = json_id_set - xml_id_set  # IDs dans JSON mais pas dans XML
    communs = xml_id_set & json_id_set  # IDs présents dans les deux

    # Compter les occurrences (pour détecter les doublons)
    xml_counts = Counter(item["id"] for item in xml_ids)
    json_counts = Counter(item["id"] for item in json_ids)

    # Trouver les doublons
    doublons_xml = {id: count for id, count in xml_counts.items() if count > 1}
    doublons_json = {id: count for id, count in json_counts.items() if count > 1}

    return {
        "total_xml": len(xml_ids),
        "total_json": len(json_ids),
        "total_unique_xml": len(xml_id_set),
        "total_unique_json": len(json_id_set),
        "communs": len(communs),
        "manquants_json": sorted(manquants_json),
        "en_trop_json": sorted(en_trop_json),
        "doublons_xml": doublons_xml,
        "doublons_json": doublons_json,
    }


def afficher_comparaison(comparaison, xml_ids, json_ids):
    """
    Affiche les résultats de la comparaison de manière lisible

    Args:
        comparaison: Résultats de la comparaison
        xml_ids: Liste des idsyceron du XML
        json_ids: Liste des para_id du JSON
    """
    print("\n" + "=" * 80)
    print("COMPARAISON DES IDSYCERON")
    print("=" * 80)

    print(f"\n📊 STATISTIQUES GLOBALES:")
    print(f"   Total d'éléments dans XML (brut)    : {comparaison['total_xml']}")
    print(f"   Total unique dans XML               : {comparaison['total_unique_xml']}")
    print(f"   Total d'éléments dans JSON (trans.) : {comparaison['total_json']}")
    print(
        f"   Total unique dans JSON              : {comparaison['total_unique_json']}"
    )
    print(f"   Identifiants communs                : {comparaison['communs']}")

    # Doublons
    if comparaison["doublons_xml"]:
        print(f"\n⚠️  DOUBLONS dans le XML ({len(comparaison['doublons_xml'])}):")
        for id, count in sorted(comparaison["doublons_xml"].items()):
            print(f"   {id}: {count} occurrences")

    if comparaison["doublons_json"]:
        print(f"\n⚠️  DOUBLONS dans le JSON ({len(comparaison['doublons_json'])}):")
        for id, count in sorted(comparaison["doublons_json"].items()):
            print(f"   {id}: {count} occurrences")

    # Manquants dans JSON
    if comparaison["manquants_json"]:
        print(
            f"\n❌ IDs MANQUANTS dans le JSON ({len(comparaison['manquants_json'])}):"
        )
        print(f"   (présents dans XML mais absents du JSON transformé)")

        # Créer un dictionnaire pour retrouver les infos
        xml_dict = {item["id"]: item for item in xml_ids}

        for id in comparaison["manquants_json"][:20]:  # Limiter à 20 pour l'affichage
            info = xml_dict.get(id, {})
            print(f"   • {id} ({info.get('tag', 'N/A')})")

        if len(comparaison["manquants_json"]) > 20:
            print(f"   ... et {len(comparaison['manquants_json']) - 20} autres")
    else:
        print(f"\n✅ Aucun ID manquant dans le JSON")

    # En trop dans JSON
    if comparaison["en_trop_json"]:
        print(f"\n⚠️  IDs EN TROP dans le JSON ({len(comparaison['en_trop_json'])}):")
        print(f"   (présents dans JSON mais absents du XML brut)")

        # Créer un dictionnaire pour retrouver les infos
        json_dict = {item["id"]: item for item in json_ids}

        for id in comparaison["en_trop_json"][:20]:  # Limiter à 20
            info = json_dict.get(id, {})
            print(f"   • {id} ({info.get('orateur', 'N/A')})")

        if len(comparaison["en_trop_json"]) > 20:
            print(f"   ... et {len(comparaison['en_trop_json']) - 20} autres")
    else:
        print(f"\n✅ Aucun ID en trop dans le JSON")

    # Taux de correspondance
    if comparaison["total_unique_xml"] > 0:
        taux = (comparaison["communs"] / comparaison["total_unique_xml"]) * 100
        print(f"\n📈 TAUX DE CORRESPONDANCE: {taux:.1f}%")

        if taux == 100.0:
            print("   🎉 Parfait ! Tous les IDs du XML sont présents dans le JSON")
        elif taux >= 95.0:
            print("   ✅ Très bon taux de correspondance")
        elif taux >= 80.0:
            print("   ⚠️  Taux de correspondance correct mais des IDs manquent")
        else:
            print("   ❌ Taux de correspondance faible, vérifier la transformation")


if __name__ == "__main__":
    # Comparaison TAZ ↔ JSON transformé
    base = Path(__file__).parent.parent
    taz_path = base / "data" / "raw" / "2018" / "AN_2018001.taz"
    json_path = base / "data" / "transformed" / "2018" / "2018-01-16.json"

    print(f"Extraction des idsyceron depuis le TAZ: {taz_path.name}")
    print(f"Extraction des para_id depuis le JSON: {json_path.name}")

    # Extraire les IDs depuis le TAZ (XML contenu dans le .taz)
    xml_ids = extraire_idsyceron_depuis_taz(taz_path)
    if not xml_ids:
        print("⚠ Impossible d'extraire le XML du TAZ ou aucun idsyceron trouvé. Vérifiez le chemin du TAZ.")
        raise SystemExit(1)
    json_ids = extraire_para_id_json(json_path)

    # Comparer
    comparaison = comparer_ids(xml_ids, json_ids)

    # Afficher les résultats
    afficher_comparaison(comparaison, xml_ids, json_ids)

    # Sauvegarder le rapport détaillé
    rapport = {
        "fichiers": {"taz": str(taz_path), "json": str(json_path)},
        "statistiques": {
            "total_xml": comparaison["total_xml"],
            "total_json": comparaison["total_json"],
            "total_unique_xml": comparaison["total_unique_xml"],
            "total_unique_json": comparaison["total_unique_json"],
            "communs": comparaison["communs"],
            "taux_correspondance": (
                (comparaison["communs"] / comparaison["total_unique_xml"] * 100)
                if comparaison["total_unique_xml"] > 0
                else 0
            ),
        },
        "differences": {
            "manquants_json": comparaison["manquants_json"],
            "en_trop_json": comparaison["en_trop_json"],
        },
        "doublons": {
            "xml": comparaison["doublons_xml"],
            "json": comparaison["doublons_json"],
        },
    }

    output_path = Path(__file__).parent / "comparaison_idsyceron.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Rapport détaillé sauvegardé dans: {output_path.name}")
    print("=" * 80)
