"""
Script pour identifier et afficher les données dupliquées dans le fichier JSON transformé
"""

import json
from pathlib import Path
from collections import defaultdict


def analyser_doublons(json_path):
    """
    Analyse et affiche les doublons dans le fichier JSON

    Args:
        json_path: Chemin vers le fichier JSON
    """
    # Charger les données
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Grouper par para_id
    para_id_groups = defaultdict(list)

    for idx, item in enumerate(data):
        para_id = item.get("para_id")
        if para_id:
            para_id_groups[para_id].append({"index": idx, "item": item})

    # Trouver les doublons
    doublons = {
        pid: entries for pid, entries in para_id_groups.items() if len(entries) > 1
    }

    # Statistiques
    total_items = len(data)
    total_unique = len(para_id_groups)
    total_doublons = len(doublons)
    total_duplicated_entries = sum(len(entries) for entries in doublons.values())

    print("=" * 80)
    print("ANALYSE DES DOUBLONS - FICHIER TRANSFORMÉ")
    print("=" * 80)

    print(f"\n📊 STATISTIQUES:")
    print(f"   Total d'entrées dans le fichier : {total_items}")
    print(f"   Nombre de para_id uniques       : {total_unique}")
    print(f"   Nombre de para_id dupliqués     : {total_doublons}")
    print(f"   Total d'entrées dupliquées      : {total_duplicated_entries}")

    if doublons:
        print(f"\n⚠️  {total_doublons} PARA_ID ONT DES DOUBLONS:")
        print(f"   (affichage limité aux 20 premiers)\n")

        # Trier par nombre de doublons (décroissant)
        sorted_doublons = sorted(
            doublons.items(), key=lambda x: len(x[1]), reverse=True
        )

        for i, (para_id, entries) in enumerate(sorted_doublons[:20], 1):
            print(f"\n{'-'*80}")
            print(f"#{i} - para_id: {para_id} ({len(entries)} occurrences)")
            print(f"{'-'*80}")

            for j, entry in enumerate(entries, 1):
                item = entry["item"]
                index = entry["index"]

                print(f"\n   Occurrence {j} (index {index}):")
                print(f"   • Orateur    : {item.get('orateur_nom', 'N/A')}")
                print(f"   • Fonction   : {item.get('fonction', 'N/A')}")
                print(f"   • Section    : {item.get('section_titre', 'N/A')}")
                print(f"   • Sous-sect. : {item.get('sous_section_titre', 'N/A')}")
                print(f"   • Texte      : {item.get('texte', 'N/A')[:100]}...")

                # Vérifier si les contenus sont identiques
                if j > 1:
                    prev_item = entries[j - 2]["item"]
                    if item == prev_item:
                        print(f"   ⚠️  IDENTIQUE à l'occurrence précédente")
                    else:
                        differences = []
                        for key in item.keys():
                            if item.get(key) != prev_item.get(key):
                                differences.append(key)
                        if differences:
                            print(
                                f"   ⚠️  DIFFÉRENCES avec l'occurrence précédente: {', '.join(differences)}"
                            )

        if len(sorted_doublons) > 20:
            print(f"\n... et {len(sorted_doublons) - 20} autres para_id avec doublons")

        # Analyse des types de doublons
        print(f"\n\n{'='*80}")
        print("ANALYSE DES TYPES DE DOUBLONS")
        print(f"{'='*80}")

        doublons_identiques = 0
        doublons_differents = 0

        for para_id, entries in doublons.items():
            # Comparer toutes les occurrences
            items = [e["item"] for e in entries]
            if all(item == items[0] for item in items):
                doublons_identiques += 1
            else:
                doublons_differents += 1

        print(f"\n   Doublons PARFAITEMENT IDENTIQUES : {doublons_identiques}")
        print(f"   Doublons avec DIFFÉRENCES         : {doublons_differents}")

    else:
        print(f"\n✅ Aucun doublon détecté!")

    print(f"\n{'='*80}\n")

    return doublons


def sauvegarder_doublons(doublons, output_path):
    """
    Sauvegarde les doublons dans un fichier JSON

    Args:
        doublons: Dictionnaire des doublons
        output_path: Chemin de sortie
    """
    # Convertir pour la sérialisation JSON
    doublons_json = {}
    for para_id, entries in doublons.items():
        doublons_json[para_id] = [
            {"index": e["index"], "data": e["item"]} for e in entries
        ]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(doublons_json, f, indent=2, ensure_ascii=False)

    print(f"💾 Doublons sauvegardés dans: {output_path.name}")


if __name__ == "__main__":
    # Chemin du fichier JSON transformé
    json_path = (
        Path(__file__).parent.parent
        / "data"
        / "transformed"
        / "2018"
        / "2018-04-12.json"
    )

    print(f"\nAnalyse du fichier: {json_path.name}\n")

    # Analyser les doublons
    doublons = analyser_doublons(json_path)

    # Sauvegarder le rapport
    if doublons:
        output_path = Path(__file__).parent / "doublons_rapport.json"
        sauvegarder_doublons(doublons, output_path)
