import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Dictionnaire : année → nombre de fichiers à télécharger
from var import *

def creer_dossier_sortie(annee: int) -> str:
    """Crée le dossier pour une année donnée et retourne son chemin."""
    dossier_annee = os.path.join(DOSSIER_SORTIE, str(annee))
    os.makedirs(dossier_annee, exist_ok=True)
    return dossier_annee


def generer_url_fichier(annee: int, numero: int) -> str:
    """Construit l'URL du fichier à télécharger."""
    return f"https://echanges.dila.gouv.fr/OPENDATA/Debats/AN/{annee}/AN_{annee}{str(numero).zfill(3)}.taz"


def telecharger_fichier(url: str, chemin_fichier: str) -> tuple:
    """Télécharge un seul fichier depuis une URL."""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(chemin_fichier, 'wb') as f:
                f.write(response.content)
            return True, f"✅ {os.path.basename(chemin_fichier)} téléchargé avec succès."
        else:
            return False, f"❌ Erreur {response.status_code} pour {url}"
    except Exception as e:
        return False, f"⚠️ Erreur lors du téléchargement de {url} : {e}"


def telecharger_annee(annee: int, max_workers: int = 5):
    """Télécharge tous les fichiers d'une année donnée en parallèle."""
    if annee not in NB_FICHIERS_PAR_AN:
        print(f"⚠️ Aucun nombre de fichiers défini pour {annee}")
        return

    nb_fichiers = NB_FICHIERS_PAR_AN[annee]
    dossier_annee = creer_dossier_sortie(annee)

    print(f"\n📦 Téléchargement de {nb_fichiers} fichiers pour {annee}")
    
    # Préparer la liste des téléchargements nécessaires
    taches = []
    for i in range(1, nb_fichiers + 1):
        nom_fichier = f"AN_{annee}{str(i).zfill(3)}.taz"
        chemin_fichier = os.path.join(dossier_annee, nom_fichier)

        if os.path.exists(chemin_fichier):
            print(f"✅ Déjà présent : {nom_fichier}")
            continue

        url = generer_url_fichier(annee, i)
        taches.append((url, chemin_fichier))

    # Téléchargement parallèle
    if not taches:
        print(f"ℹ️ Tous les fichiers de {annee} sont déjà téléchargés.")
        return

    print(f"🚀 Lancement de {len(taches)} téléchargements en parallèle (max {max_workers} workers)...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(telecharger_fichier, url, chemin): (url, chemin)
            for url, chemin in taches
        }

        for future in as_completed(futures):
            url, chemin = futures[future]
            try:
                success, message = future.result()
                print(message)
            except Exception as e:
                print(f"⚠️ Erreur inattendue pour {url}: {e}")


def telecharger_plusieurs_annees(annees, max_workers: int = 5):
    """Télécharge les fichiers pour plusieurs années."""
    if isinstance(annees, int):
        annees = [annees]

    for annee in annees:
        telecharger_annee(annee, max_workers=max_workers)


if __name__ == "__main__":
    # 💡 Exemple 1 : une seule année
    # telecharger_plusieurs_annees(2022, max_workers=10)

    # 💡 Exemple 2 : plusieurs années avec 10 threads simultanés
    time_start = time.time()
    telecharger_plusieurs_annees([2022, 2023], max_workers=10)
    print(f"\n⏱️ Temps total de téléchargement : {time.time() - time_start:.2f} secondes.")