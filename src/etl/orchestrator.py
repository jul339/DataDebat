"""
Orchestrateur ETL pour les débats de l'Assemblée Nationale
Coordonne l'extraction, la transformation et le chargement des données
"""

import os
import sys
from pathlib import Path
from typing import List, Optional

from monitoring import ESMonitor

# Ajouter le répertoire src au path pour les imports
ROOT = Path(__file__).resolve().parents[1]  # pointe sur .../src
sys.path.insert(0, str(ROOT))

from db.es_connection import ESConnection
from etl.transform import ANDebatsTransformer
from etl.extract import telecharger_plusieurs_annees


class ETLOrchestrator:
    """Orchestrateur du pipeline ETL pour les débats de l'Assemblée Nationale"""

    def __init__(self, es_host: str = "http://localhost:9200"):
        """
        Initialise l'orchestrateur avec les composants ETL

        Args:
            es_host: URL du serveur Elasticsearch
        """
        self.es_conn = ESConnection(es_host)
        self.transformer = ANDebatsTransformer()
        self.raw_dir = "./data/raw"
        self.transformed_dir = "./data/transformed"

    def setup_index(self, recreate: bool = False):
        """
        Configure l'index Elasticsearch

        Args:
            recreate: Si True, supprime et recrée l'index
        """
        if recreate:
            self.es_conn.create_index()
        print(f"✓ Index '{self.es_conn.index_name}' prêt")

    # ========== EXTRACT ==========

    def extract(self, annees: List[int], max_workers: int = 5):
        """
        Télécharge les fichiers TAZ pour les années spécifiées

        Args:
            annees: Liste des années à télécharger
            max_workers: Nombre de workers pour le téléchargement parallèle
        """
        print(f"\n{'='*60}")
        print(f"📥 EXTRACT: Téléchargement des fichiers")
        print(f"{'='*60}")

        telecharger_plusieurs_annees(annees, max_workers=max_workers)

    # ========== TRANSFORM ==========

    def transform_file(
        self, taz_path: str, output_dir: Optional[str] = None
    ) -> List[dict]:
        """
        Transforme un fichier TAZ en documents structurés

        Args:
            taz_path: Chemin vers le fichier TAZ
            output_dir: Répertoire de sortie (optionnel)

        Returns:
            Liste des documents extraits
        """
        output = output_dir or self.transformed_dir
        return self.transformer.process_taz_file(taz_path, output)

    def transform_directory(
        self,
        directory: str,
        output_dir: Optional[str] = None,
        save_transform_file: bool = False,
    ) -> List[dict]:
        """
        Transforme tous les fichiers TAZ d'un répertoire

        Args:
            directory: Répertoire contenant les fichiers TAZ
            output_dir: Répertoire de sortie (optionnel)
            save_transform_file: Sauvegarder les informations de transformation
        Returns:
            Liste de tous les documents extraits
        """
        output = output_dir or self.transformed_dir
        return self.transformer.process_directory(
            directory, output, save_transform_file
        )

    def transform_year(
        self,
        year: int,
        output_dir: Optional[str] = None,
        save_transform_file: bool = True,
    ) -> List[dict]:
        """
        Transforme tous les fichiers TAZ d'une année

        Args:
            year: Année à traiter
            output_dir: Répertoire de sortie (optionnel)
            save_transform_file: Sauvegarder les informations de transformation
        Returns:
            Liste de tous les documents extraits
        """
        directory = os.path.join(self.raw_dir, str(year))
        return self.transform_directory(directory, output_dir, save_transform_file)

    # ========== LOAD ==========

    def load(
        self,
        documents: List[dict],
        batch_size: int = 500,
        replace_existing: bool = True,
    ):
        """
        Charge les documents dans Elasticsearch

        Args:
            documents: Liste des documents à indexer
            batch_size: Taille des lots pour l'indexation
        """
        if not documents:
            print("⚠ Aucun document à charger")
            return

        print(f"\n{'='*60}")
        print(f"📤 LOAD: Indexation de {len(documents)} documents")
        print(f"{'='*60}")

        self.es_conn.bulk_index(documents, batch_size, replace_existing)

    # ========== ETL COMPLET ==========

    def run_etl_file(
        self, taz_path: str, index_to_es: bool = True, replace_existing: bool = True
    ):
        """
        Exécute le pipeline ETL complet pour un fichier

        Args:
            taz_path: Chemin vers le fichier TAZ
            index_to_es: Si True, indexe dans Elasticsearch
        """
        print(f"\n{'='*60}")
        print(f"🔄 ETL: Pipeline complet pour {os.path.basename(taz_path)}")
        print(f"{'='*60}")

        # Transform
        documents = self.transform_file(taz_path)

        # Load
        if index_to_es and documents:
            self.load(documents, replace_existing=replace_existing)

        return documents

    def run_etl_year(self, year: int, download: bool = False, index_to_es: bool = True):
        """
        Exécute le pipeline ETL complet pour une année

        Args:
            year: Année à traiter
            download: Si True, télécharge d'abord les fichiers
            index_to_es: Si True, indexe dans Elasticsearch
        """
        print(f"\n{'='*60}")
        print(f"🔄 ETL: Pipeline complet pour l'année {year}")
        print(f"{'='*60}")

        # Extract (optionnel)
        if download:
            self.extract([year])

        # Transform
        documents = self.transform_year(year, save_transform_file=True)

        # Load
        if index_to_es and documents:
            self.load(documents)

        return documents

    def run_etl_years(
        self,
        years: List[int],
        download: bool = False,
        index_to_es: bool = True,
        save_transform_file: bool = True,
    ):
        """
        Exécute le pipeline ETL complet pour plusieurs années

        Args:
            years: Liste des années à traiter
            download: Si True, télécharge d'abord les fichiers
            index_to_es: Si True, indexe dans Elasticsearch
        """
        print(f"\n{'='*60}")
        print(f"🔄 ETL: Pipeline complet pour {len(years)} année(s)")
        print(f"{'='*60}")

        # Extract (optionnel)
        if download:
            self.extract(years)

        # Transform & Load
        all_documents = []
        for year in years:
            print(f"\n📅 Traitement de l'année {year}")
            documents = self.transform_year(year, save_transform_file=True)
            all_documents.extend(documents)

            if index_to_es and documents:
                self.load(documents)

        print(f"\n{'='*60}")
        print(f"✅ ETL terminé: {len(all_documents)} documents traités")
        print(f"{'='*60}")

        return all_documents

    def get_stats(self) -> dict:
        """
        Retourne les statistiques du pipeline

        Returns:
            Dictionnaire avec les statistiques
        """
        return {
            "index_name": self.es_conn.index_name,
            "document_count": self.es_conn.get_document_count(),
        }

    def print_stats(self):
        """Affiche les statistiques du pipeline"""
        stats = self.get_stats()
        print(f"\n📊 Statistiques:")
        print(f"   Index: {stats['index_name']}")
        print(f"   Documents indexés: {stats['document_count']}")


def main():
    """Fonction principale"""

    # Créer l'orchestrateur
    orchestrator = ETLOrchestrator()

    # Setup de l'index (recreate=True pour repartir de zéro)
    # orchestrator.setup_index(recreate=True)

    # Option 1: Traiter un seul fichier
    # orchestrator.run_etl_file("./data/raw/2022/AN_2022002.taz")

    # Option 2: Traiter une année complète
    orchestrator.run_etl_year(2019, download=False, index_to_es=False)

    # Option 3: Traiter plusieurs années avec téléchargement (ancien mode)
    # orchestrator.run_etl_years(
    #     [2018, 2019, 2020], download=False, index_to_es=True, save_transform_file=True
    # )

    # Option 4: Mode BATCH - traitement parallèle avec progression (recommandé)
    # orchestrator.run_batch(
    #     years=[2018, 2019, 2020, 2021, 2022, 2023],
    #     download=True,
    #     parallel=True,
    #     max_workers=10,
    #     skip_existing=True,
    #     index_to_es=True
    # )

    # Option 5: Juste transformer sans indexer
    # documents = orchestrator.transform_year(2022)
    monitor = ESMonitor(orchestrator.es_conn)
    monitor.print_status()
    # Afficher les stats
    orchestrator.print_stats()


if __name__ == "__main__":
    main()
