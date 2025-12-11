"""
Batch Loader pour fichiers TAZ de l'Assemblée Nationale
Charge massivement des dizaines/centaines de fichiers dans Elasticsearch
S'intègre avec ANDebatsTransformer et ESConnection
"""

import os
import sys
import time
import argparse
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Ajouter le répertoire src au path pour les imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.es_connection import ESConnection
from etl.transform import ANDebatsTransformer


class BatchLoader:
    """Chargeur de masse pour fichiers TAZ utilisant le transformer existant"""
    
    def __init__(self, es_conn: Optional[ESConnection] = None, 
                 transformer: Optional[ANDebatsTransformer] = None,
                 es_host: str = "http://localhost:9200",
                 max_workers: int = 3,
                 transformed_dir: str = "./data/transformed"):
        """
        Initialise le batch loader
        
        Args:
            es_conn: Instance ESConnection existante (optionnel)
            transformer: Instance ANDebatsTransformer existante (optionnel)
            es_host: URL Elasticsearch (utilisé si es_conn non fourni)
            max_workers: Nombre de workers parallèles (recommandé: 2-4)
            transformed_dir: Répertoire pour les fichiers JSON transformés
        """
        self.es_conn = es_conn or ESConnection(es_host)
        self.transformer = transformer or ANDebatsTransformer()
        self.max_workers = max_workers
        self.transformed_dir = transformed_dir
        self.stats = self._init_stats()
    
    def _init_stats(self) -> Dict:
        """Initialise les statistiques"""
        return {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'documents_indexed': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
    
    def find_taz_files(self, base_dir: str, pattern: str = "*.taz") -> List[Path]:
        """
        Trouve tous les fichiers TAZ dans l'arborescence
        
        Args:
            base_dir: Répertoire racine
            pattern: Pattern de recherche
            
        Returns:
            Liste des chemins vers les fichiers TAZ
        """
        base_path = Path(base_dir)
        taz_files = list(base_path.rglob(pattern))
        return sorted(taz_files)
    
    def get_year_from_path(self, taz_path: Path) -> str:
        """Extrait l'année depuis le chemin du fichier"""
        # Chercher un dossier année (4 chiffres)
        for part in taz_path.parts:
            if part.isdigit() and len(part) == 4:
                return part
        
        # Sinon, extraire du nom de fichier: AN_2022001.taz
        filename = taz_path.stem  # AN_2022001
        if len(filename) >= 9 and filename[3:7].isdigit():
            return filename[3:7]
        
        return "unknown"
    
    def organize_files_by_year(self, taz_files: List[Path]) -> Dict[str, List[Path]]:
        """
        Organise les fichiers par année
        
        Args:
            taz_files: Liste des fichiers TAZ
            
        Returns:
            Dictionnaire {année: [liste de fichiers]}
        """
        by_year = {}
        for taz_file in taz_files:
            year = self.get_year_from_path(taz_file)
            if year not in by_year:
                by_year[year] = []
            by_year[year].append(taz_file)
        
        return by_year
    
    def check_if_already_indexed(self, taz_file: Path) -> bool:
        """
        Vérifie si un fichier a déjà été indexé via son para_id
        
        Args:
            taz_file: Fichier TAZ à vérifier
            
        Returns:
            True si déjà indexé, False sinon
        """
        # Extraire l'année et le numéro du nom de fichier
        filename = taz_file.stem  # AN_2022001
        
        # Pattern: AN_AAAANNNN
        if len(filename) < 11:
            return False
        
        try:
            year = int(filename[3:7])
            num = int(filename[7:10])
            
            # Chercher dans ES si des documents de cette année et numéro existent
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"annee": year}},
                            {"term": {"publication_numero": num}}
                        ]
                    }
                },
                "size": 0
            }
            
            response = self.es_conn.es.count(
                index=self.es_conn.index_name, 
                body=query
            )
            return response['count'] > 0
            
        except (ValueError, IndexError):
            return False
        except Exception:
            # Si ES n'est pas accessible, on ne skip pas
            return False
    
    def process_single_file(self, taz_file: Path, skip_existing: bool = True,
                            index_to_es: bool = True) -> Dict:
        """
        Traite un seul fichier TAZ
        
        Args:
            taz_file: Chemin vers le fichier TAZ
            skip_existing: Si True, skip les fichiers déjà indexés
            index_to_es: Si True, indexe dans Elasticsearch
            
        Returns:
            Dictionnaire avec le résultat du traitement
        """
        result = {
            'file': str(taz_file),
            'status': 'pending',
            'documents': 0,
            'error': None,
            'duration': 0
        }
        
        start_time = time.time()
        
        try:
            # Vérifier si déjà indexé
            if skip_existing and self.check_if_already_indexed(taz_file):
                result['status'] = 'skipped'
                result['documents'] = 0
                return result
            
            # Transformer le fichier
            documents = self.transformer.process_taz_file(
                str(taz_file), 
                self.transformed_dir
            )
            
            if not documents:
                result['status'] = 'failed'
                result['error'] = 'Aucun document extrait'
                return result
            
            # Indexer dans ES si demandé
            if index_to_es:
                self.es_conn.bulk_index(documents, replace_existing=False)
            
            result['status'] = 'success'
            result['documents'] = len(documents)
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
        
        finally:
            result['duration'] = time.time() - start_time
        
        return result
    
    def process_files_sequential(self, taz_files: List[Path], skip_existing: bool = True,
                                  index_to_es: bool = True):
        """
        Traite les fichiers séquentiellement avec barre de progression
        
        Args:
            taz_files: Liste des fichiers à traiter
            skip_existing: Skip les fichiers déjà indexés
            index_to_es: Indexer dans Elasticsearch
        """
        print(f"\n🔄 Traitement séquentiel de {len(taz_files)} fichiers")
        print("="*80)
        
        with tqdm(total=len(taz_files), desc="Progression", unit="fichier") as pbar:
            for taz_file in taz_files:
                result = self.process_single_file(taz_file, skip_existing, index_to_es)
                self._update_stats(result)
                
                # Mettre à jour la barre
                pbar.set_postfix({
                    'Succès': self.stats['success'],
                    'Échecs': self.stats['failed'],
                    'Skippés': self.stats['skipped']
                })
                pbar.update(1)
    
    def process_files_parallel(self, taz_files: List[Path], skip_existing: bool = True,
                                index_to_es: bool = True):
        """
        Traite les fichiers en parallèle avec barre de progression
        
        Args:
            taz_files: Liste des fichiers à traiter
            skip_existing: Skip les fichiers déjà indexés
            index_to_es: Indexer dans Elasticsearch
        """
        print(f"\n🚀 Traitement parallèle de {len(taz_files)} fichiers")
        print(f"   Workers: {self.max_workers}")
        print("="*80)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Soumettre tous les jobs
            futures = {
                executor.submit(
                    self.process_single_file, 
                    taz_file, 
                    skip_existing,
                    index_to_es
                ): taz_file
                for taz_file in taz_files
            }
            
            # Barre de progression
            with tqdm(total=len(taz_files), desc="Progression", unit="fichier") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    self._update_stats(result)
                    
                    # Mettre à jour la barre
                    pbar.set_postfix({
                        'Succès': self.stats['success'],
                        'Échecs': self.stats['failed'],
                        'Skippés': self.stats['skipped']
                    })
                    pbar.update(1)
    
    def _update_stats(self, result: Dict):
        """Met à jour les statistiques avec le résultat d'un fichier"""
        self.stats['total'] += 1
        
        if result['status'] == 'success':
            self.stats['success'] += 1
            self.stats['documents_indexed'] += result.get('documents', 0)
        elif result['status'] == 'failed':
            self.stats['failed'] += 1
            self.stats['errors'].append({
                'file': result['file'],
                'error': result['error']
            })
        elif result['status'] == 'skipped':
            self.stats['skipped'] += 1
    
    def run(self, base_dir: str, parallel: bool = False, skip_existing: bool = True,
            years: List[str] = None, index_to_es: bool = True) -> Dict:
        """
        Lance le chargement de masse
        
        Args:
            base_dir: Répertoire racine contenant les fichiers TAZ
            parallel: Si True, utilise le traitement parallèle
            skip_existing: Si True, skip les fichiers déjà indexés
            years: Liste des années à traiter (None = toutes)
            index_to_es: Si True, indexe dans Elasticsearch
            
        Returns:
            Statistiques du traitement
        """
        # Reset stats
        self.stats = self._init_stats()
        self.stats['start_time'] = datetime.now()
        
        print(f"\n{'='*80}")
        print(f"🚀 BATCH LOADER - Assemblée Nationale")
        print(f"{'='*80}")
        print(f"📁 Répertoire: {base_dir}")
        print(f"🔍 Recherche des fichiers TAZ...")
        
        # Trouver tous les fichiers
        taz_files = self.find_taz_files(base_dir)
        
        if not taz_files:
            print(f"❌ Aucun fichier TAZ trouvé dans {base_dir}")
            return self.stats
        
        print(f"✅ {len(taz_files)} fichiers TAZ trouvés")
        
        # Organiser par année
        by_year = self.organize_files_by_year(taz_files)
        
        print(f"\n📊 Répartition par année:")
        for year in sorted(by_year.keys()):
            print(f"   • {year}: {len(by_year[year])} fichiers")
        
        # Filtrer par années si spécifié
        if years:
            years_str = [str(y) for y in years]
            taz_files = [
                f for year in years_str 
                for f in by_year.get(year, [])
            ]
            print(f"\n🎯 Filtrage: {len(taz_files)} fichiers pour les années {years}")
        
        if not taz_files:
            print("❌ Aucun fichier à traiter après filtrage")
            return self.stats
        
        # Traiter les fichiers
        if parallel:
            self.process_files_parallel(taz_files, skip_existing, index_to_es)
        else:
            self.process_files_sequential(taz_files, skip_existing, index_to_es)
        
        self.stats['end_time'] = datetime.now()
        
        # Afficher le résumé
        self.print_summary()
        
        # Sauvegarder le rapport
        self.save_report()
        
        return self.stats
    
    def print_summary(self):
        """Affiche un résumé du traitement"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print(f"\n{'='*80}")
        print(f"📊 RÉSUMÉ DU TRAITEMENT")
        print(f"{'='*80}")
        print(f"⏱️  Durée totale: {duration:.2f} secondes ({duration/60:.1f} minutes)")
        print(f"📁 Total de fichiers: {self.stats['total']}")
        print(f"✅ Succès: {self.stats['success']} ({self.stats['success']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"⏭️  Skippés: {self.stats['skipped']} ({self.stats['skipped']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"❌ Échecs: {self.stats['failed']} ({self.stats['failed']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"📄 Documents indexés: {self.stats['documents_indexed']}")
        
        if self.stats['success'] > 0:
            avg_time = duration / self.stats['success']
            print(f"⚡ Temps moyen par fichier: {avg_time:.2f} secondes")
        
        if self.stats['errors']:
            print(f"\n❌ Erreurs rencontrées ({len(self.stats['errors'])}):")
            for i, error in enumerate(self.stats['errors'][:5], 1):
                print(f"   {i}. {Path(error['file']).name}: {error['error'][:60]}...")
            
            if len(self.stats['errors']) > 5:
                print(f"   ... et {len(self.stats['errors']) - 5} autres erreurs")
        
        print(f"{'='*80}\n")
    
    def save_report(self, output_file: str = "batch_load_report.json"):
        """
        Sauvegarde un rapport JSON du traitement
        
        Args:
            output_file: Nom du fichier de rapport
        """
        report = {
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': self.stats['end_time'].isoformat(),
            'duration_seconds': (self.stats['end_time'] - self.stats['start_time']).total_seconds(),
            'statistics': {
                'total': self.stats['total'],
                'success': self.stats['success'],
                'failed': self.stats['failed'],
                'skipped': self.stats['skipped'],
                'documents_indexed': self.stats['documents_indexed']
            },
            'errors': self.stats['errors']
        }
        existing_runs = []
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and 'runs' in data:
                        existing_runs = data['runs']
                    elif isinstance(data, dict):
                        existing_runs = [data]
                    elif isinstance(data, list):
                        existing_runs = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"⚠ Avertissement: Impossible de lire le fichier existant {output_file}: {e}")
        
        existing_runs.append(report)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({'runs': existing_runs}, f, indent=2, ensure_ascii=False)
        
        
        print(f"💾 Rapport sauvegardé: {output_file}")


def main():
    """Fonction principale avec arguments CLI"""
    parser = argparse.ArgumentParser(
        description="Batch Loader pour fichiers TAZ de l'Assemblée Nationale",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  # Charger tous les fichiers de data/raw/
  python load_batch.py data/raw/
  
  # Charger en parallèle avec 4 workers
  python load_batch.py data/raw/ --parallel --workers 4
  
  # Charger seulement 2022 et 2023
  python load_batch.py data/raw/ --years 2022 2023
  
  # Forcer le rechargement (sans skip)
  python load_batch.py data/raw/ --no-skip
  
  # Mode dry-run (simulation)
  python load_batch.py data/raw/ --dry-run
  
  # Transformer seulement (pas d'indexation ES)
  python load_batch.py data/raw/ --no-index
        """
    )
    
    parser.add_argument(
        'base_dir',
        type=str,
        help='Répertoire racine contenant les fichiers TAZ'
    )
    
    parser.add_argument(
        '--es-host',
        type=str,
        default='http://localhost:9200',
        help='URL du serveur Elasticsearch (défaut: http://localhost:9200)'
    )
    
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Activer le traitement parallèle'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=3,
        help='Nombre de workers pour le traitement parallèle (défaut: 3)'
    )
    
    parser.add_argument(
        '--no-skip',
        action='store_true',
        help='Ne pas skipper les fichiers déjà indexés'
    )
    
    parser.add_argument(
        '--years',
        nargs='+',
        type=str,
        help='Liste des années à traiter (ex: --years 2022 2023)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Mode simulation: liste les fichiers sans les traiter'
    )
    
    parser.add_argument(
        '--create-index',
        action='store_true',
        help='Créer/recréer l\'index Elasticsearch avant le chargement'
    )
    
    parser.add_argument(
        '--no-index',
        action='store_true',
        help='Ne pas indexer dans Elasticsearch (transformation seulement)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./data/transformed',
        help='Répertoire de sortie pour les fichiers JSON transformés'
    )
    
    args = parser.parse_args()
    
    # Vérifier que le répertoire existe
    if not os.path.exists(args.base_dir):
        print(f"❌ Erreur: Le répertoire '{args.base_dir}' n'existe pas")
        sys.exit(1)
    
    # Mode dry-run
    if args.dry_run:
        print(f"\n🔍 MODE DRY-RUN: Simulation du chargement\n")
        loader = BatchLoader(
            es_host=args.es_host, 
            max_workers=args.workers,
            transformed_dir=args.output_dir
        )
        taz_files = loader.find_taz_files(args.base_dir)
        by_year = loader.organize_files_by_year(taz_files)
        
        print(f"📁 Répertoire: {args.base_dir}")
        print(f"📊 {len(taz_files)} fichiers TAZ trouvés\n")
        
        for year in sorted(by_year.keys()):
            files = by_year[year]
            print(f"📅 Année {year}: {len(files)} fichiers")
            for f in files[:3]:
                print(f"   • {f.name}")
            if len(files) > 3:
                print(f"   ... et {len(files) - 3} autres fichiers")
            print()
        
        return
    
    # Créer la connexion ES
    es_conn = ESConnection(args.es_host)
    
    # Créer l'index si demandé
    if args.create_index:
        print("\n🔨 Création de l'index Elasticsearch...")
        es_conn.create_index()
    
    # Lancer le batch loader
    loader = BatchLoader(
        es_conn=es_conn,
        max_workers=args.workers,
        transformed_dir=args.output_dir
    )
    
    loader.run(
        base_dir=args.base_dir,
        parallel=args.parallel,
        skip_existing=not args.no_skip,
        years=args.years,
        index_to_es=not args.no_index
    )


if __name__ == "__main__":
    main()
