"""
Module de monitoring pour le pipeline ETL DataDebat
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict, field


@dataclass
class FileStats:
    """Statistiques pour un fichier traité"""
    filename: str
    status: str  # "success", "failed", "skipped"
    documents_count: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    year: Optional[int] = None


@dataclass  
class ImportReport:
    """Rapport d'import enrichi"""
    start_time: str = ""
    end_time: str = ""
    duration_seconds: float = 0.0
    
    # Statistiques globales
    total_files: int = 0
    success_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    total_documents: int = 0
    
    # Détails par fichier
    files_details: List[Dict] = field(default_factory=list)
    
    # Stats par année
    documents_by_year: Dict[str, int] = field(default_factory=dict)
    
    # État Elasticsearch
    es_health: Dict = field(default_factory=dict)
    
    def add_file_stats(self, stats: FileStats):
        """Ajoute les stats d'un fichier"""
        self.files_details.append(asdict(stats))
        
        if stats.status == "success":
            self.success_files += 1
            self.total_documents += stats.documents_count
            
            # Compter par année
            if stats.year:
                year_key = str(stats.year)
                self.documents_by_year[year_key] = \
                    self.documents_by_year.get(year_key, 0) + stats.documents_count
        elif stats.status == "failed":
            self.failed_files += 1
        else:
            self.skipped_files += 1
        
        self.total_files += 1
    
    def save(self, filepath: str = "import_report.json"):
        """Sauvegarde le rapport en JSON"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(asdict(self), f, indent=2, ensure_ascii=False)
        print(f"📊 Rapport sauvegardé: {filepath}")


class ESMonitor:
    """Moniteur de santé Elasticsearch"""
    
    def __init__(self, es_connection):
        """
        Args:
            es_connection: Instance de ESConnection
        """
        self.es = es_connection.es
        self.index_name = es_connection.index_name
    
    def get_cluster_health(self) -> Dict:
        """Récupère l'état du cluster"""
        try:
            health = self.es.cluster.health()
            return {
                "status": health["status"],  # green, yellow, red
                "number_of_nodes": health["number_of_nodes"],
                "active_shards": health["active_shards"],
                "relocating_shards": health["relocating_shards"],
                "unassigned_shards": health["unassigned_shards"]
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def get_index_stats(self) -> Dict:
        """Récupère les statistiques de l'index"""
        try:
            if not self.es.indices.exists(index=self.index_name):
                return {"exists": False}
            
            stats = self.es.indices.stats(index=self.index_name)
            primaries = stats["_all"]["primaries"]
            
            return {
                "exists": True,
                "documents_count": primaries["docs"]["count"],
                "documents_deleted": primaries["docs"]["deleted"],
                "size_bytes": primaries["store"]["size_in_bytes"],
                "size_mb": round(primaries["store"]["size_in_bytes"] / 1e6, 2),
                "indexing_total": primaries["indexing"]["index_total"],
                "indexing_time_ms": primaries["indexing"]["index_time_in_millis"]
            }
        except Exception as e:
            return {"exists": False, "error": str(e)}
    
    def get_documents_by_year(self) -> Dict[str, int]:
        """Compte les documents par année"""
        try:
            query = {
                "size": 0,
                "aggs": {
                    "by_year": {
                        "terms": {
                            "field": "annee",
                            "size": 50
                        }
                    }
                }
            }
            
            result = self.es.search(index=self.index_name, body=query)
            
            return {
                str(bucket["key"]): bucket["doc_count"]
                for bucket in result["aggregations"]["by_year"]["buckets"]
            }
        except Exception as e:
            return {"error": str(e)}
    
    def get_full_status(self) -> Dict:
        """Récupère un état complet"""
        return {
            "timestamp": datetime.now().isoformat(),
            "cluster": self.get_cluster_health(),
            "index": self.get_index_stats(),
            "documents_by_year": self.get_documents_by_year()
        }
    
    def print_status(self):
        """Affiche l'état de façon lisible"""
        status = self.get_full_status()
        
        cluster = status["cluster"]
        index = status["index"]
        
        # Emoji selon le statut
        status_emoji = {
            "green": "🟢",
            "yellow": "🟡", 
            "red": "🔴",
            "error": "❌"
        }
        
        print("\n" + "="*50)
        print("📊 ÉTAT ELASTICSEARCH")
        print("="*50)
        
        print(f"\n🔗 Cluster:")
        print(f"   Status: {status_emoji.get(cluster.get('status', 'error'), '❓')} {cluster.get('status', 'N/A')}")
        print(f"   Nodes: {cluster.get('number_of_nodes', 'N/A')}")
        print(f"   Shards actifs: {cluster.get('active_shards', 'N/A')}")
        
        print(f"\n📁 Index '{self.index_name}':")
        if index.get("exists"):
            print(f"   Documents: {index.get('documents_count', 0):,}")
            print(f"   Taille: {index.get('size_mb', 0):.1f} MB")
        else:
            print("   ⚠️  Index non trouvé")
        
        print(f"\n📅 Documents par année:")
        by_year = status.get("documents_by_year", {})
        if not isinstance(by_year, dict) or "error" in by_year:
            print("   ⚠️  Impossible de récupérer les stats")
        else:
            for year in sorted(by_year.keys()):
                print(f"   {year}: {by_year[year]:,} documents")
        
        print("="*50 + "\n")
        
        return status