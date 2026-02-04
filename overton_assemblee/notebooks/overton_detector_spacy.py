"""
Overton Window Detection Module (spaCy version - lightweight)
Analyzes temporal context shifts using spaCy word vectors (no heavy dependencies).
"""

import yaml
import json
import re
from typing import List, Dict, Tuple, Optional
import numpy as np
from datetime import datetime
from elasticsearch import Elasticsearch


def load_config(config_path: str) -> Dict:
    """Load YAML configuration file."""
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def connect_elasticsearch(host: str) -> Elasticsearch:
    """Establish Elasticsearch connection."""
    es = Elasticsearch(host)
    if es.ping():
        print("✅ Connexion à Elasticsearch réussie")
        return es
    else:
        raise ConnectionError(f"Cannot connect to Elasticsearch at {host}")


def extract_passages_for_word(es: Elasticsearch, index: str, word: str, 
                              field: str = "texte", size: int = 1000) -> List[Dict]:
    """Extract all passages containing a specific word."""
    query = {
        "match": {
            field: {
                "query": word,
                "operator": "or"
            }
        }
    }
    try:
        res = es.search(index=index, query=query, size=size, track_total_hits=True)
        hits = res.get("hits", {}).get("hits", [])
        return [hit.get("_source", {}) for hit in hits]
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction pour '{word}': {e}")
        return []


def extract_all_passages_by_seed_words(es: Elasticsearch, index: str, 
                                       seed_words: List[str], field: str = "texte",
                                       size: int = 1000) -> Dict[str, List[Dict]]:
    """Extract passages for all seed words."""
    passages_by_word = {}
    for word in seed_words:
        passages_by_word[word] = extract_passages_for_word(es, index, word, field, size)
        print(f"  ✓ {word}: {len(passages_by_word[word])} passages")
    return passages_by_word


def parse_date_from_passage(passage: Dict) -> Optional[datetime]:
    """Extract and parse date from passage metadata."""
    date_field = passage.get("date_seance") or passage.get("date_parution")
    if date_field:
        try:
            return datetime.fromisoformat(date_field.split("T")[0])
        except Exception:
            return None
    return None


def extract_year_from_passage(passage: Dict) -> Optional[int]:
    """Extract year from passage."""
    date = parse_date_from_passage(passage)
    return date.year if date else passage.get("annee")


def filter_passages_by_year_range(passages: List[Dict], start_year: int, 
                                   end_year: int) -> List[Dict]:
    """Filter passages to a specific year range."""
    return [p for p in passages if start_year <= extract_year_from_passage(p) <= end_year]


def group_passages_by_year(passages: List[Dict]) -> Dict[int, List[Dict]]:
    """Group passages by year."""
    grouped = {}
    for passage in passages:
        year = extract_year_from_passage(passage)
        if year:
            if year not in grouped:
                grouped[year] = []
            grouped[year].append(passage)
    return dict(sorted(grouped.items()))


def load_spacy_model(model_name: str = "fr_core_news_sm"):
    """Load spaCy language model."""
    import spacy
    try:
        nlp = spacy.load(model_name)
        print(f"✓ spaCy model '{model_name}' loaded")
        return nlp
    except Exception as e:
        print(f"❌ Failed to load spaCy model: {e}")
        print(f"Install with: python -m spacy download {model_name}")
        raise


def get_passage_vector(passage: Dict, nlp, text_field: str = "texte") -> Optional[np.ndarray]:
    """Get averaged word vectors for a passage (exclude stopwords)."""
    text = passage.get(text_field, "")
    if not text or not isinstance(text, str):
        return None
    
    text = re.sub(r"\s+", " ", text).strip()[:500]  # Limit to 500 chars for speed
    try:
        doc = nlp(text)
        # Average vectors of non-stop, non-punct words
        vectors = [token.vector for token in doc if not token.is_stop and not token.is_punct and token.has_vector]
        if vectors:
            return np.mean(vectors, axis=0)
    except Exception as e:
        print(f"  Warning: {e}")
    return None


def get_passage_vectors(passages: List[Dict], nlp, text_field: str = "texte") -> Dict[int, np.ndarray]:
    """Get vectors for multiple passages."""
    vectors = {}
    for i, passage in enumerate(passages):
        vec = get_passage_vector(passage, nlp, text_field)
        if vec is not None:
            vectors[i] = vec
    return vectors


def compute_centroid(vectors: Dict[int, np.ndarray]) -> Optional[np.ndarray]:
    """Compute centroid (mean) of vectors."""
    if not vectors:
        return None
    vec_list = list(vectors.values())
    return np.mean(vec_list, axis=0)


def compute_centroids_by_year(passages_by_year: Dict[int, List[Dict]], nlp,
                              text_field: str = "texte") -> Dict[int, np.ndarray]:
    """Compute centroid vectors for each year."""
    centroids = {}
    for year, passages in passages_by_year.items():
        vectors = get_passage_vectors(passages, nlp, text_field)
        centroid = compute_centroid(vectors)
        if centroid is not None:
            centroids[year] = centroid
            print(f"  ✓ Year {year}: {len(vectors)} passages vectorized")
    return centroids


def cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    try:
        return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2) + 1e-8)
    except:
        return 0.0


def compute_temporal_shift(centroids: Dict[int, np.ndarray]) -> Dict[Tuple[int, int], float]:
    """Compute cosine distance between consecutive year centroids."""
    years = sorted(centroids.keys())
    shifts = {}
    for i in range(len(years) - 1):
        year1, year2 = years[i], years[i + 1]
        sim = cosine_similarity(centroids[year1], centroids[year2])
        distance = 1 - sim
        shifts[(year1, year2)] = distance
        print(f"  Year {year1}->{year2}: distance={distance:.4f}")
    return shifts


def score_passage_anomaly(passage: Dict, centroid: np.ndarray, nlp, 
                          text_field: str = "texte") -> Optional[float]:
    """Score how anomalous a passage is from its year's centroid."""
    vec = get_passage_vector(passage, nlp, text_field)
    if vec is None:
        return None
    sim = cosine_similarity(vec, centroid)
    return 1 - sim


def score_passages_by_year(passages_by_year: Dict[int, List[Dict]], 
                          centroids: Dict[int, np.ndarray], nlp,
                          text_field: str = "texte") -> Dict[int, List[Tuple[Dict, float]]]:
    """Score all passages for anomaly."""
    scored_by_year = {}
    for year, passages in passages_by_year.items():
        if year not in centroids:
            continue
        centroid = centroids[year]
        scored = []
        for passage in passages:
            score = score_passage_anomaly(passage, centroid, nlp, text_field)
            if score is not None:
                scored.append((passage, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        scored_by_year[year] = scored
    return scored_by_year


def get_top_anomalies(scored_by_year: Dict[int, List[Tuple[Dict, float]]], 
                     top_n: int = 5) -> Dict[int, List[Tuple[Dict, float]]]:
    """Extract top N most anomalous passages per year."""
    return {year: scored[:top_n] for year, scored in scored_by_year.items()}


def format_passage_summary(passage: Dict, score: float, max_length: int = 200) -> str:
    """Format passage for display."""
    text = passage.get("texte", "N/A")
    if isinstance(text, str):
        text = re.sub(r"\s+", " ", text).strip()[:max_length]
    year = extract_year_from_passage(passage)
    return f"[{year}] (anomaly={score:.3f}) {text}..."


def print_temporal_analysis(shifts: Dict[Tuple[int, int], float]):
    """Print temporal shift analysis."""
    print("\n" + "="*70)
    print("📊 TEMPORAL CONTEXT SHIFTS (Cosine Distance)")
    print("="*70)
    for (y1, y2), distance in shifts.items():
        marker = "🔴" if distance > 0.15 else "🟡" if distance > 0.10 else "🟢"
        print(f"{marker} {y1} → {y2}: {distance:.4f}")


def print_anomalies(top_anomalies: Dict[int, List[Tuple[Dict, float]]], word: str):
    """Print top anomalies per year."""
    print("\n" + "="*70)
    print(f"🎯 TOP ANOMALIES for '{word}' (Outliers per year)")
    print("="*70)
    for year in sorted(top_anomalies.keys()):
        anomalies = top_anomalies[year]
        print(f"\n📅 {year} ({len(anomalies)} anomalies):")
        for i, (passage, score) in enumerate(anomalies, 1):
            summary = format_passage_summary(passage, score)
            print(f"  {i}. {summary}")


def save_results_json(results: Dict, output_path: str):
    """Save analysis results to JSON file."""
    serializable = {}
    for key, value in results.items():
        if isinstance(value, dict):
            serializable[key] = {str(k): v for k, v in value.items()}
        else:
            serializable[key] = value
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n✅ Résultats sauvegardés dans {output_path}")


def run_overton_analysis(config_path: str, index: str, seed_words: List[str],
                        start_year: int = 2015, end_year: int = 2024,
                        text_field: str = "texte", output_dir: str = "results"):
    """Main analysis pipeline."""
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "="*70)
    print("🔍 OVERTON WINDOW DETECTION PIPELINE (spaCy version)")
    print("="*70)
    
    # 1. Load config and connect
    print("\n1️⃣ Loading configuration...")
    cfg = load_config(config_path)
    es = connect_elasticsearch(cfg["elasticsearch"]["host"])
    
    # 2. Load spaCy model
    print("\n2️⃣ Loading spaCy model...")
    nlp = load_spacy_model("fr_core_news_sm")
    
    # 3. Extract passages
    print("\n3️⃣ Extracting passages from Elasticsearch...")
    passages_by_word = extract_all_passages_by_seed_words(es, index, seed_words, text_field)
    
    # 4. Analyze each word
    results = {"analysis_date": datetime.now().isoformat(), "words": {}}
    
    for word in seed_words:
        print(f"\n4️⃣ Analyzing '{word}'...")
        passages = passages_by_word[word]
        passages_filtered = filter_passages_by_year_range(passages, start_year, end_year)
        
        if not passages_filtered:
            print(f"  ⚠️ No passages found for '{word}'")
            continue
        
        # 5. Group by year
        passages_by_year = group_passages_by_year(passages_filtered)
        print(f"  ✓ Grouped into {len(passages_by_year)} years")
        
        # 6. Compute vectors and centroids
        print(f"  ✓ Computing word vectors and centroids...")
        centroids = compute_centroids_by_year(passages_by_year, nlp, text_field)
        
        # 7. Detect temporal shifts
        print(f"  ✓ Detecting temporal context shifts...")
        shifts = compute_temporal_shift(centroids)
        print_temporal_analysis(shifts)
        
        # 8. Score anomalies
        print(f"  ✓ Scoring passage anomalies...")
        scored_by_year = score_passages_by_year(passages_by_year, centroids, nlp, text_field)
        top_anomalies = get_top_anomalies(scored_by_year, top_n=5)
        print_anomalies(top_anomalies, word)
        
        # Store results
        results["words"][word] = {
            "total_passages": len(passages),
            "passages_in_range": len(passages_filtered),
            "years_analyzed": list(sorted(passages_by_year.keys())),
            "temporal_shifts": {str(k): v for k, v in shifts.items()}
        }
    
    # 9. Save results
    output_file = os.path.join(output_dir, "overton_analysis.json")
    save_results_json(results, output_file)
    
    print("\n" + "="*70)
    print("✅ Analysis complete!")
    print("="*70)


if __name__ == "__main__":
    # Example usage
    config_path = "overton_assemblee/config/settings.yaml"
    index = "debats_assemblee_nationale"
    seed_words = ["immigration", "migrant", "immigré", "étranger", "réfugié", "demandeur d'asile"]
    
    run_overton_analysis(config_path, index, seed_words, start_year=2018, end_year=2023)
