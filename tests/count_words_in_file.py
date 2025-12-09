#!/usr/bin/env python3
"""
Script pour compter le nombre total de mots dans tous les champs 'texte'
d'un fichier JSON.
"""

import json
import sys
from pathlib import Path


def count_words_in_text(text):
    """Compte le nombre de mots dans un texte."""
    if not text or not isinstance(text, str):
        return 0
    # Divise par espaces et filtre les chaînes vides
    return len([word for word in text.split() if word.strip()])


def count_words_in_file(file_path):
    """
    Compte le nombre total de mots dans tous les champs 'texte' d'un fichier JSON.
    
    Args:
        file_path: Chemin vers le fichier JSON
        
    Returns:
        Tuple (nombre_total_mots, nombre_documents)
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Le fichier {file_path} n'existe pas")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("Le fichier JSON doit contenir une liste de documents")
    
    total_words = 0
    total_documents = 0
    documents_without_text = 0
    
    for doc in data:
        if not isinstance(doc, dict):
            continue
            
        total_documents += 1
        
        if 'texte' in doc and doc['texte']:
            words = count_words_in_text(doc['texte'])
            total_words += words
        else:
            documents_without_text += 1
    
    return total_words, total_documents, documents_without_text


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python count_words_in_file.py <chemin_vers_fichier.json>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    try:
        total_words, total_docs, docs_without_text = count_words_in_file(file_path)
        
        print(f"\n{'='*60}")
        print(f"Résultats pour: {file_path}")
        print(f"{'='*60}")
        print(f"Nombre total de mots: {total_words:,}")
        print(f"Nombre de documents: {total_docs:,}")
        print(f"Documents sans texte: {docs_without_text:,}")
        print(f"Documents avec texte: {total_docs - docs_without_text:,}")
        if total_docs - docs_without_text > 0:
            print(f"Moyenne de mots par document: {total_words / (total_docs - docs_without_text):.1f}")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"Erreur: {e}", file=sys.stderr)
        sys.exit(1)


