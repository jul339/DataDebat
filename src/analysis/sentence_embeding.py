import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # pointe sur .../src
sys.path.insert(0, str(ROOT))

from sentence_transformers import SentenceTransformer
from elasticsearch.helpers import scan
from db.es_connection import ESConnection
import re
from tqdm import tqdm
import numpy as np

ANNEE = 2018
WORD = "migrant"


def get_sentence_interventions_for_year(annee: int = ANNEE, text_field: str = "texte"):
    """Récupère les textes bruts (pas tokenisés) pour l'encodage"""
    es_conn = ESConnection()

    # Scan pour streamer tous les documents de l'année
    scan_query = {"query": {"term": {"annee": annee}}}

    discours = []
    docs = scan(es_conn.es, query=scan_query, index="debats_assemblee_nationale")
    for doc in docs:
        texte = doc.get("_source", {}).get(text_field, "")
        if texte and texte.strip():
            discours.append(texte)  # Garder le texte brut pour l'encoding

    if not discours:
        raise ValueError(f"Aucun discours trouvé pour l'année {annee}")

    print(f"Nombre de discours récupérés: {len(discours)}")
    return discours


def tokenizer(texte):
    """Tokenise le texte pour la recherche de mots"""
    texte = texte.lower()
    texte = re.sub(r"[^\w\s]", "", texte)
    return texte.split()


# Récupérer les textes bruts
textes = get_sentence_interventions_for_year()
print(f"Nombre de textes: {len(textes)}")

# Charger le modèle
print("Chargement du modèle...")
model = SentenceTransformer(
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

# Encoder par batches pour éviter la surcharge mémoire
print("Encodage des embeddings (cela peut prendre plusieurs minutes)...")
batch_size = 1000
embeddings_list = []

for i in tqdm(range(0, len(textes), batch_size), desc="Encodage"):
    batch = textes[i : i + batch_size]
    batch_embeddings = model.encode(batch, show_progress_bar=False)
    embeddings_list.append(batch_embeddings)

# Concaténer tous les embeddings
embeddings = np.vstack(embeddings_list)
print(f"✓ {len(embeddings)} embeddings générés (dimension: {embeddings.shape[1]})")

# Tokeniser pour la recherche de mots
sentences_tokens = [tokenizer(texte) for texte in textes]

from sklearn.metrics.pairwise import cosine_similarity

if WORD not in set(w for s in sentences_tokens for w in s):
    print(f"Mot '{WORD}' non trouvé dans le corpus extrait.")
else:
    # Créer un vocabulaire (mot -> index dans l'embedding)
    vocab = {}
    word_vectors = []
    for s_embed, s_tokens in zip(embeddings, sentences_tokens):
        for word in s_tokens:
            if word not in vocab:
                vocab[word] = len(word_vectors)
                word_vectors.append(
                    s_embed
                )  # Approche simple: utilise le vecteur de la phrase contenant le mot

    # Génère une matrice (nb_mots, dim)
    word_vectors = np.array(word_vectors)
    if WORD not in vocab:
        print(f"Le mot '{WORD}' n'a pas pu être associé à une phrase.")
    else:
        idx_word = vocab[WORD]
        vec_word = word_vectors[idx_word].reshape(1, -1)
        sims = cosine_similarity(vec_word, word_vectors)[0]
        top_indices = sims.argsort()[::-1][1:11]  # On saute le mot lui-même

        inv_vocab = {i: w for w, i in vocab.items()}
        print(f"\nLes mots les plus proches de '{WORD}' (embedding de phrase):")
        for i in top_indices:
            print(f"  {inv_vocab[i]:25s}: similarité = {sims[i]:.4f}")
