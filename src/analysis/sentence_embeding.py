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
    "CATIE-AQ/camembert-base-embedding"
)

# Encoder par batches pour éviter la surcharge mémoire
print("Encodage des embeddings (cela peut prendre plusieurs minutes)...")
batch_size = 1000
embeddings_list = []

for i in tqdm(range(0, len(textes), batch_size), desc="Encodage"):
    batch = textes[i : i + batch_size]
    batch_embeddings = model.encode(batch, show_progress_bar=False, normalize_embeddings=True)
    embeddings_list.append(batch_embeddings)

# Concaténer tous les embeddings
embeddings = np.vstack(embeddings_list)
print(f"✓ {len(embeddings)} embeddings générés (dimension: {embeddings.shape[1]})")
assert embeddings.shape[0] == len(textes)
assert embeddings.ndim == 2
# Tokeniser pour la recherche de mots
sentences_tokens = [tokenizer(texte) for texte in textes]

output_dir = Path("data/embeddings")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / f"embeddings_{ANNEE}.npz"

np.savez(
    output_path,
    embeddings=embeddings,
    annee=ANNEE,
    nb_textes=len(textes),
)

print(f"✓ Embeddings sauvegardés dans : {output_path}")

# data = np.load("data/embeddings/embeddings_2018.npz")
# embeddings = data["embeddings"]