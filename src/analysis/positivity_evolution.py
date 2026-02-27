"""
Analyse de sentiment des interventions (modèle AventIQ-AI).
Retourne le vecteur de probabilités (5 classes: very_negative .. very_positive).
Usage:
  python -m src.analysis.positivity_evolution --test "Une phrase."
  python -m src.analysis.positivity_evolution [--word sécurité]
"""

import argparse
from pathlib import Path

import torch
import numpy as np
from transformers import BertForSequenceClassification, BertTokenizer
from tqdm import tqdm

from src.db.es_connection import ESConnection

BATCH_SIZE = 32
MAX_LENGTH = 256
OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "result" / "sentiments"
LABELS = ("very_negative", "negative", "neutral", "positive", "very_positive")


def load_model():
    model_name = "AventIQ-AI/sentiment_analysis_for_political_sentiment"
    model = BertForSequenceClassification.from_pretrained(model_name).eval().half()
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
    return model, tokenizer


def sentiment_score(text: str, model, tokenizer) -> np.ndarray:
    """Retourne le vecteur de probas (5 classes)."""
    inputs = tokenizer(
        text, return_tensors="pt", truncation=True, padding=True, max_length=MAX_LENGTH
    )
    inputs = {k: v.to(model.device) for k, v in inputs.items()}
    with torch.no_grad():
        logits = model(**inputs).logits
    return torch.softmax(logits, dim=-1).cpu().numpy()[0]


def sentiment_scores_batch(texts: list[str], model, tokenizer) -> list[np.ndarray]:
    if not texts:
        return []
    inputs = tokenizer(
        texts,
        return_tensors="pt",
        truncation=True,
        padding=True,
        max_length=MAX_LENGTH,
    )
    inputs = {k: v.to(model.device) for k, v in inputs.items()}
    with torch.no_grad():
        logits = model(**inputs).logits
    probs = torch.softmax(logits, dim=-1).cpu().numpy()
    return [p for p in probs]


def run_test(phrase: str):
    model, tokenizer = load_model()
    if torch.cuda.is_available():
        model = model.cuda()
    probs = sentiment_score(phrase, model, tokenizer)
    print("Vecteur (very_negative .. very_positive):")
    for label, p in zip(LABELS, probs):
        print(f"  {label}: {p:.4f}")
    print(f"  → {probs}")


def run_full(word: str | None):
    conn = ESConnection()
    interventions = conn.get_interventions_containing_word(word)
    print(f"{len(interventions)} intervention(s)")

    if not interventions:
        return

    model, tokenizer = load_model()
    if torch.cuda.is_available():
        model = model.cuda()

    texts = [i.get("texte") or "" for i in interventions]
    all_probs = []
    for i in tqdm(range(0, len(texts), BATCH_SIZE), desc="Sentiment"):
        batch = texts[i : i + BATCH_SIZE]
        all_probs.extend(sentiment_scores_batch(batch, model, tokenizer))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "sentiments2.csv"
    cols = ",".join(["para_id"] + list(LABELS))
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(cols + "\n")
        for inv, probs in zip(interventions, all_probs):
            para_id = inv.get("para_id", "")
            line = para_id + "," + ",".join(f"{p:.4f}" for p in probs)
            f.write(line + "\n")
    print(f"Écrit: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Score de sentiment des interventions")
    parser.add_argument(
        "--test", type=str, metavar="PHRASE", help="Tester sur une seule phrase"
    )
    parser.add_argument(
        "--word",
        type=str,
        default=None,
        help="Filtrer les interventions par mot (défaut: toutes)",
    )
    args = parser.parse_args()

    if args.test is not None:
        run_test(args.test)
    else:
        run_full(args.word)


if __name__ == "__main__":
    main()
