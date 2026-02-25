"""
Tests de l'indicateur de sentiment (positivity_evolution).
Lancer avec: python tests/test_sentiment.py
Charge le modèle une fois puis exécute les contrastes et les phrases de référence.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import torch

from src.analysis.positivity_evolution import load_model, sentiment_score


def _score_from_probs(probs: np.ndarray) -> float:
    """Moyenne pondérée 0..4 pour les tests de contraste/ordre."""
    return float(np.dot(np.arange(5), probs))


def test_sentiment_returns_vector(model, tokenizer):
    """sentiment_score retourne un vecteur de 5 probas qui somment à 1."""
    probs = sentiment_score("Test.", model, tokenizer)
    assert probs.shape == (5,), probs.shape
    assert np.isclose(probs.sum(), 1.0), probs.sum()


def run_contrast_tests(model, tokenizer):
    """Pour chaque paire (négatif, positif), on exige score(neg) < score(pos). Au moins 3/4 paires doivent passer."""
    pairs = [
        ("Je déteste cette politique.", "J'approuve cette politique."),
        ("La France va très mal.", "La France va très bien."),
        ("C'est une réforme catastrophique.", "C'est une réforme bénéfique."),
        ("Le gouvernement ment.", "Le gouvernement est transparent."),
    ]
    errors = []
    for neg, pos in pairs:
        p_neg = sentiment_score(neg, model, tokenizer)
        p_pos = sentiment_score(pos, model, tokenizer)
        s_neg = _score_from_probs(p_neg)
        s_pos = _score_from_probs(p_pos)
        if s_neg >= s_pos:
            errors.append(f"  '{neg}' ({s_neg:.3f}) >= '{pos}' ({s_pos:.3f})")
    # Au moins 3 paires sur 4 doivent respecter l'ordre (le modèle peut se tromper sur une)
    if len(errors) > 1:
        return errors
    return []


def run_reference_ordering(model, tokenizer):
    """
    Phrases de référence par catégorie. On affiche les scores et on vérifie au minimum:
    très_neg < très_pos (extrêmes dans le bon ordre) et neutre dans [1, 3].
    """
    refs = [
        ("very_negative", ["Cette loi est une catastrophe.", "Le gouvernement nous trahit."]),
        ("negative", ["Je suis opposé à cette réforme.", "La situation se dégrade."]),
        ("neutral", ["La séance est suspendue.", "La parole est à M. le rapporteur."]),
        ("positive", ["Je soutiens cette proposition.", "C'est un progrès pour les citoyens."]),
        ("very_positive", ["Bravo pour ce travail remarquable.", "Nous adhérons totalement à ce projet."]),
    ]
    means = []
    for label, phrases in refs:
        vectors = [sentiment_score(p, model, tokenizer) for p in phrases]
        scores = [_score_from_probs(v) for v in vectors]
        mean = sum(scores) / len(scores)
        means.append((label, mean))
    errors = []
    very_neg_m = next(m for l, m in means if l == "very_negative")
    very_pos_m = next(m for l, m in means if l == "very_positive")
    neutral_m = next(m for l, m in means if l == "neutral")
    if very_neg_m >= very_pos_m:
        errors.append(f"  very_negative ({very_neg_m:.3f}) >= very_positive ({very_pos_m:.3f})")
    if not (1 <= neutral_m <= 3):
        errors.append(f"  neutral ({neutral_m:.3f}) hors de [1, 3]")
    return errors, means


def main():
    print("Chargement du modèle...")
    model, tokenizer = load_model()
    if torch.cuda.is_available():
        model = model.cuda()
    print("  OK")

    print("Test: sentiment_score retourne un vecteur (5 probas)...")
    test_sentiment_returns_vector(model, tokenizer)
    print("  OK")

    print("Tests de contraste (négatif < positif)...")
    errs = run_contrast_tests(model, tokenizer)
    if errs:
        print("  ÉCHEC:")
        for e in errs:
            print(e)
        sys.exit(1)
    print("  OK")

    print("Ordre des phrases de référence...")
    errs, means = run_reference_ordering(model, tokenizer)
    for label, m in means:
        print(f"  {label}: {m:.3f}")
    if errs:
        print("  ÉCHEC (ordre non monotone):")
        for e in errs:
            print(e)
        sys.exit(1)
    print("  OK")

    print("\nTous les tests sont passés.")


if __name__ == "__main__":
    main()
