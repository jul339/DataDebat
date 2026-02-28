
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
