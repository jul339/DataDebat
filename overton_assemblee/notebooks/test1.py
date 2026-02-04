import yaml
import json
import re
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

# Vérifier la connexion
if es.ping():
    print("✅ Connexion à Elasticsearch réussie")
    
    # Informations sur le cluster
    info = es.info()
    print(f"\n📌 Version Elasticsearch: {info['version']['number']}")
    print(f"📌 Nom du cluster: {info['cluster_name']}")
else:
    print("❌ Impossible de se connecter à Elasticsearch")


index_name = "debats_assemblee_nationale" #Change le nom de l'index si nécessaire

# Vérifier si l'index existe
if es.indices.exists(index=index_name):
    print(f"✅ L'index '{index_name}' existe\n")
    
    # Statistiques de l'index
    stats = es.indices.stats(index=index_name)
    count = es.count(index=index_name)
    
    print(f"📊 Statistiques de l'index:")
    print(f"   • Nombre total de documents: {count['count']:,}")
    print(f"   • Taille de l'index: {stats['indices'][index_name]['total']['store']['size_in_bytes'] / (1024*1024):.2f} MB")
    
    # Mapping de l'index
    mapping = es.indices.get_mapping(index=index_name)
    properties = mapping[index_name]['mappings']['properties']
    print(f"   • Nombre de champs: {len(properties)}")
    
else:
    print(f"❌ L'index '{index_name}' n'existe pas")
    print("Exécutez d'abord le script d'extraction pour créer l'index")

# Charger la configuration depuis le fichier YAML
with open("overton_assemblee/config/settings.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

SEED_WORDS = cfg["topic"]["seed_words"]
INDEX = cfg["elasticsearch"]["index_debats"]
BATCH_SIZE = cfg["elasticsearch"]["batch_size"]


def build_query(cfg, seed_words):
    """Build an Elasticsearch bool 'should' query from config and seed words.

    Configurable options (under top-level `query` key in settings.yaml):
      - fields: list of fields to search (default ['text'])
      - type: 'multi_match' or 'match_phrase' (default 'multi_match')
      - operator: 'or' or 'and' (default 'or')
      - fuzziness: e.g. 'AUTO' or None
      - minimum_should_match: int or string (default 1)
      - boost: numeric boost to apply (optional)
    """
    qcfg = cfg.get("query", {})
    fields = qcfg.get("fields", ["text"])
    match_type = qcfg.get("type", "multi_match")
    operator = qcfg.get("operator", "or")
    minimum_should_match = qcfg.get("minimum_should_match", 1)
    fuzziness = qcfg.get("fuzziness")
    boost = qcfg.get("boost")

    should = []
    for w in seed_words:
        if match_type == "match_phrase":
            for f in fields:
                clause = {"match_phrase": {f: {"query": w}}}
                if boost is not None:
                    clause["match_phrase"][f]["boost"] = boost
                should.append(clause)
        else:
            clause = {"multi_match": {"query": w, "fields": fields, "operator": operator}}
            if fuzziness:
                clause["multi_match"]["fuzziness"] = fuzziness
            if boost is not None:
                clause["multi_match"]["boost"] = boost
            should.append(clause)

    return {"query": {"bool": {"should": should, "minimum_should_match": minimum_should_match}}}


def test_seed_words_in_field(es, index, seed_words, field="section_titre"):
    """Test if SEED_WORDS are present in a given text field.

    Shows per-word document counts and example values from the field.
    """
    print("\n" + "="*70)
    print(f"🧪 TEST: Présence de SEED_WORDS dans '{field}'")
    print("="*70)

    results = {}
    for word in seed_words:
        query = {
            "query": {
                "match": {
                    field: {
                        "query": word,
                        "operator": "or"
                    }
                }
            }
        }
        try:
            res = es.search(index=index, query=query.get("query"), size=1, track_total_hits=True)
            total = res.get("hits", {}).get("total", {})
            count = total.get("value", 0) if isinstance(total, dict) else total
            results[word] = count
        except Exception as e:
            print(f"  ❌ Erreur pour '{word}': {e}")
            results[word] = None

    # Afficher les résultats
    print("\n📊 Fréquence par mot:")
    for word, count in results.items():
        status = "✅" if count and count > 0 else "❌"
        count_str = f"{count:,}" if count else "N/A"
        print(f"  {status} {word:20s} → {count_str:>8} documents")

    # Exemple de valeurs trouvées
    total_all = sum(c for c in results.values() if c is not None)
    if total_all > 0:
        print(f"\n📥 Exemple de valeurs du champ '{field}' (5 premiers):")
        global_query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {field: w}} for w in seed_words
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        try:
            res = es.search(index=index, query=global_query.get("query"), size=5)
            # prepare regex to find any seed word (word boundaries, case-insensitive)
            pattern = re.compile(r"\b(" + "|".join(re.escape(w) for w in seed_words) + r")\b", re.IGNORECASE)
            for i, hit in enumerate(res.get("hits", {}).get("hits", []), 1):
                val = hit.get("_source", {}).get(field, "N/A")
                to_print = val if isinstance(val, str) else str(val)
                # Find a match and extract the full sentence if possible
                m = pattern.search(to_print)
                if m:
                    start_idx = m.start()
                    # find sentence start
                    prev_punct = max(to_print.rfind(p, 0, start_idx) for p in ('.', '!', '?', '\n'))
                    if prev_punct == -1:
                        sent_start = max(0, start_idx - 80)
                    else:
                        sent_start = prev_punct + 1
                    # find sentence end
                    next_puncts = [to_print.find(p, m.end()) for p in ('.', '!', '?', '\n') if to_print.find(p, m.end()) != -1]
                    if next_puncts:
                        sent_end = min(next_puncts) + 1
                    else:
                        sent_end = min(len(to_print), m.end() + 80)
                    snippet = to_print[sent_start:sent_end].strip()
                    # replace newlines and compress spaces
                    snippet = re.sub(r"\s+", " ", snippet)
                    print(f"    {i}. ...{snippet}...")
                else:
                    # fallback: show beginning truncated (clean newlines first)
                    cleaned = to_print[:200].replace('\n', ' ')
                    print(f"    {i}. {cleaned}")
        except Exception as e:
            print(f"    Erreur: {e}")

    return results


if __name__ == "__main__":
    # Construire et afficher la requête pour vérification
    query = build_query(cfg, SEED_WORDS)
    print("\n🔍 Requête construite:")
    print(json.dumps(query, ensure_ascii=False, indent=2))

    # Aperçu rapide des résultats (5 premiers documents)
    # Vérifier que l'index configuré existe, sinon proposer une bascule
    idx_to_use = INDEX
    if not es.indices.exists(index=idx_to_use):
        print(f"⚠️ Index configuré '{idx_to_use}' introuvable.")
        if es.indices.exists(index=index_name):
            print(f"➡️ Utilisation de l'index local '{index_name}' à la place.")
            idx_to_use = index_name
        else:
            print("❌ Aucun index adéquat trouvé.")
            try:
                existing = list(es.indices.get_alias("*").keys())
                if existing:
                    print("Indices existants:")
                    for i in existing:
                        print(f"  - {i}")
                else:
                    print("  (liste d'indices vide)")
            except Exception:
                print("  (impossible de lister les indices via l'API)")
            print(f"Créez l'index '{idx_to_use}' ou mettez à jour 'overton_assemblee/config/settings.yaml'.")
            raise SystemExit(1)

    try:
        # Use the `query` parameter instead of deprecated `body`
        res = es.search(index=idx_to_use, query=query.get("query"), size=5)
        hits = res.get("hits", {}).get("hits", [])
        print(f"\n📥 Aperçu: {len(hits)} hits")
        for i, h in enumerate(hits, 1):
            source = h.get("_source", {})
            print(f" {i} • id={h.get('_id')} — {list(source.keys())[:5]}")
    except Exception as e:
        print(f"Erreur lors de la recherche d'aperçu: {e}")
    
    # Tester la présence des SEED_WORDS dans le champ `texte`
    print("\n" + "-"*70)
    test_results = test_seed_words_in_field(es, idx_to_use, SEED_WORDS, field="texte")
