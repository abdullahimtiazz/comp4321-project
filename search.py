import math
from nltk.stem import PorterStemmer
import re
from collections import Counter, defaultdict

def parse_query(query):
    """
    Returns (terms, phrases)
    terms: list of single stemmed words
    phrases: list of list of stemmed words (each phrase)
    """
    stemmer = PorterStemmer()
    # Extract phrases in quotes
    phrases = re.findall(r'"([^"]+)"', query)
    phrase_terms = [ [stemmer.stem(w.lower()) for w in re.findall(r"\b[\w']+\b", p)] for p in phrases ]
    # Remove phrases from query
    query_wo_phrases = re.sub(r'"[^"]+"', '', query)
    # Remaining single terms
    terms = [stemmer.stem(w.lower()) for w in re.findall(r"\b[\w']+\b", query_wo_phrases)]
    return terms, phrase_terms

def get_docs_for_term(crawler, word):
    """Return set of page_ids with this word in body or title."""
    # Use inverted index for body and title
    body_docs = crawler.index.get_docs_containing_word_body(word)
    title_docs = crawler.index.get_docs_containing_word_title(word)
    # These should now return page_ids
    return set(body_docs) | set(title_docs)

def get_docs_for_phrase(crawler, phrase):
    """
    Return set of page_ids where the phrase appears consecutively in body.
    phrase: list of stemmed words
    """
    docs = get_docs_for_term(crawler, phrase[0])
    result = set()
    for page_id in docs:
        positions = set(crawler.get_body_positions(page_id, phrase[0]))
        if not positions:
            continue
        for pos in positions:
            found = True
            for offset, word in enumerate(phrase[1:], 1):
                next_positions = crawler.get_body_positions(page_id, word)
                if (pos + offset) not in next_positions:
                    found = False
                    break
            if found:
                result.add(page_id)
                break
    return result

def search_engine(crawler, query, top_k=50):
    terms, phrases = parse_query(query)
    N = crawler.index.get_total_doc_count()
    if N == 0:
        print("No documents in DB. Did you crawl yet?")
        return []

    # 1. Get candidate docs for each term/phrase
    doc_sets = []
    for t in terms:
        doc_sets.append(get_docs_for_term(crawler, t))
    for phrase in phrases:
        doc_sets.append(get_docs_for_phrase(crawler, phrase))
    if not doc_sets:
        print("No query terms found.")
        return []
    candidate_docs = set.union(*doc_sets) if doc_sets else set()

    # 2. Build query vector (weight per term)
    query_counts = Counter(terms)
    for phrase in phrases:
        query_counts[' '.join(phrase)] += 1

    query_vector = {}
    for term in query_counts:
        if ' ' in term:
            # phrase: get df
            phrase_words = term.split()
            df = max(1, sum(1 for d in candidate_docs if d in get_docs_for_phrase(crawler, phrase_words)))
        else:
            df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
            if df == 0: df = 1
        idf = math.log(N / df)
        tf = query_counts[term]
        max_tf = tf
        query_vector[term] = (tf / max_tf) * idf  # always idf for query

    # 3. For each doc, build document vector (weight per term in the document)
    doc_vectors = {}
    TITLE_WEIGHT = 2.0  # Weight multiplier for title matches
    for page_id in candidate_docs:
        vec = {}
        all_terms = crawler.get_all_terms_in_doc(page_id)
        # Calculate max_tf as the maximum (tf_body + TITLE_WEIGHT * tf_title) over all terms in this page
        tf_values = [
            crawler.calculate_body_tf(page_id, term) + TITLE_WEIGHT * crawler.calculate_title_tf(page_id, term)
            for term in all_terms
        ]
        max_tf = max(tf_values) if tf_values else 1  # Avoid division by zero

        for term in all_terms:
            tf_body = crawler.calculate_body_tf(page_id, term)
            tf_title = crawler.calculate_title_tf(page_id, term)
            tf = tf_body + TITLE_WEIGHT * tf_title
            df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
            if df == 0: df = 1
            idf = math.log(N / df)
            vec[term] = (tf * idf) / max_tf if max_tf > 0 else 0.0
        doc_vectors[page_id] = vec

    # 4. Cosine similarity
    results = []
    for page_id, vec in doc_vectors.items():
        dot = sum(vec.get(t, 0) * query_vector.get(t, 0) for t in query_vector)  # Use .get to handle missing terms
        doc_norm = math.sqrt(sum(v**2 for v in vec.values()))
        query_norm = math.sqrt(sum(v**2 for v in query_vector.values()))
        score = dot / (doc_norm * query_norm) if doc_norm and query_norm else 0.0
        results.append((page_id, score))

    # 5. Top 50
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:top_k]

def print_results(crawler, results):
    if not results:
        print("No results found.")
        return
    print("\nTop Results:")
    for rank, (page_id, score) in enumerate(results, 1):
        # Fetch title and url from DB using page_id
        crawler.index.cursor.execute("SELECT title, url FROM pages WHERE page_id=?", (page_id,))
        row = crawler.index.cursor.fetchone()
        title = row[0] if row else "No Title"
        url = row[1] if row else "No URL"
        print(f"{rank}. [{title.strip() if title else 'No Title'}]")
        print(f"   URL: {url}")
        print(f"   Score: {score:.4f}")
        print()