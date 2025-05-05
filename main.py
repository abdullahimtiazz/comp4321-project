import math
import re
from collections import Counter, defaultdict
from crawler import Crawler

from nltk.stem import PorterStemmer

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
    """Return set of URLs with this word in body or title."""
    # Use inverted index for body and title
    body_docs = crawler.index.get_docs_containing_word_body(word)
    title_docs = crawler.index.get_docs_containing_word_title(word)
    return set(body_docs) | set(title_docs)

def get_docs_for_phrase(crawler, phrase):
    """
    Return set of URLs where the phrase appears consecutively in body.
    phrase: list of stemmed words
    """
    docs = get_docs_for_term(crawler, phrase[0])
    result = set()
    for url in docs:
        positions = set(crawler.get_body_positions(url, phrase[0]))
        if not positions:
            continue
        for pos in positions:
            found = True
            for offset, word in enumerate(phrase[1:], 1):
                next_positions = crawler.get_body_positions(url, word)
                if (pos + offset) not in next_positions:
                    found = False
                    break
            if found:
                result.add(url)
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
    for doc in candidate_docs:
        vec = {}
        # Retrieve all terms in the document
        all_terms = crawler.get_all_terms_in_doc(doc)  # Assume this method retrieves all terms in the document
        max_tf = max(
            crawler.calculate_body_tf(doc, term)[0] + TITLE_WEIGHT * crawler.calculate_title_tf(doc, term)[0]
            for term in all_terms
        )
        max_tf = max(max_tf, 1)  # Ensure max_tf is at least 1 to avoid division by zero

        for term in all_terms:  # Use all terms in the document, not just query terms
            tf_body, _ = crawler.calculate_body_tf(doc, term)
            tf_title, _ = crawler.calculate_title_tf(doc, term)
            tf = tf_body + TITLE_WEIGHT * tf_title  # Apply title weight multiplier
            df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
            if df == 0: df = 1
            idf = math.log(N / df)
            vec[term] = (tf * idf) / max_tf if max_tf > 0 else 0.0
        doc_vectors[doc] = vec

    # 4. Cosine similarity
    results = []
    for doc, vec in doc_vectors.items():
        dot = sum(vec.get(t, 0) * query_vector.get(t, 0) for t in query_vector)  # Use .get to handle missing terms
        doc_norm = math.sqrt(sum(v**2 for v in vec.values()))
        query_norm = math.sqrt(sum(v**2 for v in query_vector.values()))
        score = dot / (doc_norm * query_norm) if doc_norm and query_norm else 0.0
        results.append((doc, score))

    # 5. Top 50
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:top_k]

def print_results(crawler, results):
    if not results:
        print("No results found.")
        return
    print("\nTop Results:")
    for rank, (url, score) in enumerate(results, 1):
        # Fetch title from DB
        crawler.index.cursor.execute("SELECT title FROM pages WHERE url=?", (url,))
        row = crawler.index.cursor.fetchone()
        title = row[0] if row else "No Title"
        print(f"{rank}. [{title.strip() if title else 'No Title'}]")
        print(f"   URL: {url}")
        print(f"   Score: {score:.4f}")
        print()

def main():
    # 1. Start crawler (if you want to recrawl, uncomment next lines)
    # start_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
    # crawler = Crawler(start_url)
    # crawler.crawl()
    # crawler.generate_spider_result()

    # Or, just connect to existing DB:
    start_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
    crawler = Crawler(start_url)

    while True:
        print("\n=== SEARCH ENGINE ===")
        query = input('Enter your query (or type "exit" to quit): ').strip()
        if query.lower() == "exit":
            break
        results = search_engine(crawler, query)
        print_results(crawler, results)

if __name__ == "__main__":
    main()

