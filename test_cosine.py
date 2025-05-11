from search import search_engine, parse_query
import math
import sqlite3

# Import your actual Crawler class if available
# from crawler import Crawler

class DummyCrawler:
    def __init__(self, db_path):
        from crawler import Crawler  # Import here to avoid errors if not present
        self.crawler = Crawler(db_path)
        self.index = self.crawler.index

    def __getattr__(self, name):
        return getattr(self.crawler, name)

def test_cosine_similarity_loop():
    db_path = "search_engine.db"
    crawler = DummyCrawler(db_path)
    query = "information retrieval"

    # Get query_vector as in search_engine
    terms, phrases = parse_query(query)
    N = crawler.index.get_total_doc_count()
    query_counts = {}
    from collections import Counter
    query_counts = Counter(terms)
    for phrase in phrases:
        query_counts[' '.join(phrase)] += 1

    query_vector = {}
    for term in query_counts:
        if ' ' in term:
            phrase_words = term.split()
            dfs = [crawler.calculate_body_df(w) + crawler.calculate_title_df(w) for w in phrase_words]
            df = min(dfs) if dfs else 1
            if df == 0: df = 1
        else:
            df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
            if df == 0: df = 1
        idf = math.log(N / df)
        tf = query_counts[term]
        max_tf = tf
        query_vector[term] = (tf / max_tf) * idf

    results = search_engine(crawler, query, top_k=5)

    print("Cosine Similarity Loop Results:")
    for doc, score in results:
        # Get doc vector as in search_engine
        TITLE_WEIGHT = 2.0
        body_maxtf = crawler.calculate_body_maxtf(doc)
        title_maxtf = crawler.calculate_title_maxtf(doc)
        max_tf = max(body_maxtf, TITLE_WEIGHT * title_maxtf, 1)
        all_terms = crawler.get_all_terms_in_doc(doc)
        vec = {}
        for term in all_terms:
            tf_body = crawler.calculate_body_tf(doc, term)
            tf_title = crawler.calculate_title_tf(doc, term)
            tf = tf_body + TITLE_WEIGHT * tf_title
            df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
            if df == 0: df = 1
            idf = math.log(N / df)
            vec[term] = (tf * idf) / max_tf if max_tf > 0 else 0.0

        dot = sum(vec.get(t, 0) * query_vector.get(t, 0) for t in query_vector)
        doc_norm = math.sqrt(sum(v**2 for v in vec.values()))
        query_norm = math.sqrt(sum(v**2 for v in query_vector.values()))
        print(f"Doc: {doc}")
        print(f"  dot: {dot}")
        print(f"  doc_norm: {doc_norm}")
        print(f"  query_norm: {query_norm}")
        print(f"  score: {score}")
        print("-" * 40)

if __name__ == "__main__":
    test_cosine_similarity_loop()