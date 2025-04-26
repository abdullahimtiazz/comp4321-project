# main.py

import sys
import re
import math
import requests
from crawler import Crawler
from nltk.stem import PorterStemmer

def extract_phrases_and_terms(query):
    """
    Return (phrase_lists, term_list)
    - phrase_lists: list of lists of words for each quoted phrase
    - term_list: list of standalone words
    """
    phrase_texts = re.findall(r'"([^"]+)"', query)
    remainder = re.sub(r'"[^"]+"', '', query)
    terms = re.findall(r'\b\w+\b', remainder.lower())
    phrases = [ph.lower().split() for ph in phrase_texts]
    return phrases, terms

def phrase_in_doc(phrase_terms, crawler, url):
    """
    Check if phrase_terms appear consecutively (in title or body) for a URL.
    """
    positions = {}
    for term in phrase_terms:
        positions[term] = (
            crawler.get_title_positions(url, term) +
            crawler.get_body_positions(url, term)
        )
    base = positions.get(phrase_terms[0], [])
    for p in base:
        if all((p + i) in positions.get(t, []) for i, t in enumerate(phrase_terms[1:], 1)):
            return True
    return False

def main():
    if len(sys.argv) != 4:
        print("Usage: python main.py <start_url> <max_pages> \"<query>\"")
        sys.exit(1)

    start_url = sys.argv[1]
    max_pages = int(sys.argv[2])
    query     = sys.argv[3]

    # 1. Crawl & index
    crawler = Crawler(start_url, max_pages)
    print(f"Crawling up to {max_pages} pages from {start_url}…")
    crawler.crawl()
    crawler.generate_spider_result()
    print("Crawling & indexing complete.\n")

    # 2. Load all indexed URLs
    cursor = crawler.index.cursor
    cursor.execute("SELECT url FROM pages")
    rows = cursor.fetchall()
    urls = [r[0] for r in rows]
    N = len(urls)
    if N == 0:
        print("No pages indexed—exiting.")
        crawler.index.close()
        return

    # 3. Parse and stem query
    phrase_texts, term_texts = extract_phrases_and_terms(query)
    stemmer = PorterStemmer()
    stemmed_terms = [stemmer.stem(w) for w in term_texts]
    stemmed_phrases = [[stemmer.stem(w) for w in ph] for ph in phrase_texts]
    all_terms = stemmed_terms + [w for ph in stemmed_phrases for w in ph]

    # 4. Build query vector (tf × idf / max(tf))
    q_tf = {}
    for t in all_terms:
        q_tf[t] = q_tf.get(t, 0) + 1
    max_q_tf = max(q_tf.values(), default=1)
    q_vec = {}
    for t, freq in q_tf.items():
        df = max(crawler.calculate_body_df(t), crawler.calculate_title_df(t), 1)
        idf = math.log(N / df)
        q_vec[t] = (freq / max_q_tf) * idf

    # 5. Score each document
    scores = {}
    for url in urls:
        # phrase check
        if any(not phrase_in_doc(ph, crawler, url) for ph in stemmed_phrases):
            continue

        # build document vector
        doc_vec = {}
        for t in q_vec:
            _, tf_b = crawler.calculate_body_tf(url, t)
            _, tf_t = crawler.calculate_title_tf(url, t)
            df = max(crawler.calculate_body_df(t), crawler.calculate_title_df(t), 1)
            idf = math.log(N / df)
            # boost title hits by 2×
            doc_vec[t] = (tf_b * idf) + (tf_t * idf * 2.0)

        # cosine similarity
        num = sum(q_vec[t] * doc_vec.get(t, 0) for t in q_vec)
        norm_q = math.sqrt(sum(v*v for v in q_vec.values()))
        norm_d = math.sqrt(sum(v*v for v in doc_vec.values()))
        if norm_q > 0 and norm_d > 0:
            scores[url] = num / (norm_q * norm_d)

    # 6. Output top 50
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:50]
    print(f"\nTop {len(ranked)} results for query {query!r}:\n")
    for i, (url, sc) in enumerate(ranked, 1):
        print(f"{i:2d}. {url} (score={sc:.4f})")

    crawler.index.close()

if __name__ == "__main__":
    main()
