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
    #phrases = re.findall(r'"([^"]+)"', query)
    
    # Remove phrases from query
    query_wo_phrases = re.sub(r'"[^"]+"', '', query)
    # Remaining single terms
    terms = [stemmer.stem(w.lower()) for w in re.findall(r"\b[\w']+\b", query_wo_phrases)]
    return terms

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
    terms= parse_query(query)
    N = crawler.index.get_total_doc_count()
    if N == 0:
        print("No documents in DB. Did you crawl yet?")
        return []

    # 1. Get candidate docs for each term/phrase
    doc_sets = []
    for t in terms:
        doc_sets.append(get_docs_for_term(crawler, t))
    #for phrase in terms:
     #   doc_sets.append(get_docs_for_phrase(crawler, phrase))
    if not doc_sets:
        print("No query terms found.")
        return []
    candidate_docs = set.union(*doc_sets) if doc_sets else set()
    #return candidate_docs

    # 2. Build query vector (weight per term)
    query_counts = Counter(terms)
    
    #we have tf
    query_vector = {}
    query_maxtf = max(query_counts.values()) if query_counts else 1  # Ensure at least 1
    

    for term, tf in query_counts.items():
        # fetch distinct docs containing the term in body or title
            body_docs  = set(crawler.index.get_docs_containing_word_body(term))
            title_docs = set(crawler.index.get_docs_containing_word_title(term))
            df = len(body_docs | title_docs) 
               # union = unique docs
            if df == 0:
                df = 1                           # avoid division by zero
            idf = math.log(N / df)              # correct IDF
            query_vector[term] = (tf / query_maxtf) * idf

    # for term in query_counts:
    #     if ' ' in term:
    #         # phrase: get df using min df of words in phrase
    #         phrase_words = term.split()
    #         dfs = [crawler.calculate_body_df(w) + crawler.calculate_title_df(w) for w in phrase_words]
    #         df = min(dfs) if dfs else 1
    #         if df == 0: df = 1
    #     else:
    #         df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
    #         if df == 0: df = 1
    #     idf = math.log(N / df)
    #     tf = query_counts[term]
    #     max_tf = tf
        #wrong!
    dot_title = defaultdict(float)
    dot_body  = defaultdict(float)
    doc_score_title = defaultdict(float)
    doc_score_body = defaultdict(float)

    for term, weight in query_vector.items():
        # Get documents containing the term in body or title
        body_docs = set(crawler.index.get_docs_containing_word_body(term))
        title_docs = set(crawler.index.get_docs_containing_word_title(term))

        for doc in title_docs:
            title_tf = crawler.calculate_title_tf(doc, term)
            title_df = len(title_docs)
            title_maxtf = crawler.calculate_title_maxtf(doc)
            title_idf = math.log(N / title_df) if title_df > 0 else 0
            doc_score_title[doc] += ((title_tf * title_idf) / title_maxtf )*query_vector[term] if title_maxtf > 0 else 0
            

        for doc in body_docs:
            body_tf = crawler.calculate_body_tf(doc, term)
            body_df = len(body_docs)
            body_maxtf = crawler.calculate_body_maxtf(doc)
            body_idf = math.log(N / body_df) if body_df > 0 else 0
            doc_score_body[doc] += ((body_tf * body_idf) / body_maxtf)*query_vector[term] if body_maxtf > 0 else 0

    # Combine scores
    # total_doc_score = {}
    # for doc in set(doc_score_title.keys()).union(doc_score_body.keys()):
    #     title_score = doc_score_title[doc]
    #     body_score = doc_score_body[doc]
    #     total_score = 2 * title_score + body_score
        #total_doc_score[doc] = total_score
    # Precompute norm of query vector

    query_norm = math.sqrt(sum(w ** 2 for w in query_vector.values()))
    total_doc_score = defaultdict(float)
    TITLE_WEIGHT = 2.0
    # Accumulate dot products for each document
    for term, weight in query_vector.items():
        # Get documents containing the term in body or title
        body_docs  = set(crawler.index.get_docs_containing_word_body(term))
        title_docs = set(crawler.index.get_docs_containing_word_title(term))
        #dik =tf*idf/max_tf
        
        for doc in body_docs:
            total_doc_score[doc] +=  doc_score_body[doc] / (query_norm * crawler.get_page_length_body(doc))
        for doc in title_docs:
            total_doc_score[doc] +=  (doc_score_title[doc] / (query_norm * crawler.get_page_length_title(doc)))* TITLE_WEIGHT
    # Sort and rank by total doc score
    sorted_docs = sorted(total_doc_score.items(), key=lambda x: x[1], reverse=True)
    results = sorted_docs[:top_k]

    # Return ranked results
    return [(doc, score) for doc, score in results]
    



    # 4. Normalize into two cosine‐scores and combine
    



    # # 3. For each doc, build document vector (weight per term in the document)
    # doc_vectors = {}
    # TITLE_WEIGHT = 2.0  # Weight multiplier for title matches
    # for doc in candidate_docs:
    #     vec = {}
    #     # Efficiently get max_tf using DB-backed methods
    #     body_maxtf = crawler.calculate_body_maxtf(doc)
    #     title_maxtf = crawler.calculate_title_maxtf(doc)
    #     max_tf = max(body_maxtf, TITLE_WEIGHT * title_maxtf, 1)  # Ensure at least 1

        # assume get_page_length_body get_page_length_title(url)
        # q and title  , get cosine similarity = score1
        # q and body , get cosine similarity = score2
        #score = 2 * score1 + score2

        # get_page_length_body(doc) is the length of the body
        # get_page_length_title(doc) is the length of the title
       
        # dot product
        # Use .get to handle missing terms
        # for term in query_vector:
        # score1= dot / (doc_norm1 * query_norm) if doc_norm and query_norm else 0.0
        # doc_norm2 = math.sqrt(crawler.get_page_length_title(doc))
        # score2= dot / (doc_norm2 * query_norm) if doc_norm and query_norm else 0.0
        # score = TITLE_WEIGHT * score1 + score2


        # Retrieve all terms in the document
    #all_terms = crawler.get_all_terms_in_doc(doc)  # Assume this method retrieves all terms in the document
        # for term in all_terms:  # Use all terms in the document, not just query terms
        #     tf_body = crawler.calculate_body_tf(doc, term)
        #     tf_title = crawler.calculate_title_tf(doc, term)
        #     tf = tf_body + TITLE_WEIGHT * tf_title  # Apply title weight multiplier
        #     df = crawler.calculate_body_df(term) + crawler.calculate_title_df(term)
        #     if df == 0: df = 1
        #     idf = math.log(N / df)
        #     vec[term] = (tf * idf) / max_tf if max_tf > 0 else 0.0
    #doc_vectors[doc] = vec

    # 4. Cosine similarity
    
    # results=[]
    # for doc in candidate_docs:
    # # Get L2 norms of title and body vectors
    #     norm_title = crawler.index.get_page_length_title(doc)
    #     norm_body  = crawler.index.get_page_length_body(doc)

    #     # Compute cosine similarities (with division guard)
    #     score1 = dot_title[doc] / (query_norm * norm_title) if norm_title > 0 else 0.0
    #     score2 = dot_body[doc]  / (query_norm * norm_body)  if norm_body > 0 else 0.0

    #     # Final weighted score
    #     score = score1 + score2
    #     results.append((score, doc))

    # # Sort and return top-k
    # results.sort(reverse=True, key=lambda x: x[0])
    # return [doc for (score, doc) in results[:top_k]]

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