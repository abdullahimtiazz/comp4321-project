
#pseudo code
def search_engine(query):
    for word in query:
        #get pages which contains this word
        #Using select pages WHERE word_id = word

        for di in pages:
            tf = calculate_body_tf(word, di) #2
            df = calculate_body_df(word) #Will add another inverted index to store each word's df #2
            idf = log(N/df)
            max_tf = calculate_body_maxtf(di) #2 docu, max_tf  WHERE page_id = di MAX(frequency)
            
            #document1: I:1 like:1 comp comp:2
            #docu2: comp, like like, like max_tf(docu2) = 3
            #docu3: I
            # query: ust comp; ust: 1*log(1/df)


            w = tf*idf/max_tf # term word's weight in page di

            #add to cosine distance
            
            dict[di] += dik*qk
    
    return a dictionary: {document1: cosine similarity score with this query, document5: score}



def rank(dict):









