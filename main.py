from crawler import Crawler
crawler = Crawler(start_url= "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm", max_pages= 300)
crawler.crawl()
crawler.generate_spider_result()
# print("total frequency for cse in all page bodies:", crawler.get_word_frequency_body("hkust"))
# print("total frequency for cse in all page titles:", crawler.get_word_frequency_title("hkust"))
from database import Database
from nltk.stem import PorterStemmer

# url = "https://comp4321-hkust.github.io/testpages/ust_cse/PG.htm"
# word = "program"

# url = "https://www.cse.ust.hk/~kwtleung/COMP4321/ust_cse/PG.htm"
# word = "postgradu"
# word = "program"
# word = "pg"

url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
word = "hkust"

page_id = crawler.get_page_id(url)

# 1. Get term frequency in body
tf_body = crawler.calculate_body_tf(page_id, word)
print(f"Frequency in body of '{word}': {tf_body}")

# 2. Get word positions in body
positions_body = crawler.get_body_positions(page_id, word) #Position: where does it appear in the page. May helps with finding a phrase
print(f"Positions in body of '{word}': {positions_body}")

# 3. Get document frequency in body
df_body = crawler.calculate_body_df(word)
print(f"DF in body of '{word}': {df_body} documents")

#4. Get max tf in a document in body
max_tf = crawler.calculate_body_maxtf(page_id)
print(f"Max tf in document '{url}': {max_tf}")

# 1. Get term frequency in title
tf_body = crawler.calculate_title_tf(page_id, word)
print(f"Frequency in title of '{word}': {tf_body}")

# 2. Get word positions in title
positions_body = crawler.get_title_positions(page_id, word) #Position: where does it appear in the page. May helps with finding a phrase
print(f"Positions in title of '{word}': {positions_body}")

# 3. Get document frequency in title
df_body = crawler.calculate_title_df(word)
print(f"DF in title of '{word}': {df_body} documents")

#4. Get max tf in a document in title
max_tf = crawler.calculate_title_maxtf(page_id)
print(f"Max tf in document in title '{url}': {max_tf}")

crawler.index.close()
