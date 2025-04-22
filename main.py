from crawler import Crawler
crawler = Crawler(start_url= "https://comp4321-hkust.github.io/testpages/testpage.htm", max_pages= 30)
crawler.crawl()
crawler.generate_spider_result()
# print("total frequency for cse in all page bodies:", crawler.get_word_frequency_body("hkust"))
# print("total frequency for cse in all page titles:", crawler.get_word_frequency_title("hkust"))


# url = "https://comp4321-hkust.github.io/testpages/ust_cse/PG.htm"
# word = "program"

url = "https://comp4321-hkust.github.io/testpages/ust_cse/PG.htm"
word = "postgradu"
#The word needs to be stemmed before put into the calculate_body_tf() function

# 1. Get term frequency in title
freq_body, tf_body = crawler.calculate_body_tf(url, word)
print(f"Frequency in body of '{word}': {freq_body}")
print(f"TF in body of '{word}': {tf_body}") # TF = frequency of that word / the max word frenquency in that page

# 2. Get word positions in title
positions_body = crawler.get_body_positions(url, word) #Position: where does it appear in the page. May helps with finding a phrase
print(f"Positions in body of '{word}': {positions_body}")

# 3. Get document frequency in titles
df_body = crawler.calculate_body_df(word)
print(f"DF in body of '{word}': {df_body} documents")

crawler.index.close()
