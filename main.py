from crawler import Crawler
crawler = Crawler(start_url= "https://comp4321-hkust.github.io/testpages/testpage.htm", max_pages= 30)
crawler.crawl()
crawler.generate_spider_result()
print("total frequency for cse in all page bodies:", crawler.get_word_frequency_body("hkust"))
print("total frequency for cse in all page titles:", crawler.get_word_frequency_title("hkust"))
crawler.index.close()
