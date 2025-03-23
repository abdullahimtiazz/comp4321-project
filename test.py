from crawler import Crawler 

crawler = Crawler(start_url="https://comp4321-hkust.github.io/testpages/news/cnn.htm", max_pages=2)
crawler.generate_spider_result()
crawler.index.close()