from crawler import Crawler

crawler = Crawler(start_url= "https://comp4321-hkust.github.io/testpages/testpage.htm", max_pages= 5)
crawler.crawl()
crawler.generate_spider_result()
crawler.index.close()