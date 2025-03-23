from crawler import Crawler

crawler = Crawler(start_url= "https://comp4321-hkust.github.io/testpages/testpage.htm", max_pages= 30) 
# you may choose to change the start_url to another url for testing, and max_pages to a different number.

crawler.crawl() # start crawling
crawler.generate_spider_result()    # output the contents in the database to spider_result.txt in the format specified in README.md
crawler.index.close()   # gracefully close the database connection