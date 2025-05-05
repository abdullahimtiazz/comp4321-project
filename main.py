from crawler import Crawler
from search import parse_query, search_engine, print_results    

def main():
    # 1. Start crawler (if you want to recrawl, uncomment next lines)
    # start_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
    # crawler = Crawler(start_url)
    # crawler.crawl()
    # crawler.generate_spider_result()

    # Or, just connect to existing DB:
    start_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
    crawler = Crawler(start_url)

    while True:
        print("\n=== SEARCH ENGINE ===")
        query = input('Enter your query (or type "exit" to quit): ').strip()
        if query.lower() == "exit":
            break
        results = search_engine(crawler, query)
        print_results(crawler, results)

if __name__ == "__main__":
    main()

