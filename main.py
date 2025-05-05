from crawler import Crawler
from search import parse_query, search_engine, print_results    
<<<<<<< HEAD
=======

def search_logic(query):
    # Implement your search logic here
    # This is a dummy implementation for demonstration
    return [
        {'title': f'Result {i}', 'url': f'http://example.com/{i}', 'snippet': f'This is a sample result {i} for query: {query}'}
        for i in range(1, 51)  # Simulating 50 results
    ]
>>>>>>> 7ff61d81bb451ab9660094485e5139bc48e06ce4

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

