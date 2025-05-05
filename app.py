import flask as f
from search import search_engine
from crawler import Crawler

app = f.Flask(__name__)
START_URL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"

@app.route('/')
def home():
    return f.render_template('index.html', results=None)

@app.route('/search', methods=['GET', 'POST'])
def search():
    # Create a new crawler instance for each request
    crawler = Crawler(START_URL)
    
    if f.request.method == 'POST':
        query = f.request.form['query']
        page = 1
    else:
        query = f.request.args.get('query', '')
        page = int(f.request.args.get('page', 1))

    results_per_page = 7

    if query:
        # Call the search engine with the query
        search_results = search_engine(crawler, query)
        all_results = []
        # Format the results into the required structure
        for rank, (url, score) in enumerate(search_results, 1):
        # Fetch title from DB
            crawler.index.cursor.execute("SELECT title FROM pages WHERE url=?", (url,))
            row = crawler.index.cursor.fetchone()
            # title = row[0] if row else "No Title"
            # print(f"{rank}. [{title.strip() if title else 'No Title'}]")
            # print(f"   URL: {url}")
            # print(f"   Score: {score:.4f}")
            all_results.append(
                {
                    'title': row[0] if row else "No Title",
                    'score': score,
                    'url': url,
                    'rank': rank
                }
            )
        print(all_results)
        # Close the database connection
        crawler.close()
    else:
        all_results = []

    # Paginate results
    start = (page - 1) * results_per_page
    end = start + results_per_page
    paginated_results = all_results[start:end]
    total_pages = (len(all_results) + results_per_page - 1) // results_per_page

    return f.render_template('index.html', 
                           results=paginated_results, 
                           query=query, 
                           page=page, 
                           total_pages=total_pages)

if __name__ == '__main__':
    app.run(debug=True)
