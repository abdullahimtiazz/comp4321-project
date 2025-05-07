import flask as f
from search import search_engine
from crawler import Crawler
from dotenv import load_dotenv
import os

load_dotenv()
app = f.Flask(__name__)
app.secret_key = os.getenv('FLASH_SECRET_KEY')
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

        if search_results:
            # Format the results into the required structure
            for rank, (url, score) in enumerate(search_results, 1):
                # Fetch title from DB
                crawler.index.cursor.execute("SELECT title, last_modified, size FROM pages WHERE url=?", (url,))
                row = crawler.index.cursor.fetchone()
                top_keywords = crawler._get_top_keywords(url)
                parent_links = crawler._get_parent_links(url)
                child_links = crawler._get_child_links(url)
                all_results.append(
                    {
                        'title': row[0] if row else "No Title",
                        'score': score,
                        'url': url,
                        'rank': rank,
                        'last_modified': row[1] if row else "Last Modified Not Found",
                        'size': row[2] if row else "Size Not Found",
                        'keywords': top_keywords,
                        'parent_links': parent_links,
                        'child_links': child_links
                    }
                )
            
        else: 
            all_results = []
            f.flash(f'No results found for "{query}"', 'info')
            print("I got here Abdullah!")
        # Handle empty results case with a flash message

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

@app.route('/similar', methods=['GET'])
def get_similar_pages():
    url = f.request.args.get('url', '')
    if not url:
        f.flash('No URL provided for finding similar pages.', 'error')
        return f.redirect(f.url_for('home'))

    # Create a new crawler instance
    crawler = Crawler(START_URL)
    try:
        # Debug log for the URL being processed
        print(f"Finding similar pages for URL: {url}")

        # Fetch the top-5 keywords for the given URL
        top_keywords = crawler._get_top_keywords(url)
        if not top_keywords:
            f.flash(f'No keywords found for "{url}".', 'info')
            print(f"No keywords found for URL: {url}")
            paginated_results = []
            total_pages = 0
        else:
            # Construct a new query using the top-5 keywords
            new_query = ' '.join(top_keywords)
            print(f"New query constructed from top-5 keywords: {new_query}")

            # Use the search engine to find similar pages
            search_results = search_engine(crawler, new_query)
            all_results = []
            for rank, (similar_url, score) in enumerate(search_results, 1):
                crawler.index.cursor.execute("SELECT title, last_modified, size FROM pages WHERE url=?", (similar_url,))
                row = crawler.index.cursor.fetchone()
                top_keywords = crawler._get_top_keywords(similar_url)
                parent_links = crawler._get_parent_links(similar_url)
                child_links = crawler._get_child_links(similar_url)
                all_results.append(
                    {
                        'title': row[0] if row else "No Title",
                        'score': score,
                        'url': similar_url,
                        'rank': rank,
                        'last_modified': row[1] if row else "Last Modified Not Found",
                        'size': row[2] if row else "Size Not Found",
                        'keywords': top_keywords,
                        'parent_links': parent_links,
                        'child_links': child_links
                    }
                )
            
            # Paginate results
            results_per_page = 7
            page = int(f.request.args.get('page', 1))
            start = (page - 1) * results_per_page
            end = start + results_per_page
            paginated_results = all_results[start:end]
            total_pages = (len(all_results) + results_per_page - 1) // results_per_page
    except Exception as e:
        # Log the exception for debugging
        print(f"Error while finding similar pages for URL {url}: {e}")
        f.flash('An error occurred while finding similar pages.', 'error')
        paginated_results = []
        total_pages = 0
    finally:
        # Close the database connection
        crawler.close()

    return f.render_template('index.html', 
                           results=paginated_results, 
                           query=new_query if 'new_query' in locals() else '', 
                           page=page, 
                           total_pages=total_pages)

if __name__ == '__main__':
    app.run(debug=True)
