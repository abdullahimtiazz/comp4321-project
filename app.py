import flask as f
from main import search_logic  # Import the search logic function from main.py

app = f.Flask(__name__)

@app.route('/')
def home():
    return f.render_template('index.html', results=None)

@app.route('/search', methods=['GET', 'POST'])
def search():
    if f.request.method == 'POST':
        query = f.request.form['query']
        page = 1  # Start at the first page for a new search
    else:
        query = f.request.args.get('query', '')  # Retrieve query from URL parameters
        page = int(f.request.args.get('page', 1))  # Get the current page, default to 1

    results_per_page = 7  # Number of results per page

    # Call the search logic from main.py
    all_results = search_logic(query)  # Fetch all results using the backend logic

    # Paginate results
    start = (page - 1) * results_per_page
    end = start + results_per_page
    paginated_results = all_results[start:end]
    total_pages = (len(all_results) + results_per_page - 1) // results_per_page  # Calculate total pages

    return f.render_template('index.html', results=paginated_results, query=query, page=page, total_pages=total_pages)

if __name__ == '__main__':
    app.run(debug=True)
