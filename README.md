# Welcome to this enterprise search engine :

i'll be creating this as a project for a course on search engines at HKUST. 
stay tuned :))

# To the TAs:
CODE DIRECTORY:
- crawler.py: Stores the functionality for crawling through the web and displaying data from database to spider_result.txt
- database.py: Stores the SQLite3 database set up and functions to support interaction with database.
- main.py: Program entry point, can adjust the no. of max. pages to crawl through and the starting URL from here in the class constructor call. 
- requirements.txt: Text file to store dependency information. For installation, refer to Installation section.
- search_engine.db: Database to store crawled webpages and data.
- spider_result.txt: Output file. For format, refer to Output Format section.
- stopwords.txt: Text file to store a list of all stopwords. 



INSTRUCTIONS:
- python 3.12.2
- other dependencies: nltk, BeautifulSoup4, requests, sqlite3 (may have others, this is all from what I remembered)
 All dependencies along with version details are stored in requirements.txt

1. Create a virtual environment: python -m venv .venv
2. Open this virtual environment: .venv/Scripts/activate (or .venv/bin/activate on MacOS)
3. Install requirements: pip install - r requirements.txt
4. Test program and get "spider_result.txt": python main.py


OUTPUT FORMAT:
The output is written into spider_result.txt. The information is displayed as follows:
    Page title:
    URL:
    Last Modification Date:
    Size: 
    Keywords: 
    Parent Links:
    Child Links:

    ---------------

    the labelling is self-explanatory. Keywords is formatted as <word>(<frequency>); 

DATABASE SCHEMA:


