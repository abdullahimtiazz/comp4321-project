from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import re
from typing import List
from database import Database
from collections import deque
import time
from nltk.stem import PorterStemmer


class Crawler:
    def __init__(self, start_url: str, max_pages: int = 30):
        self.title=""
        self.start_url = start_url
        self.max_pages = max_pages
        self.index = Database()
        self.visited = set()
        self.queue = deque([(start_url, None)])  # (url, parent_url), BFS queue
        self.stopwords = self._load_stopwords("stopwords.txt")
        self.upload_file = "spider_result.txt"

    def close(self):
        """Close the database connection"""
        if hasattr(self, 'index'):
            self.index.close()

    def _load_stopwords(self, path: str) -> set:
        """Load and stem stopwords from a file."""

        stemmer = PorterStemmer()
        try:
            with open(path, "r", encoding="utf-8") as f:
                return set(stemmer.stem(line.strip()) for line in f)
        except FileNotFoundError:
            print(f"Warning: {path} not found. Using an empty stopwords set.")
            return set()
        except UnicodeDecodeError:
            print(f"Error decoding {path}. Check the file encoding.")
            raise

    def _should_fetch(self, url: str) -> bool:
        """Check if the URL needs to be fetched (based on last_modified)."""

        self.index.cursor.execute('SELECT last_modified FROM pages WHERE url = ?', (url,))
        row = self.index.cursor.fetchone()
        if not row:
            return True  
        else:
            response = requests.get(url, timeout=10)
            if row[0] != response.headers.get("Last-Modified", ""):
                return True
            return False

    def _extract_title(self, html: str) -> str:
        """Extract title from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        self.title = soup.find("title")
        return self.title.get_text() if self.title else ""

    def crawl(self):
        """Crawl using BFS (queue structure used) and populate the database."""

        stemmer = PorterStemmer()

        page_count = 0
        while self.queue and page_count < self.max_pages:       #queue used for BFS
            url, parent_url = self.queue.popleft()
            if url in self.visited or not self._should_fetch(url):
                continue

            print(f"Crawling: {url}")
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                html = response.text
                soup = BeautifulSoup(html, "html.parser")


                # Extract and index title words
                self.title = self._extract_title(html)
                title_words = []
                title_words_positions = []
                pos = 0
                for word in re.findall(r"\b[\w']+\b", self.title):
                    if stemmer.stem(word.lower()) not in self.stopwords:
                        title_words.append(stemmer.stem(word.lower()))
                        title_words_positions.append(pos)
                    pos += 1

                # title_words = [
                #     stemmer.stem(word.lower()) for word in re.findall(r"\b[\w']+\b", self.title)
                #     if stemmer.stem(word.lower()) not in self.stopwords
                # ]

                # Extract and index body words (filter stopwords)
                body_text = soup.get_text(separator=" ", strip=True)
                body_words = []
                body_words_positions = []
                pos = 0
                for word in re.findall(r"\b[\w']+\b", body_text):
                    if stemmer.stem(word.lower()) not in self.stopwords:
                        body_words.append(stemmer.stem(word.lower()))
                        body_words_positions.append(pos)
                    pos += 1

                # body_words = [
                #     stemmer.stem(word.lower()) for word in re.findall(r"\b[\w']+\b", body_text)
                #     if stemmer.stem(word.lower()) not in self.stopwords
                # ]

                self.index.add_entry_body(self.title,
                    url, body_words, body_words_positions,
                    last_modified=response.headers.get("Last-Modified", ""),
                    size=len(response.content)
                )

                self.index.add_entry_title(
                    self.title,
                    url, title_words, title_words_positions,
                    last_modified=response.headers.get("Last-Modified", ""),
                    size=len(response.content)
                )

                # Extract links and add to queue
                links = []
                for tag in soup.find_all("a", href=True):
                    href = tag["href"].strip()
                    if href and not href.startswith("javascript:"):
                        absolute_url = urljoin(url, href)
                        links.append(absolute_url)
                        self.queue.append((absolute_url, url))

                # Record parent-child links
                if parent_url:
                    self.index.add_parent_child_link(self.title, parent_url, url)

                self.visited.add(url)
                page_count += 1
                time.sleep(1)

            except requests.RequestException as e:
                print(f"Error fetching {url}: {e}")

    def generate_spider_result(self):
        """Generate spider_result.txt with per-page blocks separated by hyphens."""

        with open(self.upload_file, "w") as f:
            # Fetch all crawled pages
            self.index.cursor.execute('''
                SELECT title, url, last_modified, size FROM pages
            ''')
            pages = self.index.cursor.fetchall()

            for idx, (title, url, last_modified, size) in enumerate(pages):
                # Page metadata
                f.write(f"Page title: {title}\n")
                f.write(f"URL: {url}\n")
                f.write(f"Last Modification Date: {last_modified if last_modified else 'N/A'}\n")
                f.write(f"Size: {size} bytes\n")

                # Top 5 keywords (excluding stopwords)
                keywords = self._get_top_keywords(url)
                f.write(f"Keywords: {keywords}\n")

                # Parent links
                parents = self._get_parent_links(url)
                f.write(f"Parent Links: {', '.join(parents) if parents else 'None'}\n")

                # Child links
                children  = self._get_child_links(url)
                f.write(f"Child Links: {',\n'.join(children) if children else 'None'}\n")

                # Add separator (hyphens) after each page except the last one
                if idx < len(pages) - 1:
                    f.write("\n----------------\n\n")

    def _get_child_links(self, url: str) -> List[str]:
        self.index.cursor.execute('''
                    SELECT p2.url 
                    FROM parent_child_links pc
                    JOIN pages p1 ON pc.parent_id = p1.page_id
                    JOIN pages p2 ON pc.child_id = p2.page_id
                    WHERE p1.url = ?
                    LIMIT 10
                ''', (url,))
        children = [row[0] for row in self.index.cursor.fetchall()]
        return children if children else []
    
    def _get_parent_links(self, url: str) -> List[str]:
        self.index.cursor.execute('''
                    SELECT p1.url 
                    FROM parent_child_links pc
                    JOIN pages p1 ON pc.parent_id = p1.page_id
                    JOIN pages p2 ON pc.child_id = p2.page_id
                    WHERE p2.url = ?
                ''', (url,))
        parents = [row[0] for row in self.index.cursor.fetchall()]
        return parents if parents else []

    def _get_top_keywords(self, url: str) -> str:
        """Get top 5 stemmed keywords (excluding stopwords) for a page."""
        self.index.cursor.execute('''
            SELECT w.word, 
                (COALESCE(ib.frequency, 0) + COALESCE(it.frequency, 0)) AS total
            FROM words w
            LEFT JOIN inverted_index_body ib 
                ON w.word_id = ib.word_id 
                AND ib.page_id = (SELECT page_id FROM pages WHERE url = ?)
            LEFT JOIN inverted_index_title it 
                ON w.word_id = it.word_id 
                AND it.page_id = (SELECT page_id FROM pages WHERE url = ?)
            WHERE w.word NOT IN ({})
            ORDER BY total DESC
            LIMIT 5
        '''.format(', '.join(['?'] * len(self.stopwords))), 
        (url, url, *self.stopwords))
        
        keywords = [f"{word}({total})" for word, total in self.index.cursor.fetchall()]
        return '; '.join(keywords) if keywords else "None"    
        
    # def _get_top_keywords(self, url: str) -> str:
    #     """Get top 5 stemmed keywords (excluding stopwords) for a page."""
    #     self.index.cursor.execute('''
    #         SELECT w.word, 
    #             (COALESCE(ib.frequency, 0) + COALESCE(it.frequency, 0)) AS total
    #         FROM words w
    #         LEFT JOIN inverted_index_body ib 
    #             ON w.word_id = ib.word_id 
    #             AND ib.page_id = (SELECT page_id FROM pages WHERE url = ?)
    #         LEFT JOIN inverted_index_title it 
    #             ON w.word_id = it.word_id 
    #             AND it.page_id = (SELECT page_id FROM pages WHERE url = ?)
    #         WHERE w.word NOT IN ({})
    #         ORDER BY total DESC
    #         LIMIT 5
    #     '''.format(','.join(['?'] * len(self.stopwords))), 
    #     (url, url, *self.stopwords))
        
    #     keywords = [f"{word}({total})" for word, total in self.index.cursor.fetchall()]
    #     return '; '.join(keywords) if keywords else "None"
    
    # def get_word_frequency_body(self, word: str) -> int:
    #     """Get the total frequency of a word in the body across all pages."""
    #     self.index.cursor.execute('''
    #         SELECT page_frequency 
    #         FROM inverted_index_body 
    #         WHERE word_id = (SELECT word_id FROM words WHERE word = ?)
    #     ''', (word,))
    #     row = self.index.cursor.fetchone()
    #     return row[0] if row else 0
    
    # def get_word_frequency_title(self, word: str) -> int:
    #     """Get the total frequency of a word in the body across all pages."""
    #     self.index.cursor.execute('''
    #         SELECT page_frequency 
    #         FROM inverted_index_title 
    #         WHERE word_id = (SELECT word_id FROM words WHERE word = ?)
    #     ''', (word,))
    #     row = self.index.cursor.fetchone()
    #     return row[0] if row else 0

    def calculate_body_tf(self, url: str, word: str):
        """
        Input: word (Needs to be stemmed in advance)
        Calculate term frequency (TF) of a word in a document's body.
        Returns TF (how many times does this word appear in a certain document's body.)
        """
        # Get page_id and word_id
        self.index.cursor.execute("SELECT page_id FROM pages WHERE url=?", (url,))
        page_row = self.index.cursor.fetchone()
        if not page_row:
            return 0 
        page_id = page_row[0]
        
        word_id = self.index._get_or_create_word_id(word)
        
        # Get this word's frequency in the document
        self.index.cursor.execute('''
            SELECT frequency FROM inverted_index_body 
            WHERE word_id=? AND page_id=?
        ''', (word_id, page_id))
        result = self.index.cursor.fetchone()
        if not result:
            return 0
        word_freq = result[0]
        
        # Return TF
        return word_freq 

    def get_body_positions(self, url: str, word: str) -> List[int]:
        """
        Get all positions where a word appears in a document's body.
        Returns list of positions (empty list if word not found).
        """
        # Get page_id and word_id
        self.index.cursor.execute("SELECT page_id FROM pages WHERE url=?", (url,))
        page_row = self.index.cursor.fetchone()
        if not page_row:
            return []
        page_id = page_row[0]
        
        word_id = self.index._get_or_create_word_id(word)
        
        # Get positions string from database
        self.index.cursor.execute('''
            SELECT positions FROM inverted_index_body 
            WHERE word_id=? AND page_id=?
        ''', (word_id, page_id))
        result = self.index.cursor.fetchone()
        if not result or not result[0]:
            return []
        
        # Convert comma-separated string to list of integers
        return [int(pos) for pos in result[0].split(',')]
        # return result[0]
    
    def calculate_body_df(self, word: str) -> int:
        """
        Calculate document frequency (DF) of a word in all bodies.
        Returns number of documents containing this word in their body.
        """
        word_id = self.index._get_or_create_word_id(word)
        
        self.index.cursor.execute('''
            SELECT df FROM inverted_index_body_word2df
            WHERE word_id=?
        ''', (word_id, ))
        result = self.index.cursor.fetchone()
        if not result:
            return 0
        word_df = result[0]

        return word_df
    
    def calculate_body_maxtf(self, url: str):
        """
        Calculate a document's max term frequency (max_tf) 
        Returns the max count of words in a document
        """
        page_id = None
        self.index.cursor.execute('SELECT page_id FROM pages WHERE url = ?', (url,)) #Get the page_id of the url
        page_row = self.index.cursor.fetchone()
        if page_row:     # If the page is already in the table, get the page_id
            page_id = page_row[0]
            self.index.cursor.execute('''
                SELECT maxtf FROM forward_index_body_page2maxtf
                WHERE page_id=?
            ''', (page_id, ))
            result = self.index.cursor.fetchone()
            if not result:
                return 0
            else:
                maxtf = result[0]
                return maxtf

        else:
            return 0 # This url doesn't exist, the maxtf of it is 0 


    def calculate_title_tf(self, url: str, word: str) -> float:
        """
        Calculate term frequency (TF) of a word in a document's title.
        Returns TF (how many times does this word appear in a certain document's title.)
        """
        # Get page_id and word_id
        self.index.cursor.execute("SELECT page_id FROM pages WHERE url=?", (url,))
        page_row = self.index.cursor.fetchone()
        if not page_row:
            return 0
        page_id = page_row[0]
        
        word_id = self.index._get_or_create_word_id(word)
        
        # Get this word's frequency in the title
        self.index.cursor.execute('''
            SELECT frequency FROM inverted_index_title 
            WHERE word_id=? AND page_id=?
        ''', (word_id, page_id))
        result = self.index.cursor.fetchone()
        if not result:
            return 0
        word_freq = result[0]
        
        return word_freq
    
    def get_title_positions(self, url: str, word: str) -> List[int]:
        """
        Get all positions where a word appears in a document's title.
        Returns list of positions (empty list if word not found).
        """
        # Get page_id and word_id
        self.index.cursor.execute("SELECT page_id FROM pages WHERE url=?", (url,))
        page_row = self.index.cursor.fetchone()
        if not page_row:
            return []
        page_id = page_row[0]
        
        word_id = self.index._get_or_create_word_id(word)
        
        # Get positions string from database
        self.index.cursor.execute('''
            SELECT positions FROM inverted_index_title 
            WHERE word_id=? AND page_id=?
        ''', (word_id, page_id))
        result = self.index.cursor.fetchone()
        if not result or not result[0]:
            return []
        
        # Convert comma-separated string to list of integers
        return [int(pos) for pos in result[0].split(',')]
        # return result[0]
    
    def calculate_title_df(self, word: str) -> int:
        """
        Calculate document frequency (DF) of a word in all titles.
        Returns number of documents containing this word in their title.
        """
        word_id = self.index._get_or_create_word_id(word)
        
        self.index.cursor.execute('''
            SELECT df FROM inverted_index_title_word2df
            WHERE word_id=?
        ''', (word_id, ))
        result = self.index.cursor.fetchone()
        if not result:
            return 0
        word_df = result[0]

        return word_df

    def get_all_terms_in_doc(self, url: str) -> List[str]:
        """
        Retrieve all terms (stemmed) from the body and title of a document.
        Returns a list of unique terms.
        """
        # Get page_id
        self.index.cursor.execute("SELECT page_id FROM pages WHERE url=?", (url,))
        page_row = self.index.cursor.fetchone()
        if not page_row:
            return []
        page_id = page_row[0]

        # Retrieve terms from the body
        self.index.cursor.execute('''
            SELECT w.word FROM words w
            JOIN inverted_index_body ib ON w.word_id = ib.word_id
            WHERE ib.page_id = ?
        ''', (page_id,))
        body_terms = [row[0] for row in self.index.cursor.fetchall()]

        # Retrieve terms from the title
        self.index.cursor.execute('''
            SELECT w.word FROM words w
            JOIN inverted_index_title it ON w.word_id = it.word_id
            WHERE it.page_id = ?
        ''', (page_id,))
        title_terms = [row[0] for row in self.index.cursor.fetchall()]

        # Combine and return unique terms
        return list(set(body_terms + title_terms))


    def calculate_title_maxtf(self, url: str):
        """
        Calculate a document's max term frequency (max_tf) in title
        Returns the max count of words in a document's title
        """
        page_id = None
        self.index.cursor.execute('SELECT page_id FROM pages WHERE url = ?', (url,)) #Get the page_id of the url
        page_row = self.index.cursor.fetchone()
        if page_row:     # If the page is already in the table, get the page_id
            page_id = page_row[0]
            self.index.cursor.execute('''
                SELECT maxtf FROM forward_index_title_page2maxtf
                WHERE page_id=?
            ''', (page_id, ))
            result = self.index.cursor.fetchone()
            if not result:
                return 0
            else:
                maxtf = result[0]
                return maxtf

        else:
            return 0 # This url doesn't exist, the maxtf of it is 0
