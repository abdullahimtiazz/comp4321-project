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
        self.queue = deque([(start_url, None)])  # (url, parent_url)
        self.stopwords = self._load_stopwords("stopwords.txt")
        self.upload_file = "spider_result.txt"

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
            return True  # New URL
        else:
            # Implement logic to check last_modified (requires HTTP HEAD request)
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
        """Crawl using BFS and populate the database."""
        stemmer = PorterStemmer()

        page_count = 0
        while self.queue and page_count < self.max_pages:
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
                title_words = [
                    stemmer.stem(word.lower()) for word in re.findall(r"\b[\w']+\b", self.title)
                    if stemmer.stem(word.lower()) not in self.stopwords
                ]

                # Extract and index body words (filter stopwords)
                body_text = soup.get_text(separator=" ", strip=True)
                body_words = [
                    stemmer.stem(word.lower()) for word in re.findall(r"\b[\w']+\b", body_text)
                    if stemmer.stem(word.lower()) not in self.stopwords
                ]

                self.index.add_entry_body(self.title,
                    url, body_words,
                    last_modified=response.headers.get("Last-Modified", ""),
                    size=len(response.content)
                )

                self.index.add_entry_title(
                    self.title,
                    url, title_words,
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
                self.index.cursor.execute('''
                    SELECT p1.url 
                    FROM parent_child_links pc
                    JOIN pages p1 ON pc.parent_id = p1.page_id
                    JOIN pages p2 ON pc.child_id = p2.page_id
                    WHERE p2.url = ?
                ''', (url,))
                parents = [row[0] for row in self.index.cursor.fetchall()]
                f.write(f"Parent Links: {', '.join(parents) if parents else 'None'}\n")

                # Child links
                self.index.cursor.execute('''
                    SELECT p2.url 
                    FROM parent_child_links pc
                    JOIN pages p1 ON pc.parent_id = p1.page_id
                    JOIN pages p2 ON pc.child_id = p2.page_id
                    WHERE p1.url = ?
                    LIMIT 10
                ''', (url,))
                children = [row[0] for row in self.index.cursor.fetchall()]
                f.write(f"Child Links: {',\n'.join(children) if children else 'None'}\n")

                # Add separator (hyphens) after each page except the last one
                if idx < len(pages) - 1:
                    f.write("\n----------------\n\n")

    def _get_top_keywords(self, url: str) -> str:
        """Get top 5 stemmed keywords (excluding stopwords) for a page."""
        self.index.cursor.execute('''
            SELECT w.word, 
                (COALESCE(ib.frequency, 0) + COALESCE(it.frequency, 0)) AS total
            FROM words w
            LEFT JOIN forward_index_body ib 
                ON w.word_id = ib.word_id 
                AND ib.page_id = (SELECT page_id FROM pages WHERE url = ?)
            LEFT JOIN forward_index_title it 
                ON w.word_id = it.word_id 
                AND it.page_id = (SELECT page_id FROM pages WHERE url = ?)
            WHERE w.word NOT IN ({})
            ORDER BY total DESC
            LIMIT 5
        '''.format(','.join(['?'] * len(self.stopwords))), 
        (url, url, *self.stopwords))
        
        keywords = [f"{word}({total})" for word, total in self.index.cursor.fetchall()]
        return '; '.join(keywords) if keywords else "None"
