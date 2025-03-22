from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import re
from typing import List
from invertedindex import InvertedIndex
from collections import deque
import time

class Crawler:
    def __init__(self, start_url: str, max_pages: int = 30):
        self.start_url = start_url
        self.max_pages = max_pages
        self.index = InvertedIndex()
        self.visited = set()
        self.queue = deque([(start_url, None)])  # (url, parent_url)
        self.stopwords = self._load_stopwords("stopwords.txt")

    def _load_stopwords(self, path: str) -> set:
        """Load stopwords from a file."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return set(line.strip() for line in f)
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
            return True  # Simplified for example

    def _extract_title(self, html: str) -> str:
        """Extract title from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        return title_tag.get_text() if title_tag else ""

    def crawl(self):
        """Crawl using BFS and populate the database."""
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

                # Extract and index body words (filter stopwords)
                body_text = soup.get_text(separator=" ", strip=True)
                body_words = [
                    word.lower() for word in re.findall(r"\b[\w']+\b", body_text)
                    if word.lower() not in self.stopwords
                ]
                self.index.add_entry_body(
                    url, body_words,
                    last_modified=response.headers.get("Last-Modified", ""),
                    size=len(response.content)
                )

                # Extract and index title words
                title = self._extract_title(html)
                title_words = [
                    word.lower() for word in re.findall(r"\b[\w']+\b", title)
                    if word.lower() not in self.stopwords
                ]
                self.index.add_entry_title(
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
                    self.index.add_parent_child_link(parent_url, url)

                self.visited.add(url)
                page_count += 1
                time.sleep(1)

            except requests.RequestException as e:
                print(f"Error fetching {url}: {e}")

    def generate_spider_result(self):
        """Generate spider_result.txt per the project specifications."""
        with open("spider_result.txt", "w") as f:
        # Parent-child links section
            f.write("Parent-Child Links:\n")
            self.index.cursor.execute('''
            SELECT p1.url, GROUP_CONCAT(p2.url, ', ')
            FROM parent_child_links pc
            JOIN pages p1 ON pc.parent_id = p1.page_id
            JOIN pages p2 ON pc.child_id = p2.page_id
            GROUP BY p1.url
        ''')
            for parent, children in self.index.cursor.fetchall():
                f.write(f"Parent: {parent}\n")
                f.write(f"Children: {children}\n\n")

            # Inverted index section (body and title)
            f.write("\nInverted Index (Body and Title):\n")
            self.index.cursor.execute('''
                SELECT p.url, w.word, 
                    COALESCE(ib.frequency, 0) AS body_freq,
                    COALESCE(it.frequency, 0) AS title_freq
                FROM pages p
                LEFT JOIN inverted_index_body ib ON p.page_id = ib.page_id
                LEFT JOIN inverted_index_title it ON p.page_id = it.page_id
                LEFT JOIN words w ON w.word_id = ib.word_id OR w.word_id = it.word_id
                ORDER BY p.url, w.word
            ''')
            current_url = None
            for url, word, body_freq, title_freq in self.index.cursor.fetchall():
                if url != current_url:
                    current_url = url
                    f.write(f"\nURL: {url}\n")
                    f.write(f"Keywords: {self._get_top_keywords(url)}\n")  # Top 5 keywords
                if word:
                    f.write(f"{word} (Body={body_freq}, Title={title_freq})\n")

            # Page metadata (last modified and size)
            f.write("\nPage Metadata:\n")
            self.index.cursor.execute('SELECT url, last_modified, size FROM pages')
            for url, last_modified, size in self.index.cursor.fetchall():
                f.write(f"{url} | Last Modified: {last_modified} | Size: {size} bytes\n")

    def _get_top_keywords(self, url: str) -> str:
        """Get top 5 stemmed keywords (excluding stopwords) for a page."""
        self.index.cursor.execute('''
            SELECT w.word, (COALESCE(ib.frequency, 0) + COALESCE(it.frequency, 0)) AS total
            FROM words w
            LEFT JOIN inverted_index_body ib ON w.word_id = ib.word_id
            LEFT JOIN inverted_index_title it ON w.word_id = it.word_id
            LEFT JOIN pages p ON ib.page_id = p.page_id OR it.page_id = p.page_id
            WHERE p.url = ? AND w.word NOT IN ({})
            ORDER BY total DESC
            LIMIT 5
        '''.format(','.join(['?']*len(self.stopwords))), (url, *self.stopwords))
        
        keywords = [f"{word}({total})" for word, total in self.index.cursor.fetchall()]
        return '; '.join(keywords) if keywords else "None"

if __name__ == "__main__":
    crawler = Crawler("https://comp4321-hkust.github.io/testpages/testpage.htm")
    crawler.crawl()
    crawler.generate_spider_result()
    crawler.index.close()