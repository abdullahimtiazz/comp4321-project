import sqlite3
from typing import List, Tuple

class Database:
    def __init__(self, db_name: str = "search_engine.db"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """Create all tables per the schema design."""
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS pages (
                title TEXT,
                body TEXT,
                page_id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                last_modified TEXT,
                size INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS words (
                word_id INTEGER PRIMARY KEY AUTOINCREMENT,
                word TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS forward_index_body (
                word_id INTEGER,
                page_id INTEGER,
                frequency INTEGER,
                PRIMARY KEY (word_id, page_id),
                FOREIGN KEY (word_id) REFERENCES words(word_id),
                FOREIGN KEY (page_id) REFERENCES pages(page_id)
            );
            
            CREATE TABLE IF NOT EXISTS forward_index_title (
                word_id INTEGER,
                page_id INTEGER,
                frequency INTEGER,
                PRIMARY KEY (word_id, page_id),
                FOREIGN KEY (word_id) REFERENCES words(word_id),
                FOREIGN KEY (page_id) REFERENCES pages(page_id)
            );
                                  
            CREATE TABLE IF NOT EXISTS inverted_index_body (
                word_id INTEGER,
                page_frequency INTEGER,
                PRIMARY KEY (word_id)
                FOREIGN KEY (word_id) REFERENCES words(word_id)
            );
                                  
            CREATE TABLE IF NOT EXISTS inverted_index_title (
                word_id INTEGER,
                page_frequency INTEGER,
                PRIMARY KEY (word_id)
                FOREIGN KEY (word_id) REFERENCES words(word_id)
            );
            
            CREATE TABLE IF NOT EXISTS parent_child_links (
                parent_id INTEGER,
                child_id INTEGER,
                PRIMARY KEY (parent_id, child_id),
                FOREIGN KEY (parent_id) REFERENCES pages(page_id),
                FOREIGN KEY (child_id) REFERENCES pages(page_id)
            );
        ''')
        self.conn.commit()

    def _get_or_create_word_id(self, word: str) -> int:
        """Get word_id or insert a new word into `words` table."""
        self.cursor.execute('SELECT word_id FROM words WHERE word = ?', (word,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        else:
            self.cursor.execute('INSERT INTO words (word) VALUES (?)', (word,))
            self.conn.commit()
            return self.cursor.lastrowid

    def _get_or_create_page_id(self, title:str, url: str, last_modified: str, size: int) -> int:
        """Get page_id or insert a new page into `pages` table."""
        self.cursor.execute('SELECT page_id FROM pages WHERE url = ?', (url,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        else:
            self.cursor.execute('''
                INSERT INTO pages (title, url, last_modified, size)
                VALUES (?, ?, ?, ?)
            ''', (title, url, last_modified, size))
            self.conn.commit()
            return self.cursor.lastrowid

    def add_entry_body(self, title: str, url: str, words: List[str], last_modified: str, size: int):
        """Add words from the page body to forward_index_body."""
        page_id = self._get_or_create_page_id(title, url, last_modified, size)
        word_freq = {}
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1

        for word, freq in word_freq.items():
            word_id = self._get_or_create_word_id(word)
            self.cursor.execute('''
                INSERT OR REPLACE INTO forward_index_body (word_id, page_id, frequency)
                VALUES (?, ?, COALESCE((SELECT frequency FROM forward_index_body WHERE word_id=? AND page_id=?), 0) + ?)
            ''', (word_id, page_id, word_id, page_id, freq))
        self.conn.commit()

    def add_entry_title(self, title: str,url: str, words: List[str], last_modified: str, size: int):
        """Add words from the page title to forward_index_title."""
        page_id = self._get_or_create_page_id(title, url, last_modified, size)
        word_freq = {}
        for word in words:
            word_freq[word] = word_freq.get(word, 0) + 1

        for word, freq in word_freq.items():
            word_id = self._get_or_create_word_id(word)
            self.cursor.execute('''
                INSERT OR REPLACE INTO forward_index_title (word_id, page_id, frequency)
                VALUES (?, ?, COALESCE((SELECT frequency FROM forward_index_title WHERE word_id=? AND page_id=?), 0) + ?)
            ''', (word_id, page_id, word_id, page_id, freq))
        self.conn.commit()

    def add_parent_child_link(self, title: str, parent_url: str, child_url: str):
        """Add parent-child relationship."""
        parent_id = self._get_or_create_page_id(title, parent_url, None, None)
        child_id = self._get_or_create_page_id(title, child_url, None, None)
        self.cursor.execute('''
            INSERT OR IGNORE INTO parent_child_links (parent_id, child_id)
            VALUES (?, ?)
        ''', (parent_id, child_id))
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.conn.close()