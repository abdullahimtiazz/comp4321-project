import sqlite3
from typing import List, Tuple

class Database:
    def __init__(self, db_name: str = "search_engine.db"):  # Create a database connection and cursor at search_engine.db
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

            CREATE TABLE IF NOT EXISTS inverted_index_body (
                word_id INTEGER,
                page_id INTEGER,
                frequency INTEGER,
                positions TEXT,   
                PRIMARY KEY (word_id, page_id),
                FOREIGN KEY (word_id) REFERENCES words(word_id),
                FOREIGN KEY (page_id) REFERENCES pages(page_id)
            );
            
            CREATE TABLE IF NOT EXISTS inverted_index_title (
                word_id INTEGER,
                page_id INTEGER,
                frequency INTEGER,
                positions TEXT, 
                PRIMARY KEY (word_id, page_id),
                FOREIGN KEY (word_id) REFERENCES words(word_id),
                FOREIGN KEY (page_id) REFERENCES pages(page_id)
            );
            
            
            CREATE TABLE IF NOT EXISTS parent_child_links (
                parent_id INTEGER,
                child_id INTEGER,
                PRIMARY KEY (parent_id, child_id),
                FOREIGN KEY (parent_id) REFERENCES pages(page_id),
                FOREIGN KEY (child_id) REFERENCES pages(page_id)
            );
            
            CREATE TABLE IF NOT EXISTS inverted_index_body_word2df (
                word_id INTEGER,
                df INTEGER,
                PRIMARY KEY (word_id)
                FOREIGN KEY (word_id) REFERENCES words(word_id)
            );
                                  
            CREATE TABLE IF NOT EXISTS inverted_index_title_word2df (
                word_id INTEGER,
                df INTEGER,
                PRIMARY KEY (word_id)
                FOREIGN KEY (word_id) REFERENCES words(word_id)
            );
            
            CREATE TABLE IF NOT EXISTS forward_index_body_page2maxtf (
                page_id INTEGER,
                maxtf INTEGER,
                PRIMARY KEY (page_id)
                FOREIGN KEY (page_id) REFERENCES pages(page_id)
            );
                                  
            CREATE TABLE IF NOT EXISTS forward_index_title_page2maxtf (
                page_id INTEGER,
                maxtf INTEGER,
                PRIMARY KEY (page_id)
                FOREIGN KEY (page_id) REFERENCES pages(page_id)
            );
                                  
        ''')
        self.conn.commit()

    def _get_or_create_word_id(self, word: str) -> int: 
        """Get word_id or insert a new word into `words` table."""

        self.cursor.execute('SELECT word_id FROM words WHERE word = ?', (word,))
        row = self.cursor.fetchone()
        if row:     # If the word is already in the table, return the word_id
            return row[0]
        else:       # else, insert the word and return the word_id.
            self.cursor.execute('INSERT INTO words (word) VALUES (?)', (word,))
            self.conn.commit()
            return self.cursor.lastrowid

    def _get_or_create_page_id(self, title:str, url: str, last_modified: str, size: int) -> int:
        """Get page_id or insert a new page into `pages` table."""

        self.cursor.execute('SELECT page_id FROM pages WHERE url = ?', (url,))
        row = self.cursor.fetchone()
        if row:     # If the page is already in the table, return the page_id
            return row[0]
        else:       # else, insert the page and return the page_id.
            self.cursor.execute('''
                INSERT INTO pages (title, url, last_modified, size)
                VALUES (?, ?, ?, ?)
            ''', (title, url, last_modified, size))
            self.conn.commit()
            return self.cursor.lastrowid
    
    # def _update_inverted_index_body(self, word_id: int):
    #     """Update the inverted_index_body table with the total frequency of a word."""
    #     # print("ok")
    #     self.cursor.execute('''
    #         SELECT SUM(frequency) 
    #         FROM forward_index_body 
    #         WHERE word_id = ?
    #     ''', (word_id,))
    #     total_frequency = self.cursor.fetchone()[0] or 0

    #     self.cursor.execute('''
    #         INSERT OR REPLACE INTO inverted_index_body (word_id, page_frequency)
    #         VALUES (?, ?)
    #     ''', (word_id, total_frequency))
    #     # print("word_id: ", word_id, "total_frequency: ", total_frequency)
    #     self.conn.commit()

    # def _update_inverted_index_title(self, word_id: int):
    #     """Update the inverted_index_title table with the total frequency of a word."""
    #     self.cursor.execute('''
    #         SELECT SUM(frequency) 
    #         FROM forward_index_title 
    #         WHERE word_id = ?
    #     ''', (word_id,))
    #     total_frequency = self.cursor.fetchone()[0] or 0

    #     self.cursor.execute('''
    #         INSERT OR REPLACE INTO inverted_index_title (word_id, page_frequency)
    #         VALUES (?, ?)
    #     ''', (word_id, total_frequency))
    #     self.conn.commit()

    def add_entry_body(self, title: str, url: str, words: List[str], words_positions: List[int], last_modified: str, size: int):  
        """Add words from the page body to inverted_index_body."""
        page_id = self._get_or_create_page_id(title, url, last_modified, size)
        word_data = {}
        for word, pos in zip(words, words_positions):      # Count the frequency of each word in the body of the page.
            if word not in word_data:
                word_data[word] = {
                    'frequency': 0,
                    'positions': []
                }
            word_data[word]['frequency'] += 1
            word_data[word]['positions'].append(str(pos))

        max_tf = 0
        for word, data in word_data.items():    #Insert into the database.
            word_id = self._get_or_create_word_id(word)
            positions_str = ','.join(data['positions'])  # Convert positions list to comma-separated string
            self.cursor.execute('''
                INSERT OR REPLACE INTO inverted_index_body 
                (word_id, page_id, frequency, positions)
                VALUES (?, ?, 
                    COALESCE((SELECT frequency FROM inverted_index_body WHERE word_id=? AND page_id=?), 0) + ?,
                    COALESCE((SELECT positions FROM inverted_index_body WHERE word_id=? AND page_id=?), '') || ?
                )
            ''', 
                (word_id, page_id, 
                word_id, page_id, data['frequency'],
                word_id, page_id, positions_str)
            )

            max_tf = max(max_tf, data['frequency'])

            #COALESCE((SELECT positions FROM inverted_index_body WHERE word_id=? AND page_id=?), '') || ?
            # ',' + positions_str if data['frequency'] > 1 else positions_str

            # # print("ok")
            # # self._update_inverted_index_body(word_id)  # Update inverted index
            self.cursor.execute('''
                INSERT OR REPLACE INTO inverted_index_body_word2df (word_id, df)
                VALUES (?, COALESCE((SELECT df FROM inverted_index_body_word2df WHERE word_id=?), 0) + ?)
            ''', (word_id, word_id, 1))
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO forward_index_body_page2maptf (page_id, maxtf)
            VALUES (?, ?)
        ''', (page_id, max_tf))

        self.conn.commit()

    def add_entry_title(self, title: str,url: str, words: List[str], words_positions: List[int], last_modified: str, size: int):
        """Add words from the page title to inverted_index_title."""

        page_id = self._get_or_create_page_id(title, url, last_modified, size)
        word_data = {}
        for word, pos in zip(words, words_positions):      # Count the frequency of each word in the body of the page.
            if word not in word_data:
                word_data[word] = {
                    'frequency': 0,
                    'positions': []
                }
            word_data[word]['frequency'] += 1
            word_data[word]['positions'].append(str(pos))

        max_tf = 0
        for word, data in word_data.items():    #Insert into the database.
            word_id = self._get_or_create_word_id(word)
            positions_str = ','.join(data['positions'])  # Convert positions list to comma-separated string
            self.cursor.execute('''
                INSERT OR REPLACE INTO inverted_index_title 
                (word_id, page_id, frequency, positions)
                VALUES (?, ?, 
                    COALESCE((SELECT frequency FROM inverted_index_title WHERE word_id=? AND page_id=?), 0) + ?,
                    COALESCE((SELECT positions FROM inverted_index_title WHERE word_id=? AND page_id=?), '') || ?
                )
            ''', (
                word_id, 
                page_id, 
                word_id, page_id, data['frequency'],
                word_id, page_id, positions_str
            ))
            max_tf = max(max_tf, data['frequency'])
            # # self._update_inverted_index_title(word_id)  # Update inverted index
            self.cursor.execute('''
                INSERT OR REPLACE INTO inverted_index_title_word2df (word_id, df)
                VALUES (?, COALESCE((SELECT df FROM inverted_index_title_word2df WHERE word_id=?), 0) + ?)
            ''', (word_id, word_id, 1))

        self.cursor.execute('''
            INSERT OR REPLACE INTO forward_index_title_page2maptf (page_id, maxtf)
            VALUES (?, ?)
        ''', (page_id, max_tf))

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
    def get_docs_containing_word_body(self, word: str):
        """Return list of URLs where 'word' appears in the body."""
        self.cursor.execute("""
            SELECT DISTINCT p.url
            FROM inverted_index_body i
            JOIN pages p ON i.page_id = p.page_id
            JOIN words w ON i.word_id = w.word_id
            WHERE w.word = ?
        """, (word,))
        return [row[0] for row in self.cursor.fetchall()]

    def get_docs_containing_word_title(self, word: str):
        """Return list of URLs where 'word' appears in the title."""
        self.cursor.execute("""
            SELECT DISTINCT p.url
            FROM inverted_index_title i
            JOIN pages p ON i.page_id = p.page_id
            JOIN words w ON i.word_id = w.word_id
            WHERE w.word = ?
        """, (word,))
        return [row[0] for row in self.cursor.fetchall()]

    def get_total_doc_count(self):
        """Return the total number of documents in the database."""
        self.cursor.execute("SELECT COUNT(*) FROM pages")
        row = self.cursor.fetchone()
        return row[0] if row else 0
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()


