import sqlite3
import json
from datetime import datetime
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sensibull.db')

def get_db():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    
    # Table to store unique profiles
    c.execute('''
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT,
            url TEXT
        )
    ''')
    
    # Table to store every raw snapshot (optional, can be large, maybe we only store changes?)
    # For now, let's store only if something changed, but we need the "latest" state to compare.
    # Actually, let's store every scheduled fetch's status or just the changes.
    # User wants: "Every cell has the number of times the person have taken a trade in that day... clickable link... table which has time and trade data... change with respect to previous time"
    # To support this, we need to store the full snapshot whenever it changes, so we can diff it against the previous one on demand, 
    # OR store the diffs directly. Storing full snapshots is safer and easier to rebuild.
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            raw_data JSON NOT NULL,
            created_at_source TEXT,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
    ''')
    
    # Table to record that a change was detected (for easy indexing)
    c.execute('''
        CREATE TABLE IF NOT EXISTS position_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            profile_id INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            diff_summary TEXT, -- JSON summary of what changed (e.g. "Added 1 trade, Removed 1 trade")
            FOREIGN KEY (snapshot_id) REFERENCES snapshots (id),
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized.")

def sync_profiles():
    # Helper to load profiles from file and ensure they exist in DB
    URLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'urls.txt')
    if not os.path.exists(URLS_FILE):
        return

    with open(URLS_FILE, 'r') as f:
        # Filter lines that are empty or start with #
        slugs = [line.strip().split('/')[-1] for line in f if line.strip() and not line.startswith('#')]
        # If full URL is given, extract slug, else assume slug
        clean_slugs = []
        for s in slugs:
            if 'sensibull.com' in s:
                 clean_slugs.append(s.split('/')[-1])
            else:
                 clean_slugs.append(s)
    
    conn = get_db()
    c = conn.cursor()
    for slug in clean_slugs:
        c.execute("INSERT OR IGNORE INTO profiles (slug, name) VALUES (?, ?)", (slug, slug))
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
