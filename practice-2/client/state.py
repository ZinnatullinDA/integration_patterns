import os
import sqlite3
from pathlib import Path
from uuid import uuid4


def open_client_db():
    path = os.getenv('CLIENT_STATE_PATH', '/data/client.sqlite3')
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    initialize_numbers(db)
    return db


def initialize_numbers(db):
    with db:
        db.execute('''CREATE TABLE IF NOT EXISTS request_numbers (
            number INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL UNIQUE
        )''')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='startup_requests'").fetchone():
            db.execute('''INSERT INTO request_numbers(request_id)
                SELECT s.request_id FROM startup_requests s
                WHERE NOT EXISTS (SELECT 1 FROM request_numbers n WHERE n.request_id=s.request_id)
                ORDER BY s.request_id''')


def request_number(db, request_id):
    if not isinstance(request_id, str):
        request_id = f'invalid-request:{uuid4()}'
    with db:
        db.execute('''INSERT INTO request_numbers(request_id)
            SELECT ? WHERE NOT EXISTS (SELECT 1 FROM request_numbers WHERE request_id=?)''',
                   (request_id, request_id))
        return db.execute('SELECT number FROM request_numbers WHERE request_id=?', (request_id,)).fetchone()[0]
