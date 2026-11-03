import json
from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import time
import uuid


class Store:
    def __init__(self, path=None):
        self.path = path or os.getenv('LAB_DB', '.data/kitchen.sqlite')
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS effects (
                    id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, order_id TEXT NOT NULL,
                    stage TEXT NOT NULL, attempt_id TEXT NOT NULL, timestamp REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS completed (
                    order_id TEXT NOT NULL, stage TEXT NOT NULL, effect_id INTEGER NOT NULL,
                    PRIMARY KEY(order_id, stage));
                CREATE TABLE IF NOT EXISTS attempts (
                    order_id TEXT NOT NULL, stage TEXT NOT NULL, count INTEGER NOT NULL,
                    PRIMARY KEY(order_id, stage));
                CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL);
            ''')
            db.execute('INSERT OR IGNORE INTO meta VALUES (?, ?)', ('session_id', str(uuid.uuid4())))

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=10000')
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, key, default=None):
        with self.connect() as db:
            row = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
            if row is None:
                return default
            if key == 'session_id':
                return row['value']
            return json.loads(row['value'])

    def set(self, key, value):
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO meta VALUES (?, ?)', (key, json.dumps(value)))

    def accept(self, task):
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO orders VALUES (?, ?)', (task.order_id, task.model_dump_json()))

    def save_event(self, event):
        with self.connect() as db:
            return db.execute('INSERT OR IGNORE INTO events(event_id,payload) VALUES (?,?)',
                              (event.event_id, event.model_dump_json())).rowcount == 1

    def history(self):
        with self.connect() as db:
            return ([json.loads(row['payload']) for row in db.execute('SELECT payload FROM orders ORDER BY rowid')],
                    [json.loads(row['payload']) for row in db.execute('SELECT payload FROM events ORDER BY seq')])

    def next_attempt(self, order_id, stage):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO attempts VALUES (?,?,1) ON CONFLICT(order_id,stage) DO UPDATE SET count=count+1', (order_id, stage))
            return db.execute('SELECT count FROM attempts WHERE order_id=? AND stage=?', (order_id, stage)).fetchone()['count']

    def effect(self, task, stage, attempt_id, idempotency):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # Reject stale work even if a reset raced an old delivery.
            if db.execute("SELECT value FROM meta WHERE key='session_id'").fetchone()['value'] != task.session_id:
                raise ValueError('Stale session cannot record effects')
            found = db.execute('SELECT effect_id FROM completed WHERE order_id=? AND stage=?',
                               (task.order_id, stage)).fetchone()
            if idempotency and found:
                return False, found['effect_id']
            cursor = db.execute('INSERT INTO effects(session_id,order_id,stage,attempt_id,timestamp) VALUES (?,?,?,?,?)',
                                (task.session_id, task.order_id, stage, attempt_id, time.time()))
            db.execute('INSERT OR IGNORE INTO completed VALUES (?,?,?)', (task.order_id, stage, cursor.lastrowid))
            return True, cursor.lastrowid

    def effects(self, order_id=None):
        with self.connect() as db:
            if order_id:
                rows = db.execute('SELECT * FROM effects WHERE order_id=? ORDER BY id', (order_id,))
            else:
                rows = db.execute('SELECT stage, count(*) AS count FROM effects GROUP BY stage')
            return [dict(row) for row in rows]

    def import_completed_snapshot(self, snapshot):
        orders = snapshot['orders']
        if len(orders) != snapshot['accepted_count'] or any(order['category'] != 'ready' for order in orders):
            raise ValueError('Drain the v0 demo before importing its completed snapshot')
        from contracts import Task
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT count(*) FROM orders').fetchone()[0] or db.execute('SELECT count(*) FROM effects').fetchone()[0]:
                raise ValueError('Migration requires an empty new lab store')
            db.execute("UPDATE meta SET value=? WHERE key='session_id'", (snapshot['session_id'],))
            for order in orders:
                task = Task(**{key: order[key] for key in ('order_id','session_id','label','pizza_type')})
                db.execute('INSERT INTO orders VALUES (?,?)', (task.order_id,task.model_dump_json()))
            db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', ('legacy_states', json.dumps({order['order_id']: {
                'category':'ready','stage':'pack','rank':6,'history_unavailable':True,
                'redelivered':order.get('redelivered',False)} for order in orders})))

    def reset(self):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for table in ('effects', 'completed', 'attempts', 'orders', 'events', 'meta'):
                db.execute(f'DELETE FROM {table}')
            session = str(uuid.uuid4())
            db.execute('INSERT INTO meta VALUES (?,?)', ('session_id', session))
            return session
