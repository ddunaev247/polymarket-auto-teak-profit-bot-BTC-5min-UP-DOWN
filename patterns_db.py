import os
from dotenv import load_dotenv
load_dotenv()
import sqlite3
import time
import os
from collections import defaultdict
import threading
import logging

logger = logging.getLogger("PatternsDB")

DB_PATH = os.path.join(os.path.dirname(__file__), "patterns.db")
_db_lock = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Создает таблицы для сырых тиков и собранных паттернов."""
    with _db_lock:
        with _get_conn() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS event_ticks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    elapsed_seconds INTEGER NOT NULL,
                    price_offset REAL NOT NULL,
                    prob_ratio REAL NOT NULL
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_event_ticks 
                ON event_ticks (event_id, elapsed_seconds)
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pattern_vectors (
                    event_id TEXT PRIMARY KEY,
                    vector_data TEXT NOT NULL,   -- JSON массив
                    outcome TEXT                 -- "UP" или "DOWN"
                )
            ''')
            conn.commit()

# Инициализация при импорте
init_db()

def insert_tick(event_id: str, elapsed_seconds: int, price_offset: float, prob_ratio: float):
    if os.getenv("RECORD_EVENTS", "true").lower() != "true": return
    """
    Записывает сырой тик в базу. Вызывается асинхронно или из фонового треда.
    """
    if not event_id:
        return
        
    def _write():
        with _db_lock:
            try:
                with _get_conn() as conn:
                    conn.execute('''
                        INSERT INTO event_ticks 
                        (event_id, timestamp, elapsed_seconds, price_offset, prob_ratio)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (event_id, time.time(), elapsed_seconds, price_offset, prob_ratio))
                    conn.commit()
            except Exception as e:
                logger.error(f"Ошибка записи паттерна: {e}")
                
    threading.Thread(target=_write, daemon=True).start()

def aggregate_patterns(event_id: str, outcome: str = None):
    """
    Выгружает все тики события, нормализует (например, 100 точек по 3 сек = 300 сек окна)
    и сохраняет как готовый массив (вектор).
    """
    import json
    with _db_lock:
        with _get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT elapsed_seconds, price_offset, prob_ratio 
                FROM event_ticks 
                WHERE event_id = ? 
                ORDER BY elapsed_seconds ASC
            ''', (event_id,))
            rows = cursor.fetchall()
            
            if not rows:
                return
                
            # Интерполируем в стандартный вектор: размер 100 точек для 300 секунд (каждые 3 сек).
            vector = []
            row_dict = {r["elapsed_seconds"]: (r["price_offset"], r["prob_ratio"]) for r in rows}
            
            for t in range(0, 300, 3):
                # Простая линейная аппроксимация (или ближайший сосед):
                closest = min(row_dict.keys(), key=lambda k: abs(k - t), default=0)
                if closest in row_dict:
                    vector.append({
                        "t": t,
                        "p": round(row_dict[closest][0], 2),
                        "r": round(row_dict[closest][1], 2)
                    })
            
            vec_json = json.dumps(vector)
            
            conn.execute('''
                INSERT OR REPLACE INTO pattern_vectors (event_id, vector_data, outcome)
                VALUES (?, ?, ?)
            ''', (event_id, vec_json, outcome))
            conn.commit()

def fetch_all_patterns():
    """
    Возвращает список агрегированных паттернов для UI.
    """
    import json
    patterns = []
    with _db_lock:
        with _get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT event_id, vector_data, outcome FROM pattern_vectors LIMIT 50')
            for row in cursor.fetchall():
                try:
                    data = json.loads(row["vector_data"])
                    patterns.append({
                        "event_id": row["event_id"],
                        "outcome": row["outcome"],
                        "data": data
                    })
                except:
                    pass
    return patterns
