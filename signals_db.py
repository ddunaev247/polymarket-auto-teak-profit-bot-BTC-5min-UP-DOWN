import os
from dotenv import load_dotenv
load_dotenv()
import sqlite3
import os
import json
import logging
from collections import defaultdict
import threading
import math

logger = logging.getLogger("SignalsDB")

SIGNALS_DB_PATH = os.path.join(os.path.dirname(__file__), "signals.db")
PATTERNS_DB_PATH = os.path.join(os.path.dirname(__file__), "patterns.db")

_db_lock = threading.Lock()
_cached_signals = None
_last_cache_time = 0

def _get_conn():
    conn = sqlite3.connect(SIGNALS_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Создает базу для записи прибыльных/успешных сделок пользователя."""
    with _db_lock:
        with _get_conn() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS profitable_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    buy_price REAL NOT NULL,
                    sell_price REAL NOT NULL,
                    profit REAL NOT NULL,
                    buy_time REAL NOT NULL,
                    sell_time REAL NOT NULL,
                    pattern_vector TEXT NOT NULL
                )
            ''')
            conn.commit()

# Инициализация при импорте
init_db()

def record_successful_trade(event_id: str, side: str, buy_price: float, sell_price: float,
                              profit: float, buy_time: float, sell_time: float, window_start: int):
    if os.getenv("RECORD_EVENTS", "true").lower() != "true": return
    """
    Выгружает тики из patterns.db до момента buy_time и сохраняет их как паттерн сигнала.
    """
    if not event_id:
        return

    def _task():
        try:
            # Получаем сырые данные из базы тепловых карт
            p_conn = sqlite3.connect(PATTERNS_DB_PATH)
            p_conn.row_factory = sqlite3.Row
            cursor = p_conn.cursor()
            
            # Вычисляем секунду внутри 5-минутного окна, когда мы ВХОДИЛИ в сделку
            buy_elapsed = int(buy_time - window_start)
            
            # Нам важен период "подготовки": берем тики с начала окна до момента покупки
            cursor.execute('''
                SELECT elapsed_seconds, price_offset, prob_ratio 
                FROM event_ticks 
                WHERE event_id = ? AND elapsed_seconds <= ?
                ORDER BY elapsed_seconds ASC
            ''', (event_id, buy_elapsed))
            
            rows = cursor.fetchall()
            p_conn.close()
            
            if not rows:
                logger.warning(f"No ticks found for event {event_id} up to {buy_elapsed}s. Signal not saved.")
                return
                
            # Интерполируем в стандартный вектор, как в паттернах, но только до момента входа (buy_elapsed)
            # Это и есть наш "паттерн для входа/сигнала"
            vector = []
            dict_rows = {r["elapsed_seconds"]: (r["price_offset"], r["prob_ratio"]) for r in rows}
            
            # Строим траекторию каждые 3 секунды от старта (0) до момента исполнения покупки (buy_elapsed)
            for t in range(0, max(3, buy_elapsed + 1), 3):
                closest = min(dict_rows.keys(), key=lambda k: abs(k - t), default=0)
                if closest in dict_rows:
                    vector.append({
                        "t": t,
                        "p": round(dict_rows[closest][0], 2),
                        "r": round(dict_rows[closest][1], 2)
                    })
            
            vec_json = json.dumps(vector)
            
            # Сохраняем паттерн во вторую БД (БД Сигналов)
            with _db_lock:
                 with _get_conn() as conn:
                     conn.execute('''
                         INSERT INTO profitable_trades 
                         (event_id, side, buy_price, sell_price, profit, buy_time, sell_time, pattern_vector)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                     ''', (event_id, side, buy_price, sell_price, profit, buy_time, sell_time, vec_json))
                     conn.commit()
                     logger.info(f"Recorded profitable trade signal pattern for event {event_id}. Profit: ${profit:.2f}")
        except Exception as e:
            logger.error(f"Error saving successful trade pattern: {e}")

    threading.Thread(target=_task, daemon=True).start()

def fetch_signals():
    """
    Выгружает профитные паттерны для аналитики/фронтенда.
    """
    signals = []
    with _db_lock:
        with _get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM profitable_trades ORDER BY id DESC LIMIT 50')
            for row in cursor.fetchall():
                try:
                    data = json.loads(row["pattern_vector"])
                    signals.append({
                        "id": row["id"],
                        "event_id": row["event_id"],
                        "side": row["side"],
                        "profit": row["profit"],
                        "data": data
                    })
                except:
                    pass
    return signals

def get_best_match(current_event_id: str, elapsed_seconds: int, min_ticks=5):
    """
    Сравнивает текущий массив тиков для активного события (быстрая операция)
    со всеми успешными паттернами из кэша и возвращает лучшее совпадение,
    если оно превышает определенный порог "похожести".
    Оптимизировано для вызова каждые 3 секунды без лагов.
    """
    import time
    global _cached_signals, _last_cache_time
    
    # Обновляем кэш сигналов раз в 2 минуты или при первом вызове
    now = time.time()
    if not _cached_signals or now - _last_cache_time > 120:
        _cached_signals = fetch_signals()
        _last_cache_time = now
        
    if not _cached_signals:
        return None
        
    # Извлечем текущий хвост тиков напрямую
    # Берем последние N точек (например, за прошедшие 3 минуты)
    try:
        p_conn = sqlite3.connect(PATTERNS_DB_PATH)
        p_conn.row_factory = sqlite3.Row
        cur = p_conn.cursor()
        cur.execute('''
            SELECT elapsed_seconds, price_offset, prob_ratio 
            FROM event_ticks 
            WHERE event_id = ? AND elapsed_seconds <= ?
            ORDER BY elapsed_seconds ASC
        ''', (current_event_id, elapsed_seconds))
        current_rows = cur.fetchall()
        p_conn.close()
    except Exception as e:
        logger.error(f"Best match fetch error: {e}")
        return None

    if len(current_rows) < min_ticks:
        return None # Слишком мало данных в текущем окне для поиска паттерна
        
    # Превращаем текущий поток {t: (p, r)} для быстрого доступа
    current_dict = {r["elapsed_seconds"]: (r["price_offset"], r["prob_ratio"]) for r in current_rows}
    current_start = min(current_dict.keys())
    current_end = max(current_dict.keys())
    
    best_match = None
    min_distance = float('inf')
    
    # Сравниваем
    for sig in _cached_signals:
        vector = sig["data"]
        
        # Находим кусок паттерна, который соответствует текущему окну (например до 120-й секунды)
        # Для простоты вычисляем Евклидово расстояние между ценой / соотношением
        total_dist_sq = 0
        points_compared = 0
        
        for p in vector:
            t = p["t"]
            if t > current_end:
                break # не сравниваем будущее сигнала с текущим
            # Ищем ближайший тик
            closest_t = min(current_dict.keys(), key=lambda k: abs(k - t))
            if abs(closest_t - t) <= 5: # Если точка достаточно близка по времени
                cp, cr = current_dict[closest_t]
                # MSE: Цена (с весом 1.0) и Соотношение (разброс [0, 100], нормализуем делением на 100)
                # Нормализация очень грубая, но быстрая
                d_price = cp - p["p"]
                d_ratio = (cr - p["r"]) / 100.0 
                
                total_dist_sq += (d_price**2) + (d_ratio**2)
                points_compared += 1
                
        if points_compared >= min_ticks:
            avg_dist = math.sqrt(total_dist_sq / points_compared)
            if avg_dist < min_distance:
                min_distance = avg_dist
                best_match = {
                    "signal_id": sig["id"],
                    "event_id": sig["event_id"],
                    "side": sig["side"],
                    "profit": sig["profit"],
                    "distance": round(avg_dist, 4),
                    "confidence_pct": max(0, 100 - (avg_dist * 50))
                }

    if best_match:
        return best_match

    return None
