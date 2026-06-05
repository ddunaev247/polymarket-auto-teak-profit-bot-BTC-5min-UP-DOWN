import sqlite3
import json
import math

def euclidean_dist(a, b):
    return abs(a - b)

def calculate_dtw_distance(s, t):
    """Simple Dynamic Time Warping distance between two sequences of probabilities."""
    n, m = len(s), len(t)
    dtw_matrix = [[float('inf')] * (m + 1) for _ in range(n + 1)]
    dtw_matrix[0][0] = 0

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = euclidean_dist(s[i - 1], t[j - 1])
            dtw_matrix[i][j] = cost + min(
                dtw_matrix[i - 1][j],    # insertion
                dtw_matrix[i][j - 1],    # deletion
                dtw_matrix[i - 1][j - 1] # match
            )

    return dtw_matrix[n][m] / max(n, m) # Normalized distance

class DTW_WaveAnalyzer:
    def __init__(self, db_path='ml_wave_features.db'):
        self.db_path = db_path
        self.templates = []
        self._load_templates()

    def _load_templates(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('SELECT asset_id, market_desc, outcome, first_buy_ts, history_json FROM wave_patterns')
            rows = c.fetchall()
            
            for r in rows:
                asset_id, desc, outcome, buy_ts, history_str = r
                history = json.loads(history_str)
                if len(history) < 2:
                    continue

                # Фильтруем историю: берем только те точки, которые были ДО момента покупки бота.
                # Это позволит DTW искать паттерн *входа* в сделку, а не паттерн финала (когда цена падает к 0)
                entry_history = [pt for pt in history if pt['t'] <= buy_ts + 60]
                if len(entry_history) < 2:
                    entry_history = history

                # Берем последние 15 точек перед покупкой, чтобы сравнивать сопоставимые отрезки
                entry_history = entry_history[-15:]
                
                wave = [point['p'] for point in entry_history]
                if len(wave) < 2:
                    continue

                self.templates.append({
                    'asset_id': asset_id,
                    'desc': desc,
                    'outcome': outcome,  # The outcome the user BET on (Up or Down)
                    'wave': wave
                })
            conn.close()
            print(f"Analyzer: Loaded {len(self.templates)} winning templates for DTW matching.")
        except Exception as e:
            print(f"Analyzer: Error loading templates: {e}")

    def analyze_live_wave(self, live_prices, market_side):
        """
        live_prices: List of recent probability floats (e.g., [0.5, 0.49, ...]).
        market_side: The side we're evaluating ("Up" or "Down").
        Returns: matching_score (0.0 to 1.0), best_template
        """
        if len(live_prices) < 2 or not self.templates:
            return 0.0, None

        # Фильтр против "падающей свечи":
        # Если мы оцениваем покупку исхода, но его вероятность в данный момент сильно падает
        # (что означает, что на графике базового актива идет сильное противоположное движение),
        # мы возвращаем 0. Это предотвратит сигналы "купить падающий нож", на которые жалуется пользователь.
        window = min(5, len(live_prices)) # смотрим последние 5 тиков (до 50 секунд)
        if live_prices[-1] < live_prices[-window] - 0.03: 
            # Цена упала на 3+ цента в моменте — опасный, нисходящий тренд для этого токена
            return 0.0, None

        # Фильтр "слишком далеко от таргета"
        # Если вероятность исхода стала слишком низкой (менее 15%), значит базовый актив
        # сильно ушел против нас. Пользователь жаловался, что бот рекомендует такие позиции.
        if live_prices[-1] < 0.15:
            return 0.0, None

        # Строгий трендовый фильтр: мы требуем, чтобы цена либо росла, либо находилась в сильной консолидации
        # (в ожидании пробоя), но никак не находилась в очевидном даун-тренде. 
        # Если средняя цена последних 3 тиков МЕНЬШЕ средней цены предыдущих 7 тиков, значит мы глобально падаем.
        # Мы не должны покупать токен, если он в среднесрочном (минутном) масштабе летит вниз на графике.
        if len(live_prices) >= 10:
            avg_last_3 = sum(live_prices[-3:]) / 3
            avg_prev_7 = sum(live_prices[-10:-3]) / 7
            if avg_last_3 < avg_prev_7 - 0.01:
                # Актив находится в локальном даун-тренде (упал в среднем больше чем на 1 цент относительно недавнего времени)
                return 0.0, None

        best_score = 0.0
        best_template = None

        for temp in self.templates:
            # We compare the live wave against template waves that had the same outcome
            if temp['outcome'] != market_side:
                continue

            # Calculate raw DTW distance
            dist = calculate_dtw_distance(live_prices, temp['wave'])
            
            # Convert distance to similarity score (0% to 100%)
            # Max possible dist between probabilities [0, 1] is 1.0
            similarity = max(0.0, 1.0 - dist)
            
            if similarity > best_score:
                best_score = similarity
                best_template = temp

        return best_score, best_template
