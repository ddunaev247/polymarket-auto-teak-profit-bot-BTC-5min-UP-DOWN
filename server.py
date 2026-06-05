"""
Polymarket BTC Up/Down 5m — Web Trading Bot
Flask + SocketIO backend with Polymarket CLOB integration.
Monitors prices every 1 second, places limit buy/sell orders.
"""

import sys
import eventlet
eventlet.monkey_patch()

import os
import time
import threading
import logging
import patterns_db  # <-- Hooking the pattern recorder
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from dotenv import load_dotenv
load_dotenv()

import requests as http_requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, send_from_directory
from flask_socketio import SocketIO, emit

try:
    from py_clob_client_v2 import (
        ClobClient,
        OrderArgs,
        OrderType,
        BalanceAllowanceParams,
        AssetType,
        OpenOrderParams,
        OrderPayload,
    )
    _IS_CLOB_V2 = True
except ImportError:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType, OpenOrderParams
    OrderPayload = None
    _IS_CLOB_V2 = False

# ═══════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════

PRIVATE_KEY = os.getenv("PRIVATE_KEY")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS")
PROXY_ADDRESS = os.getenv("PROXY_ADDRESS", "")
PROXY_URL = os.getenv("PROXY_URL", "")
# У части сетей HTTP/2 к clob.polymarket.com падает без status_code — попробуйте CLOB_USE_HTTP2=0
CLOB_USE_HTTP2 = os.getenv("CLOB_USE_HTTP2", "1").lower() not in ("0", "false", "no")

HOST = os.getenv("HOST", "https://clob.polymarket.com")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))

ORDER_SIZE = float(os.getenv("ORDER_SIZE", "10"))
BUY_OFFSET = float(os.getenv("BUY_OFFSET", "0.04"))
SELL_OFFSET = float(os.getenv("SELL_OFFSET", "0.10"))
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "1"))
WEB_PORT = int(os.getenv("WEB_PORT", "5000"))
# После MATСHED покупки CLOB часто ещё видит balance=0, пока не sync + пока не пришёл settlement на Polygon
SELL_PLACE_MAX_RETRIES = int(os.getenv("SELL_PLACE_MAX_RETRIES", "90"))
CONDITIONAL_SHARE_DECIMALS = 6
SELL_MIN_EXECUTABLE_SHARES = float(os.getenv("SELL_MIN_EXECUTABLE_SHARES", "0.05"))

# Диапазоны для отступов из UI (доли цены, напр. 0.04 = 4¢)
MIN_TRADE_OFFSET = float(os.getenv("MIN_TRADE_OFFSET", "0.00"))
MAX_TRADE_OFFSET = float(os.getenv("MAX_TRADE_OFFSET", "0.50"))

GAMMA_API = "https://gamma-api.polymarket.com"
# Spot BTC и открытие 5m-свечи (визуально ≈ price-to-beat для окна)
BINANCE_API = os.getenv("BINANCE_API_URL", "https://api.binance.com")

# USDC contract on Polygon
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

# ═══════════════════════════════════════════
#  LOGGING SETUP
# ═══════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("trade_bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("PolyBot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("engineio").setLevel(logging.WARNING)
logging.getLogger("socketio").setLevel(logging.WARNING)

# ═══════════════════════════════════════════
#  PROXY SETUP (for geo-blocked regions)
# ═══════════════════════════════════════════

http_session = http_requests.Session()
# Пул соединений + параллельные запросы к midpoint без лишних TCP-handshake
_adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=Retry(total=0))
http_session.mount("https://", _adapter)
http_session.mount("http://", _adapter)
if PROXY_URL:
    http_session.proxies = {"http": PROXY_URL, "https": PROXY_URL}


def _patch_py_clob_httpx():
    """Единый httpx для py_clob_client: таймауты, опционально прокси и HTTP/2."""
    try:
        import httpx
        if _IS_CLOB_V2:
            from py_clob_client_v2.http_helpers import helpers as _clob_http
        else:
            from py_clob_client.http_helpers import helpers as _clob_http

        timeout = httpx.Timeout(45.0, connect=20.0)
        kwargs = {"timeout": timeout}
        if CLOB_USE_HTTP2:
            kwargs["http2"] = True
        if PROXY_URL:
            kwargs["proxy"] = PROXY_URL
        _clob_http._http_client = httpx.Client(**kwargs)
        logger.info(
            "CLOB httpx: http2=%s, proxy=%s",
            CLOB_USE_HTTP2,
            "on" if PROXY_URL else "off",
        )
    except Exception as e:
        logger.warning("Не удалось настроить httpx для py_clob_client: %s", e)


_patch_py_clob_httpx()

# Параллельная загрузка UP/DOWN midpoint (один поток опроса — два HTTP за один цикл)
_midpoint_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pm_mid")

# ═══════════════════════════════════════════
#  FLASK APP
# ═══════════════════════════════════════════

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = os.urandom(24).hex()
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# ═══════════════════════════════════════════
#  GLOBAL STATE
# ═══════════════════════════════════════════

class BotState:
    """Shared state between threads."""
    def __init__(self):
        self.client = None
        self.up_token_id = None
        self.down_token_id = None
        self.up_price = None
        self.down_price = None
        self.event_name = ""
        self.window_end = 0
        self.window_start = 0
        self.condition_id = None
        self.gamma_market_id = None
        self.neg_risk = True
        self.btc_spot = None
        self.btc_target_price = None
        self.btc_target_window = 0
        self.btc_target_last_try = 0.0
        self.current_confidence = 0.0
        # Balance in USDC
        self.balance = None
        # Daily PnL anchor (current local day)
        self.balance_day_key = None
        self.balance_day_start = None
        # Active orders
        self.orders = []
        self.positions = {}
        
        # ML Analyzer Tracker
        self.live_up_prices = []
        self.live_down_prices = []
        
        self.lock = threading.Lock()

state = BotState()

try:
    from wave_analyzer import DTW_WaveAnalyzer
    ml_analyzer = DTW_WaveAnalyzer()
except ImportError:
    ml_analyzer = None
    logger.warning("ML Wave Analyzer not available.")

# ═══════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/patterns')
def get_patterns():
    # Return matched historical patterns
    from flask import jsonify
    return jsonify(patterns_db.fetch_all_patterns())

@app.route('/api/signals')
def get_signals():
    # Return recorded profitable trades patterns
    from flask import jsonify
    import signals_db
    return jsonify(signals_db.fetch_signals())

# ═══════════════════════════════════════════
#  SOCKET EVENTS
# ═══════════════════════════════════════════

@socketio.on('connect')
def handle_connect():
    logger.info("Client connected via WebSocket")
    emit('config', {
        'order_size': ORDER_SIZE,
        'buy_offset': BUY_OFFSET,
        'sell_offset': SELL_OFFSET,
        'min_trade_offset': MIN_TRADE_OFFSET,
        'max_trade_offset': MAX_TRADE_OFFSET,
    })
    emit('prices', {
        'up_price': state.up_price,
        'down_price': state.down_price,
        'event_name': state.event_name,
        'window_end': state.window_end,
        'window_start': state.window_start,
        'btc_spot': state.btc_spot,
        'btc_target_price': state.btc_target_price,
        'ts': time.time(),
    })
    emit('balance', _balance_payload())
    emit('orders', {'orders': _serialize_orders()})


def _parse_trade_offsets(payload):
    """Отступ покупки от рынка и наценка продажи от цены покупки (из UI или .env)."""
    try:
        bo = float((payload or {}).get('buy_offset', BUY_OFFSET))
    except (TypeError, ValueError):
        bo = BUY_OFFSET
    try:
        so = float((payload or {}).get('sell_offset', SELL_OFFSET))
    except (TypeError, ValueError):
        so = SELL_OFFSET
    bo = max(MIN_TRADE_OFFSET, min(MAX_TRADE_OFFSET, bo))
    so = max(MIN_TRADE_OFFSET, min(MAX_TRADE_OFFSET, so))
    return bo, so


@socketio.on('place_order')
def handle_place_order(data):
    side = data.get('side', '').upper()
    if side not in ('UP', 'DOWN'):
        emit('trade_result', {'success': False, 'message': f'Неверная сторона: {side}'})
        return

    if not state.client:
        emit('trade_result', {'success': False, 'message': 'Клиент не инициализирован'})
        return

    token_id = state.up_token_id if side == 'UP' else state.down_token_id
    current_price = state.up_price if side == 'UP' else state.down_price

    if not token_id or current_price is None:
        emit('trade_result', {'success': False, 'message': 'Цены ещё не загружены, подождите'})
        return

    # Защита ордера: делаем быстрый запрос актуальной цены перед просчетом,
    # чтобы поймать падение рынка пока летел сокет-запрос
    try:
        fresh_resp = state.client.get_midpoint(token_id)
        if fresh_resp and fresh_resp.get('mid'):
            fresh_mid = float(fresh_resp.get('mid'))
            if fresh_mid < current_price:
                _emit_log('info', '🛡️', f'Защита ордера: маркет опустился ({current_price:.2f} ➡ {fresh_mid:.2f}) пока шел запрос. Покупаем дешевле!')
                current_price = fresh_mid
            elif fresh_mid > current_price and (fresh_mid - current_price) >= 0.05:
                emit('trade_result', {'success': False, 'message': f'Резкий рост цены! ({current_price:.2f} ➡ {fresh_mid:.2f}). Ордер отменён.'})
                _emit_log('warning', '⚠️', f'Резкий скачок цены вверх ({current_price:.2f} ➡ {fresh_mid:.2f}), покупка отменена для защиты.')
                return
    except Exception as e:
        logger.debug(f"Не удалось проверить свежий мидпоинт для защиты: {e}")

    buy_off, sell_off = _parse_trade_offsets(data)

    # User provides amount in USD
    amount_usd = float(data.get('size', ORDER_SIZE))
    if amount_usd <= 0:
        amount_usd = 1

    # Calculate buy price: current - offset (ниже рынка)
    if buy_off == 0:
        buy_price = round(current_price, 2)
    else:
        buy_price = round(current_price - buy_off, 2)
        
    if buy_price <= 0.01:
        buy_price = 0.01
    if buy_price >= 0.99:
        buy_price = 0.99

    # Calculate number of shares
    shares_to_buy = round(amount_usd / buy_price, 2)

    # Sell price: buy + profit offset
    sell_price = round(buy_price + sell_off, 2)
    if sell_price >= 0.99:
        sell_price = 0.98

    _emit_log('trade', '📋',
              f'Выставляю лимитку на покупку {side} по ${buy_price:.2f} '
              f'(рынок: ${current_price:.2f}, сумма: ${amount_usd}, '
              f'размер: {shares_to_buy} акций)')

    try:
        order_args = OrderArgs(
            token_id=token_id,
            price=buy_price,
            size=shares_to_buy,
            side="BUY",
        )

        signed_order = state.client.create_order(order_args)
        resp = state.client.post_order(signed_order, OrderType.GTC)

        if resp and resp.get("success"):
            order_id = resp.get("orderID", "unknown")
            logger.info(f"BUY order placed: {order_id} | {side} @ ${buy_price} | investment=${amount_usd} shares={shares_to_buy}")

            order_entry = {
                'id': order_id,
                'side': side,
                'buy_order_id': order_id,
                'sell_order_id': None,
                'buy_price': buy_price,
                'sell_price': sell_price,
                'size': shares_to_buy,
                'buy_offset': buy_off,
                'sell_offset': sell_off,
                'status': 'pending',
                'token_id': token_id,
                'created_at': time.time(),
                'condition_id': state.condition_id,
                'window_start': state.window_start,
            }

            with state.lock:
                state.orders.append(order_entry)

            emit('trade_result', {
                'success': True,
                'message': f'Лимитка на покупку {side} по ${buy_price:.2f} выставлена! '
                           f'(ордер: {order_id[:12]}..., продажа по ${sell_price:.2f})'
            })
            _broadcast_orders()

            _emit_log('info', '🎯',
                      f'После исполнения покупки → автоматическая продажа по ${sell_price:.2f} '
                      f'(+{int(round(sell_off * 100))}¢)')
        else:
            error_msg = str(resp) if resp else "Пустой ответ API"
            emit('trade_result', {'success': False, 'message': f'Ошибка API: {error_msg}'})
            _emit_log('error', '❌', f'API ошибка при покупке: {error_msg}')

    except Exception as e:
        error_str = str(e)
        logger.error(f"Order placement error: {error_str}")
        emit('trade_result', {'success': False, 'message': f'Ошибка: {error_str}'})
        _emit_log('error', '❌', f'Ошибка при выставлении ордера: {error_str}')


@socketio.on('cancel_order')
def handle_cancel_order(data):
    oid = (data or {}).get('order_id') or (data or {}).get('id')
    if not oid:
        emit('cancel_result', {'success': False, 'message': 'Не указан ордер'})
        return

    if not state.client:
        emit('cancel_result', {'success': False, 'message': 'Клиент не инициализирован'})
        return

    with state.lock:
        target = next((o for o in state.orders if o.get('id') == oid), None)

    if not target:
        emit('cancel_result', {'success': False, 'message': 'Ордер не найден'})
        return

    st = target.get('status')
    if st == 'pending':
        clob_id = target.get('buy_order_id')
    elif st == 'selling':
        clob_id = target.get('sell_order_id')
    else:
        emit('cancel_result', {'success': False, 'message': 'Этот ордер нельзя отменить'})
        return

    if not clob_id:
        emit('cancel_result', {'success': False, 'message': 'Нет ID ордера на бирже'})
        return

    try:
        if _IS_CLOB_V2:
            state.client.cancel_order(OrderPayload(orderID=clob_id))
        else:
            state.client.cancel(clob_id)
        with state.lock:
            for o in state.orders:
                if o.get('id') == oid:
                    o['status'] = 'cancelled'
                    o['completion_time'] = time.time()
                    break

        side = target.get('side', '?')
        if st == 'pending':
            _emit_log('warning', '⚠️', f'Лимитка на покупку {side} отменена')
        else:
            _emit_log('warning', '⚠️', f'Лимитка на продажу {side} отменена (токены остались на кошельке)')

        emit('cancel_result', {'success': True, 'message': 'Ордер отменён'})
        _broadcast_orders()

    except Exception as e:
        err = str(e)
        logger.error(f"Cancel order error: {err}")
        emit('cancel_result', {'success': False, 'message': f'Ошибка отмены: {err}'})


@socketio.on('close_position')



@socketio.on('close_position')
def handle_close_position(data):
    if not state.client: return

    oid = data.get('order_id')
    side_pos = data.get('side')
    
    target_side = None
    target_size = None

    if oid:
        with state.lock:
            target_order = next((o for o in state.orders if o.get('id') == oid), None)
        if not target_order: return

        target_side = target_order.get('side')
        target_size = target_order.get('size')
        st = target_order.get('status')
        
        if st in ('selling', 'pending'):
            clob_id = target_order.get('sell_order_id') if st == 'selling' else target_order.get('buy_order_id')
            if clob_id:
                try:
                    if _IS_CLOB_V2:
                        state.client.cancel_order(OrderPayload(orderID=clob_id))
                    else:
                        state.client.cancel(clob_id)
                except Exception: pass
            with state.lock:
                target_order['status'] = 'cancelled'
                target_order['completion_time'] = time.time()
                
        if st == 'pending':
            _emit_log('trade', '📤', f'Ордер отменен. В позиции ничего нет.')
            _broadcast_orders()
            return

        socketio.sleep(1.0)
    elif side_pos:
        target_side = side_pos
        target_size = state.positions.get(target_side)
        if not target_size or target_size <= 0:
            return

    if not target_side or not target_size:
        return

    token_id = state.up_token_id if target_side == 'UP' else state.down_token_id
    if not token_id: return

    try:
        if _IS_CLOB_V2:
            from py_clob_client_v2 import OrderArgs, OrderType
        else:
            from py_clob_client.clob_types import OrderArgs, OrderType

        current_price = state.up_price if target_side == 'UP' else state.down_price
        if current_price is None:
            _emit_log('error', '❌', f'Нет цены для {target_side}, невозможно закрыть.')
            return

        sell_price = round(current_price * 0.95, 2)
        if sell_price < 0.01: sell_price = 0.01

        order_args = OrderArgs(
            price=sell_price,
            size=target_size,
            side='SELL',
            token_id=token_id
        )

        signed_m = state.client.create_order(order_args)
        resp = state.client.post_order(signed_m, OrderType.FOK)

        if resp and resp.get('success'):
            if oid and target_order:
                with state.lock:
                    target_order['status'] = 'selling'
                    target_order['sell_price'] = sell_price
                    target_order['sell_order_id'] = resp.get('orderID')
            elif side_pos:
                new_order = {
                    'id': resp.get('orderID') or f"ext_{int(time.time())}",
                    'side': target_side,
                    'buy_order_id': None,
                    'sell_order_id': resp.get('orderID'),
                    'buy_price': None,
                    'sell_price': sell_price,
                    'size': target_size,
                    'token_id': token_id,
                    'status': 'selling',
                    'timestamp': time.time(),
                    'sell_retries': 0
                }
                with state.lock:
                    state.orders.append(new_order)
                    
            _emit_log('success', '✅', f'Ордер на закрытие {target_side} по рынку успешно выставлен!')
        else:
            msg = resp.get('error_msg', 'Неизвестная ошибка')
            _emit_log('error', '❌', f'Не удалось закрыть по маркету: {msg}')

        _fetch_balance()
        _broadcast_orders()
    except Exception as e:
        logger.error(f"Close position error: {e}")
        _emit_log('error', '❌', f'Ошибка рыночного ордера: {e}')


# ═══════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════

def _emit_log(level, icon, message):
    """Send log to all connected clients."""
    socketio.emit('log', {'level': level, 'icon': icon, 'message': message})
    logger.info(f"[{level}] {message}")


def _broadcast_orders():
    """Send current orders to all clients."""
    socketio.emit('orders', {'orders': _serialize_orders()})


def _serialize_orders():
    """Convert orders to JSON-serializable list."""
    with state.lock:
        res = [
            {
                'id': o['id'],
                'side': o['side'],
                'buy_price': o['buy_price'],
                'sell_price': o['sell_price'],
                'size': o['size'],
                'status': o['status'],
                'is_external': False
            }
            for o in state.orders
        ]

        active_sides = set(o['side'] for o in state.orders if o['status'] in ('pending', 'selling'))
        for side, size in state.positions.items():
            if size > 0 and side not in active_sides:
                res.append({
                    'id': f'EXT_{side}',
                    'side': side,
                    'buy_price': None,
                    'sell_price': None,
                    'size': round(size, 2),
                    'status': 'bought',
                    'is_external': True
                })
        return res


# ═══════════════════════════════════════════
#  BACKGROUND: PRICE POLLING (every 1 second)
# ═══════════════════════════════════════════

def _get_current_5m_window():
    """Calculate current 5-minute window timestamps."""
    now = int(time.time())
    window_start = now - (now % 300)
    window_end = window_start + 300
    return window_start, window_end


def _fetch_btc_spot_usd():
    """Текущая цена BTC/USDT с Binance (публичный API)."""
    try:
        # DO NOT use http_session directly because it has Polymarket proxies which might be blocked by Binance!
        r = http_requests.get(
            f"{BINANCE_API}/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout=3,
        )
        if r.status_code != 200:
            return None
        return float(r.json().get("price", 0)) or None
    except Exception as e:
        logger.debug("Binance spot: %s", e)
        return None


def _fetch_btc_5m_open_usd(window_start_sec: int):
    """
    Цена открытия 5m-свечи Binance для интервала, начинающегося с window_start_sec.
    Визуально близко к «price to beat» в окне BTC Up/Down (не on-chain oracle).
    """
    if window_start_sec <= 0:
        return None
    try:
        r = http_requests.get(
            f"{BINANCE_API}/api/v3/klines",
            params={
                "symbol": "BTCUSDT",
                "interval": "5m",
                "startTime": int(window_start_sec) * 1000,
                "limit": 1,
            },
            timeout=4,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not data or not isinstance(data, list):
            return None
        return float(data[0][1])
    except Exception as e:
        logger.debug("Binance 5m open: %s", e)
        return None


def _sync_btc_target_for_window(window_start_sec: int):
    """Обновить таргет, если сменилось 5m-окно."""
    if window_start_sec <= 0:
        return
    if state.btc_target_window == window_start_sec and state.btc_target_price is not None:
        return
    if state.btc_target_window == window_start_sec and state.btc_target_price is None:
        if time.time() - state.btc_target_last_try < 12.0:
            return
    state.btc_target_last_try = time.time()
    op = _fetch_btc_5m_open_usd(window_start_sec)
    state.btc_target_price = op
    state.btc_target_window = window_start_sec
    if op is not None:
        logger.info("BTC target (5m open, окно %s): $%.2f", window_start_sec, op)


def _parse_btc_event_markets(markets):
    """Из markets Gamma — UP/DOWN clob id, conditionId, gamma id, negRisk."""
    import json

    up_token = None
    down_token = None
    condition_id = None
    gamma_market_id = str(markets[0].get("id") or "") or None if markets else None
    neg_risk = True
    for m in markets:
        cid = m.get("conditionId")
        if cid:
            condition_id = cid
        if m.get("negRisk") is not None:
            neg_risk = bool(m.get("negRisk"))
        clob_ids_str = m.get("clobTokenIds")
        outcomes_str = m.get("outcomes")
        if clob_ids_str and outcomes_str:
            try:
                clob_ids = json.loads(clob_ids_str)
                outcomes = json.loads(outcomes_str)
                for i, outcome_str in enumerate(outcomes):
                    o_upper = outcome_str.upper()
                    if "UP" in o_upper:
                        up_token = clob_ids[i]
                    elif "DOWN" in o_upper:
                        down_token = clob_ids[i]
            except Exception as parse_e:
                logger.error(f"Parse error: {parse_e}")
    return up_token, down_token, condition_id, gamma_market_id, neg_risk


def _fetch_active_event():
    """Fetch the active btc-updown-5m event from Gamma API."""
    window_start, window_end = _get_current_5m_window()
    slug = f"btc-updown-5m-{window_start}"

    try:
        resp = http_session.get(
            f"{GAMMA_API}/events",
            params={"slug": slug},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        events = data if isinstance(data, list) else [data]

        for event in events:
            markets = event.get("markets", [])
            if not markets:
                continue

            up_token, down_token, condition_id, gamma_market_id, neg_risk = _parse_btc_event_markets(
                markets
            )
            if up_token and down_token:
                # Trigger aggregation for the previous event before overwriting
                if state.condition_id and state.condition_id != condition_id:
                    threading.Thread(target=patterns_db.aggregate_patterns, args=(state.condition_id,), daemon=True).start()

                state.event_name = event.get("title", f"BTC Up/Down 5m ({window_start})")
                state.window_start = window_start
                state.window_end = window_end
                state.condition_id = condition_id
                state.neg_risk = neg_risk
                state.up_token_id = up_token
                state.down_token_id = down_token
                state.gamma_market_id = gamma_market_id
                state.live_up_prices = []
                state.live_down_prices = []
                _sync_btc_target_for_window(window_start)
                return True

        # If no event found with this slug, try next window
        next_start = window_start + 300
        next_end = next_start + 300
        slug_next = f"btc-updown-5m-{next_start}"

        resp2 = http_session.get(
            f"{GAMMA_API}/events",
            params={"slug": slug_next},
            timeout=10
        )
        resp2.raise_for_status()
        data2 = resp2.json()
        events2 = data2 if isinstance(data2, list) else [data2]

        for event in events2:
            markets = event.get("markets", [])
            if not markets:
                continue

            up_token, down_token, condition_id, gamma_market_id, neg_risk = _parse_btc_event_markets(
                markets
            )
            if up_token and down_token:
                # Trigger aggregation for the previous event before overwriting
                if state.condition_id and state.condition_id != condition_id:
                    threading.Thread(target=patterns_db.aggregate_patterns, args=(state.condition_id,), daemon=True).start()

                state.event_name = event.get("title", f"BTC Up/Down 5m ({next_start})")
                state.window_start = next_start
                state.window_end = next_end
                state.condition_id = condition_id
                state.neg_risk = neg_risk
                state.up_token_id = up_token
                state.down_token_id = down_token
                state.gamma_market_id = gamma_market_id
                state.live_up_prices = []
                state.live_down_prices = []
                _sync_btc_target_for_window(next_start)
                return True

        return False

    except Exception as e:
        logger.error(f"Gamma API error: {e}")
        return False


def _fetch_midpoint(token_id):
    """GET /midpoint for one token; used in parallel for UP/DOWN."""
    try:
        r = http_session.get(
            f"{HOST}/midpoint",
            params={"token_id": token_id},
            timeout=4,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        mid = data.get("mid")
        if mid:
            return float(mid)
    except Exception as e:
        logger.error(f"Midpoint fetch error: {e}")
    return None


def _fetch_prices():
    """Fetch current prices from CLOB API (UP и DOWN параллельно)."""
    up_id = state.up_token_id
    down_id = state.down_token_id
    if not up_id or not down_id:
        return

    try:
        fu_up = _midpoint_executor.submit(_fetch_midpoint, up_id)
        fu_down = _midpoint_executor.submit(_fetch_midpoint, down_id)
        up_mid = fu_up.result(timeout=6)
        down_mid = fu_down.result(timeout=6)

        if up_mid is not None:
            state.up_price = up_mid
        if down_mid is not None:
            state.down_price = down_mid

    except Exception as e:
        logger.error(f"Price fetch error: {e}")


def _fetch_balance():
    """Fetch USDC balance directly from Polymarket CLOB API."""
    if not state.client:
        return

    try:
        if _IS_CLOB_V2:
            from py_clob_client_v2 import BalanceAllowanceParams, AssetType
        else:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        res = state.client.get_balance_allowance(params)
        
        # 'balance' in the response is usually as a string of raw integer, e.g. "4998900"
        if isinstance(res, dict) and 'balance' in res:
            raw_balance = int(res['balance'])
            state.balance = raw_balance / 1e6  # USDC 6 decimals
        elif isinstance(res, str):
            import json
            data = json.loads(res)
            raw_balance = int(data.get('balance', 0))
            state.balance = raw_balance / 1e6

        if state.balance is not None:
            day_key = datetime.now().strftime("%Y-%m-%d")
            if state.balance_day_key != day_key or state.balance_day_start is None:
                state.balance_day_key = day_key
                state.balance_day_start = state.balance

        # Active positions for current markets
        pos_up = 0.0
        if state.up_token_id:
            try:
                p_up = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=state.up_token_id)
                res_up = state.client.get_balance_allowance(p_up)
                if isinstance(res_up, dict) and 'balance' in res_up:
                    pos_up = int(res_up['balance']) / 1e6
                elif isinstance(res_up, str):
                    import json
                    d_up = json.loads(res_up)
                    pos_up = int(d_up.get('balance', 0)) / 1e6
            except Exception as e_up:
                logger.debug(f"Position fetch UP error: {e_up}")

        pos_down = 0.0
        if state.down_token_id:
            try:
                p_down = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=state.down_token_id)
                res_down = state.client.get_balance_allowance(p_down)
                if isinstance(res_down, dict) and 'balance' in res_down:
                    pos_down = int(res_down['balance']) / 1e6
                elif isinstance(res_down, str):
                    import json
                    d_down = json.loads(res_down)
                    pos_down = int(d_down.get('balance', 0)) / 1e6
            except Exception as e_down:
                logger.debug(f"Position fetch DOWN error: {e_down}")

        with state.lock:
            state.positions = {
                'UP': pos_up,
                'DOWN': pos_down
            }

    except Exception as e:
        logger.error(f"Balance fetch error (CLOB API): {e}")


def _balance_payload():
    """Current balance + daily PnL from today's starting balance."""
    pnl_day = None
    if state.balance is not None and state.balance_day_start is not None:
        pnl_day = state.balance - state.balance_day_start
    return {
        'balance': state.balance,
        'pnl_day': pnl_day,
    }


def _shares_to_raw_conditional(size_shares: float) -> int:
    """Shares в «сырых» единицах CLOB (6 знаков), как в ошибке order amount: 7550000."""
    return max(1, int(round(float(size_shares) * (10**CONDITIONAL_SHARE_DECIMALS))))


def _raw_to_shares(raw_amount: int) -> float:
    """Raw amount (6 decimals) -> shares float, truncated to 6 decimals."""
    return max(0.0, int(raw_amount) / float(10**CONDITIONAL_SHARE_DECIMALS))


def _sync_conditional_balance_allowance(token_id) -> None:
    """Подтянуть on-chain баланс/allowance исхода в кэш CLOB (нужно перед SELL)."""
    if not state.client or not token_id:
        return
    try:
        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=str(token_id),
        )
        state.client.update_balance_allowance(params)
    except Exception as e:
        logger.warning(f"update_balance_allowance CONDITIONAL: {e}")


def _clob_raw_int(val):
    """Поля balance/allowance в ответе CLOB иногда строкой."""
    if val is None:
        return 0
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return 0
    return int(val)


def _read_conditional_balance_allowance(token_id):
    """(balance_raw, allowance_raw) или (None, None)."""
    if not state.client or not token_id:
        return None, None
    try:
        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=str(token_id),
        )
        res = state.client.get_balance_allowance(params)
        if isinstance(res, dict):
            b = _clob_raw_int(res.get("balance"))
            a = _clob_raw_int(res.get("allowance"))
            return b, a
        if isinstance(res, str):
            import json
            data = json.loads(res)
            b = _clob_raw_int(data.get("balance"))
            a = _clob_raw_int(data.get("allowance"))
            return b, a
    except Exception as e:
        logger.warning(f"get_balance_allowance CONDITIONAL: {e}")
    return None, None


def _conditional_sell_readiness(token_id, size_shares, order=None):
    """
    Обновляет кэш CLOB и проверяет готовность к SELL.
    Важно: для CONDITIONAL CLOB часто отдаёт allowance=0 при рабочем approve
    (github.com/Polymarket/clob-client/issues/128) — ордер всё равно проходит.
    Поэтому блокируем только по balance, не по allowance.
    Возвращает (ready, detail, executable_shares).
    """
    _sync_conditional_balance_allowance(token_id)
    need = _shares_to_raw_conditional(size_shares)
    bal, allow = _read_conditional_balance_allowance(token_id)
    if bal is None:
        return True, "", float(size_shares)
    if bal <= 0:
        return False, f"CLOB balance {bal} < нужно {need} (ещё не зачислились токены?)", 0.0
    if allow < need and order is not None and not order.get("_sell_allowance_api_quirk_warned"):
        order["_sell_allowance_api_quirk_warned"] = True
        _emit_log(
            "info",
            "ℹ️",
            "CLOB: allowance в API = 0 при достаточном balance — не блокируем продажу (известная особенность).",
        )
    executable_raw = min(need, bal)
    executable_shares = _raw_to_shares(executable_raw)
    # Не отправляем микроскопические "пылевые" продажи.
    if executable_shares < SELL_MIN_EXECUTABLE_SHARES:
        return False, (
            f"CLOB balance {bal} (< {SELL_MIN_EXECUTABLE_SHARES} shares), "
            f"ждём накопления исполнимого объёма"
        ), 0.0
    return True, "", executable_shares




def _sync_external_orders():
    if not state.client or not state.up_token_id or not state.down_token_id:
        return
    try:
        ext_orders = state.client.get_open_orders() if _IS_CLOB_V2 else state.client.get_orders()
        if not isinstance(ext_orders, list):
            # some versions return dict with {"data": [...]}
            ext_orders = ext_orders.get('data', []) if isinstance(ext_orders, dict) else []
            
        new_ext_orders = []
        with state.lock:
            bot_ids = {o['id'] for o in state.orders if not o.get('is_api_ext')}
            
            for xo in ext_orders:
                oid = xo.get('id')
                if oid in bot_ids:
                    continue
                
                asset_id = xo.get('asset_id')
                if asset_id == state.up_token_id:
                    our_side = 'UP'
                elif asset_id == state.down_token_id:
                    our_side = 'DOWN'
                else:
                    continue
                    
                side = xo.get('side', '').upper()
                price = float(xo.get('price', 0))
                size = float(xo.get('original_size', xo.get('size', 0)))
                
                new_ext_orders.append({
                    'id': oid,
                    'ts': time.time(),
                    'side': our_side,
                    'buy_price': price if side == 'BUY' else None,
                    'sell_price': price if side == 'SELL' else None,
                    'size': size,
                    'status': 'pending' if side == 'BUY' else 'selling',
                    'buy_order_id': oid if side == 'BUY' else None,
                    'sell_order_id': oid if side == 'SELL' else None,
                    'is_api_ext': True
                })
                
            active_bot_orders = [o for o in state.orders if not o.get('is_api_ext')]
            state.orders = active_bot_orders + new_ext_orders
            
    except Exception as e:
        logger.debug(f"Sync external orders error: {e}")



def slow_polling_loop():
    logger.info("Slow polling thread started (10s/30s intervals)")
    socketio.sleep(3)

    last_event_fetch = 0
    last_balance_fetch = 0

    while True:
        try:
            now = time.time()
            
            if now - last_event_fetch > 30 or now > state.window_end:
                found = _fetch_active_event()
                last_event_fetch = now
                if found:
                    _emit_log('info', '📊', f'Активное событие: {state.event_name}')
                else:
                    _emit_log('warning', '⚠️', 'Не удалось найти активное событие btc-updown-5m')
            
            if now - last_balance_fetch > 10:
                _fetch_balance()
                _sync_external_orders()
                last_balance_fetch = now
                socketio.emit('balance', _balance_payload())
                _broadcast_orders()
                
        except Exception as e:
            logger.error(f"Slow polling error: {e}")
        
        socketio.sleep(2)


def price_polling_loop():
    """Background thread: polls prices every 1 second and broadcasts to clients."""
    logger.info(f"Price polling thread started (interval: {os.getenv('PRICE_INTERVAL', '0.3')}s)")

    socketio.sleep(2)
    _emit_log('info', '🚀', 'Бот запущен! Загрузка данных рынка...')
    last_pattern_tick = 0

    while True:
        try:
            now = time.time()

            # Fetch prices every iteration (1s)
            _fetch_prices()
            spot = _fetch_btc_spot_usd()
            if spot is not None:
                state.btc_spot = spot
            if state.window_start:
                _sync_btc_target_for_window(state.window_start)

            # --- Patterns DB (3s interval) ---
            if state.condition_id and state.window_start and state.window_end:
                if now - last_pattern_tick >= 3.0:
                    last_pattern_tick = now
                    elapsed_seconds = int(now - state.window_start)
                    # Только если событие не завершено (закончилось или осталось немного)
                    # Пишем тики (ok to write slightly past window_end due to close delay, DB handles overlap via ID/Elapsed)
                    if 0 <= elapsed_seconds <= 310 and state.btc_spot and state.btc_target_price:
                        price_offset = state.btc_spot - state.btc_target_price
                        prob_up = state.up_price or 0.0
                        prob_down = state.down_price or 0.0
                        # prob_ratio: если down=0, то limit to artificial max, else up/down
                        prob_ratio = prob_up / prob_down if prob_down > 0 else 99.0
                        patterns_db.insert_tick(
                            event_id=state.condition_id,
                            elapsed_seconds=elapsed_seconds,
                            price_offset=price_offset,
                            prob_ratio=prob_ratio
                        )

                        # --- SIGNALS BLOCK DISABLED (temporary) ---
                        # Подсказки прогнозов (AI + pattern match) отключены по запросу.
                        # Чтобы вернуть, раскомментируйте блок ниже.
                        state.current_confidence = 0.0
                        # if not hasattr(state, 'last_wave_tick'):
                        #     state.last_wave_tick = 0
                        #
                        # if now - state.last_wave_tick >= 10.0:
                        #     state.last_wave_tick = now
                        #     state.live_up_prices.append(prob_up)
                        #     state.live_down_prices.append(prob_down)
                        #     # Keep only last 35 points (~ 5 minutes of data)
                        #     if len(state.live_up_prices) > 35:
                        #         state.live_up_prices.pop(0)
                        #         state.live_down_prices.pop(0)
                        #
                        #     logger.info(f"ML Wave Length: {len(state.live_up_prices)} (needs 3 to analyze)")
                        #     if ml_analyzer and len(state.live_up_prices) >= 3:
                        #         up_score, up_tmpl = ml_analyzer.analyze_live_wave(state.live_up_prices, "Up")
                        #         down_score, down_tmpl = ml_analyzer.analyze_live_wave(state.live_down_prices, "Down")
                        #
                        #         logger.info(f"ML Scores - Up: {up_score:.2f}, Down: {down_score:.2f}")
                        #
                        #         best_signal = "N/A"
                        #         conf = 0.0
                        #         if up_score > 0.70 and up_score > down_score:
                        #             best_signal = "UP"
                        #             conf = up_score
                        #         elif down_score > 0.70 and down_score > up_score:
                        #             best_signal = "DOWN"
                        #             conf = down_score
                        #
                        #         if best_signal != "N/A":
                        #             logger.info(f"⚡ AI SIGNAL: {best_signal} ({conf*100:.1f}%)")
                        #             socketio.emit('ai_recommendation', {
                        #                 'side': best_signal,
                        #                 'confidence': conf * 100
                        #             })
                        #
                        # import signals_db
                        # match_info = signals_db.get_best_match(state.condition_id, elapsed_seconds)
                        # if match_info:
                        #     state.current_confidence = round(match_info['confidence_pct'], 1)
                        #
                        #     # Отправляем отдельный сигнал только при 85%+ (для вспышки кнопок UI)
                        #     if match_info['confidence_pct'] >= 85.0:
                        #         socketio.emit('pattern_match', {
                        #             'side': match_info['side'],
                        #             'confidence': state.current_confidence,
                        #             'profit': match_info['profit'],
                        #             'match_id': match_info['signal_id']
                        #         })
                        #     # Логгируем для истории только если высокий % (чтобы не флудить)
                        #     if match_info['confidence_pct'] >= 95.0:
                        #         _emit_log('info', '🔥', f"СИГНАЛ: Найдено 95%+ совпадение с прибыльным паттерном ({match_info['side']} +${match_info['profit']:.2f})")
                        # else:
                        #     state.current_confidence = 0.0

            # Broadcast to all clients
            socketio.emit('prices', {
                'up_price': state.up_price,
                'down_price': state.down_price,
                'event_name': state.event_name,
                'window_end': state.window_end,
                'window_start': state.window_start,
                'btc_spot': state.btc_spot,
                'btc_target_price': state.btc_target_price,
                'ts': time.time(),
                'current_confidence': state.current_confidence,
            })

        except Exception as e:
            logger.error(f"Price loop error: {e}")

        socketio.sleep(float(os.getenv("PRICE_INTERVAL", "0.3")))  # fast price polling


# ═══════════════════════════════════════════
#  BACKGROUND: ORDER MONITORING (every 1 second)
# ═══════════════════════════════════════════

def order_monitoring_loop():
    """Background thread: monitors pending buy orders and places sell orders when filled."""
    logger.info("Order monitoring thread started (1s interval)")
    socketio.sleep(5)

    while True:
        try:
            with state.lock:
                now = time.time()
                orders_to_keep = []
                for order in state.orders:
                    # Cleanup old completed/cancelled orders after 5 seconds
                    if order.get('status') in ('sold', 'cancelled', 'failed'):
                        if now - order.get('completion_time', now) > 5:
                            continue  # drop it
                    orders_to_keep.append(order)
                state.orders = orders_to_keep
                orders_snapshot = list(state.orders)

            for order in orders_snapshot:
                if order['status'] == 'pending':
                    _check_buy_order(order)
                elif order['status'] == 'bought':
                    _place_sell_order(order)
                elif order['status'] == 'selling':
                    _check_sell_order(order)

        except Exception as e:
            logger.error(f"Order monitoring error: {e}")

        socketio.sleep(CHECK_INTERVAL_SEC)  # 1 second


def _check_buy_order(order):
    """Check if a BUY order has been filled."""
    try:
        if not order.get('buy_order_id'):
            return

        order_info = state.client.get_order(order['buy_order_id'])
        if not order_info:
            order['missing_count'] = order.get('missing_count', 0) + 1
            if order['missing_count'] >= 10:
                with state.lock:
                    order['status'] = 'cancelled'
                    order['completion_time'] = time.time()
                _emit_log('warning', '⚠️', f'Ордер на покупку {order.get("side", "")} не существует на сервере (закрыт вручную?). Удалён из UI.')
                _broadcast_orders()
            return
        else:
            order['missing_count'] = 0

        status = order_info.get("status", "").upper()

        if status == "MATCHED":
            with state.lock:
                order['status'] = 'bought'
                order['buy_matched_time'] = time.time()

            _emit_log('success', '✅',
                      f'🟢 ПОКУПКА ИСПОЛНЕНА: {order["side"]} по ${order["buy_price"]:.2f} '
                      f'({order["size"]} акций)')
            _broadcast_orders()

        elif status in ("CANCELLED", "EXPIRED"):
            with state.lock:
                order['status'] = 'cancelled'
                order['completion_time'] = time.time()
            _emit_log('warning', '⚠️',
                      f'Ордер на покупку {order["side"]} отменён/истёк')
            _broadcast_orders()

        elif status in ("LIVE", "OPEN", "PENDING"):
            # Ордер еще в стакане, проверяем не ушла ли цена ниже
            current_price = state.up_price if order['side'] == 'UP' else state.down_price
            if current_price is not None:
                buy_off = order.get('buy_offset', 0)
                amount_usd = round(order['size'] * order['buy_price'], 2)  # Восстанавливаем сумму USD

                new_buy_price = round(current_price - buy_off, 2)
                if new_buy_price <= 0.01: new_buy_price = 0.01
                if new_buy_price >= 0.99: new_buy_price = 0.99

                # Если новая целевая цена МЕНЬШЕ нашей текущей, значит рынок упал
                # и мы можем купить дешевле
                if new_buy_price < order['buy_price']:
                    old_price = order['buy_price']
                    _emit_log('info', '🛡️', f'Маркет упал ({old_price:.2f} ➡ {new_buy_price:.2f}). Пробуем переставить ордер...')
                    
                    try:
                        # 1. Отменяем старый
                        if _IS_CLOB_V2:
                            cancel_resp = state.client.cancel_order(OrderPayload(orderID=order['buy_order_id']))
                        else:
                            cancel_resp = state.client.cancel(order['buy_order_id'])
                        if not cancel_resp or not cancel_resp.get("success"):
                            _emit_log('warning', '⚠️', 'Ордер уже исполнен или исчез, отмена не удалась. Ждем следующего тика.')
                            return
                        
                        # 2. Выставляем новый
                        new_shares = round(amount_usd / new_buy_price, 2)
                        order_args = OrderArgs(
                            token_id=order['token_id'],
                            price=new_buy_price,
                            size=new_shares,
                            side="BUY",
                        )
                        signed_order = state.client.create_order(order_args)
                        resp = state.client.post_order(signed_order, OrderType.GTC)
                        
                        if resp and resp.get("success"):
                            new_id = resp.get("orderID")
                            with state.lock:
                                order['buy_order_id'] = new_id
                                order['buy_price'] = new_buy_price
                                order['size'] = new_shares
                                # Пересчитываем таргет продажи
                                order['sell_price'] = min(0.98, round(new_buy_price + order.get('sell_offset', 0.10), 2))
                            
                            _emit_log('success', '✅', f'Ордер успешно переставлен! Новая цена покупки: ${new_buy_price:.2f}')
                            _broadcast_orders()
                        else:
                            _emit_log('error', '❌', 'Не удалось переставить ордер (ошибка размещения API)')
                    except Exception as e:
                        _emit_log('error', '❌', f'Ошибка при перестановке ордера: {e}')

    except Exception as e:
        logger.error(f"Check buy order error: {e}")


def _place_sell_order(order):
    """Place a SELL order after buy is filled."""
    sell_price = order['sell_price']

    # Initialize retry counter
    if 'sell_retries' not in order:
        order['sell_retries'] = 0
        _emit_log('trade', '📋',
                  f'Выставляю лимитку на продажу {order["side"]} по ${sell_price:.2f} '
                  f'(+{int(round(order.get("sell_offset", SELL_OFFSET) * 100))}¢ от покупки ${order["buy_price"]:.2f})')

    ready, wait_detail, executable_size = _conditional_sell_readiness(order['token_id'], order['size'], order)
    if not ready:
        order['sell_retries'] += 1
        if order['sell_retries'] > SELL_PLACE_MAX_RETRIES:
            with state.lock:
                order['status'] = 'failed'
                order['completion_time'] = time.time()
            _emit_log('error', '❌',
                      f'Не дождались токенов в CLOB для продажи после {SELL_PLACE_MAX_RETRIES} попыток. {wait_detail}')
            _broadcast_orders()
            return
        if order['sell_retries'] == 1 or order['sell_retries'] % 5 == 0:
            _emit_log('warning', '⏳',
                      f'{wait_detail} (попытка {order["sell_retries"]}/{SELL_PLACE_MAX_RETRIES})')
        return
    if executable_size <= 0:
        order['sell_retries'] += 1
        if order['sell_retries'] > SELL_PLACE_MAX_RETRIES:
            with state.lock:
                order['status'] = 'failed'
                order['completion_time'] = time.time()
            _emit_log('error', '❌', 'Не удалось определить доступный размер для продажи')
            _broadcast_orders()
        return

    try:
        sell_size = round(executable_size, 6)
        # Use actual executable size if matched balance is a bit lower than planned.
        if sell_size < float(order['size']):
            _emit_log(
                'info',
                'ℹ️',
                f'Продажа частичным объёмом: доступно {sell_size} из {order["size"]} акций'
            )
        order['size'] = sell_size

        order_args = OrderArgs(
            token_id=order['token_id'],
            price=sell_price,
            size=sell_size,
            side="SELL",
        )

        signed_order = state.client.create_order(order_args)
        resp = state.client.post_order(signed_order, OrderType.GTC)

        if resp and resp.get("success"):
            sell_order_id = resp.get("orderID", "unknown")

            with state.lock:
                order['sell_order_id'] = sell_order_id
                order['status'] = 'selling'

            _emit_log('success', '📋',
                      f'🔴 ПРОДАЖА ВЫСТАВЛЕНА: {order["side"]} по ${sell_price:.2f} '
                      f'(ордер: {sell_order_id[:12]}...)')
            _broadcast_orders()
        else:
            error_msg = str(resp) if resp else "No response"
            order['sell_retries'] += 1
            if order['sell_retries'] > SELL_PLACE_MAX_RETRIES:
                with state.lock:
                    order['status'] = 'failed'
                    order['completion_time'] = time.time()
                _emit_log('error', '❌', f'Критическая ошибка: не удалось выставить продажу после {SELL_PLACE_MAX_RETRIES} попыток. {error_msg}')
                _broadcast_orders()
            else:
                _emit_log('warning', '⏳', f'Ожидание токенов на кошельке... (попытка {order["sell_retries"]}/{SELL_PLACE_MAX_RETRIES})')
                logger.warning(f"Sell order delayed: {error_msg}")

    except Exception as e:
        logger.error(f"Sell order error: {e}")
        order['sell_retries'] += 1
        if order['sell_retries'] > SELL_PLACE_MAX_RETRIES:
            with state.lock:
                order['status'] = 'failed'
                order['completion_time'] = time.time()
            _emit_log('error', '❌', f'Ошибка при продаже: {str(e)}')
            _broadcast_orders()


def _check_sell_order(order):
    """Check if a SELL order has been filled."""
    try:
        if not order.get('sell_order_id'):
            return

        order_info = state.client.get_order(order['sell_order_id'])
        if not order_info:
            order['missing_count'] = order.get('missing_count', 0) + 1
            if order['missing_count'] >= 10:
                with state.lock:
                    order['status'] = 'cancelled'
                    order['completion_time'] = time.time()
                _emit_log('warning', '⚠️', f'Ордер на продажу {order.get("side", "")} не существует на сервере (закрыт вручную?). Удалён из UI.')
                _broadcast_orders()
            return
        else:
            order['missing_count'] = 0

        status = order_info.get("status", "").upper()

        if status == "MATCHED":
            profit = order['sell_price'] - order['buy_price']
            profit_total = profit * order['size']

            with state.lock:
                order['status'] = 'sold'
                order['completion_time'] = time.time()
                order['sell_matched_time'] = time.time()

            if profit_total > 0 and 'condition_id' in order and 'window_start' in order:
                import signals_db
                signals_db.record_successful_trade(
                    event_id=order['condition_id'],
                    side=order['side'],
                    buy_price=order['buy_price'],
                    sell_price=order['sell_price'],
                    profit=profit_total,
                    buy_time=order['buy_matched_time'],
                    sell_time=order['sell_matched_time'],
                    window_start=order['window_start']
                )

            _emit_log('profit', '💰',
                      f'💰 ПРОДАЖА ЗАВЕРШЕНА: {order["side"]} | '
                      f'Покупка: ${order["buy_price"]:.2f} → Продажа: ${order["sell_price"]:.2f} | '
                      f'Профит: +${profit:.2f}/акция (+${profit_total:.2f} всего)')
            _broadcast_orders()

            # Refresh balance after sell
            _fetch_balance()
            socketio.emit('balance', _balance_payload())
            
        elif status in ("CANCELLED", "EXPIRED"):
            with state.lock:
                order['status'] = 'failed'
                order['completion_time'] = time.time()
            _emit_log('warning', '⚠️',
                      f'Ордер на продажу {order["side"]} отменён/истёк')
            _broadcast_orders()

    except Exception as e:
        logger.error(f"Check sell order error: {e}")


# ═══════════════════════════════════════════
#  INITIALIZATION
# ═══════════════════════════════════════════

def _probe_clob_reachability():
    """Детальная ошибка, если py_clob вернул Request exception без HTTP-кода."""
    try:
        import httpx
        if _IS_CLOB_V2:
            from py_clob_client_v2.http_helpers import helpers as _clob_http
        else:
            from py_clob_client.http_helpers import helpers as _clob_http

        r = _clob_http._http_client.get(HOST.rstrip("/") + "/", timeout=20.0)
        logger.info("Доступность CLOB: GET %s/ → HTTP %s", HOST.rstrip("/"), r.status_code)
    except Exception as ex:
        logger.error(
            "Сеть до %s: %s: %s",
            HOST,
            type(ex).__name__,
            ex,
        )
        logger.error(
            "Если раньше работало: проверьте VPN/прокси/файрвол, DNS; в .env задайте PROXY_URL=... "
            "или отключите HTTP/2: CLOB_USE_HTTP2=0 (перезапуск после правки)."
        )


def init_clob_client():
    """Initialize Polymarket CLOB client with authentication."""
    if not PRIVATE_KEY or not FUNDER_ADDRESS:
        logger.error("Missing PRIVATE_KEY or FUNDER_ADDRESS in .env!")
        return False

    try:
        client = ClobClient(
            host=HOST,
            key=PRIVATE_KEY,
            chain_id=CHAIN_ID,
            signature_type=1,  # 0 for EOA, 1 for POLY_PROXY, 2 for POLY_GNOSIS_SAFE
            funder=FUNDER_ADDRESS
        )

        derived = client.get_address()
        if derived and derived.lower() != FUNDER_ADDRESS.lower():
            logger.warning(
                "Ключ подписывает адрес %s, FUNDER в .env %s — при Polymarket Proxy (signature_type=1) "
                "это обычно нормально: FUNDER должен совпадать с вашим торговым кошельком в Polymarket.",
                derived,
                FUNDER_ADDRESS,
            )

        creds = client.create_or_derive_api_key() if _IS_CLOB_V2 else client.create_or_derive_api_creds()
        client.set_api_creds(creds)

        state.client = client
        logger.info("CLOB client initialized for funder %s (signer %s)", FUNDER_ADDRESS, derived or "—")
        return True

    except Exception as e:
        logger.error("CLOB client init failed: %s", e)
        _probe_clob_reachability()
        return False


# ═══════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════

if __name__ == '__main__':
    logger.info("=" * 50)
    logger.info("  Polymarket BTC Up/Down 5m — Web Trader")
    logger.info("=" * 50)

    # Initialize CLOB
    if not init_clob_client():
        logger.error("Failed to initialize. Check your .env settings.")
        sys.exit(1)

    # Fetch initial balance
    _fetch_balance()
    if state.balance is not None:
        logger.info(f"USDC Balance: ${state.balance:.2f}")

    # Start background threads
    socketio.start_background_task(slow_polling_loop)
    socketio.start_background_task(price_polling_loop)
    socketio.start_background_task(order_monitoring_loop)

    logger.info(f"Web interface starting on http://localhost:{WEB_PORT}")
    logger.info(f"Polling interval: {CHECK_INTERVAL_SEC}s | Buy offset: -{int(BUY_OFFSET*100)}¢ | Sell offset: +{int(SELL_OFFSET*100)}¢")

    socketio.run(app, host='0.0.0.0', port=WEB_PORT, debug=False)
