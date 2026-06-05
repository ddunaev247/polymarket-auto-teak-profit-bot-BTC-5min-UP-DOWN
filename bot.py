import sys
import os
import time
import logging

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
from rich.logging import RichHandler
from rich.console import Console

# Официальная библиотека Polymarket
try:
    from py_clob_client_v2 import ClobClient, TradeParams, OrderArgs
    from py_clob_client_v2.exceptions import PolyException as PolyApiException
    _IS_CLOB_V2 = True
except ImportError:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import TradeParams, OrderArgs
    from py_clob_client.exceptions import PolyApiException
    _IS_CLOB_V2 = False

load_dotenv()

# === НАСТРОЙКА ПРОКСИ (для обхода гео-блокировки Polymarket) ===
PROXY_URL = os.getenv("PROXY_URL", "")
if PROXY_URL:
    import httpx
    if _IS_CLOB_V2:
        from py_clob_client_v2.http_helpers import helpers as _clob_http
    else:
        from py_clob_client.http_helpers import helpers as _clob_http
    _clob_http._http_client = httpx.Client(http2=True, proxy=PROXY_URL)

# Индивидуальная настройка логирования консоли
console = Console()
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%H:%M:%S]",
    handlers=[RichHandler(console=console, rich_tracebacks=True, markup=True, show_path=False)]
)
logger = logging.getLogger("ATP_Bot")

# Подавляем спам HTTP-запросов от httpx / httpcore
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Загрузка переменных окружения (конфигурации)
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS")
PROXY_ADDRESS = os.getenv("PROXY_ADDRESS", "")  # Polymarket Proxy Wallet
PROFIT_MULTIPLIER = float(os.getenv("PROFIT_MULTIPLIER", "1.15"))
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "3"))

HOST = os.getenv("HOST", "https://clob.polymarket.com")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))

# Адрес для чтения сделок: приоритет PROXY_ADDRESS, далее FUNDER_ADDRESS
TRADE_ADDRESS = PROXY_ADDRESS or FUNDER_ADDRESS

if not PRIVATE_KEY or not FUNDER_ADDRESS:
    logger.error("🚫 Отсутствует PRIVATE_KEY или FUNDER_ADDRESS в .env файле! Сначала переименуйте .env.example в .env и заполните.")
    exit(1)

# Хранилище ID обработанных сделок, чтобы не выставлять лимитки повторно для одних и тех же.
# В production боте лучше использовать базу данных (SQLite) вместо set()!
processed_trades = set()

def place_take_profit_order(client: ClobClient, trade: dict):
    """
    Функция расчета цели (take profit) и выставления лимитного ордера на продажу.
    """
    trade_id = trade.get('id')
    asset_id = trade.get('asset_id')
    original_price = float(trade.get('price', 0))
    size = float(trade.get('size', 0))
    side = trade.get('side') # BUY or SELL

    # Если мы продаем (SELL), нам Take-profit не нужен. Игнорируем.
    if side != 'BUY':
        return

    # Проверка: если мы уже выставляли ордер на эту покупку, мы ее скипаем.
    if trade_id in processed_trades:
        return

    processed_trades.add(trade_id)

    # 1. Расчет цены (Пример: 0.50 * 1.15 = 0.575)
    target_price = original_price * PROFIT_MULTIPLIER

    # 2. Математическое округление (до 2 знаков после запятой, шаг Polymarket - $0.01)
    target_price = round(target_price, 2)
    
    # 3. Защита: цена доли на Polymarket не может превышать $1.00. 
    # Ставим максимум 0.99, иначе API отвергнет ордер.
    if target_price >= 0.99:
        logger.warning(f"Желаемая цена {target_price} превышает или равна $0.99, ограничиваем до $0.98")
        target_price = 0.98

    logger.info(f"Обнаружена новая покупка: Сделка [bold cyan]{trade_id}[/] | Актив [bold yellow]{asset_id}[/] | Размер: [bold]{size}[/] акций | Цена покупки: [bold green]${original_price}[/]")
    logger.info(f"Выставляю лимитный ордер на [bold red]ПРОДАЖУ[/] (Auto-Take-Profit) по цене: [bold green]${target_price}[/]")

    try:
        # Для выставления ордера в py_clob_client используется датакласс OrderArgs
        order_args = OrderArgs(
            token_id=asset_id,        
            price=target_price,
            size=size,
            side="SELL",
        )
        
        # client.create_and_post_order из PyClob автоматически:
        # 1. Создает payload ордера
        # 2. Подписывает его локально ключом L2(EIP-712)
        # 3. Транслирует в Центральную книгу реестров (CLOB API). Транзакция бесплатна по газу.
        resp = client.create_and_post_order(order_args)
        
        if resp and resp.get("success"):
            order_id = resp.get("orderID")
            logger.info(f"[green]Успех![/] Лимитный ордер [bold cyan]{order_id}[/] моментально выставлен в стакан.")
            
            # Верификация факта нахождения ордера в стакане:
            # Запросим статус ордера спустя 0.5с (чтобы сервер успел синхронизировать базу).
            time.sleep(0.5)
            order_info = client.get_order(order_id)
            if order_info and order_info.get("status") == "LIVE":
                logger.info(f"Проверка API пройдена: Ордер [bold cyan]{order_id}[/] status = [bold green]LIVE[/] в стакане!")
            else:
                logger.warning(f"Ордер отправлен, но в стакане не [bold green]LIVE[/]: {order_info}")
        else:
            logger.error(f"Ошибка вызова Polymarket API: {resp}")

    except PolyApiException as api_e:
        logger.error(f"API Polymarket отклонил ордер [bold red](HTTP {api_e.status_code})[/]: {api_e.error_msg}")
    except Exception as e:
        logger.error(f"Критическая ошибка при генерации лимитного ордера: {e}")

def watch_trades():
    logger.info("Initializing Polymarket CLOB Client...")
    
    client = ClobClient(
        host=HOST,
        key=PRIVATE_KEY,
        chain_id=CHAIN_ID,
        signature_type=1, # EOA = Externally Owned Account (Ваш кошелек)
        funder=FUNDER_ADDRESS
    )

    # Проверка: адрес из приватного ключа должен совпадать с FUNDER_ADDRESS
    derived_address = client.get_address()
    if derived_address:
        if derived_address.lower() != FUNDER_ADDRESS.lower():
            logger.warning(f"[bold red]ВНИМАНИЕ:[/] Адрес из PRIVATE_KEY = [bold]{derived_address}[/]")
            logger.warning(f"[bold red]ВНИМАНИЕ:[/] FUNDER_ADDRESS в .env = [bold]{FUNDER_ADDRESS}[/]")
            logger.warning("Они НЕ совпадают! Исправьте .env, иначе ордера будут отклонены (403).")
        else:
            logger.info(f"Адрес кошелька подтвержден: [bold green]{derived_address}[/]")

    try:
        # Авторизация и создание рабочей сессии L2 (создается L2 API key для CLOB)
        creds = client.create_or_derive_api_key() if _IS_CLOB_V2 else client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        
        logger.info(f"[b]Бот запущен![/b] Целевой алгоритм профита: [bold green]+{int((PROFIT_MULTIPLIER - 1) * 100)}%[/]")
        logger.info(f"Отслеживаем сделки по адресу: [bold cyan]{TRADE_ADDRESS}[/]")

        # === СНИМОК НАЧАЛЬНЫХ СДЕЛОК ===
        logger.info("Загружаю историю сделок для начального снимка (snapshot)...")
        try:
            initial_trades = client.get_trades(TradeParams(maker_address=TRADE_ADDRESS))
            if initial_trades:
                for t in initial_trades:
                    tid = t.get('id')
                    if tid:
                        processed_trades.add(tid)
                logger.info(f"Snapshot готов: [bold cyan]{len(processed_trades)}[/] исторических сделок помечены как обработанные.")
            else:
                logger.info("Исторических сделок не найдено. Бот готов к работе с чистого листа.")
        except Exception as snap_e:
            logger.warning(f"Не удалось загрузить snapshot: {snap_e}")
        
        logger.info(f"Режим отслеживания включен. Пингуем биржу каждые [bold cyan]{CHECK_INTERVAL_SEC}[/] сек.")
        logger.info("Ожидаю новые покупки...")

        while True:
            try:
                # Получаем последние сделки пользователя с CLOB по нашему адресу (Мейкер).
                trades = client.get_trades(TradeParams(maker_address=TRADE_ADDRESS))
                if trades:
                    new_count = 0
                    for trade in trades:
                        tid = trade.get('id')
                        if tid and tid not in processed_trades:
                            new_count += 1
                            place_take_profit_order(client, trade)
                    if new_count == 0:
                        logger.debug("Новых сделок нет.")

            except Exception as loop_e:
                logger.error(f"Ошибка в главном цикле парсинга API (возможно Rate Limit HTTP): {loop_e}")

            # Ждем 3 секунды (как запросил пользователь) перед следующим пингом
            time.sleep(CHECK_INTERVAL_SEC)
            
    except Exception as setup_e:
         logger.error(f"Ошибка аутентификации клиента с сетью: {setup_e}")

if __name__ == "__main__":
    try:
        watch_trades()
    except KeyboardInterrupt:
        logger.info("Работа скрипта-помощника остановлена пользователем командой CTRL+C.")
