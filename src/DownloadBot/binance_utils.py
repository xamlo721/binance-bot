import requests
import time
import aiohttp
import asyncio

from logger import logger
from DownloadBot.config import *

from datetime import datetime

from bot_types import KlineRecord

from collections import deque
from typing import List
from typing import Optional
from typing import Callable
from typing import Any

# ============== Rate Limiter для Binance API ==============

class BinanceRateLimiter:
    """Ограничитель запросов для Binance API"""
    
    def __init__(self, requests_per_minute: int = BINANCE_API_LIMIT):
        """
        Args:
            requests_per_minute: Максимальное количество запросов в минуту (Binance: 1200)
        """
        self.requests_per_minute = requests_per_minute
        self.request_timestamps = deque()
        self._lock = asyncio.Lock()
        
    async def wait_if_needed(self):
        """Ожидает, если превышен лимит запросов"""
        async with self._lock:
            now = time.time()
            
            # Удаляем запросы старше 1 минуты
            while self.request_timestamps and self.request_timestamps[0] < now - 60:
                self.request_timestamps.popleft()
            
            # Если достигнут лимит, ждем
            if len(self.request_timestamps) >= self.requests_per_minute:
                oldest = self.request_timestamps[0]
                wait_time = 60 - (now - oldest)
                if wait_time > 0:
                    logger.warning(f"Достигнут лимит запросов Binance. Ожидание {wait_time:.2f} секунд...")
                    await asyncio.sleep(wait_time)
                    
                    # После ожидания очищаем старые записи
                    now = time.time()
                    while self.request_timestamps and self.request_timestamps[0] < now - 60:
                        self.request_timestamps.popleft()
            
            # Добавляем текущий запрос
            self.request_timestamps.append(now)


# ============== Модифицированные функции с ограничением ==============

def get_trading_symbols():
    """Получение списка торгующихся тикеров"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url)
        data = response.json()
        
        symbols = []
        for symbol_info in data['symbols']:
            if (symbol_info['status'] == 'TRADING' and symbol_info.get('contractType') == 'PERPETUAL'):
                # Исключаем тикеры с "USDC"
                symbol_name = symbol_info['symbol']
                if not symbol_name.startswith("USDC") and not symbol_name.endswith("USDC"):
                    symbols.append(symbol_name)
                #symbols.append(symbol_info['symbol'])
        
        return symbols
    except Exception as e:
        logger.error(f"Ошибка при получении списка тикеров: {str(e)}")
        return []

async def fetch_klines_for_symbols(
    session: aiohttp.ClientSession,
    symbols: List[str],
    count: int = 1,
    end_timestamp: Optional[int] = None,
    max_concurrent: int = THREAD_POOL_SIZE
) -> List[List[KlineRecord]]:
    """
    Загружает `count` минутных свечей для всех тикеров.
    
    Args:
        session: aiohttp ClientSession
        symbols: список тикеров
        count: количество минут (по умолчанию 1)
        end_timestamp: конечная метка времени в мс. Если None → текущая завершённая минута - 1 сек.
        max_concurrent: макс. параллельных запросов
    
    Returns:
        List[List[KlineRecord]]: список по минутам, где каждая минута — список свечей тикеров
    """
    if count <= 0:
        raise ValueError("count must be positive")

    # Авто-расчёт end_timestamp
    if end_timestamp is None:
        now_ms = int(time.time() * 1000)
        end_timestamp = now_ms - (now_ms % 60000) - 1

    semaphore = asyncio.Semaphore(min(max_concurrent, 50))
    limiter = BinanceRateLimiter(requests_per_minute=BINANCE_API_LIMIT)

    tasks = []
    for symbol in symbols:
        task = asyncio.create_task(
            fetch_ticker_1m_volumes_for_time(session, symbol, count, limiter, end_timestamp)
        )
        tasks.append(task)

    # Выполняем и собираем
    tickers_data: List[List[KlineRecord]] = []
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            if result:
                tickers_data.append(result)
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")

    # Транспонируем: [tickers][minutes] → [minutes][tickers]
    if not tickers_data:
        return [[] for _ in range(count)]

    minutes_data: List[List[KlineRecord]] = [[] for _ in range(count)]
    for ticker_candles in tickers_data:
        for i, candle in enumerate(ticker_candles):
            if i < count:
                minutes_data[i].append(candle)

    logger.info(f"Загружено {len(minutes_data)} минут по {len(minutes_data[0])} тикерам")
    return minutes_data

async def fetch_ticker_1m_volumes_for_time(session, symbol, count: int, limiter, end_timestamp: int) -> list[KlineRecord] | None:
    """Асинхронное получение данных для одного тикера для конкретного времени"""
    await limiter.wait_if_needed()  # Ждем разрешения от rate limiter

    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': '1m',
        'limit': count,
        'endTime': end_timestamp
    }

        # Логирование запроса
    # Формируем полный URL для логирования
    from urllib.parse import urlencode
    full_url = f"{url}?{urlencode(params)}"
    logger.debug(f"Запрос к Binance API: {full_url}")    

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                data = await response.json()

                response_size = len(str(data))
                logger.debug(f"🟢 ДАННЫЕ: Получено {len(data)} свечей, размер ответа ~{response_size} байт")

                if data and len(data) > 0:

                        # Логируем первую и последнюю свечу для отладки
                    first_kline = data[0]
                    last_kline = data[-1]

                    first_time = datetime.fromtimestamp(first_kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    last_time = datetime.fromtimestamp(last_kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                    logger.debug(f"📊 ДИАПАЗОН: {symbol} с {first_time} по {last_time} ({len(data)} свечей)")
                    
                    candles = []
                    for kline in data:
                        # Структура, согласно документации
                        # https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
                        candle = KlineRecord(
                            symbol=symbol,
                            open_time=kline[0],
                            open=float(kline[1]),
                            high=float(kline[2]),
                            low=float(kline[3]),
                            close=float(kline[4]),
                            volume=float(kline[5]),
                            close_time=int(kline[6]),
                            quote_assets_volume=float(kline[7]),
                            num_of_trades=kline[8],
                            taker_buy_base_volume=float(kline[9]),
                            taker_buy_quote_volume=float(kline[10])
                        )
                        candles.append(candle)
                    logger.debug(f"Загружено {len(candles)} свечей для {symbol}")
                    return candles

            else:
                logger.error(f"❌ Ошибка HTTP {response.status} для {symbol}")
                await asyncio.sleep(1)
                
    except Exception as e:
        logger.error(f"❌ Ошибка для {symbol}: {str(e)}")
    
    return None
