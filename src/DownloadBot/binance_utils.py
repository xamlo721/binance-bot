import requests
import time
import aiohttp
import asyncio

from logger import logger
from DownloadBot.config import *

from datetime import datetime

from bot_types import KlineRecord

from collections import OrderedDict
from collections import deque
from typing import List
from typing import Optional
from typing import Callable
from typing import Any

from urllib.parse import urlencode


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
        
        return symbols
    except Exception as e:
        logger.error(f"Ошибка при получении списка тикеров: {str(e)}")
        return []

# ============== Rate Limiter для Binance API ==============

def get_kline_weight(limit: int) -> int:
    if limit <= 100:
        return 1
    elif limit <= 500:
        return 2
    elif limit <= 1000:
        return 5
    else:
        return 10
    
class BinanceRateLimiter:
    """Ограничитель запросов для Binance API"""
    
    def __init__(self, requests_per_minute: int, requests_weight_per_minute: int):
        """
        Args:
            requests_per_minute: Максимальное количество запросов в минуту (Binance: 1200)
        """
        self.weight_limit = requests_weight_per_minute
        self.requests_limit = requests_per_minute
        self.requests = deque()
        self._lock = asyncio.Lock()
        
    async def wait_if_needed(self, weight: int = 1):
        """Ожидает, если превышен лимит запросов"""
        async with self._lock:
            now = time.time()
            
            # Удаляем запросы старше 1 минуты
            while self.requests and self.requests[0][0] < now - 60:
                self.requests.popleft()
            
            # Текущий вес запросов
            total_weight = sum(w for _, w in self.requests)
            
            # Если достигнут лимит, ждем
            while total_weight + weight > self.weight_limit or len(self.requests) >= self.requests_limit:
                # Ждём, пока освободится достаточно веса
                # Для простоты ждём до истечения самого старого запроса

                if not self.requests:
                    # Очередь пуста, но лимит формально превышен (маловероятно) – просто ждём 1 сек
                    await asyncio.sleep(1)
                    now = time.time()
                    continue

                # Ждём, пока истечёт самый старый запрос
                oldest = self.requests[0][0]
                wait_time = 60 - (now - oldest)

                if wait_time > 0:
                    # Много потоков спамят в консоль, когда стукаются об лимит.
                    # Обычно первый поток стукается в лимитер и остальные его догоняют
                    # У первого задержка в 40-55 секунд, а у остальных 0,0ХХ секунд (зависит от камня)
                    if wait_time > 1:
                        logger.warning(f"Достигнут лимит запросов Binance. Ожидание {wait_time:.2f} секунд...")
                    await asyncio.sleep(wait_time)
                    
                # После ожидания очищаем старые записи
                now = time.time()
                while self.requests and self.requests[0][0] < now - 60:
                    self.requests.popleft()
                        
                # Пересчитываем общий вес для следующей итерации цикла
                total_weight = sum(w for _, w in self.requests)
            
            # Добавляем текущий запрос
            self.requests.append((time.time(), weight))


# ============== Модифицированные функции с ограничением ==============


async def fetch_klines_paginated(session: aiohttp.ClientSession, symbol: str, count: int, end_timestamp: int, limiter, semaphore: asyncio.Semaphore) -> list[KlineRecord] | None:
    """
    Получает исторические свечи с пагинацией (максимум 1500 за запрос).
    
    Args:
        session: aiohttp ClientSession
        symbol: тикер
        count: общее количество требуемых минут
        end_timestamp: конечная метка времени в мс
        limiter: BinanceRateLimiter для контроля лимитов
    
    Returns:
        List[KlineRecord]: список свечей
    """
    async with semaphore:  # ← применяем семафор к каждому запросу!

        if count <= 0:
            raise ValueError("count must be positive")
        

        # Авто-расчёт end_timestamp
        if end_timestamp is None:
            now_ms = int(time.time() * 1000)
            end_timestamp = now_ms - (now_ms % 60000) - 1    
            
        all_candles: List[KlineRecord] = []
        current_end = end_timestamp
        max_per_request = MAX_CANDLES_PER_REQUEST  # Лимит Binance для одного запроса

        while len(all_candles) < count:
            remaining = count - len(all_candles)
            request_count = min(remaining, max_per_request)

            if limiter:
                # Выясняем вес запроса
                request_weight: int = get_kline_weight(request_count)
                # Ждем разрешения от rate limiter
                await limiter.wait_if_needed(request_weight)

                url = "https://fapi.binance.com/fapi/v1/klines"
                params = {
                    'symbol': symbol,
                    'interval': '1m',
                    'limit': request_count,
                    'endTime': current_end
                }

                # Формируем полный URL для логирования
                full_url = f"{url}?{urlencode(params)}"
                logger.debug(f"Запрос к Binance API: {full_url}")    

                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()

                            response_size = len(str(data))
                            logger.debug(f"🟢 ДАННЫЕ: Получено {len(data)} свечей, размер ответа ~{response_size} байт")

                            if data and len(data) > 0:
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
                                    all_candles.append(candle)
                                logger.debug(f"Загружено {len(all_candles)} свечей для {symbol}")

                                # Обновляем end_timestamp для следующего запроса
                                if data:
                                    current_end = data[0][0] - 1   # <-- ИСПРАВЛЕНО

                        else:
                            logger.error(f"❌ Ошибка HTTP {response.status} для {symbol}")
                            await asyncio.sleep(1)
                            break
                            
                except Exception as e:
                    logger.error(f"❌ Ошибка для {symbol}: {str(e)}")
                    break

        # Логируем первую и последнюю свечу полного диапазона (после пагинации)
        if all_candles:
            first_kline = all_candles[0]
            last_kline = all_candles[-1]

            first_time = datetime.fromtimestamp(first_kline.open_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            last_time = datetime.fromtimestamp(last_kline.open_time / 1000).strftime('%Y-%m-%d %H:%M:%S')

            logger.debug(f"📊 ДИАПАЗОН: {symbol} с {first_time} по {last_time} ({len(all_candles)} свечей)")
        else:
            logger.warning(f"⚠️ Не получено данных для {symbol}")

        return all_candles

async def fetch_klines_for_symbols(
    session: aiohttp.ClientSession,
    symbols: List[str],
    count: int = 1,
    end_timestamp: Optional[int] = None,
    max_concurrent: int = THREAD_POOL_SIZE
) -> OrderedDict[int, list[KlineRecord]]:
    """
    Загружает `count` минутных свечей для всех тикеров и возвращает их,
    сгруппированными по абсолютному номеру минуты (open_time // 60000).
    
    Args:
        session: aiohttp ClientSession
        symbols: список тикеров
        count: количество минут (по умолчанию 1)
        end_timestamp: конечная метка времени в мс. Если None → текущая завершённая минута - 1 сек.
        max_concurrent: макс. параллельных запросов

    Returns:
        OrderedDict[int, list[KlineRecord]]:
            ключ – номер минуты (open_time // 60000),
            значение – список записей для всех тикеров за эту минуту.
            Порядок ключей соответствует возрастанию минут (от старых к новым).
    """
    if count <= 0:
        raise ValueError("count must be positive")

    # Авто-расчёт end_timestamp
    if end_timestamp is None:
        now_ms = int(time.time() * 1000)
        end_timestamp = now_ms - (now_ms % 60000) - 1

    semaphore = asyncio.Semaphore(min(max_concurrent, 50))
    limiter = BinanceRateLimiter(BINANCE_API_REQUEST_LIMIT, BINANCE_API_WEIGHT_LIMIT)

    tasks = []
    for symbol in symbols:
        task = asyncio.create_task(
            fetch_klines_paginated(session, symbol, count, end_timestamp, limiter, semaphore)
        )
        tasks.append(task)

    # Собираем результаты в словарь "минута -> список записей"
    minute_to_records: dict[int, list[KlineRecord]] = {}

    for task in asyncio.as_completed(tasks):

        try:
            result = await task
            if not result:
                continue

            # Добавляем каждую свечу в соответствующую минуту
            for candle in result:
                minute_key = candle.open_time // 60000
                minute_to_records.setdefault(minute_key, []).append(candle)

        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
                

    sorted_minutes = sorted(minute_to_records.keys())
    result = OrderedDict((minute, minute_to_records[minute]) for minute in sorted_minutes)

    #logger.info(f"Загружено {len(result)} минут по {len(symbols)} тикерам")
    non_empty = sum(1 for recs in result.values() if recs)
    #logger.info(f"Реально загружено {non_empty} минут (из {count})")
    return result
