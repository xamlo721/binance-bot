import requests
import time
import aiohttp
import asyncio

from datetime import datetime

from collections import OrderedDict
from typing import List
from typing import Optional

from urllib.parse import urlencode

from logger import logger
from bot_types import KlineRecord
from DownloadBot.config import *
from DownloadBot.binance_limiter import get_kline_weight
from DownloadBot.binance_limiter import BinanceRateLimiter

async def get_binance_server_time(session: aiohttp.ClientSession) -> Optional[int]:
    """
    Возвращает серверное время Binance в миллисекундах.
    """
    url = "https://fapi.binance.com/fapi/v1/time"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data['serverTime']
            else:
                logger.error(f"Ошибка получения времени Binance: HTTP {resp.status}")
                return None
    except Exception as e:
        logger.error(f"Исключение при запросе времени Binance: {e}")
        return None
    
async def get_trading_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Получение списка торгующихся тикеров"""
    """Асинхронное получение списка торгующихся тикеров с повторными попытками"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:

            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    symbols = []
                    for symbol_info in data['symbols']:
                        if (symbol_info['status'] == 'TRADING' and 
                            symbol_info.get('contractType') == 'PERPETUAL'):
                            symbol_name = symbol_info['symbol']
                            if not symbol_name.startswith("USDC") and not symbol_name.endswith("USDC"):
                                symbols.append(symbol_name)
                    logger.info(f"✅ Получено {len(symbols)} тикеров через асинхронный запрос")
                    return symbols
                else:
                    logger.warning(f"HTTP {response.status} при получении списка тикеров, попытка {attempt+1}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Ошибка при получении списка тикеров (попытка {attempt+1}): {e}")
            if attempt == max_retries - 1:
                logger.error("Не удалось получить список тикеров после всех попыток")
                return []
            await asyncio.sleep(2 ** attempt)  # экспоненциальная задержка

    return []

# ============== Модифицированные функции с ограничением ==============

async def fetch_klines_paginated(session: aiohttp.ClientSession, symbol: str, count: int, end_timestamp: int, limiter: BinanceRateLimiter, semaphore: asyncio.Semaphore, max_retries = 5) -> list[KlineRecord] | None:
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
    # Автоповторы в случае ошибок
    for attempt in range(max_retries):
        try:
            
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
                                        # Пустой ответ – достигли начала истории
                                        logger.warning(f"⚠️ Для {symbol} нет данных за период {datetime.fromtimestamp(current_end / 1000).strftime('%Y-%m-%d %H:%M:%S')}(пустой ответ).")
                                        break

                                else:
                                    logger.error(f"❌ Ошибка HTTP {response.status} для {symbol}")
                                    await asyncio.sleep(1)
                                    break
                                    
                        except Exception as e:
                            logger.error(f"❌ Ошибка для {symbol}: {type(e).__name__}: {e}")
                            # при необходимости добавить traceback
                            # logger.debug(traceback.format_exc())
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
            
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == max_retries - 1:
                logger.error(f"❌ Ошибка для {symbol} после {max_retries} попыток: {e}")
                return None
            wait = 2 ** attempt  # экспоненциальная задержка
            logger.warning(f"⚠️ Попытка {attempt+1} для {symbol} не удалась ({e}), повтор через {wait} сек")
            await asyncio.sleep(wait)
    
async def fetch_klines_for_symbols(
    session: aiohttp.ClientSession,
    symbols: List[str],
    limiter: BinanceRateLimiter,
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
