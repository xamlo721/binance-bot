# main.py
"""
Главный модуль бота.
Сохраняет в памяти последние 24 часа (1440 минут) свечей всех доступных тикеров и
каждую минуту скачивает новые данные.
"""

import socket
import sys
import time
import aiohttp
import asyncio
from collections import OrderedDict


from datetime import datetime
from pathlib import Path

src_path = Path(__file__).resolve().parent.parent
download_bot_src_path = Path(__file__).resolve().parent
sys.path.append(str(src_path))
sys.path.append(str(download_bot_src_path))

from binance_utils import get_binance_server_time
from binance_utils import get_trading_symbols
from binance_utils import fetch_klines_for_symbols
from DownloadBot.binance_limiter import BinanceRateLimiter

from bot_types import KlineRecord

from logger import *
from config import *
from udp_server import UDPMarketDataServer

# Список всех отметок за MINUTE_CANDLES_LIMIT минут 
#                   <НОМЕР_МИНУТЫ List<МИНУТНАЯ_ЗАПИСЬ>>
global_data: OrderedDict[int, list[KlineRecord]] = OrderedDict()

# Глобальное смещение (серверное время - локальное время), миллисекунды
time_offset_ms: int = 0

def _format_ts(ts_ms: int) -> str:
    """Преобразует timestamp в миллисекундах в строку ГГГГ-ММ-ДД ЧЧ:ММ:СС"""
    return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

async def update_time_offset(session: aiohttp.ClientSession) -> None:
    """Обновляет смещение времени между локальной машиной и Binance."""
    global time_offset_ms
    server_time = await get_binance_server_time(session)
    if server_time is not None:
        local_time = int(time.time() * 1000)
        new_offset = server_time - local_time
        if abs(new_offset - time_offset_ms) > 500:  # логируем только значительные изменения
            logger.info(f"Коррекция времени: смещение изменено с {time_offset_ms} мс на {new_offset} мс")
        time_offset_ms = new_offset
    else:
        logger.warning("Не удалось получить время Binance, смещение не обновлено")

def get_adjusted_now_ms() -> int:
    """Возвращает текущее время в миллисекундах с учётом смещения с Binance."""
    return int(time.time() * 1000) + time_offset_ms

def is_storage_consistent(candle_dict: dict[int, list[KlineRecord]]) -> bool:
    """
    Проверяет корректность словаря минутных свечей.

    Условия:
      1. Для каждого KlineRecord поле open_time должно совпадать с ключом (номером минуты),
         под которым запись хранится.
      2. Ключи словаря (номера минут) должны образовывать непрерывную
         возрастающую последовательность без пропусков и повторов.

    Args:
        candle_dict: словарь вида {номер_минуты: список[CandleRecord]}

    Returns:
        True, если обе проверки пройдены успешно, иначе False.
    """
    # Пустой словарь считаем корректным (или можно изменить логику при необходимости)
    if not candle_dict:
        return True

    # 1. Проверка непрерывности ключей
    keys = list(candle_dict.keys())
    for i in range(1, len(keys)):
        if keys[i] != keys[i-1] + 1:
            print(f"Ошибка: разрыв в последовательности минут между {_format_ts(keys[i-1] * 60000)} и {_format_ts(keys[i] * 60000)}")
            return False

    # 2. Проверка совпадения open_time с ключом
    for minute, records in candle_dict.items():
        for record in records:
            if record.open_time != minute * 60000:
                print(f"Ошибка: запись {record} имеет open_time={_format_ts(record.open_time)}, "
                      f"не совпадающий с ключом {_format_ts(minute * 60000)} ")
                return False

    return True

async def fetch_candles(session: aiohttp.ClientSession, symbols: list[str], limiter: BinanceRateLimiter, count: int = 1440) -> None:
    """
    Получаем последние `count` минут (по умолчанию 24 часа) и сохраняем их в глобальном хранилище.
    """
    # Текущий момент
    now_timestamp = get_adjusted_now_ms()
    # Последняя завершенная минута
    end_timestamp = now_timestamp - (now_timestamp % 60000) - 1

    # Запрос к Binance: все тикеры за указанное количество минут до `end_timestamp`
    period_data: OrderedDict[int, list[KlineRecord]] = await fetch_klines_for_symbols(session, symbols, limiter, count, end_timestamp)
    logger.debug(f"fetch вернул {len(period_data)} отметок")

    logger.debug(f"До сохранения там {len(global_data)} отметок")
    # Сохраняем свечи по абсолютному номеру минуты
    for minute_key, records in period_data.items():
        # Если минута уже существует, добавляем записи (предполагаем, что дубликатов нет)
        if minute_key in global_data:   
            global_data[minute_key].extend(records)
        else:
            global_data[minute_key] = records
    logger.debug(f"После сохранения в global_data {len(global_data)} минут")

def cleanup_storage(storage_imit: int):
    # Убираем старые данные – оставляем только последние 1440 минут
    if len(global_data) > storage_imit:
        sorted_keys = sorted(global_data.keys())
        for key in sorted_keys[:len(global_data) - storage_imit]:
            del global_data[key]

    if (is_storage_consistent(global_data)):
        logger.info(f"✅ Хранилище консистентно. Период хранения с"
                    f" {_format_ts(list(global_data.keys())[0] * 60000)} по {_format_ts(list(global_data.keys())[-1] * 60000)}")
    else:
        logger.error(f"❌ Хранилище неконсистентно. Период хранения с"
                     f" {_format_ts(list(global_data.keys())[0] * 60000)} по {_format_ts(list(global_data.keys())[-1] * 60000)}")

def check_space(now_ms: int) -> int:
    """
    Определяет количество пропущенных минут в хранилище global_data.
    
    Args:
        now_ms: текущее время в миллисекундах.
    
    Returns:
        int: сколько минут нужно догрузить, чтобы хранилище содержало все завершённые
             минуты до текущего момента включительно.
    """
    if not global_data:
        # Хранилище пусто, нужно загрузить MAX_CACHED_CANDLES минут
        return MAX_CACHED_CANDLES

    # Последняя завершенная минута
    last_completed_minute = (now_ms - (now_ms % 60000) - 1) // 60000
    last_stored_minute = max(global_data.keys())

    if last_stored_minute >= last_completed_minute:
        return 0
    else:
        return last_completed_minute - last_stored_minute

def create_session():
    connector = aiohttp.TCPConnector(
        resolver = aiohttp.resolver.ThreadedResolver(),
        # resolver=aiohttp.resolver.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1", "208.67.222.222"]),
        family=socket.AF_INET,
        limit=0,
        use_dns_cache=True,
        ttl_dns_cache=600
    )
    # Увеличиваем общий таймаут и таймаут подключения
    timeout = aiohttp.ClientTimeout(total=30, connect=15)
    return aiohttp.ClientSession(connector=connector, timeout=timeout)

async def main_loop(limiter: BinanceRateLimiter, session: aiohttp.ClientSession, server: UDPMarketDataServer):
    """
    Main loop: update last candles and fetch new minute candles every minute.
    """

    symbols: list[str] = []

    while True:
        await update_time_offset(session) 
        tick_start_time = time.time()
        now_ms = get_adjusted_now_ms()
        server.set_busy(True)
        server.set_time_offset(time_offset_ms)
        missing = check_space(now_ms)

        try:

            if missing > 0:
                logger.info(f"Доступно минут для скачивания: {missing}. Догоняем...")

                # ==================================================================== # 
                logger.info(f"Обновляем список тикеров")
                symbols = await get_trading_symbols(session)
                if not symbols:
                    logger.error("Не удалось получить список тикеров")
                    continue
                logger.info(f"✅ Получено {len(symbols)} тикеров")
                # ==================================================================== # 

                server.update_symbols(symbols)

                await fetch_candles(
                    session = session, 
                    symbols = symbols, 
                    limiter = limiter,  
                    count = missing
                )

                cleanup_storage(MAX_CACHED_CANDLES)
                server.update_data(global_data)
        
        except Exception as e:
            logger.error(f"Ошибка обработки цикла: {e}")
            

        # Вычисляем сколько осталось ждать
        elapsed = time.time() - tick_start_time
        wait_time = max(0, 5 - elapsed)  # минимум 0 секунд

        server.set_busy(False)         # <- освобождаем

        if missing != 0:
            logger.info(f"✅ Updated {len(global_data)} / {len(symbols)} tickers for {elapsed:.2f} seconds")

        await asyncio.sleep(wait_time)


async def main():
    """
    Main entry point.
    """

    server = UDPMarketDataServer(host=DOWNLOADER_UDP_IP, port=DOWNLOADER_UDP_PORT)
    limiter = BinanceRateLimiter(BINANCE_API_REQUEST_LIMIT, BINANCE_API_WEIGHT_LIMIT)

    try:

        async with create_session() as session:
            await update_time_offset(session)
            server.set_time_offset(time_offset_ms)

            logger.info(f"Обновляем список тикеров")
            symbols = await get_trading_symbols(session)
            if not symbols:
                logger.error("Не удалось получить список тикеров")
                return
            logger.info(f"✅ Получено {len(symbols)} тикеров seconds")
            server.update_symbols(symbols)
            
            # Берём только первые 10 тикеров (для дебага)
            symbols = symbols# [:50]
            
            # Скачиваем архивные свечи перед запуском        
            total_requests = len(symbols) * ((MAX_CACHED_CANDLES + MAX_CANDLES_PER_REQUEST - 1) // MAX_CANDLES_PER_REQUEST)
            # Оценка по весу (средний вес 10)
            estimated_min_by_weight = (10 * total_requests) / BINANCE_API_WEIGHT_LIMIT
            # Оценка по количеству запросов
            estimated_min_by_count = total_requests / BINANCE_API_REQUEST_LIMIT
            # Берём максимум как пессимистичную оценку
            estimated_minutes = max(estimated_min_by_weight, estimated_min_by_count)

            logger.info(f"Скачиваем {MAX_CACHED_CANDLES} минутных отметок по каждому тикеру. .")
            logger.info(f"Всего {MAX_CACHED_CANDLES * len(symbols)} свечей. Понадобится {total_requests} запросов.")
            logger.info(f"Ориентировочно это займет {estimated_minutes * 60} секунд при соблюдении лимитов Binance.")
            await fetch_candles(
                session = session, 
                symbols = symbols, 
                limiter = limiter, 
                count = MAX_CACHED_CANDLES
            )

            logger.info(f"✅ Updated {len(global_data)} tickers!")
            await server.start()
            logger.info("UDP сервер запущен")

            await main_loop(limiter, session, server)

    except KeyboardInterrupt:

        logger.info("Получен сигнал прерывания...")
        server.stop()
        logger.info("UDP сервер остановлен")
        logger.info("Остановлено пользователем")

    finally:
        server.stop()
        logger.info("UDP сервер остановлен")

asyncio.run(main())
