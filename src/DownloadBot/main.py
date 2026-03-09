# main.py
"""
Главный модуль бота.
Сохраняет в памяти последние 24 часа (1440 минут) свечей всех доступных тикеров и
каждую минуту скачивает новые данные.
"""

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

from binance_utils import get_trading_symbols
from binance_utils import fetch_klines_for_symbols

from bot_types import KlineRecord

from logger import *
from config import *

# Список всех отметок за MINUTE_CANDLES_LIMIT минут 
#                   <НОМЕР_МИНУТЫ List<МИНУТНАЯ_ЗАПИСЬ>>
global_data: OrderedDict[int, list[KlineRecord]] = OrderedDict()

def _format_ts(ts_ms: int) -> str:
    """Преобразует timestamp в миллисекундах в строку ГГГГ-ММ-ДД ЧЧ:ММ:СС"""
    return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

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

async def fetch_candles(session: aiohttp.ClientSession, symbols: list[str], count: int = 1440) -> None:
    """
    Получаем последние `count` минут (по умолчанию 24 часа) и сохраняем их в глобальном хранилище.
    """
    # Текущий момент
    now_timestamp = int(time.time() * 1000)
    # Последняя завершенная минута
    end_timestamp = now_timestamp - (now_timestamp % 60000) - 1

    # Запрос к Binance: все тикеры за указанное количество минут до `end_timestamp`
    period_klines = await fetch_klines_for_symbols(session, symbols, count, end_timestamp)

    # Сохраняем свечи по абсолютному номеру минуты (timestamp // 60000)
    for ticker_candles in period_klines:
        for candle in ticker_candles:
            minute_key = int(candle.open_time // 60000)   # абсолютный номер минуты
            global_data.setdefault(minute_key, []).append(candle)


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

async def main_loop():
    """
    Main loop: update last candles and fetch new minute candles every minute.
    """
    
    async with aiohttp.ClientSession() as session:  # ← создаём сессию

        
        logger.info(f"Обновляем список тикеров")
        symbols = get_trading_symbols()
        if not symbols:
            logger.error("Не удалось получить список тикеров")
            return
        logger.info(f"✅ Получено {len(symbols)} тикеров seconds")

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
        await fetch_candles(session, symbols, count = MAX_CACHED_CANDLES)

        while True:
            tick_start_time = time.time()
            now_ms = int(time.time() * 1000)

            missing = check_space(now_ms)
            if missing > 0:
                logger.info(f"Обнаружено пропущенных минут: {missing}. Догоняем...")
                await fetch_candles(session, symbols, missing)
            else:
                logger.info("Обновляем свечи за текущую минуту.")
                await fetch_candles(session, symbols, 1)

            cleanup_storage(MAX_CACHED_CANDLES)

            # Вычисляем сколько осталось ждать
            elapsed = time.time() - tick_start_time
            wait_time = max(0, 60 - elapsed)  # минимум 0 секунд
            logger.info(f"✅ Updated {len(global_data)} tickers for {elapsed:.2f} seconds")
            await asyncio.sleep(wait_time)

async def main():
    """
    Main entry point.
    """

    await main_loop()

asyncio.run(main())
