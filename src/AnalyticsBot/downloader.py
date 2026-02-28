import asyncio
import time

from logger import logger
from config import *

from datetime import datetime
from datetime import timedelta
from typing import List

from binance_utils.my_binance_utils import get_trading_symbols
from binance_utils.my_binance_utils import fetch_all_tickers_volumes
from binance_utils.my_binance_utils import fetch_all_tickers_volumes_for_time
from binance_utils.my_binance_utils import fetch_ticker_1m_volumes_for_time

from ramstorage.ram_storage_utils import save_klines_to_ram
from ramstorage.ram_storage_utils import CandleRecord

async def download_more_candles(symbols: list[str], count: int, start_time: datetime)-> List[List[CandleRecord]]:
    "Скачивает count минутных свечей начиная от datetime"
    start_ts = time.time()

    # Конвертируем datetime в timestamp в миллисекундах
    end_timestamp = int(start_time.timestamp() * 1000)

    logger.info(f"Загрузка {count} минут начиная от {start_time}.")
    
    # Скачиваем свечи для текущей минуты
    period_candles = await fetch_all_tickers_volumes_for_time(symbols, count, end_timestamp)
    
    if len(period_candles) != count: 
            logger.info(f"У нас что-то пошло не так при загрузке {count} минутных начиная от {start_time}.")
            logger.info(f"Скачано только {len(period_candles)} минутных наборов")
            return []
 
    end_time = time.time()
    logger.info(f"Всего загружено {len(period_candles)} минут за {end_time - start_ts:.2f} секунд")

    return period_candles


async def download_current_1m_Candles(symbols: list[str]):
    start_time = time.time()
    
    candles = await fetch_all_tickers_volumes(symbols, 1, max_concurrent=200)
    
    if not candles:
        logger.error("❌ Не удалось получить данные по тикерам")
        return
    
        
    # Получаем время открытия свечи из первого результата
    open_time = candles[0].open_time
    open_time_dt = datetime.fromtimestamp(open_time / 1000)
    save_klines_to_ram(candles)
    end_time = time.time()
    logger.info(f"✅ Готово! Обработано {len(candles)} тикеров за {end_time - start_time:.2f} секунд")

if __name__ == "__main__":
    symbols = get_trading_symbols()
    if not symbols:
        logger.error("Не удалось получить список тикеров")

    # asyncio.run(download_1m_Candles())
    asyncio.run(download_more_candles(symbols, 60, datetime.now()))
