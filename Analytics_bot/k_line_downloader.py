import asyncio
import time

from logger import logger
from config import *

from datetime import datetime
from datetime import timedelta

from binance_utils.my_binance_utils import get_trading_symbols
from binance_utils.my_binance_utils import fetch_all_tickers_volumes
from binance_utils.my_binance_utils import fetch_all_tickers_volumes_for_time
from binance_utils.my_binance_utils import fetch_ticker_1m_volumes_for_time

from filestorage.file_storage_utils import write_1m_candles_to_csv
from filestorage.file_storage_utils import cleanup_old_files

from ramstorage.ram_storage_utils import save_to_ram
from ramstorage.ram_storage_utils import CandleRecord
from ramstorage.ram_storage_utils import candle_1m_records


async def download_more_candles(count: int, datetime: datetime):
    "Скачивает count минутных свечей начиная от datetime"
    start_time = time.time()
    symbols = get_trading_symbols()
    if not symbols:
        logger.error(k_line_SCRIPT_NAME + "Не удалось получить список тикеров")
        return
        
    logger.info(k_line_SCRIPT_NAME + f"Найдено торгующихся тикеров: {len(symbols)}")
    
    # Конвертируем datetime в timestamp в миллисекундах
    end_timestamp = int(datetime.timestamp() * 1000)

    logger.info(k_line_SCRIPT_NAME + f"Загрузка {count} минут начиная от {datetime}.")
    
    # Скачиваем свечи для текущей минуты
    period_candles = await fetch_all_tickers_volumes_for_time(symbols, count, end_timestamp)
    
    if len(period_candles) != count: 
            logger.info(k_line_SCRIPT_NAME + f"У нас что-то пошло не так при загрузке {count} минутных начиная от {datetime}.")
            logger.info(k_line_SCRIPT_NAME + f"Скачано только {len(period_candles)} минутных наборов")
            return
 
    for candles in period_candles:

        # Сохраняем каждую минуту в RAM (в начало списка)
        save_to_ram(candles)
        
        # Сохраняем в файл
        open_time_dt = datetime.fromtimestamp(candles[0].open_time / 1000)
        filename = f"K_line_{open_time_dt.strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = write_1m_candles_to_csv(filename, candles)
        
        if filepath:
            logger.info(k_line_SCRIPT_NAME + f"Сохранена минута {open_time_dt}. Файл: {filepath}" )

    # Очистка старых файлов
    cleanup_old_files(MINUTES_KLINE_FOLDER, "K_line_*.csv", MINUTE_CANDLE_FILE_LIMIT)
    
    end_time = time.time()
    logger.info(k_line_SCRIPT_NAME + f"Всего загружено {len(period_candles)} минут за {end_time - start_time:.2f} секунд")


async def download_current_1m_Candles():
    start_time = time.time()
    symbols = get_trading_symbols()
    if not symbols:
        logger.error(k_line_SCRIPT_NAME + "Не удалось получить список тикеров")
        return
        
    logger.info(k_line_SCRIPT_NAME + f"Найдено торгующихся тикеров: {len(symbols)}")
    
    candles = await fetch_all_tickers_volumes(symbols, 1, max_concurrent=200)
    
    if not candles:
        logger.error(k_line_SCRIPT_NAME + "Не удалось получить данные по тикерам")
        return
    
        
    # Получаем время открытия свечи из первого результата
    open_time = candles[0].open_time
    open_time_dt = datetime.fromtimestamp(open_time / 1000)
    filename = f"K_line_{open_time_dt.strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = write_1m_candles_to_csv(filename, candles)

    save_to_ram(candles)

    if filepath:
        # Max Количество файлов
        cleanup_old_files(MINUTES_KLINE_FOLDER, "K_line_*.csv", MINUTE_CANDLE_FILE_LIMIT)
        end_time = time.time()
        logger.info(k_line_SCRIPT_NAME + f"Готово! Обработано {len(candles)} тикеров за {end_time - start_time:.2f} секунд")
        logger.info(k_line_SCRIPT_NAME + f"Данные сохранены в файл: {filepath}")

if __name__ == "__main__":
    # asyncio.run(download_1m_Candles())
    asyncio.run(download_more_candles(60, datetime.now()))
