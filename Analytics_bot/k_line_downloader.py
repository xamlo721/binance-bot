import asyncio
import time

from logger import logger
from config import *

from binance_utils.my_binance_utils import get_trading_symbols
from binance_utils.my_binance_utils import fetch_all_tickers_volumes

from filestorage.file_storage_utils import save_to_csv
from filestorage.file_storage_utils import cleanup_old_files

from ramstorage.ram_storage_utils import save_to_ram
from ramstorage.ram_storage_utils import CandleRecord
from ramstorage.ram_storage_utils import candle_records

async def main():
    start_time = time.time()
    symbols = get_trading_symbols()
    if not symbols:
        logger.error(k_line_SCRIPT_NAME + "Не удалось получить список тикеров")
        return
        
    logger.info(k_line_SCRIPT_NAME + f"Найдено торгующихся тикеров: {len(symbols)}")
    
    results = await fetch_all_tickers_volumes(symbols, max_concurrent=200)
    
    if not results:
        logger.error(k_line_SCRIPT_NAME + "Не удалось получить данные по тикерам")
        return
    
    filepath = save_to_csv(results)
    save_to_ram(results)

    candles: list[CandleRecord] = candle_records[0]
    single_candle: CandleRecord = candles[34]
    
    logger.info(f"Свеча {single_candle.symbol} имеет отметку: {single_candle.open}")


    if filepath:
        # Max Количество файлов
        cleanup_old_files(k_line_CLEAN_OLD_FILES)
        end_time = time.time()
        logger.info(k_line_SCRIPT_NAME + f"Готово! Обработано {len(results)} тикеров за {end_time - start_time:.2f} секунд")
        logger.info(k_line_SCRIPT_NAME + f"Данные сохранены в файл: {filepath}")

if __name__ == "__main__":
    asyncio.run(main())