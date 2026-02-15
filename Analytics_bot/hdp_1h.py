import os
import pandas as pd
import numpy as np
from pathlib import Path
import time
from datetime import datetime
import glob
from concurrent.futures import ThreadPoolExecutor
from logger import logger
from config import *

from filestorage.file_storage_utils import read_1m_candles_from_csv
from filestorage.file_storage_utils import get_sorted_files
from filestorage.file_storage_utils import cleanup_old_files

def create_1h_directories():
    """Создает необходимые директории если они не существуют"""
    Path(MINUTES_KLINE_FOLDER).mkdir(exist_ok=True)
    Path(HOURS_KLINE_FOLDER).mkdir(exist_ok=True)

def process_new_data() -> bool:
    """Основная функция обработки данных"""
    all_files = get_sorted_files(MINUTES_KLINE_FOLDER, "K_line_*.csv", hdp_1h_FILES_TO_WORK)
    
    # Параллельное чтение файлов с правильной фильтрацией
    all_data = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(read_1m_candles_from_csv, all_files)
        for result in results:
            if result is not None:
                all_data.append(result)
    
    if not all_data:
        logger.warning(hdp_1h_SCRIPT_NAME + "Нет данных для обработки")
        return False
    
    # Объединение данных одним вызовом
    combined_df = pd.concat(all_data, ignore_index=True)
    
    if combined_df.empty:
        logger.warning(hdp_1h_SCRIPT_NAME + "Объединенные данные пусты")
        return False
    
    # Добавляем волатильность
    combined_df['volatility'] = (combined_df['high'] - combined_df['low']) / combined_df['open']
    
    # Сортируем по времени для всего датафрейма один раз
    combined_df = combined_df.sort_values(['symbol', 'open_time'])
    
    # Векторизованные вычисления с groupby и agg
    agg_dict = {
        'open': 'first',
        'close': 'last', 
        'high': ['max', 'std'],
        'low': ['min', 'std'],
        'quote_volume': ['mean', 'std', 'sum'],  # Добавлен 'sum' для total_volume
        'taker_buy_base_volume': ['mean', 'std'],
        'taker_buy_quote_volume': ['mean', 'std'],
        'trades': ['mean', 'std'],
        'volatility': ['mean', 'std']
    }
    
    result_df = combined_df.groupby('symbol').agg(agg_dict)
    
    # Выравниваем мультииндекс колонок
    result_df.columns = [
        'open', 
        'close', 
        'high', 
        'high_std', 
        'low', 
        'low_std',
        'quote_volume_1m_avg', 
        'quote_volume_std', 
        'total_volume',  # total_volume добавлена
        'taker_buy_base_volume_1m_avg', 
        'taker_buy_base_volume_std',
        'taker_buy_quote_volume_1m_avg', 
        'taker_buy_quote_volume_std',
        'trades_1m_avg', 
        'trades_std', 
        'volatility_1m_avg', 
        'volatility_std'
    ]
    
    # Переупорядочиваем колонки для лучшей читаемости
    columns_order = [
        'symbol', 
        'open', 
        'close', 
        'high', 
        'high_std', 
        'low', 
        'low_std',
        'total_volume', 
        'quote_volume_1m_avg', 
        'quote_volume_std',
        'taker_buy_base_volume_1m_avg', 
        'taker_buy_base_volume_std',
        'taker_buy_quote_volume_1m_avg', 
        'taker_buy_quote_volume_std',
        'trades_1m_avg', 
        'trades_std', 
        'volatility_1m_avg', 
        'volatility_std'
    ]
    
    # Создаем DataFrame с результатами
    result_df = result_df.reset_index()
    result_df = result_df[columns_order]
    
    # Формируем имя файла с текущей датой и временем
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(HOURS_KLINE_FOLDER, f"Historical_values_1h_{timestamp}.csv")
    # Сохраняем результаты
    result_df.to_csv(output_file, index=False)
    
    logger.info(hdp_1h_SCRIPT_NAME + f"Сохранен файл: Historical_values_1h_{timestamp}.csv")
    
    # Очищаем старые файлы
    logger.info(hdp_1h_SCRIPT_NAME + f"Чистим устаревшие файлы.")
    cleanup_old_files(HOURS_KLINE_FOLDER, "Historical_values_1h_*.csv", 20)
    logger.info(hdp_1h_SCRIPT_NAME + f"Чистка окончена.")
    
    return True # Возвращаем успешное завершение

def onTick():
    """Функция для однократного запуска обработки"""
    logger.info(hdp_1h_SCRIPT_NAME + "Запуск обработки данных...")
    create_1h_directories()
    
    all_files = get_sorted_files(MINUTES_KLINE_FOLDER, "K_line_*.csv", hdp_1h_FILES_TO_WORK)
    if not all_files:
        logger.error(hdp_1h_SCRIPT_NAME + "Нет файлов для обработки в директории")
        return False
    
    logger.info(hdp_1h_SCRIPT_NAME + f"Найдено файлов для обработки: {len(all_files)}")
    
    start_time = time.time()
    success = process_new_data()
    end_time = time.time()
    
    if success:
        logger.info(hdp_1h_SCRIPT_NAME + f"Обработка завершена успешно за {end_time - start_time:.2f} секунд")
    else:
        logger.error(hdp_1h_SCRIPT_NAME + "Обработка завершена с ошибками")
    
    return success

if __name__ == "__main__":
    onTick()