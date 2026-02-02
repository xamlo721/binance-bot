import os
import pandas as pd
import numpy as np
from pathlib import Path
import time
from datetime import datetime
import glob
from concurrent.futures import ThreadPoolExecutor
import warnings
from logger import logger


warnings.filterwarnings('ignore')

# Настройки
SCRIPT_NAME = "HDP_1H      :  "
K_LINES_DIR = "Data/K_lines/1M"         # Папка с минутными свечами
RESULTS_DIR = "Data/K_lines/1H"         # Папка с результатом
FILES_TO_WORK = 60                                      # Количество файлов обработки
MAX_RESULT_FILES = 720                                  # Максимум файлов результата

def ensure_directories():
    """Создает необходимые директории если они не существуют"""
    Path(K_LINES_DIR).mkdir(exist_ok=True)
    Path(RESULTS_DIR).mkdir(exist_ok=True)

def get_sorted_files():
    """Возвращает отсортированный список самых новых файлов по имени"""
    files = glob.glob(os.path.join(K_LINES_DIR, "K_line_*.csv"))
    files_sorted = sorted(files)
    newest_files = files_sorted[-FILES_TO_WORK:]
    return newest_files

def read_file(file):
    """Чтение одного файла с обработкой ошибок"""
    try:
        df = pd.read_csv(file)
        # Используем только нужные колонки для экономии памяти
        required_columns = ['symbol', 'open', 'close', 'high', 'low', 'quote_volume', 
                          'taker_buy_base_volume', 'taker_buy_quote_volume', 'trades', 'open_time']
        # Проверяем, что все требуемые колонки присутствуют
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(SCRIPT_NAME + f"В файле {file} отсутствуют колонки: {missing_columns}")
            return None
        df = df[required_columns]
        return df
    except Exception as e:
        logger.error(SCRIPT_NAME + f"Ошибка чтения файла {file}: {e}")
        return None

def process_new_data():
    """Основная функция обработки данных"""
    all_files = get_sorted_files()
    
    # Параллельное чтение файлов с правильной фильтрацией
    all_data = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(read_file, all_files)
        for result in results:
            if result is not None and not result.empty:
                all_data.append(result)
    
    if not all_data:
        logger.warning(SCRIPT_NAME + "Нет данных для обработки")
        return False
    
    # Объединение данных одним вызовом
    combined_df = pd.concat(all_data, ignore_index=True)
    
    if combined_df.empty:
        logger.warning(SCRIPT_NAME + "Объединенные данные пусты")
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
        'open', 'close', 'high', 'high_std', 'low', 'low_std',
        'quote_volume_1m_avg', 'quote_volume_std', 'total_volume',  # total_volume добавлена
        'taker_buy_base_volume_1m_avg', 'taker_buy_base_volume_std',
        'taker_buy_quote_volume_1m_avg', 'taker_buy_quote_volume_std',
        'trades_1m_avg', 'trades_std', 
        'volatility_1m_avg', 'volatility_std'
    ]
    
    # Переупорядочиваем колонки для лучшей читаемости
    columns_order = [
        'symbol', 'open', 'close', 'high', 'high_std', 'low', 'low_std',
        'total_volume', 'quote_volume_1m_avg', 'quote_volume_std',
        'taker_buy_base_volume_1m_avg', 'taker_buy_base_volume_std',
        'taker_buy_quote_volume_1m_avg', 'taker_buy_quote_volume_std',
        'trades_1m_avg', 'trades_std', 
        'volatility_1m_avg', 'volatility_std'
    ]
    
    # Создаем DataFrame с результатами
    result_df = result_df.reset_index()
    result_df = result_df[columns_order]
    
    # Формируем имя файла с текущей датой и временем
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(RESULTS_DIR, f"Historical_values_1h_{timestamp}.csv")
    # Сохраняем результаты
    result_df.to_csv(output_file, index=False)
    
    logger.info(SCRIPT_NAME + f"Сохранен файл: Historical_values_1h_{timestamp}.csv")
    
    # Очищаем старые файлы
    cleanup_result_files()
    
    return True # Возвращаем успешное завершение

def cleanup_result_files():
    """Удаляет старые файлы результатов если их больше MAX_RESULT_FILES"""
    files = glob.glob(os.path.join(RESULTS_DIR, "Historical_values_1h_*.csv"))
    files.sort()
    
    while len(files) >= MAX_RESULT_FILES + 1:
        oldest_file = files.pop(0)
        try:
            os.remove(oldest_file)
            logger.info(SCRIPT_NAME + f"Удален старый файл: {os.path.basename(oldest_file)}")
        except OSError as e:
            logger.error(SCRIPT_NAME + f"Ошибка удаления файла {oldest_file}: {e}")

def run_processing():
    """Функция для однократного запуска обработки"""
    logger.info(SCRIPT_NAME + "Запуск обработки данных...")
    ensure_directories()
    
    all_files = get_sorted_files()
    if not all_files:
        logger.error(SCRIPT_NAME + "Нет файлов для обработки в директории")
        return False
    
    logger.info(SCRIPT_NAME + f"Найдено файлов для обработки: {len(all_files)}")
    
    start_time = time.time()
    success = process_new_data()
    end_time = time.time()
    
    if success:
        logger.info(SCRIPT_NAME + f"Обработка завершена успешно за {end_time - start_time:.2f} секунд")
    else:
        logger.error(SCRIPT_NAME + "Обработка завершена с ошибками")
    
    return success

if __name__ == "__main__":
    run_processing()