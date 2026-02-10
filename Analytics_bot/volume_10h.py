#!/usr/bin/env python3
import os
import time
import pandas as pd
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
import glob
from logger import logger
from config import *

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Определяем базовую директорию проекта
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Поднимаемся на нужное количество уровней
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(BASE_DIR)))


# Создаем директории, если их нет
os.makedirs(VAL_10H_input_dir, exist_ok=True)
os.makedirs(VAL_10H_hdr_1h_RESULTS_DIR, exist_ok=True)


class CSVFileHandler(FileSystemEventHandler):
    def __init__(self, input_dir, output_dir, num_files=10):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.num_files = num_files
        
        # Создаем выходную папку, если её нет
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Обработка существующих файлов при запуске
        self.process_existing_files()
    
    def on_created(self, event):
        """Обработка создания нового файла"""
        if not event.is_directory and event.src_path.endswith('.csv'):
            # Небольшая задержка для гарантии завершения записи файла
            time.sleep(1)
            self.process_new_file(Path(event.src_path))
    
    def process_existing_files(self):
        """Обработка существующих файлов при запуске"""
        csv_files = sorted(self.input_dir.glob('Historical_values_1h_*.csv'))
        if csv_files:
            self.aggregate_volumes(csv_files[-1])
    
    def process_new_file(self, new_file):
        """Обработка нового CSV файла"""
        #logger.info(f"Обнаружен новый файл: {new_file.name}")
        logger.info(VAL_10H_SCRIPT_NAME + f"Обнаружен новый файл: {new_file.name}")
        csv_files = sorted(self.input_dir.glob('Historical_values_1h_*.csv'))
        
        if len(csv_files) >= self.num_files + 1:
            self.aggregate_volumes(new_file)
    
    def aggregate_volumes(self, latest_file):
        """Агрегация объемов из 10 последних файлов (исключая самый свежий)"""
        # Получаем список всех CSV файлов, сортированных по времени
        csv_files = sorted(self.input_dir.glob('Historical_values_1h_*.csv'))
        
        # Берем 10 самых свежих файлов, исключая самый последний
        if len(csv_files) >= self.num_files + 1:
            files_to_process = csv_files[-(self.num_files + 1):-1]
        else:
            files_to_process = csv_files[:-1] if len(csv_files) > 1 else []
        
        if len(files_to_process) < 10:
            #logger.warning(f"Недостаточно файлов для обработки. Найдено: {len(files_to_process)}")
            logger.warning(VAL_10H_SCRIPT_NAME + f"Недостаточно файлов для обработки. Найдено: {len(files_to_process)}")
            return
        
        #logger.info(f"Обработка {len(files_to_process)} файлов для агрегации")
        logger.info(VAL_10H_SCRIPT_NAME + f"Обработка {len(files_to_process)} файлов для агрегации")
        
        # Словарь для хранения объемов по тикерам
        volumes_by_symbol = {}
        
        # Считываем данные из каждого файла
        for i, file_path in enumerate(files_to_process, 1):
            try:
                df = pd.read_csv(file_path)
                
                # Извлекаем символы и объемы
                for _, row in df.iterrows():
                    symbol = row['symbol']
                    total_volume = row['total_volume']
                    
                    if symbol not in volumes_by_symbol:
                        volumes_by_symbol[symbol] = {}
                    
                    # Сохраняем объем для текущего файла
                    volumes_by_symbol[symbol][f'total_volume_{i}'] = total_volume
                    
            except Exception as e:
                #logger.error(f"Ошибка при чтении файла {file_path}: {e}")
                logger.error(VAL_10H_SCRIPT_NAME + f"Ошибка при чтении файла {file_path}: {e}")
        
        # Создаем DataFrame с агрегированными данными
        aggregated_data = []
        for symbol, volumes in volumes_by_symbol.items():
            # Создаем запись для каждого символа
            record = {'symbol': symbol}
            
            # Добавляем объемы в правильном порядке
            for i in range(1, self.num_files + 1):
                vol_key = f'total_volume_{i}'
                record[vol_key] = volumes.get(vol_key, 0)
            
            aggregated_data.append(record)
        
        if aggregated_data:
            # Создаем имя выходного файла на основе временной метки
            timestamp = latest_file.stem.split('_')[-2]  # Извлекаем дату из имени файла
            output_filename = f"Volume_10H_{timestamp}.csv"
            output_path = self.output_dir / output_filename
            
            # Сохраняем в CSV
            result_df = pd.DataFrame(aggregated_data)
            columns_order = ['symbol'] + [f'total_volume_{i}' for i in range(1, self.num_files + 1)]
            result_df = result_df[columns_order]
            result_df.to_csv(output_path, index=False)
            
            #logger.info(f"Создан файл: {output_path} с {len(aggregated_data)} тикерами")
            logger.info(VAL_10H_SCRIPT_NAME + f"Создан файл: {output_path} с {len(aggregated_data)} тикерами")
            
            # Очищаем старые файлы
            cleanup_result_files()

        else:
            #logger.warning("Нет данных для агрегации")
            logger.warning(VAL_10H_SCRIPT_NAME + "Нет данных для агрегации")


def cleanup_result_files():
    """Удаляет старые файлы результатов если их больше MAX_RESULT_FILES"""
    files = glob.glob(os.path.join(VAL_10H_hdr_1h_RESULTS_DIR, "Volume_10H_*.csv"))
    files.sort()
    
    while len(files) >= VAL_10H_MAX_RESULT_FILES + 1:
        oldest_file = files.pop(0)
        try:
            os.remove(oldest_file)
            logger.info(VAL_10H_SCRIPT_NAME + f"Удален старый файл: {os.path.basename(oldest_file)}")
        except OSError as e:
            logger.error(VAL_10H_SCRIPT_NAME + f"Ошибка удаления файла {oldest_file}: {e}")


def main():
      # Проверка существования входной папки
    if not os.path.exists(VAL_10H_input_dir):
        #logger.error(f"Входная папка не существует: {input_dir}")
        logger.error(VAL_10H_SCRIPT_NAME + f"Входная папка не существует: {VAL_10H_input_dir}")
        return
    
    # Создаем обработчик событий
    event_handler = CSVFileHandler(VAL_10H_input_dir, VAL_10H_hdr_1h_RESULTS_DIR, num_files=10)
    
    # Создаем наблюдатель
    observer = Observer()
    observer.schedule(event_handler, VAL_10H_input_dir, recursive=False)
    
    #logger.info(f"Начало мониторинга папки: {input_dir}")
    logger.info(VAL_10H_SCRIPT_NAME + f"Начало мониторинга папки: {VAL_10H_input_dir}")
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info(VAL_10H_SCRIPT_NAME + "Мониторинг остановлен")
        #logger.info("Мониторинг остановлен")
    
    observer.join()

if __name__ == "__main__":
    main()