import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import re
from pathlib import Path
import glob
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# Настройки
SCRIPT_NAME = "T_VOL_24H   :  "                                     # Имя скрипта
K_LINES_1H_DIR = "C:/workspace/Analytics_bot/Data/K_lines/1H"                  # Папка с часовыми свечами
K_LINES_DIN_DIR = "C:/workspace/Analytics_bot/Data/K_lines/Dynamic"            # Папка с динамическим файлом
OUTPUT_FOLDER = "C:/workspace/Analytics_bot/Data/Total_volume_24H"             # Папка с результатом
H_COUNT = 23                                                        # Количество файлов обработки
MAX_RESULT_FILES = 2                                                # Максимум файлов результата

class CSVFileHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self._h1_files_cache = None
        self._cache_timestamp = 0
        self._cache_ttl = 60  # Кэшируем на 60 секунд
        
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        if file_path.endswith('.csv') and 'Historical_values_dynamic' in file_path:
            file_name = Path(file_path).name
            print(SCRIPT_NAME + f"Обнаружен новый файл: {file_name}")
            self.process_file(file_path)
    
    def wait_for_file_stability(self, file_path, check_interval=0.5, max_attempts=15):  # Уменьшено время ожидания
        """Ожидает стабилизации файла (прекращения изменений)"""
        #print(SCRIPT_NAME + "Ожидание стабилизации файла...")
        previous_size = -1
        stable_count = 0
        attempts = 0
        
        while attempts < max_attempts:
            try:
                current_size = os.path.getsize(file_path)
                if current_size == previous_size:
                    stable_count += 1
                    if stable_count >= 2:  # Уменьшено до 2 проверок
                        #print(SCRIPT_NAME + "Файл стабилизировался")
                        return True
                else:
                    stable_count = 0
                    previous_size = current_size
                
                time.sleep(check_interval)
                attempts += 1
            except Exception as e:
                print(SCRIPT_NAME + f"Ошибка при проверке стабильности файла: {e}")
                time.sleep(check_interval)
                attempts += 1
        
        print(SCRIPT_NAME + "Превышено время ожидания стабилизации файла")
        return False
    
    def get_latest_h1_files(self, count=H_COUNT):
        """Получает самые свежие часовые файлы с кэшированием"""
        current_time = time.time()

        # Вычисляем количество часовых файлов, в зависимости от реального времени
        MINUTE = datetime.now().minute
        if MINUTE == 0:
            count = H_COUNT + 1

        print(SCRIPT_NAME + f"Будет обработано {count} часовых файлов")

        # Используем кэш, если он еще актуален
        if (self._h1_files_cache and 
            current_time - self._cache_timestamp < self._cache_ttl):
            return self._h1_files_cache[:count]
        
        try:
            # Используем glob для более быстрого поиска файлов
            pattern = os.path.join(K_LINES_1H_DIR, "*Historical_values_1h*.csv")
            h1_files = glob.glob(pattern)
            
            # Получаем время создания без сортировки всех файлов
            file_times = []
            for file_path in h1_files:
                try:
                    ctime = os.path.getctime(file_path)
                    file_times.append((file_path, ctime))
                except OSError:
                    continue
            
            # Сортируем только по времени создания и берем нужное количество
            file_times.sort(key=lambda x: x[1], reverse=True)
            result_files = [file[0] for file in file_times[:count]]
            
            # Обновляем кэш
            self._h1_files_cache = [file[0] for file in file_times]
            self._cache_timestamp = current_time
            
            return result_files
        
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при получении часовых файлов: {e}")
            return []
    
    def process_single_file(self, file_path):
        """Обрабатывает один файл и возвращает сумму total_volume для каждого тикера"""
        try:
            # Используем более быстрые параметры чтения CSV
            # Читаем только нужные колонки - total_volume вместо high
            df = pd.read_csv(
                file_path, 
                usecols=['symbol', 'total_volume'],  # Изменено с high на total_volume
                dtype={'symbol': 'category', 'total_volume': np.float64}  # Используем float64 для суммирования
            )
            
            # Группируем и СУММИРУЕМ total_volume для каждого символа
            sum_volumes = df.groupby('symbol', observed=False)['total_volume'].sum()
            return sum_volumes.to_dict()
            
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при обработке файла {file_path}: {e}")
            return {}
    
    def extract_timestamp(self, filename):
        """Извлекает timestamp из имени файла для сортировки"""
        match = re.search(r'(\d{8}_\d{6})', filename)
        if match:
            return match.group(1)
        return "00000000_000000"
    
    def process_file(self, dynamic_file_path):
        """Обрабатывает динамический файл и агрегирует данные"""
        try:
            # Ждем стабилизации файла
            if not self.wait_for_file_stability(dynamic_file_path):
                return
            
            # Получаем самые свежие часовые файлы
            h1_files = self.get_latest_h1_files()
            #print(SCRIPT_NAME + f"Найдено {len(h1_files)} часовых файлов для обработки")
            
            if not h1_files:
                print(SCRIPT_NAME + "Не найдено часовых файлов для обработки")
                return
            
            # Список всех файлов для обработки
            all_files = [dynamic_file_path] + h1_files
            
            # Обрабатываем файлы параллельно
            total_volume_values = {}
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(self.process_single_file, all_files))
            
            # ОБЪЕДИНЯЕМ РЕЗУЛЬТАТЫ СУММИРОВАНИЕМ
            for result in results:
                for symbol, volume in result.items():
                    if symbol not in total_volume_values:
                        total_volume_values[symbol] = volume
                    else:
                        total_volume_values[symbol] += volume
            
            # Создаем результирующий DataFrame более эффективно
            if total_volume_values:
                symbols = list(total_volume_values.keys())
                total_volumes = [total_volume_values[sym] for sym in symbols]
                
                result_df = pd.DataFrame({
                    'symbol': symbols,
                    'total_volume': total_volumes  # Новое имя колонки
                })
                
                # Создаем имя для выходного файла
                timestamp = self.extract_timestamp(os.path.basename(dynamic_file_path))
                output_filename = f"Total_volume_{timestamp}.csv"
                output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                
                # Сохраняем результат
                os.makedirs(OUTPUT_FOLDER, exist_ok=True)
                result_df.to_csv(output_path, index=False)
                #print(SCRIPT_NAME + f"Результат сохранен в: {output_path}")
                print(SCRIPT_NAME + f"Обработано тикеров: {len(result_df)}")
            else:
                print(SCRIPT_NAME + "Нет данных для сохранения")
            
            # Очищаем старые файлы
            cleanup_result_files() 
            
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при обработке файла {dynamic_file_path}: {e}")


def cleanup_result_files():
    """Удаляет старые файлы результатов если их больше MAX_RESULT_FILES"""
    files = glob.glob(os.path.join(OUTPUT_FOLDER, "Total_volume_*.csv"))
    files.sort()
    
    while len(files) >= MAX_RESULT_FILES + 1:
        oldest_file = files.pop(0)
        try:
            os.remove(oldest_file)
            #print(SCRIPT_NAME + f"Удален старый файл: {os.path.basename(oldest_file)}")
        except OSError as e:
            print(SCRIPT_NAME + f"Ошибка удаления файла {oldest_file}: {e}")


def main():
    # Создаем папки если они не существуют
    for folder in [K_LINES_DIN_DIR, K_LINES_1H_DIR, OUTPUT_FOLDER]:
        os.makedirs(folder, exist_ok=True)
    
    # Создаем и запускаем наблюдатель
    event_handler = CSVFileHandler()
    observer = Observer()
    observer.schedule(event_handler, K_LINES_DIN_DIR, recursive=False)
    print(SCRIPT_NAME + f"Мониторинг запущен...")
    
    try:
        observer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print(SCRIPT_NAME + "Мониторинг остановлен")
    
    observer.join()

if __name__ == "__main__":
    main()