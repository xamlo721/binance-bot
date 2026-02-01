import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import glob
import subprocess

# Настройки
SCRIPT_NAME = "LB_12H      :  "                                                             # Имя скрипта для вывода в консоль
AGR_12H_FOLDER = "/srv/ftp/Bot_v2/Data/Agr_12h"                                             # Папка с историческими агрегированными данными
K_LINES_DIR = "/srv/ftp/Bot_v2/Data/K_lines/1M"                                             # Папка с минутными свечами
OUTPUT_FOLDER = "/srv/ftp/Bot_v2/Data/Ticker_up"                                            # Папка с результатом
MAX_RESULT_FILES = 2                                                                        # Максимум файлов результата
HDP_SCRIPT_PATH = "/srv/ftp/Bot_v2/Modules/Historical_data_processor/hdp_dynamic.py"        # Путь к скрипту hdp_dynamic.py

# Создаем выходную папку если она не существует
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

class KLineFileHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self.processed_files = set()
    
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            filename = os.path.basename(event.src_path)
            print(SCRIPT_NAME + f"Обнаружен новый файл: {filename}")
            self.process_file(event.src_path)
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            # Обрабатываем только если файл еще не обрабатывался
            if event.src_path not in self.processed_files:
                print(SCRIPT_NAME + f"Файл изменен: {event.src_path}")
                self.process_file(event.src_path)
    
    def get_latest_aggregated_file(self):
        """Находит самый свежий файл в папке Agr_12h"""
        try:
            agg_files = glob.glob(os.path.join(AGR_12H_FOLDER, "Aggregated_high_*.csv"))
            if not agg_files:
                print(SCRIPT_NAME + "Не найдены файлы aggregated high")
                return None
            
            latest_file = max(agg_files, key=os.path.getctime)
            file_name = os.path.basename(latest_file)
            print(SCRIPT_NAME + f"Используется aggregated файл: {file_name}")
            return latest_file
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при поиске aggregated файла: {e}")
            return None
    
    def wait_for_file_stability(self, file_path, check_interval=1, max_attempts=30):
        """Ожидает стабилизации файла (прекращения изменений)"""
        for attempt in range(max_attempts):
            try:
                size_before = os.path.getsize(file_path)
                time.sleep(check_interval)
                size_after = os.path.getsize(file_path)
                
                if size_before == size_after:
                    #print(SCRIPT_NAME + f"Файл стабилизировался после {attempt + 1} проверок")
                    return True
            except OSError as e:
                print(SCRIPT_NAME + f"Ошибка при проверке размера файла: {e}")
                time.sleep(check_interval)
        
        print(SCRIPT_NAME + f"Файл не стабилизировался после {max_attempts} попыток")
        return False
    
    def read_csv_safe(self, file_path):
        """Безопасное чтение CSV файла с обработкой ошибок"""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка чтения файла {file_path}: {e}")
            return None
    
    # Запуск скрипта hdp_dynamic.py
    def run_hdp_dynamic_script(self):
        """Запускает скрипт hdp_dynamic.py"""
        try:
            print(SCRIPT_NAME + "Запуск HDP_DYN...")
            
            # Запускаем скрипт и ждем его завершения
            result = subprocess.run(
                ['python3', HDP_SCRIPT_PATH], 
                capture_output=True, 
                text=True, 
                cwd=os.path.dirname(HDP_SCRIPT_PATH)
            )
            
            if result.returncode == 0:
                #print(SCRIPT_NAME + "HDP_DYN успешно выполнен")
                if result.stdout:
                    output = result.stdout.strip()
                    print(output)
            else:
                print(SCRIPT_NAME + f"Ошибка выполнения HDP_DYN. Код возврата: {result.returncode}")
                if result.stderr:
                    print(SCRIPT_NAME + f"Ошибка: {result.stderr}")
                    
        except Exception as e:
            print(SCRIPT_NAME + f"Исключение при запуске скрипта HDP_DYN: {e}")

    # Обработка K-Line файла 
    def process_file(self, k_line_file_path):
        """Обрабатывает файл K-line"""
        
        # Проверяем, не обрабатывался ли уже этот файл
        if k_line_file_path in self.processed_files:
            return
        
        # Ждем стабилизации файла
        if not self.wait_for_file_stability(k_line_file_path):
            return
        
        # Получаем самый свежий aggregated файл
        aggregated_file = self.get_latest_aggregated_file()
        if not aggregated_file:
            return
        
        # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: ждем стабилизации aggregated файла тоже
        if not self.wait_for_file_stability(aggregated_file):
            print(SCRIPT_NAME + f"Aggregated файл не стабилизировался: {aggregated_file}")
            return


        # Читаем данные
        k_line_data = self.read_csv_safe(k_line_file_path)
        #k_line_data = self.read_csv_safe(k_line_file_path, max_retries=3, retry_delay=1)
        aggregated_data = self.read_csv_safe(aggregated_file)
        #aggregated_data = self.read_csv_safe(aggregated_file, max_retries=5, retry_delay=1)  # Больше попыток для aggregated
        
        if k_line_data is None or aggregated_data is None:
            return
        
        # Проверяем наличие необходимых колонок
        if 'symbol' not in k_line_data.columns or 'close' not in k_line_data.columns:
            print(SCRIPT_NAME + "В K-line файле отсутствуют необходимые колонки 'symbol' или 'close'")
            return
        
        if 'symbol' not in aggregated_data.columns or 'high' not in aggregated_data.columns:
            print(SCRIPT_NAME + "В aggregated файле отсутствуют необходимые колонки 'symbol' или 'high'")
            return
        
        # Объединяем данные по symbol
        merged_data = pd.merge(
            k_line_data[['symbol', 'close']], 
            aggregated_data[['symbol', 'high']], 
            on='symbol', 
            how='inner'
        )
        
        # Находим тикеры где close > high
        tickers_up = merged_data[merged_data['close'] > merged_data['high']]
        
        if not tickers_up.empty:
            # Сортируем по разнице (close - high) в убывающем порядке
            tickers_up = tickers_up.copy()
            tickers_up['difference'] = tickers_up['close'] - tickers_up['high']
            tickers_up = tickers_up.sort_values('difference', ascending=False)
            
            # Выводим в консоль
            print(SCRIPT_NAME + "="*60)
            print(SCRIPT_NAME + f"НАЙДЕНО ТИКЕРОВ С ПРЕВЫШЕНИЕМ HIGH - {len(tickers_up)}:")
            #print(SCRIPT_NAME + "="*60)
            print(SCRIPT_NAME + ", ".join(tickers_up['symbol'].tolist()))
            #for _, row in tickers_up.iterrows():
                #print(SCRIPT_NAME + f"{row['symbol']}: close={row['close']:.6f}, high={row['high']:.6f}, diff={row['difference']:.6f}")
            #    print(SCRIPT_NAME + f"{row['symbol']}")
            print(SCRIPT_NAME + "="*60)
            
            # Сохраняем в файл
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_filename = f"tickers_up_{timestamp}.csv"
            output_path = os.path.join(OUTPUT_FOLDER, output_filename)
            
            tickers_up[['symbol']].to_csv(output_path, index=False)
            #print(SCRIPT_NAME + f"Список сохранен в: {output_path}")
            #print(SCRIPT_NAME + f"Найдено тикеров: {len(tickers_up)}")
        else:
            print(SCRIPT_NAME + "Тикеров с превышением high не найдено")
        
        # Помечаем файл как обработанный
        self.processed_files.add(k_line_file_path)
        file_name = os.path.basename(k_line_file_path)
        #print(SCRIPT_NAME + f"Файл обработан: {file_name}")
        # Очищаем старые файлы
        cleanup_result_files()
        
        # ЗАПУСКАЕМ СКРИПТ hdp_dynamic.py ПОСЛЕ ОБРАБОТКИ ФАЙЛА
        self.run_hdp_dynamic_script()
        
def cleanup_result_files():
    """Удаляет старые файлы результатов если их больше MAX_RESULT_FILES"""
    files = glob.glob(os.path.join(OUTPUT_FOLDER, "tickers_up_*.csv"))
    files.sort()
    
    while len(files) >= MAX_RESULT_FILES + 1:
        oldest_file = files.pop(0)
        try:
            os.remove(oldest_file)
            #print(SCRIPT_NAME + f"Удален старый файл: {os.path.basename(oldest_file)}")
        except OSError as e:
            print(SCRIPT_NAME + f"Ошибка удаления файла {oldest_file}: {e}")

def main():
    # Проверяем существование директорий
    for directory in [AGR_12H_FOLDER, K_LINES_DIR]:
        if not os.path.exists(directory):
            print(SCRIPT_NAME + f"Ошибка: директория {directory} не существует")
            return
        
    # Проверяем существование скрипта hdp_dynamic.py
    if not os.path.exists(HDP_SCRIPT_PATH):
        print(SCRIPT_NAME + f"Предупреждение: скрипт {HDP_SCRIPT_PATH} не найден")
        
    print(SCRIPT_NAME + "Запуск мониторинга K-line файлов...")
    
    # Создаем и запускаем наблюдатель
    event_handler = KLineFileHandler()
    observer = Observer()
    observer.schedule(event_handler, K_LINES_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(SCRIPT_NAME + "\nОстановка мониторинга...")
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    main()