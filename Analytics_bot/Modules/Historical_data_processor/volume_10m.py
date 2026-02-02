import os
import time
import pandas as pd
import glob
import hashlib
import glob
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Определяем базовую директорию проекта
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Или, если скрипт находится глубже в структуре:
# BASE_DIR = Path(__file__).parent.parent.parent  # если нужно подняться на 3 уровня вверх

# Настройки
SCRIPT_NAME = "VOL_10M     :  "
# Настройки директорий
K_LINES_DIR = os.path.join(BASE_DIR, "Data", "K_lines", "1M")                 # Папка с минутными свечами
RESULTS_DIR = os.path.join(BASE_DIR, "Data", "Volume_10M")                 # Папка с результатом
MAX_RESULT_FILES = 2                                            # Максимум файлов результата


class FileHandler(FileSystemEventHandler):
    def __init__(self, source_dir, target_dir):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.last_processed = {}
        
        # Создаем целевую директорию если ее нет
        os.makedirs(target_dir, exist_ok=True)
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            #logger.info(f"Обнаружен новый файл: {event.src_path}")
            print(SCRIPT_NAME + f"Обнаружен новый файл: {event.src_path}")
            self.process_file(event.src_path)
    
    def wait_for_file_stabilization(self, filepath, check_interval=0.5, max_checks=10):
        """Ждет пока файл стабилизируется (перестанет меняться размер)"""
        last_size = -1
        stable_count = 0
        
        for _ in range(max_checks):
            try:
                current_size = os.path.getsize(filepath)
                if current_size == last_size:
                    stable_count += 1
                else:
                    stable_count = 0
                    last_size = current_size
                
                if stable_count >= 2:  # Файл не менялся 2 проверки подряд
                    return True
                    
                time.sleep(check_interval)
            except OSError:
                time.sleep(check_interval)
                continue
        
        #logger.warning(f"Файл {filepath} не стабилизировался за {max_checks * check_interval} секунд")
        print(SCRIPT_NAME + f"Файл {filepath} не стабилизировался за {max_checks * check_interval} секунд")
        return False
    
    def get_latest_files(self, count=10):
        """Получает count самых свежих файлов"""
        pattern = os.path.join(self.source_dir, "K_line_*.csv")
        files = glob.glob(pattern)
        
        # Сортируем по времени модификации (последние измененные - самые свежие)
        files.sort(key=os.path.getmtime, reverse=True)
        
        return files[:count]
    
    def calculate_10m_volume(self, files):
        """Рассчитывает суммарный volume за 10 минут для каждого тикера"""
        all_data = []
        
        for file in files:
            try:
                df = pd.read_csv(file)
                all_data.append(df[['symbol', 'quote_volume']])
            except Exception as e:
                #logger.error(f"Ошибка чтения файла {file}: {e}")
                print(SCRIPT_NAME + f"Ошибка чтения файла {file}: {e}")
                continue
        
        if not all_data:
            return pd.DataFrame(columns=['symbol', 'volume_10m'])
        
        # Объединяем все данные
        combined = pd.concat(all_data, ignore_index=True)
        
        # Группируем по symbol и суммируем quote_volume
        result = combined.groupby('symbol')['quote_volume'].sum().reset_index()
        result.columns = ['symbol', 'volume_10m']
        
        return result
    
    def generate_output_filename(self):
        """Генерирует имя выходного файла на основе временной метки"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        return f"volume_10m_{timestamp}.csv"
    
    def process_file(self, filepath):
        """Основной метод обработки файла"""
        # Ждем стабилизации файла
        if not self.wait_for_file_stabilization(filepath):
            return
        
        # Получаем 10 самых свежих файлов
        latest_files = self.get_latest_files(10)
        
        if len(latest_files) < 10:
            #logger.warning(f"Найдено только {len(latest_files)} файлов, нужно 10. Пропускаем.")
            print(SCRIPT_NAME + f"Найдено только {len(latest_files)} файлов, нужно 10. Пропускаем.")
            return
        
        # Рассчитываем объемы
        volume_df = self.calculate_10m_volume(latest_files)
        
        if volume_df.empty:
            #logger.warning("Нет данных для агрегации")
            print(SCRIPT_NAME + "Нет данных для агрегации")
            return
        
        # Создаем выходной файл
        output_filename = self.generate_output_filename()
        output_path = os.path.join(self.target_dir, output_filename)
        
        # Сохраняем результат
        volume_df.to_csv(output_path, index=False)
        #logger.info(f"Создан файл: {output_path}")
        print(SCRIPT_NAME + f"Создан файл: {output_path}")
        #logger.info(f"Обработано {len(volume_df)} тикеров")
        print(SCRIPT_NAME + f"Обработано {len(volume_df)} тикеров")
        
        # Очищаем старые файлы
        cleanup_result_files()
        
        # Для отладки выводим топ-5 тикеров по объему
        #if not volume_df.empty:
        #    top_5 = volume_df.nlargest(5, 'volume_10m')
        #    #logger.info(f"Топ-5 тикеров по объему:\n{top_5.to_string(index=False)}")
        #    print(SCRIPT_NAME + f"Топ-5 тикеров по объему:\n{top_5.to_string(index=False)}")


def cleanup_result_files():
    """Удаляет старые файлы результатов если их больше MAX_RESULT_FILES"""
    files = glob.glob(os.path.join(RESULTS_DIR, "volume_10m_*.csv"))
    files.sort()
    
    while len(files) >= MAX_RESULT_FILES + 1:
        oldest_file = files.pop(0)
        try:
            os.remove(oldest_file)
            print(SCRIPT_NAME + f"Удален старый файл: {os.path.basename(oldest_file)}")
        except OSError as e:
            print(SCRIPT_NAME + f"Ошибка удаления файла {oldest_file}: {e}")


def main():
    # Проверяем существование исходной директории
    if not os.path.exists(K_LINES_DIR):
        #logger.error(f"Исходная директория не существует: {K_LINES_DIR}")
        print(SCRIPT_NAME + f"Исходная директория не существует: {K_LINES_DIR}")
        return
    
    #logger.info(f"Мониторинг директории: {K_LINES_DIR}")
    print(SCRIPT_NAME + f"Мониторинг директории: {K_LINES_DIR}")
    #logger.info(f"Целевая директория: {RESULTS_DIR}")
    print(SCRIPT_NAME + f"Целевая директория: {RESULTS_DIR}")
    
    # Создаем обработчик событий
    event_handler = FileHandler(K_LINES_DIR, RESULTS_DIR)
    
    # Создаем и запускаем наблюдатель
    observer = Observer()
    observer.schedule(event_handler, K_LINES_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        #logger.info("Мониторинг остановлен")
        print(SCRIPT_NAME + "Мониторинг остановлен")
    
    observer.join()

if __name__ == "__main__":
    main()