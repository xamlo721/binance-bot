import os
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import glob
from logger import logger


SCRIPT_NAME = "ALERTS_COPY :  "                           #Имя скрипта для вывода в консоль


class AlertsHandler(FileSystemEventHandler):
    def __init__(self, alerts_folder, calc_folder):
        self.alerts_folder = alerts_folder
        self.calc_folder = calc_folder
        self.current_file = None
        self.known_rows = set()
        self.last_processed_time = 0
        self.last_file_mod_time = 0
        self.current_filename = None
        
        # Создаем папку для результатов если её нет
        os.makedirs(calc_folder, exist_ok=True)
        
        # Находим самый свежий файл при запуске
        self.update_current_file()
    
    def update_current_file(self):
        """Найти самый свежий файл в папке"""
        try:
            files = glob.glob(os.path.join(self.alerts_folder, "alerts_*.csv"))
            if not files:
                logger.warning(SCRIPT_NAME + f"Нет файлов в папке {self.alerts_folder}")
                return
            
            # Сортируем по времени создания (новейший первый)
            files.sort(key=os.path.getmtime, reverse=True)
            newest_file = files[0]
            
            if newest_file != self.current_file:
                logger.info(SCRIPT_NAME + f"Обнаружен новый файл: {os.path.basename(newest_file)}")
                self.current_file = newest_file
                self.current_filename = os.path.basename(newest_file)
                self.known_rows = self.read_file_rows(newest_file)
                self.last_file_mod_time = os.path.getmtime(newest_file)
                logger.info(SCRIPT_NAME + f"Загружено {len(self.known_rows)} строк из файла")
        
        except Exception as e:
            logger.error(SCRIPT_NAME + f"Ошибка при обновлении текущего файла: {e}")
    
    def read_file_rows(self, filepath):
        """Прочитать все строки файла и вернуть множество для сравнения"""
        try:
            rows = set()
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    # Нормализуем строку (убираем лишние пробелы и переводы строк)
                    cleaned_line = line.strip()
                    if cleaned_line:  # Игнорируем пустые строки
                        rows.add(cleaned_line)
            return rows
        except Exception as e:
            logger.error(SCRIPT_NAME + f"Ошибка при чтении файла {filepath}: {e}")
            return set()
    
    def check_for_new_rows(self):
        """Проверить наличие новых строк в текущем файле"""
        try:
            current_mod_time = os.path.getmtime(self.current_file)
            
            # Если файл не изменялся, ничего не делаем
            if current_mod_time <= self.last_file_mod_time:
                return []
            
            self.last_file_mod_time = current_mod_time
            current_rows = self.read_file_rows(self.current_file)
            
            # Находим новые строки
            new_rows = current_rows - self.known_rows
            
            if new_rows:
                logger.info(SCRIPT_NAME + f"Найдено {len(new_rows)} новых строк")
                self.known_rows = current_rows
                
                # Возвращаем новые строки как список
                return list(new_rows)
            
            return []
        
        except Exception as e:
            logger.error(SCRIPT_NAME + f"Ошибка при проверке новых строк: {e}")
            return []
    
    def process_new_rows(self, new_rows):
        """Обработать новые строки"""
        if not new_rows:
            return
        
        processed_rows = []
        header = None
        
        # Читаем заголовок из исходного файла
        try:
            with open(self.current_file, 'r', encoding='utf-8') as f:
                header = f.readline().strip()
        except Exception as e:
            logger.error(SCRIPT_NAME + f"Ошибка при чтении заголовка: {e}")
            return
        
        # Анализируем каждую новую строку
        for row in new_rows:
            # Пропускаем строку если это заголовок
            if row.startswith('ticker,volume,time'):
                continue
            
            parts = row.split(',')
            if len(parts) >= 6:
                ticker = parts[0]
                buy_price = parts[3] if len(parts) > 3 else ''
                min_price = parts[4] if len(parts) > 4 else ''
                max_price = parts[6] if len(parts) > 6 else ''
                
                # Проверяем, есть ли значения цен
                if buy_price and min_price and max_price:
                    logger.info(SCRIPT_NAME + f"Найдены цены для {ticker}")
                    processed_rows.append(row)
                else:
                    logger.warning(SCRIPT_NAME + f"Цены не заполнены для {ticker}, пропускаем")
        
        # Если есть строки для записи, записываем их в новый файл
        if processed_rows and header:
            self.write_to_calc_file(header, processed_rows)
    
    def write_to_calc_file(self, header, rows):
        """Записать строки в файл alerts_calc"""
        try:
            # Извлекаем дату из имени исходного файла
            # alerts_2026-01-06.csv -> 2026-01-06
            date_part = self.current_filename.replace('alerts_', '').replace('.csv', '')
            calc_filename = f"alerts_calc_{date_part}.csv"
            calc_filepath = os.path.join(self.calc_folder, calc_filename)
            
            # Проверяем, существует ли уже файл
            file_exists = os.path.exists(calc_filepath)
            
            # Ждем до 50-й секунды
            self.wait_for_50th_second()
            
            with open(calc_filepath, 'a', encoding='utf-8') as f:
                if not file_exists:
                    # Если файл новый, записываем заголовок
                    f.write(header + '\n')
                    logger.info(SCRIPT_NAME + f"Создан новый файл: {calc_filename}")
                
                # Записываем все строки
                for row in rows:
                    f.write(row + '\n')
                
                logger.info(SCRIPT_NAME + f"Записано {len(rows)} строк в файл {calc_filename}")
        
        except Exception as e:
            logger.error(SCRIPT_NAME + f"Ошибка при записи в файл: {e}")
    
    def wait_for_50th_second(self):
        """Ожидать до 50-й секунды текущей минуты"""
        current_second = datetime.now().second
        
        if current_second < 50:
            # Ждем до 50-й секунды
            sleep_time = 50 - current_second
            logger.info(SCRIPT_NAME + f"Ожидание {sleep_time} секунд до 50-й секунды...")
            time.sleep(sleep_time)
        elif current_second > 50:
            # Если уже прошла 50-я секунда, ждем следующей минуты
            sleep_time = 60 - current_second + 50
            logger.info(SCRIPT_NAME + f"Ожидание {sleep_time} секунд до следующей 50-й секунды...")
            time.sleep(sleep_time)
        # Если сейчас ровно 50-я секунда, не ждем
    
    def on_modified(self, event):
        """Обработчик изменения файла"""
        if not event.is_directory:
            filename = os.path.basename(event.src_path)
            
            # Проверяем, что это файл с алертами
            if filename.startswith('alerts_') and filename.endswith('.csv'):
                # Если это изменение в другом файле, обновляем текущий
                if event.src_path != self.current_file:
                    self.update_current_file()
                else:
                    # Проверяем новые строки в текущем файле
                    new_rows = self.check_for_new_rows()
                    if new_rows:
                        self.process_new_rows(new_rows)
    
    def on_created(self, event):
        """Обработчик создания нового файла"""
        if not event.is_directory:
            filename = os.path.basename(event.src_path)
            
            # Проверяем, что это новый файл с алертами
            if filename.startswith('alerts_') and filename.endswith('.csv'):
                logger.info(SCRIPT_NAME + f"Создан новый файл: {filename}")
                # Обновляем текущий файл
                time.sleep(1)  # Небольшая задержка для завершения записи
                self.update_current_file()

def main():
    # Пути к папкам
    alerts_folder = "Data/Alerts"
    calc_folder = "Alerts_calc"
    
    # Проверяем существование основной папки
    if not os.path.exists(alerts_folder):
        logger.error(SCRIPT_NAME + f"Папка {alerts_folder} не существует!")
        return
    
    logger.info(SCRIPT_NAME + f"Мониторинг папки: {alerts_folder}")
    logger.info(SCRIPT_NAME + f"Файлы результатов будут сохраняться в: {calc_folder}")
    
    # Создаем обработчик и наблюдатель
    event_handler = AlertsHandler(alerts_folder, calc_folder)
    observer = Observer()
    observer.schedule(event_handler, alerts_folder, recursive=False)
    
    try:
        logger.info(SCRIPT_NAME + "Запуск мониторинга...")
        observer.start()
        
        # Также периодически проверяем наличие новых файлов
        while True:
            time.sleep(10)  # Проверяем каждые 10 секунд
            # Периодическая проверка обновления текущего файла
            if event_handler.current_file:
                new_rows = event_handler.check_for_new_rows()
                if new_rows:
                    event_handler.process_new_rows(new_rows)
    
    except KeyboardInterrupt:
        logger.info(SCRIPT_NAME + "\nОстановка мониторинга...")
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    main()