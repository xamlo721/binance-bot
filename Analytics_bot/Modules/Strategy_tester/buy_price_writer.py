import os
import time
import pandas as pd
from datetime import datetime, timedelta
import re
from pathlib import Path
import hashlib


SCRIPT_NAME = "BP_WRITER   :  "                           #Имя скрипта для вывода в консоль

class AlertProcessor:
    def __init__(self):
        self.alerts_folder = "/srv/ftp/Bot_v2/Data/Alerts"
        self.klines_folder = "/srv/ftp/Bot_v2/Data/K_lines/1M"
        self.processed_files = {}  # Для отслеживания обработанных файлов
        self.last_alert_file = None  # Последний обработанный файл
        
    def wait_for_file_stability(self, filepath, check_interval=0.5, max_checks=10):
        """Ожидание стабилизации файла (прекращения изменений)"""
        last_size = -1
        stable_count = 0
        
        for _ in range(max_checks * 2):  # Увеличиваем количество проверок
            try:
                current_size = os.path.getsize(filepath)
                if current_size == last_size:
                    stable_count += 1
                    if stable_count >= 3:  # Файл стабилен в течение 3 проверок
                        return True
                else:
                    last_size = current_size
                    stable_count = 0
                time.sleep(check_interval)
            except (FileNotFoundError, OSError):
                time.sleep(check_interval)
                continue
        
        return False  # Файл не стабилизировался
    
    def get_latest_alert_file(self):
        """Получение самого свежего файла в папке alerts"""
        try:
            alert_files = [f for f in os.listdir(self.alerts_folder) 
                          if f.startswith('alerts_') and f.endswith('.csv')]
            
            if not alert_files:
                return None
            
            # Сортируем по времени изменения
            latest_file = max(alert_files, 
                            key=lambda x: os.path.getmtime(os.path.join(self.alerts_folder, x)))
            
            filepath = os.path.join(self.alerts_folder, latest_file)
            
            # Проверяем, не обработан ли уже этот файл
            file_hash = self.get_file_hash(filepath)
            if latest_file == self.last_alert_file and file_hash == self.processed_files.get(latest_file):
                return None  # Файл уже обработан
                
            return latest_file
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при получении файлов: {e}")
            return None
    
    def get_file_hash(self, filepath):
        """Получение хэша файла для отслеживания изменений"""
        try:
            with open(filepath, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except:
            return None
    
    def parse_timestamp_to_filename(self, timestamp_str):
        """Преобразование timestamp в имя файла K-line"""
        try:
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            # Формат: K_line_YYYYMMDD_HHMM00.csv
            filename = f"K_line_{dt.strftime('%Y%m%d_%H%M00')}.csv"
            return filename
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка преобразования времени: {e}")
            return None
    
    def find_klines_file(self, timestamp_str, max_retries=120):
        """Поиск файла K-line с повторными попытками"""
        filename = self.parse_timestamp_to_filename(timestamp_str)
        if not filename:
            return None
        
        # Пытаемся найти файл сразу
        filepath = os.path.join(self.klines_folder, filename)
        if os.path.exists(filepath):
            return filepath
        
        # Если файл не найден, ждем и проверяем снова
        print(SCRIPT_NAME + f"Файл {filename} не найден, ожидаем...")
        for attempt in range(max_retries):
            time.sleep(1)  # Ждем 1 секунду
            if os.path.exists(filepath):
                print(SCRIPT_NAME + f"Файл {filename} найден после {attempt + 1} секунд ожидания")
                return filepath
        
        print(SCRIPT_NAME + f"Файл {filename} не найден после {max_retries} секунд ожидания")
        return None
    
    def get_high_price_for_ticker(self, klines_file, ticker):
        """Получение значения close для указанного тикера из файла K-line"""
        try:
            df = pd.read_csv(klines_file)
            ticker_data = df[df['symbol'] == ticker]
            
            if not ticker_data.empty:
                return ticker_data.iloc[0]['close']
            else:
                print(SCRIPT_NAME + f"Тикер {ticker} не найден в файле {klines_file}")
                return None
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при чтении файла K-line: {e}")
            return None
    
    def update_alert_file(self, alert_file, updates):
        """Обновление файла alerts с новыми значениями buy_price, min_price, max_price"""
        try:
            filepath = os.path.join(self.alerts_folder, alert_file)
            
            # Читаем текущий файл
            df = pd.read_csv(filepath)
            # Обновляем значения
            for ticker, buy_price in updates.items():
                mask = df['ticker'] == ticker
                if mask.any():
                    df.loc[mask, 'buy\short_price'] = buy_price
                    df.loc[mask, 'min_price'] = buy_price
                    df.loc[mask, 'max_price'] = buy_price
                    #df.loc[mask, 'result'] = "PASS"
            
            # Сохраняем обратно
            df.to_csv(filepath, index=False)
            
            print(SCRIPT_NAME + f"Файл {alert_file} обновлен с {len(updates)} записями")
            return True
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при обновлении файла: {e}")
            return False
    
    def process_alert_file(self, alert_file):
        """Обработка одного файла alerts"""
        filepath = os.path.join(self.alerts_folder, alert_file)
        
        print(SCRIPT_NAME + f"Начинаем обработку файла: {alert_file}")
        
        # Ждем стабилизации файла
        if not self.wait_for_file_stability(filepath):
            print(SCRIPT_NAME + f"Файл {alert_file} не стабилизировался, пропускаем")
            return False
        
        # Читаем файл
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при чтении файла {alert_file}: {e}")
            return False
        
        # Находим записи с пустым buy_price
        empty_buy_price = df[df['buy\short_price'].isna() | (df['buy\short_price'] == '')]
        
        if empty_buy_price.empty:
            print(SCRIPT_NAME + f"В файле {alert_file} нет записей с пустым buy\short_price")
            # Помечаем файл как обработанный
            self.last_alert_file = alert_file
            self.processed_files[alert_file] = self.get_file_hash(filepath)
            return True
        
        print(SCRIPT_NAME + f"Найдено {len(empty_buy_price)} записей с пустым buy\short_price")
        
        updates = {}
        processed = 0
        
        # Обрабатываем каждую запись
        for _, row in empty_buy_price.iterrows():
            ticker = row['ticker']
            timestamp = row['time']
            
            print(SCRIPT_NAME + f"Обработка тикера {ticker} со временем {timestamp}")
            
            # Ищем файл K-line
            klines_file = self.find_klines_file(timestamp)
            
            if klines_file:
                # Получаем значение high
                high_price = self.get_high_price_for_ticker(klines_file, ticker)
                
                if high_price is not None:
                    updates[ticker] = high_price
                    processed += 1
                    print(SCRIPT_NAME + f"Для тикера {ticker} установлен buy\short_price = {high_price}")
                else:
                    print(SCRIPT_NAME + f"Не удалось получить цену для тикера {ticker}")
            else:
                print(SCRIPT_NAME + f"Файл K-line для времени {timestamp} не найден, пропускаем тикер {ticker}")
        
        # Если есть обновления, сохраняем их
        if updates:
            success = self.update_alert_file(alert_file, updates)
            if success:
                # Обновляем информацию об обработанном файле
                self.last_alert_file = alert_file
                self.processed_files[alert_file] = self.get_file_hash(filepath)
                return True
        
        return False
    
    def run(self, check_interval=10):
        """Основной цикл обработки"""
        print(SCRIPT_NAME + "Запуск мониторинга папки с alerts...")
        
        while True:
            try:
                # Проверяем новый файл
                latest_file = self.get_latest_alert_file()
                
                if latest_file:
                    print(SCRIPT_NAME + f"Обнаружен новый/измененный файл: {latest_file}")
                    
                    # Обрабатываем файл
                    self.process_alert_file(latest_file)
                else:
                    # Ждем перед следующей проверкой
                    time.sleep(check_interval)
                    
            except KeyboardInterrupt:
                print(SCRIPT_NAME + "\nОстановка скрипта...")
                break
            except Exception as e:
                print(SCRIPT_NAME + f"Ошибка в основном цикле: {e}")
                time.sleep(check_interval)

def main():
    # Создаем и запускаем процессор
    processor = AlertProcessor()
    
    # Запускаем мониторинг с проверкой каждые 10 секунд
    processor.run(check_interval=10)

if __name__ == "__main__":
    main()