import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import re
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import traceback
from typing import Optional, List, Dict, Tuple
from logger import logger
from config import *

class KLineFileHandler(FileSystemEventHandler):
    def __init__(self, k_lines_path: str, alerts_calc_path: str):
        self.k_lines_path = Path(k_lines_path)
        self.alerts_calc_path = Path(alerts_calc_path)
        self.processed_files = set()
        
    def on_created(self, event):
        if not event.is_directory:
            file_path = Path(event.src_path)
            if file_path.suffix == '.csv' and 'K_line' in file_path.name:
                logger.info(CALC_SCRIPT_NAME + f"Обнаружен новый файл: {file_path.name}")
                # Ждем стабилизации файла
                time.sleep(5)
                self.process_kline_file(file_path)
    
    def process_kline_file(self, kline_file_path: Path):
        """Обработка нового K-line файла"""
        try:
            # Читаем K-line файл
            kline_df = self.read_kline_file(kline_file_path)
            if kline_df is None or kline_df.empty:
                logger.error(CALC_SCRIPT_NAME + f"Не удалось прочитать K-line файл: {kline_file_path.name}")
                return
            
            # Получаем время из имени файла K-line
            kline_time = self.extract_time_from_kline_filename(kline_file_path.name)
            
            # Получаем 3 самых свежих файла alerts_calc
            alert_files = self.get_recent_alerts_files(3)
            
            # Обрабатываем каждый файл alerts_calc
            for alert_file in alert_files:
                self.update_alert_file(alert_file, kline_df, kline_time)
                
            self.processed_files.add(kline_file_path.name)
            logger.info(CALC_SCRIPT_NAME + f"Обработка завершена для: {kline_file_path.name}")
            
        except Exception as e:
            logger.error(CALC_SCRIPT_NAME + f"Ошибка при обработке файла {kline_file_path.name}: {str(e)}")
            traceback.print_exc()
    
    def read_kline_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Чтение K-line файла с проверкой стабильности"""
        max_attempts = 10
        attempt = 0
        
        while attempt < max_attempts:
            try:
                size_before = file_path.stat().st_size
                time.sleep(1)
                size_after = file_path.stat().st_size
                
                if size_before == size_after:
                    # Файл стабилизировался
                    df = pd.read_csv(file_path)
                    #print(SCRIPT_NAME + f"Успешно прочитан K-line файл: {file_path.name}, строк: {len(df)}")
                    return df
                else:
                    logger.warning(CALC_SCRIPT_NAME + f"Файл {file_path.name} все еще изменяется...")
                    
            except Exception as e:
                logger.warning(CALC_SCRIPT_NAME + f"Попытка {attempt + 1} не удалась: {str(e)}")
            
            attempt += 1
        
        logger.error(CALC_SCRIPT_NAME + f"Не удалось прочитать стабильный файл: {file_path.name}")
        return None
    
    def extract_time_from_kline_filename(self, filename: str) -> datetime:
        """Извлечение времени из имени файла K-line"""
        # Формат: K_line_20260105_021800.csv
        match = re.search(r'K_line_(\d{8})_(\d{6})\.csv', filename)
        if match:
            date_str = match.group(1)  # 20260105
            time_str = match.group(2)  # 021800
            
            # Преобразуем в datetime
            dt_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        
        return datetime.now()
    
    def get_recent_alerts_files(self, n: int = 3) -> List[Path]:
        """Получение N самых свежих файлов alerts_calc"""
        alert_files = list(self.alerts_calc_path.glob("alerts_calc_*.csv"))
        
        # Сортируем по дате в имени файла (самые свежие первыми)
        alert_files.sort(key=lambda x: x.name, reverse=True)
        
        # Берем N самых свежих файлов
        recent_files = alert_files[:n]
        
        #print(SCRIPT_NAME + f"Найдено {len(recent_files)} файлов alerts_calc для обработки")
        return recent_files
    
    def update_alert_file(self, alert_file_path: Path, kline_df: pd.DataFrame, kline_time: datetime):
        """Обновление одного файла alerts_calc"""
        try:
            # Проверяем, не изменяется ли файл в данный момент
            if self.is_file_locked(alert_file_path):
                logger.info(CALC_SCRIPT_NAME + f"Файл {alert_file_path.name} в процессе изменения, пропускаем")
                return
            
            # Читаем файл alerts_calc
            alert_df = self.read_alert_file(alert_file_path)
            if alert_df is None or alert_df.empty:
                return
            
            # Фильтруем записи, не старше 48 часов
            time_threshold = datetime.now() - timedelta(hours=48)
            alert_df['time_dt'] = pd.to_datetime(alert_df['time'])
            recent_alerts = alert_df[alert_df['time_dt'] >= time_threshold].copy()
            
            if recent_alerts.empty:
                logger.info(CALC_SCRIPT_NAME + f"В файле {alert_file_path.name} нет записей за последние 48 часов")
                return
            
            # Обновляем min_price и max_price
            updated_count = 0
            for idx, row in recent_alerts.iterrows():
                ticker = row['ticker']
                
                # Ищем тикер в K-line данных
                kline_row = kline_df[kline_df['symbol'] == ticker]
                if kline_row.empty:
                    continue
                
                # Получаем значения из K-line
                current_low = float(kline_row['low'].iloc[0])
                current_high = float(kline_row['high'].iloc[0])
                
                # Получаем текущие значения из alerts
                current_min_price = float(row['min_price']) if pd.notna(row['min_price']) else float(row[r'buy\short_price'])
                current_max_price = float(row['max_price']) if pd.notna(row['max_price']) else float(row[r'buy\short_price'])
                
                # Форматируем время для записи
                time_str = kline_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # Обновляем min_price если нужно
                if current_low < current_min_price:
                    alert_df.at[idx, 'min_price'] = current_low
                    alert_df.at[idx, 'min_price_time'] = time_str
                    updated_count += 1
                    logger.info(CALC_SCRIPT_NAME + f"Обновлен min_price для {ticker}: {current_min_price} -> {current_low}")
                
                # Обновляем max_price если нужно
                if current_high > current_max_price:
                    alert_df.at[idx, 'max_price'] = current_high
                    alert_df.at[idx, 'max_price_time'] = time_str
                    updated_count += 1
                    logger.info(CALC_SCRIPT_NAME + f"Обновлен max_price для {ticker}: {current_max_price} -> {current_high}")
            
            if updated_count > 0:
                # Пересчитываем все производные поля
                self.calculate_derived_fields(alert_df)
                
                # Сохраняем обновленный файл
                self.save_alert_file(alert_df, alert_file_path)
                logger.info(CALC_SCRIPT_NAME + f"Файл {alert_file_path.name} обновлен: {updated_count} записей изменено")
            else:
                logger.info(CALC_SCRIPT_NAME + f"В файле {alert_file_path.name} не требуется обновлений")
                
        except Exception as e:
            logger.error(CALC_SCRIPT_NAME + f"Ошибка при обновлении файла {alert_file_path.name}: {str(e)}")
            traceback.print_exc()
    
    def is_file_locked(self, file_path: Path) -> bool:
        """Проверка, не заблокирован ли файл другим процессом"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Пробуем открыть файл на чтение и запись
                with open(file_path, 'r+') as f:
                    # Проверяем размер файла
                    size1 = os.path.getsize(file_path)
                    time.sleep(0.5)
                    size2 = os.path.getsize(file_path)
                    
                    # Если размер изменился, файл все еще записывается
                    if size1 != size2:
                        return True
                    
                    return False
            except (IOError, OSError):
                time.sleep(1)
                continue
        
        return True
    
    def read_alert_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Чтение файла alerts_calc"""
        try:
            df = pd.read_csv(file_path)
            #print(SCRIPT_NAME + f"Успешно прочитан файл alerts_calc: {file_path.name}, строк: {len(df)}")
            return df
        except Exception as e:
            logger.error(CALC_SCRIPT_NAME + f"Ошибка при чтении файла {file_path.name}: {str(e)}")
            return None
    
    def calculate_derived_fields(self, df: pd.DataFrame):
        """Расчет всех производных полей"""
        for idx, row in df.iterrows():
            try:
                buy_price = float(row[r'buy\short_price'])
                min_price = float(row['min_price']) if pd.notna(row['min_price']) else buy_price
                max_price = float(row['max_price']) if pd.notna(row['max_price']) else buy_price
                
                # Расчет базовых процентов
                max_loss_percent = ((buy_price - min_price) / buy_price) * 100
                if max_loss_percent == 0:
                    max_loss_percent = 0.001
                max_profit_percent = ((max_price - buy_price) / buy_price) * 100
                if max_profit_percent == 0:
                    max_profit_percent = 0.001
                
                df.at[idx, 'max_loss_%'] = max_loss_percent
                df.at[idx, 'max_proffit_%'] = max_profit_percent
                
                # Получаем времена
                min_price_time = row['min_price_time'] if pd.notna(row['min_price_time']) else ''
                max_price_time = row['max_price_time'] if pd.notna(row['max_price_time']) else ''
                
                # Флаг сравнения времен
                time_compare_min_max = min_price_time < max_price_time
                time_compare_max_min = min_price_time > max_price_time
                
                # Расчет RPS полей
                for i in range(1, 6):
                    field_name = f'RPS_(30%)_SL_{i}%'
                    
                    # Проверяем, не было ли уже установлено специальное значение
                    current_value = row[field_name]
                    if current_value in [-i, 0]:
                        continue
                    
                    # Проверяем условия для установки специальных значений
                    if max_profit_percent > i and max_loss_percent < 3 and time_compare_min_max:
                        df.at[idx, field_name] = -i
                    elif max_profit_percent > i and 3 < max_loss_percent < 5 and time_compare_min_max:
                        df.at[idx, field_name] = 0
                    else:
                        # Рассчитываем по формуле
                        df.at[idx, field_name] = max_loss_percent * 0.3
                
                # Расчет RPB полей
                for i in range(1, 6):
                    field_name = f'RPB_(30%)_SL_{i}%'
                    
                    # Проверяем, не было ли уже установлено специальное значение
                    current_value = row[field_name]
                    if current_value in [-i, 0]:
                        continue
                    
                    # Проверяем условия для установки специальных значений
                    if max_loss_percent > i and max_profit_percent < 3 and time_compare_max_min:
                        df.at[idx, field_name] = -i
                    elif max_loss_percent > i and 3 < max_profit_percent < 5 and time_compare_max_min:
                        df.at[idx, field_name] = 0
                    else:
                        # Рассчитываем по формуле
                        df.at[idx, field_name] = max_profit_percent * 0.3
                        
            except Exception as e:
                logger.error(CALC_SCRIPT_NAME + f"Ошибка при расчете полей для строки {idx}: {str(e)}")
                continue
    
    def save_alert_file(self, df: pd.DataFrame, file_path: Path):
        """Сохранение обновленного файла alerts_calc"""
        try:
            # Удаляем временную колонку если она есть
            if 'time_dt' in df.columns:
                df = df.drop('time_dt', axis=1)
            
            # Сохраняем во временный файл
            temp_path = file_path.with_suffix('.tmp')
            df.to_csv(temp_path, index=False)
            
            # Заменяем оригинальный файл
            temp_path.replace(file_path)
            logger.info(CALC_SCRIPT_NAME + f"Файл успешно сохранен: {file_path.name}")
            
        except Exception as e:
            logger.error(CALC_SCRIPT_NAME + f"Ошибка при сохранении файла {file_path.name}: {str(e)}")
            raise

def main():
    # Пути к папкам
    k_lines_path = calc_k_lines_path
    alerts_calc_path = calc_alerts_calc_path
    
    # Проверяем существование папок
    if not os.path.exists(k_lines_path):
        logger.error(CALC_SCRIPT_NAME + f"Папка K_lines не существует: {k_lines_path}")
        return
    
    if not os.path.exists(alerts_calc_path):
        logger.error(CALC_SCRIPT_NAME + f"Папка Alerts_calc не существует: {alerts_calc_path}")
        return
    
    # Создаем обработчик и наблюдатель
    event_handler = KLineFileHandler(k_lines_path, alerts_calc_path)
    observer = Observer()
    observer.schedule(event_handler, k_lines_path, recursive=False)
    
    logger.info(CALC_SCRIPT_NAME + f"Начинаем мониторинг папки: {k_lines_path}")
    logger.info(CALC_SCRIPT_NAME + f"Обрабатываем файлы из папки: {alerts_calc_path}")
    logger.info(CALC_SCRIPT_NAME + "Скрипт запущен. Ожидание новых K-line файлов...")
    
    try:
        observer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(CALC_SCRIPT_NAME + "\nОстановка мониторинга...")
        observer.stop()
    except Exception as e:
        logger.error(CALC_SCRIPT_NAME + f"Критическая ошибка: {str(e)}")
        traceback.print_exc()
    finally:
        observer.join()
        logger.info(CALC_SCRIPT_NAME + "Мониторинг остановлен.")

if __name__ == "__main__":
    main()