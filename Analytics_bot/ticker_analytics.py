import os
import time
import pandas as pd
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import glob
import requests
from datetime import datetime
from datetime import timezone
from logger import logger
from config import *


# Пороговое значение (X раз) - можно изменить по необходимости
X_MULTIPLIER = 5.0  # Например, должно быть больше в 2 раза

def init_alerts_folder():
    """Инициализировать папку для хранения алертов"""
    if not os.path.exists(T_ANAL_ALERTS_FOLDER):
        os.makedirs(T_ANAL_ALERTS_FOLDER)
        logger.info(T_ANAL_SCRIPT_NAME + f"Создана папка для алертов: {T_ANAL_ALERTS_FOLDER}")

def get_current_date_str():
    """Получить текущую дату в формате для имени файла (по UTC)"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_alerts_file_path(date_str=None):
    """Получить путь к файлу алертов для указанной даты"""
    if date_str is None:
        date_str = get_current_date_str()
    filename = f"alerts_{date_str}.csv"
    return os.path.join(T_ANAL_ALERTS_FOLDER, filename)

def save_alert_to_file(ticker, reason, bot_token, chat_id, message_thread_id=None):
    """Сохранить алерт в файл с текущей датой по UTC"""
    # Инициализируем папку если нужно
    init_alerts_folder()
    
    # Получаем текущую дату и время
    current_date = get_current_date_str()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    #current_time = datetime.now().strftime("%m-%d %H:%M")
    
    # Получаем путь к файлу для текущей даты
    file_path = get_alerts_file_path(current_date)
    
    # Создаем DataFrame с данными алерта
    alert_data = {
        'ticker': [ticker],
        'volume': [reason],
        'time': [current_time],
        r'buy\short_price': [pd.NA],
        'min_price': [pd.NA],
        'min_price_time': [pd.NA],
        'max_price': [pd.NA],
        'max_price_time': [pd.NA],
        'RPS_(30%)_SL_1%': [pd.NA],
        'RPS_(30%)_SL_2%': [pd.NA],
        'RPS_(30%)_SL_3%': [pd.NA],
        'RPS_(30%)_SL_4%': [pd.NA],
        'RPS_(30%)_SL_5%': [pd.NA],
        'max_loss_%': [pd.NA],
        'RPB_(30%)_SL_1%': [pd.NA],
        'RPB_(30%)_SL_2%': [pd.NA],
        'RPB_(30%)_SL_3%': [pd.NA],
        'RPB_(30%)_SL_4%': [pd.NA],
        'RPB_(30%)_SL_5%': [pd.NA],
        'max_proffit_%': [pd.NA]
    }
    alert_df = pd.DataFrame(alert_data, dtype='object')
    
    try:
        # Проверяем, существует ли уже файл для этой даты
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            
            # Проверяем, есть ли уже такой тикер в файле
            if ticker in existing_df['ticker'].values:
                logger.warning(T_ANAL_SCRIPT_NAME + f"Тикер {ticker} уже присутствует в файле {os.path.basename(file_path)}")
                return False
            
            # Убедимся, что оба DataFrame имеют одинаковые колонки
            # Добавляем недостающие колонки в существующий или новый DataFrame
            existing_columns = set(existing_df.columns)
            alert_columns = set(alert_df.columns)
            
            # Добавляем недостающие колонки в existing_df
            for col in alert_columns - existing_columns:
                existing_df[col] = pd.NA
            
            # Добавляем недостающие колонки в alert_df
            for col in existing_columns - alert_columns:
                alert_df[col] = pd.NA
                
            # Добавляем новый алерт к существующим данным
            combined_df = pd.concat([existing_df, alert_df], ignore_index=True)
        else:
            # Создаем новый файл
            combined_df = alert_df
        
        # Сохраняем в CSV
        combined_df.to_csv(file_path, index=False)
        logger.info(T_ANAL_SCRIPT_NAME + f"Алерт для {ticker} сохранен в файл: {os.path.basename(file_path)}")
        
        # Отправляем в Telegram
        #return True, (f"Превышение объёмов: {min_ratio:.2f} x")
        message = f"🚨 #{ticker}: Превышение объёмов: {reason}x"
        #send_to_telegram(message, bot_token, chat_id, message_thread_id=message_thread_id, parse_mode="HTML")
        
        return True
        
    except Exception as e:
        logger.error(T_ANAL_SCRIPT_NAME + f"Ошибка при сохранении алерта в файл: {e}")
        # В случае ошибки всё равно отправляем в Telegram
        message = f"🚨 #{ticker}: {reason}"
        #send_to_telegram(message, bot_token, chat_id, message_thread_id=message_thread_id, parse_mode="HTML")
        return False

#def send_to_telegram(message, bot_token, chat_id, message_thread_id=None, parse_mode=None):
#    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
#    payload = {
#        'chat_id': chat_id,
#        'text': message
#    }
#    
#    # Добавляем ID темы, если он указан
#    if message_thread_id:
#        payload['message_thread_id'] = message_thread_id
#        
#    # Добавляем parse_mode, если он указан
#    if parse_mode:
#        payload['parse_mode'] = parse_mode
#        
#    try:
#        response = requests.post(url, json=payload)
#        return response.status_code == 200
#    except Exception as e:
#        print(SCRIPT_NAME + ":  " + f"Ошибка отправки в Telegram: {e}")
#        return False

# Использование
#BOT_TOKEN = "BOT_TOKEN"
#CHAT_ID = "CHAT_ID"
#CHAT_ID_2 = "CHAT_ID_2"
#CHANNEL_ID = "CHANNEL_ID"
#thread_id = "thread_id"     # для первых за сутки
#thread_id_2 = "thread_id_2"     # для повторных

def get_latest_file(folder_path, pattern=None):
    """Получить самый свежий файл в папке"""
    if pattern:
        files = glob.glob(os.path.join(folder_path, pattern))
    else:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) 
                if os.path.isfile(os.path.join(folder_path, f))]
    
    if not files:
        return None
    
    # Возвращаем файл с самым свежим временем создания
    latest_file = max(files, key=os.path.getctime)
    return latest_file

def wait_for_file_stability(file_path, check_interval=1, stable_period=2):
    """Дождаться стабилизации файла (перестает меняться размер)"""
    if not os.path.exists(file_path):
        return False
    
    #print(SCRIPT_NAME + f"Ожидание стабилизации файла: {os.path.basename(file_path)}")
    
    last_size = -1
    stable_time = 0
    start_time = time.time()
    
    while True:
        if not os.path.exists(file_path):
            return False
            
        current_size = os.path.getsize(file_path)
        
        if current_size == last_size:
            stable_time += check_interval
            if stable_time >= stable_period:
                logger.info(T_ANAL_SCRIPT_NAME + f"Файл стабилизирован за {time.time() - start_time:.2f} сек")
                return True
        else:
            last_size = current_size
            stable_time = 0
            
        time.sleep(check_interval)
        
        # Таймаут на случай, если файл пишется очень долго
        if time.time() - start_time > 30:
            logger.warning(T_ANAL_SCRIPT_NAME + f"Таймаут ожидания файла {os.path.basename(file_path)}")
            return False

def analyze_ticker(ticker, volume_10m_value, volume_10h_data):
    """Анализировать тикер по заданным условиям"""
    if volume_10m_value is None:
        return False, "Нет данных volume_10m"
    
    # Умножаем volume_10m на 6
    multiplied_volume = volume_10m_value * 6
    
    # Получаем все значения total_volume
    volume_columns = [f'total_volume_{i}' for i in range(1, 11)]
    total_volumes = []
    
    for col in volume_columns:
        if col in volume_10h_data:
            volume_value = volume_10h_data[col]
            total_volumes.append(volume_value)
    
    if not total_volumes:
        return False, "Нет данных total_volume"
    
    # Проверяем, больше ли multiplied_volume каждого из total_volumes в X раз
    all_conditions_met = True
    min_ratio = float('inf')
    
    for volume_10h_value in total_volumes:
        if volume_10h_value <= 0:
            # Если значение 0 или отрицательное, считаем что условие не выполнено
            all_conditions_met = False
            break
            
        ratio = multiplied_volume / volume_10h_value
        min_ratio = min(min_ratio, ratio)
        
        if ratio < X_MULTIPLIER:
            all_conditions_met = False
            break
    
    if all_conditions_met:
        #return True, (f"Превышение объёмов: {min_ratio:.2f} x")
        return True, (f"{min_ratio:.2f}")
    
    return False, f"Условие не выполнено (минимальный коэффициент: {min_ratio:.2f})"

def process_ticker_up_file(file_path):
    """Обработать файл с тикерами"""
    #print(SCRIPT_NAME + f"{'='*60}")
    logger.info(T_ANAL_SCRIPT_NAME + f"Начало обработки файла: {os.path.basename(file_path)}")
    #print(SCRIPT_NAME + f"{'='*60}")
    
    # Получаем самые свежие файлы для сравнения
    latest_10m_file = get_latest_file(T_ANAL_VOLUME_10M_FOLDER, "volume_10m_*.csv")
    latest_10h_file = get_latest_file(T_ANAL_VOLUME_10H_FOLDER, "Volume_10H_*.csv")
    
    if not latest_10m_file:
        logger.error(T_ANAL_SCRIPT_NAME + "Ошибка: Не найден файл volume_10m")
        return
    
    if not latest_10h_file:
        logger.error(T_ANAL_SCRIPT_NAME + "Ошибка: Не найден файл Volume_10H")
        return
    
    #print(SCRIPT_NAME + f"Используется 10M файл: {os.path.basename(latest_10m_file)}")
    #print(SCRIPT_NAME + f"Используется 10H файл: {os.path.basename(latest_10h_file)}")
    #print(SCRIPT_NAME + f"Пороговое значение X: {X_MULTIPLIER}")
    logger.info(T_ANAL_SCRIPT_NAME + "-" * 60)
    
    try:
        # Загружаем данные
        tickers_df = pd.read_csv(file_path)
        volume_10m_df = pd.read_csv(latest_10m_file)
        volume_10h_df = pd.read_csv(latest_10h_file)
        
        # Проверяем структуру данных
        logger.info(T_ANAL_SCRIPT_NAME + f"Загружено тикеров из ticker_up: {len(tickers_df)}")
        #print(SCRIPT_NAME + f"Загружено записей volume_10m: {len(volume_10m_df)}")
        #print(SCRIPT_NAME + f"Загружено записей volume_10h: {len(volume_10h_df)}")
        #print(SCRIPT_NAME + "-" * 60)
        
        # Создаем словарь для быстрого поиска volume_10m
        volume_10m_dict = dict(zip(volume_10m_df['symbol'], volume_10m_df['volume_10m']))
        
        # Обрабатываем каждый тикер
        tickers_with_alerts = 0
        processed_tickers = 0
        
        for index, row in tickers_df.iterrows():
            ticker = row['symbol']
            processed_tickers += 1
            
            # Получаем volume_10m для тикера
            volume_10m_value = volume_10m_dict.get(ticker)
            
            # Ищем тикер в 10H данных
            volume_10h_row = volume_10h_df[volume_10h_df['symbol'] == ticker]
            
            if not volume_10h_row.empty:
                volume_10h_data = volume_10h_row.iloc[0].to_dict()
                
                # Анализируем тикер
                alert, message = analyze_ticker(ticker, volume_10m_value, volume_10h_data)
                
                if alert:
                    logger.info(T_ANAL_SCRIPT_NAME + f"🚨#{ticker}: {message}")
                    tickers_with_alerts += 1
                    
                    # Сохраняем алерт в файл и отправляем в Telegram
                    #save_alert_to_file(ticker, message, BOT_TOKEN, CHANNEL_ID, message_thread_id=thread_id)
                    save_alert_to_file(ticker, message, BOT_TOKEN=123, CHANNEL_ID=123, message_thread_id=123)

                    message_2 = (f"🚨 #{ticker}: Превышение объёмов: {message}x")
                    #send_to_telegram(message_2, BOT_TOKEN, CHANNEL_ID, message_thread_id=thread_id_2, parse_mode="HTML")

                else:
                    # Для тикеров без алерта можно выводить меньше информации
                    if processed_tickers <= 10:  # Выводим только первые 10 для примера
                        logger.info(T_ANAL_SCRIPT_NAME + f"{ticker}: {message}")
            else:
                if processed_tickers <= 10:  # Выводим только первые 10 для примера
                    logger.info(T_ANAL_SCRIPT_NAME + f"{ticker}: Нет данных в volume_10h")
        
        logger.info(T_ANAL_SCRIPT_NAME + "-" * 60)
        logger.info(T_ANAL_SCRIPT_NAME + f"Обработка завершена.")
        logger.info(T_ANAL_SCRIPT_NAME + f"Обработано тикеров: {processed_tickers}")
        logger.info(T_ANAL_SCRIPT_NAME + f"Найдено тикеров с алертами: {tickers_with_alerts}")
        
        # Показываем информацию о файле алертов
        current_date = get_current_date_str()
        alerts_file = get_alerts_file_path(current_date)
        if os.path.exists(alerts_file):
            alerts_df = pd.read_csv(alerts_file)
            logger.info(T_ANAL_SCRIPT_NAME + f"Всего алертов за сегодня ({current_date}): {len(alerts_df)}")
        
        logger.info(T_ANAL_SCRIPT_NAME + f"{'='*60}")
        
    except Exception as e:
        logger.error(T_ANAL_SCRIPT_NAME + f"Ошибка при обработке файлов: {e}")
        import traceback
        traceback.print_exc()

class TickerUpHandler(FileSystemEventHandler):
    """Обработчик событий для папки Ticker_up"""
    
    def __init__(self):
        self.processed_files = set()
        self.current_alerts_date = None
        self.check_date_change()
    
    def check_date_change(self):
        """Проверить, изменилась ли дата и обновить файл алертов при необходимости"""
        current_date = get_current_date_str()
        
        if self.current_alerts_date != current_date:
            logger.info(T_ANAL_SCRIPT_NAME + f"Дата изменилась на {current_date}. Новые алерты будут записываться в новый файл.")
            self.current_alerts_date = current_date
            
            # Проверяем существование файла для новой даты
            new_file_path = get_alerts_file_path(current_date)
            if os.path.exists(new_file_path):
                logger.info(T_ANAL_SCRIPT_NAME + f"Файл алертов для {current_date} уже существует.")
            else:
                logger.error(T_ANAL_SCRIPT_NAME + f"Будет создан новый файл алертов для {current_date}.")
    
    def on_created(self, event):
        """Обработать создание нового файла"""
        if not event.is_directory and event.src_path.endswith('.csv'):
            self.check_date_change()  # Проверяем изменение даты перед обработкой
            self.handle_new_file(event.src_path)
    
    def on_modified(self, event):
        """Обработать изменение файла"""
        if not event.is_directory and event.src_path.endswith('.csv'):
            self.check_date_change()  # Проверяем изменение даты перед обработкой
            self.handle_new_file(event.src_path)
    
    def handle_new_file(self, file_path):
        """Обработать новый/измененный файл"""
        filename = os.path.basename(file_path)
        
        # Проверяем, обрабатывали ли мы уже этот файл
        if filename in self.processed_files:
            return
        
        #print(SCRIPT_NAME + f"{'#'*60}")
        logger.info(T_ANAL_SCRIPT_NAME + f"Обнаружен новый файл: {filename}")
        #print(SCRIPT_NAME + f"Полный путь: {file_path}")
        #print(SCRIPT_NAME + f"{'#'*60}")
        
        # Ждем стабилизации файла
        if wait_for_file_stability(file_path):
            # Обрабатываем файл
            process_ticker_up_file(file_path)
            
            # Добавляем файл в список обработанных
            self.processed_files.add(filename)
        else:
            logger.error(T_ANAL_SCRIPT_NAME + f"Не удалось дождаться стабилизации файла {filename}")

def main():
    """Основная функция"""
    logger.info(T_ANAL_SCRIPT_NAME + f"{'='*60}")
    logger.info(T_ANAL_SCRIPT_NAME + "Запуск системы мониторинга тикеров")
    logger.info(T_ANAL_SCRIPT_NAME + f"{'='*60}")
    logger.info(T_ANAL_SCRIPT_NAME + f"Папка Ticker_up: {T_ANAL_TICKER_UP_FOLDER}")
    logger.info(T_ANAL_SCRIPT_NAME + f"Папка Volume_10M: {T_ANAL_VOLUME_10M_FOLDER}")
    logger.info(T_ANAL_SCRIPT_NAME + f"Папка Volume_10H: {T_ANAL_VOLUME_10H_FOLDER}")
    logger.info(T_ANAL_SCRIPT_NAME + f"Папка Alerts: {T_ANAL_ALERTS_FOLDER}")
    logger.info(T_ANAL_SCRIPT_NAME + f"Пороговое значение (X): {X_MULTIPLIER}")
    logger.info(T_ANAL_SCRIPT_NAME + f"{'='*60}")
    
    # Инициализируем папку для алертов
    init_alerts_folder()
    
    # Проверяем существование папок
    for folder in [T_ANAL_TICKER_UP_FOLDER, T_ANAL_VOLUME_10M_FOLDER, T_ANAL_VOLUME_10H_FOLDER, T_ANAL_ALERTS_FOLDER]:
        if not os.path.exists(folder):
            logger.error(T_ANAL_SCRIPT_NAME + f"Ошибка: Папка не существует: {folder}")
            return
    
    # Показываем существующие файлы алертов
    alert_files = glob.glob(os.path.join(T_ANAL_ALERTS_FOLDER, "alerts_*.csv"))
    if alert_files:
        logger.info(T_ANAL_SCRIPT_NAME + "Существующие файлы алертов:")
        for file in sorted(alert_files):
            file_date = os.path.basename(file).replace("alerts_", "").replace(".csv", "")
            df = pd.read_csv(file)
            logger.info(T_ANAL_SCRIPT_NAME + f"{os.path.basename(file)} - {len(df)} алертов")
    
    # Проверяем, есть ли уже файлы в папке Ticker_up
    existing_files = glob.glob(os.path.join(T_ANAL_TICKER_UP_FOLDER, "tickers_up_*.csv"))
    if existing_files:
        latest_file = max(existing_files, key=os.path.getctime)
        logger.info(T_ANAL_SCRIPT_NAME + f"Найден существующий файл: {os.path.basename(latest_file)}")
        logger.info(T_ANAL_SCRIPT_NAME + "Обрабатываю...")
        
        handler = TickerUpHandler()
        handler.handle_new_file(latest_file)
    
    # Запускаем мониторинг
    event_handler = TickerUpHandler()
    observer = Observer()
    observer.schedule(event_handler, T_ANAL_TICKER_UP_FOLDER, recursive=False)
    
    logger.info(T_ANAL_SCRIPT_NAME + "Мониторинг запущен. Ожидание новых файлов...")
    logger.info(T_ANAL_SCRIPT_NAME + "Для остановки нажмите Ctrl+C")
    
    try:
        observer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info(T_ANAL_SCRIPT_NAME + "Мониторинг остановлен.")
    finally:
        observer.join()

if __name__ == "__main__":
    main()