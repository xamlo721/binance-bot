import glob
import csv
import pandas as pd

from datetime import datetime
from typing import List

from ramstorage.ram_storage_utils import CandleRecord
from logger import logger
from config import *

def read_1m_candles_from_csv(filepath: str) -> list[CandleRecord] | None:
    """Чтение одного файла с обработкой ошибок"""
    try:
        df = pd.read_csv(filepath)
        # Используем только нужные колонки для экономии памяти
        fieldnames = [
                'symbol',                   # Тикер
                'open',                     # Open price - цена открытия
                'close',                    # Close price - цена закрытия
                'high',                     # High price - максимальная цена за период
                'low',                      # Low price - минимальная цена за период
                'volume',                   # Volume - объем базового актива
                'quote_volume',             # Quote asset volume - объем в котировочной валюте
                'taker_buy_base_volume',    # Taker buy base asset volume - объем покупок базового актива
                'taker_buy_quote_volume',   # Taker buy quote asset volume - объем покупок котировочного актива
                'trades',                   # Number of trades - количество сделок
                'open_time'                 # Open time - время открытия свечи
        ]

        # Проверяем, что все требуемые колонки присутствуют
        missing_columns = [col for col in fieldnames if col not in df.columns]
        if missing_columns:
            logger.error(hdp_1h_SCRIPT_NAME + f"В файле {filepath} отсутствуют колонки: {missing_columns}")
            return None
        
        # Выбираем только нужные колонки
        df = df[fieldnames]
        
        # Преобразуем DataFrame в список CandleRecord
        candles: list[CandleRecord] = []
        for _, row in df.iterrows():
            candle = CandleRecord(
                symbol=row['symbol'],
                open=float(row['open']),
                close=float(row['close']),
                high=float(row['high']),
                low=float(row['low']),
                volume=float(row['volume']),
                quote_volume=float(row['quote_volume']),
                taker_buy_base_volume=float(row['taker_buy_base_volume']),
                taker_buy_quote_volume=float(row['taker_buy_quote_volume']),
                trades=int(row['trades']),
                open_time=int(row['open_time'])
            )
            candles.append(candle)
        
        logger.info(hdp_1h_SCRIPT_NAME + f"Загружено {len(candles)} свечей из файла {filepath}")
        return candles
    
    except Exception as e:
        logger.error(hdp_1h_SCRIPT_NAME + f"Ошибка чтения файла {filepath}: {e}")
        return None

def write_1m_candles_to_csv(filename: str, candles: List[CandleRecord]):
    """Сохранение результатов в CSV с именем на основе времени открытия свечи"""
    if not candles:
        logger.info(k_line_SCRIPT_NAME + "Нет данных для сохранения")
        return None
    
    # Создаем папку K_lines, если её нет
    folder = MINUTES_KLINE_FOLDER
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    filepath = os.path.join(folder, filename)
    filename = os.path.join(filename)
    
    try:

        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            # Только нужные поля в правильном порядке
            fieldnames = [
                'symbol',                   # Тикер
                'open',                     # Open price - цена открытия
                'close',                    # Close price - цена закрытия
                'high',                     # High price - максимальная цена за период
                'low',                      # Low price - минимальная цена за период
                'volume',                   # Volume - объем базового актива
                'quote_volume',             # Quote asset volume - объем в котировочной валюте
                'taker_buy_base_volume',    # Taker buy base asset volume - объем покупок базового актива
                'taker_buy_quote_volume',   # Taker buy quote asset volume - объем покупок котировочного актива
                'trades',                   # Number of trades - количество сделок
                'open_time'                 # Open time - время открытия свечи
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for candle in candles:
                writer.writerow(candle.to_dict())


    except Exception as e:
        logger.error(f"Ошибка при сохранении в CSV {filepath}: {str(e)}")

    return filename

def cleanup_old_files(folder:str, filemask: str, max_files: int = 20):
    """Удаление старых файлов, оставляя только max_files самых новых"""
    if not os.path.exists(folder):
        return
    
    # Получаем все файлы согласно маске в указанной папке
    pattern = os.path.join(folder, filemask)
    files = glob.glob(pattern)
    
    # Если файлов больше максимального количества, удаляем самые старые
    if len(files) > max_files:
        # Сортируем файлы по времени создания (сначала старые)
        #files.sort(key=os.path.getctime)

        # Сортируем файлы по времени указанном в имени
        files.sort()

        # Удаляем лишние файлы
        for i in range(len(files) - max_files):
            oldest_file = files[i] 

            os.remove(oldest_file)
            
            try:
                os.remove(oldest_file)
                logger.info(f"Удален старый файл: {os.path.basename(oldest_file)}")

            except OSError as e:
                logger.error(f"Ошибка удаления файла {oldest_file}: {e}")


def get_sorted_files(folder:str, filemask: str, max_files: int = 20):
    """Возвращает отсортированный список самых новых файлов по имени"""
    pattern = os.path.join(folder, filemask)
    files = glob.glob(pattern)
    files_sorted = sorted(files)
    newest_files = files_sorted[-max_files:]
    return newest_files
