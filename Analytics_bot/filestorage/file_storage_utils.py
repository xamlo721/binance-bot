import glob
import csv

from datetime import datetime
from typing import List

from ramstorage.ram_storage_utils import CandleRecord
from logger import logger
from config import *


def save_to_csv(candles: List[CandleRecord]):
    """Сохранение результатов в CSV с именем на основе времени открытия свечи"""
    if not candles:
        logger.info(k_line_SCRIPT_NAME + "Нет данных для сохранения")
        return None
    
    # Получаем время открытия свечи из первого результата
    open_time = candles[0].open_time
    open_time_dt = datetime.fromtimestamp(open_time / 1000)
    filename = f"K_line_{open_time_dt.strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Создаем папку K_lines, если её нет
    folder = k_K_LINES_DIR
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

def cleanup_old_files(max_files=20):
    """Удаление старых файлов, оставляя только max_files самых новых"""
    folder = k_K_LINES_DIR
    if not os.path.exists(folder):
        return
    
    # Получаем все CSV файлы в папке
    pattern = os.path.join(folder, "K_line_*.csv")
    files = glob.glob(pattern)
    
    # Если файлов больше максимального количества, удаляем самые старые
    if len(files) > max_files:
        # Сортируем файлы по времени создания (сначала старые)
        files.sort(key=os.path.getctime)
        
        # Удаляем лишние файлы
        for i in range(len(files) - max_files):
            os.remove(files[i])
