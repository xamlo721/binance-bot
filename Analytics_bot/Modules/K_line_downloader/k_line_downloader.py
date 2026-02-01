import aiohttp
import asyncio
import requests
import csv
import time
import os
import glob
from datetime import datetime

SCRIPT_NAME = "KLD_1M      :  "                       # Имя скрипта для вывода в консоль
K_LINES_DIR = "/srv/ftp/Bot_v2/Data/K_lines/1M"     # Папка с минутными свечами
CLEAN_OLD_FILES = 180                               # Max файлов в папке


def get_trading_symbols():
    """Получение списка торгующихся тикеров"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url)
        data = response.json()
        
        symbols = []
        for symbol_info in data['symbols']:
            if (symbol_info['status'] == 'TRADING' and symbol_info.get('contractType') == 'PERPETUAL'):
                # Исключаем тикеры с "USDC"
                symbol_name = symbol_info['symbol']
                if not symbol_name.startswith("USDC") and not symbol_name.endswith("USDC"):
                    symbols.append(symbol_name)
                #symbols.append(symbol_info['symbol'])
        
        return symbols
    except Exception as e:
        print(SCRIPT_NAME + f"Ошибка при получении списка тикеров: {str(e)}")
        return []

async def fetch_volume(session, symbol, semaphore):
    """Асинхронное получение данных для одного тикера"""
    async with semaphore:
        # Вычисляем время окончания последней закрытой свечи
        current_time = int(time.time() * 1000)
        end_time = current_time - (current_time % 60000) - 1000  # -1 секунда для гарантии
        
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'limit': 1,
            'endTime': end_time
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and len(data) > 0:
                        kline = data[0]
                        # Проверяем, что свеча закрыта (время закрытия меньше текущего времени)
                        close_time = kline[6]
                        if close_time < current_time:
                            return {
                                'symbol': symbol,
                                'open': float(kline[1]),                        # Open price - цена открытия
                                'high': float(kline[2]),                        # High price - максимальная цена за период
                                'low': float(kline[3]),                         # Low price - минимальная цена за период
                                'close': float(kline[4]),                       # Close price - цена закрытия
                                'volume': float(kline[5]),                      # Volume - объем базового актива
                                'quote_volume': float(kline[7]),                # Quote asset volume - объем в котировочной валюте
                                'taker_buy_base_volume': float(kline[9]),       # Taker buy base asset volume
                                'taker_buy_quote_volume': float(kline[10]),     # Taker buy quote asset volume
                                'trades': kline[8],                             # Number of trades - количество сделок
                                'open_time': kline[0]                           # Open time - время открытия свечи
                            }
                else:
                    print(SCRIPT_NAME + f"Ошибка HTTP {response.status} для {symbol}")
        except asyncio.TimeoutError:
            print(SCRIPT_NAME + f"Таймаут для {symbol}")
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка для {symbol}: {str(e)}")
        
        return None

async def fetch_all_volumes(symbols, max_concurrent=100):
    """Асинхронное получение объемов для всех тикеров"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for symbol in symbols:
            task = asyncio.create_task(fetch_volume(session, symbol, semaphore))
            tasks.append(task)
        
        results = []
        completed = 0
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                results.append(result)
            completed += 1
            if completed % 1000 == 0:
                print(SCRIPT_NAME + f"Обработано {completed}/{len(symbols)} тикеров")
        
        return results

def save_to_csv(results):
    """Сохранение результатов в CSV с именем на основе времени открытия свечи"""
    if not results:
        print(SCRIPT_NAME + "Нет данных для сохранения")
        return None
    
    # Получаем время открытия свечи из первого результата
    open_time = results[0]['open_time']
    open_time_dt = datetime.fromtimestamp(open_time / 1000)
    filename = f"K_line_{open_time_dt.strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Создаем папку K_lines, если её нет
    folder = K_LINES_DIR
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    filepath = os.path.join(folder, filename)
    filename = os.path.join(filename)
    
    with open(filepath, 'w', newline='') as csvfile:
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
        for data in results:
            # Сохраняем только нужные поля
            row_data = {field: data[field] for field in fieldnames}
            writer.writerow(row_data)
    
    #return filepath
    return filename

def cleanup_old_files(max_files=20):
    """Удаление старых файлов, оставляя только max_files самых новых"""
    folder = K_LINES_DIR
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

async def main():
    start_time = time.time()
    symbols = get_trading_symbols()
    if not symbols:
        print(SCRIPT_NAME + "Не удалось получить список тикеров")
        return
        
    print(SCRIPT_NAME + f"Найдено торгующихся тикеров: {len(symbols)}")
    
    results = await fetch_all_volumes(symbols, max_concurrent=200)
    
    if not results:
        print(SCRIPT_NAME + "Не удалось получить данные по тикерам")
        return
    
    filepath = save_to_csv(results)
    
    if filepath:
        # Max Количество файлов
        cleanup_old_files(CLEAN_OLD_FILES)
        end_time = time.time()
        print(SCRIPT_NAME + f"Готово! Обработано {len(results)} тикеров за {end_time - start_time:.2f} секунд")
        print(SCRIPT_NAME + f"Данные сохранены в файл: {filepath}")

if __name__ == "__main__":
    asyncio.run(main())