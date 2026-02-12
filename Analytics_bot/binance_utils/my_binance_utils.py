import requests
import time
import aiohttp
import asyncio

from logger import logger

from ramstorage.ram_storage_utils import CandleRecord

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
        logger.error(f"Ошибка при получении списка тикеров: {str(e)}")
        return []
    

async def fetch_ticker_1m_volumes(session, symbol, semaphore, candleDepth: int = 1) -> CandleRecord | None:
    """Асинхронное получение данных для одного тикера"""
    async with semaphore:
        # Вычисляем время окончания последней закрытой свечи
        current_time = int(time.time() * 1000)
        end_time = current_time - (current_time % 60000) - 1000  # -1 секунда для гарантии
        
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1m',
            'limit': candleDepth,
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
                            return CandleRecord (
                                symbol=symbol,
                                open=float(kline[1]),                           # Open price - цена открытия
                                high =  float(kline[2]),                        # High price - максимальная цена за период
                                low =  float(kline[3]),                         # Low price - минимальная цена за период
                                close =  float(kline[4]),                       # Close price - цена закрытия
                                volume =  float(kline[5]),                      # Volume - объем базового актива
                                quote_volume = float(kline[7]),                 # Quote asset volume - объем в котировочной валюте
                                taker_buy_base_volume = float(kline[9]),        # Taker buy base asset volume
                                taker_buy_quote_volume = float(kline[10]),      # Taker buy quote asset volume
                                trades =  kline[8],                             # Number of trades - количество сделок
                                open_time =  kline[0]                           # Open time - время открытия свечи
                            )
                else:
                    logger.error(f"Ошибка HTTP {response.status} для {symbol}")
                    
        except asyncio.TimeoutError:
            logger.error(f"Таймаут для {symbol}")

        except Exception as e:
            logger.error(f"Ошибка для {symbol}: {str(e)}")
        
        return None

async def fetch_all_tickers_volumes(symbols, max_concurrent=100):
    """Асинхронное получение объемов для всех тикеров"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for symbol in symbols:
            task = asyncio.create_task(fetch_ticker_1m_volumes(session, symbol, semaphore))
            tasks.append(task)
        
        results = []
        completed = 0
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                results.append(result)
            completed += 1
            if completed % 1000 == 0:
                logger.info(f"Обработано {completed}/{len(symbols)} тикеров")
        
        return results
