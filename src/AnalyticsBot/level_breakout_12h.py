
from ramstorage.CandleRecord import CandleRecord

from typing import List
from typing import Dict
from typing import Optional


from logger import logger
from config import *

def process_level_breakout_12h(k_line_records: List[CandleRecord],aggregated_highs: Dict[str, float]) -> Optional[Dict[str, float]]:
    """
    Сравнивает цены закрытия минутных свечей с максимальными значениями high из агрегированных данных
    
    Args:
        k_line_records: Список минутных свечей (CandleRecord)
        aggregated_highs: Словарь {symbol: max_high} с максимальными значениями high из агрегации
    
    Returns:
        Optional[Dict[str, float]]: Словарь {symbol: difference} для тикеров, где close > high,
                                   отсортированный по убыванию разницы, или None при ошибке
    """
    try:
        # Группируем последние значения close по символам
        latest_closes: Dict[str, float] = {}
        
        for candle in k_line_records:
            symbol = candle.symbol
            close = candle.close
            
            # Берем самую свежую цену закрытия для каждого символа
            if symbol not in latest_closes:
                latest_closes[symbol] = close
        
        # Находим тикеры где close > high
        tickers_up: Dict[str, float] = {}
        
        for symbol, close in latest_closes.items():
            if symbol in aggregated_highs:
                high = aggregated_highs[symbol]
                if close > high:
                    difference = close - high
                    tickers_up[symbol] = difference
        
        if tickers_up:
            # Сортируем по разнице в убывающем порядке
            sorted_tickers = dict(sorted(tickers_up.items(), key=lambda x: x[1], reverse=True))
            return sorted_tickers
        else:
            logger.info("Тикеров с превышением high не найдено")
            return {}
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        return None
