
from dataclasses import dataclass
from datetime import datetime
from typing import List
from logger import logger
from config import *

@dataclass
class CandleRecord:
    # Тикер
    symbol: str 
    # Open price - цена открытия
    open: float
    # Close price - цена закрытия
    close: float
    # High price - максимальная цена за период
    high: float
    # Low price - минимальная цена за период
    low: float
    # Volume - объем базового актива
    volume: float                      
    # Quote asset volume - объем в котировочной валюте
    quote_volume: float
    # Taker buy base asset volume - объем покупок базового актива
    taker_buy_base_volume: float
    # Taker buy quote asset volume - объем покупок котировочного актива
    taker_buy_quote_volume: float
    # Number of trades - количество сделок
    trades: int
    # Open time - время открытия свечи
    open_time: int 

    def to_dict(self) -> dict:
        """Преобразует объект в словарь для CSV"""
        return {
            'symbol': self.symbol,
            'open': self.open,
            'close': self.close,
            'high': self.high,
            'low': self.low,
            'quote_volume': self.quote_volume,
            'taker_buy_base_volume': self.taker_buy_base_volume,
            'taker_buy_quote_volume': self.taker_buy_quote_volume,
            'trades': self.trades,
            'open_time': self.open_time
        }

# Список всех отметок за 180 минут 
candle_records: list[list[CandleRecord]]= [[] for _ in range(k_line_CLEAN_OLD_FILES)]

def save_to_ram(results: List[CandleRecord]):
    """Сохраняет свечи в оперативную память с скользящим окном в 180 минут"""
    if not results:
        logger.info("Нет данных для сохранения")
        return
    
    # Добавляем новые свечи в начало (самая свежая минута)
    global candle_records
    
    # Сдвигаем все списки на одну позицию вправо (старое → новое)
    # Последний (k_line_CLEAN_OLD_FILES-1-й) список будет удален
    for i in range(k_line_CLEAN_OLD_FILES-1, 0, -1):
        candle_records[i] = candle_records[i-1]
    
    # На первую позицию (индекс 0) помещаем новые свечи
    candle_records[0] = results
    
    # Логируем результат
    logger.debug(f"Добавлено {len(results)} свечей. Всего периодов: {len(candle_records)}")
    
    # Проверяем, сколько непустых периодов у нас есть
    non_empty_periods = sum(1 for period in candle_records if period)
    logger.debug(f"Непустых периодов: {non_empty_periods}")