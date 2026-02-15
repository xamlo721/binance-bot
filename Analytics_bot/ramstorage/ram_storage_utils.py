
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

# Список всех отметок за 60 минут 
candle_1m_records: list[list[CandleRecord]]= [[] for _ in range(MINUTE_CANDLE_FILE_LIMIT)]

def save_to_ram(results: List[CandleRecord]):
    """Сохраняет свечи в оперативную память с скользящим окном в 180 минут"""
    if not results:
        logger.info("Нет данных для сохранения")
        return
    
    # Добавляем новые свечи в начало (самая свежая минута)
    global candle_1m_records

    
    results_open_time = results[0].open_time

    # Проверяем, существует ли уже запись за это время
    minute_exists = False
    existing_index = -1
    
    for i, minute_candles in enumerate(candle_1m_records):
        if minute_candles and minute_candles[0].open_time == results_open_time:
            minute_exists = True
            existing_index = i
            break
    
    if minute_exists:
        logger.debug(f"Минута {results_open_time} уже существует на позиции {existing_index}, пропускаем")
        return
    
    results_sorted = sorted(results, key=lambda x: x.open_time)

    # Находим правильную позицию для вставки на основе времени
    insert_position = -1

    for i in range(len(candle_1m_records)):
        if not candle_1m_records[i]:  # Пустой период
            if insert_position == -1:
                insert_position = i
            continue
        
        # Получаем время свечей в текущем периоде
        period_time = candle_1m_records[i][0].open_time
        
        if results_open_time > period_time:
            # Новые свечи новее текущего периода - вставляем перед ним
            insert_position = i
            break

        # Если не нашли позицию для вставки, добавляем в конец
    if insert_position == -1:
        # Ищем последний непустой индекс
        last_non_empty = -1
        for i in range(len(candle_1m_records)):
            if candle_1m_records[i]:
                last_non_empty = i
        
        if last_non_empty < len(candle_1m_records) - 1:
            insert_position = last_non_empty + 1
        else:
            # Все позиции заняты, нужно сдвинуть и вставить в начало
            # Сдвигаем все списки на одну позицию вправо
            for i in range(len(candle_1m_records)-1, 0, -1):
                candle_1m_records[i] = candle_1m_records[i-1]
            insert_position = 0
    
    # Вставляем на найденную позицию со сдвигом вправо
    if insert_position < len(candle_1m_records):
        # Сдвигаем элементы справа от insert_position
        for i in range(len(candle_1m_records)-1, insert_position, -1):
            candle_1m_records[i] = candle_1m_records[i-1]
        
        # Вставляем новые свечи
        candle_1m_records[insert_position] = results_sorted

    # Логируем результат
    logger.debug(f"Добавлено {len(results)} свечей на позицию {insert_position}")
    
    # Проверяем, сколько непустых периодов у нас есть
    non_empty_periods = sum(1 for period in candle_1m_records if period)
    logger.debug(f"Непустых периодов: {non_empty_periods}")
    
    # Логируем временной диапазон для отладки
    if non_empty_periods > 0:
        times = [period[0].open_time for period in candle_1m_records if period]
        logger.debug(f"Временной диапазон: от {min(times)} до {max(times)}")

