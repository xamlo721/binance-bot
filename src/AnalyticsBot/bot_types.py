from dataclasses import dataclass
from dataclasses import field

from typing import Optional
import pandas as pd
from datetime import datetime

@dataclass
class KlineRecord:
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
    # Close time - время закрытия свечи
    close_time: int                       
    # Quote asset volume - объем в котировочной валюте
    quote_assets_volume: float
    # Taker buy base asset volume - объем покупок базового актива
    taker_buy_base_volume: float
    # Taker buy quote asset volume - объем покупок котировочного актива
    taker_buy_quote_volume: float
    # Number of trades - количество сделок
    num_of_trades: int
    # Open time - время открытия свечи
    open_time: int

@dataclass
class Volume_10m:
    # Тикер
    ticker:str
    # Объёмы 10 минутных свечей
    volume: float
    # Время открытия перовой свечи
    open_time: int
    # Время открытия 10й свечи
    close_time:int

@dataclass
class HoursRecord:
    """Запись с часовыми статистическими показателями по тикеру"""
    
    # Тикер
    symbol: str
    
    # Цены
    open: float                      # Цена открытия часа
    close: float                      # Цена закрытия часа
    high: float                        # Максимальная цена за час
    low: float                          # Минимальная цена за час

    # Объемы
    total_volume: float               # Общий объем базового актива за час

@dataclass
class AlertRecord:
    """Запись для хранения данных алерта по тикеру"""
    
    # Основные поля
    ticker: str                          # Тикер
    volume: str                           # Причина/объем (текстовое описание)
    time: int                              # Время события (timestamp в миллисекундах)
    
    # Цены
    buy_short_price: Optional[float] = None     # Цена покупки/шорта
    min_price: Optional[float] = None           # Минимальная цена
    min_price_time: Optional[int] = None        # Время минимальной цены
    max_price: Optional[float] = None           # Максимальная цена
    max_price_time: Optional[int] = None        # Время максимальной цены
