from dataclasses import dataclass
from dataclasses import field

from typing import Optional
import pandas as pd
from datetime import datetime
from enum import IntEnum

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
    time: int                              # Время события (timestamp в миллисекундах)

class ResponseStatus(IntEnum):
    OK = 0
    NOT_FOUND = 1
    BUSY = 2

@dataclass
class UDPRequest:
    """Структура запроса к UDP серверу"""
    packet_number: int      # номер пакета (4 байта)
    minute_number: int      # номер минуты (4 байта)

@dataclass 
class UDPResponse:
    """Структура ответа от UDP сервера"""
    packet_number: int               # номер пакета (4 байта)
    minute_number: int               # номер минуты (4 байта)
    status: int               # код статуса (0=успех, 1=минута не найдена, 2=сервер занят)
    records: list[KlineRecord]       # список записей


class AlertMessageType(IntEnum):
    REGISTER = 1      # клиент -> сервер: запрос на подписку
    UNREGISTER = 2    # клиент -> сервер: отписка
    ALERT = 3         # сервер -> клиент: данные алерта

@dataclass
class AlertRegister:
    """Сообщение для подписки на алерты"""
    packet_number: int

@dataclass
class AlertUnregister:
    """Сообщение для отписки от алертов"""
    packet_number: int

@dataclass
class AlertData:
    """Сообщение, содержащее сам алерт"""
    packet_number: int
    alert: AlertRecord