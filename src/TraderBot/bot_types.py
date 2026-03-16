from dataclasses import dataclass

from typing import Optional
from enum import IntEnum

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