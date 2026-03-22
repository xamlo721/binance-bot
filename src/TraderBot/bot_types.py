from dataclasses import dataclass

from typing import Optional
from enum import IntEnum

@dataclass
class AlertRecord:
    """Запись для хранения данных алерта по тикеру"""
    
    # Основные поля
    ticker: str                          # Тикер
    time: int                              # Время события (timestamp в миллисекундах)


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