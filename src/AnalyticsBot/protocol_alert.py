
from dataclasses import dataclass
from enum import IntEnum

from bot_types import AlertRecord

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