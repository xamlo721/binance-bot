from dataclasses import dataclass

from typing import Optional

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
