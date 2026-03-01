from dataclasses import dataclass
from typing import Optional

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

    def to_dict(self) -> dict:
        """Преобразует объект в словарь для CSV"""
        return {
            'symbol': self.symbol,
            'open': self.open,
            'close': self.close,
            'high': self.high,
            'low': self.low,
            'total_volume': self.total_volume
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'HoursRecord':
        """Создает объект из словаря (для загрузки из CSV)"""
        return cls(
            symbol=data['symbol'],
            open=float(data['open']),
            close=float(data['close']),
            high=float(data['high']),
            low=float(data['low']),
            total_volume=float(data['total_volume']),
        )
    
    def __str__(self) -> str:
        """Краткое строковое представление"""
        return (f"HoursRecord(symbol={self.symbol}, "
                f"open={self.open:.2f}, close={self.close:.2f}, ")
    
    def __repr__(self) -> str:
        """Полное строковое представление"""
        return self.__str__()
