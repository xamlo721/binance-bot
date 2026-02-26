from dataclasses import dataclass


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

