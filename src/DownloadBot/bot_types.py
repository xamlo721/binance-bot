from dataclasses import dataclass

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
