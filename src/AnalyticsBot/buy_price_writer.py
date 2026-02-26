
from logger import logger
from config import *

from typing import List, Dict, Optional
from ramstorage.AlertRecord import AlertRecord
from ramstorage.CandleRecord import CandleRecord


def _high_price_by_ticker(klines: List[CandleRecord]) -> Dict[str, float]:
    """
    Возвращает максимальный high‑цену для каждого тикера из списка свечей.
    """
    prices: Dict[str, float] = {}
    for candle in klines:
        prices.setdefault(candle.symbol, 0.0)
        if candle.high > prices[candle.symbol]:
            prices[candle.symbol] = candle.high
    return prices


def update_bpw(alerts: List[AlertRecord], klines: List[CandleRecord]) -> List[AlertRecord]:
    """
    Для каждой записи с пустым buy_short_price устанавливает его и min/max цены
    равными максимальной high‑цене из предоставленных свечей.
    Возвращает обновленный список AlertRecord.
    """
    prices = _high_price_by_ticker(klines)

    for alert in alerts:
        if alert.buy_short_price is None:
            price: Optional[float] = prices.get(alert.ticker)
            if price is not None:
                alert.buy_short_price = price
                alert.min_price = price
                alert.max_price = price

    return alerts
