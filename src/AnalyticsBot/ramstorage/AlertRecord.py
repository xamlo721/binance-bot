from dataclasses import dataclass
from dataclasses import field

from typing import Optional
import pandas as pd
from datetime import datetime

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
    
    # Риск-профит для шорта (RPS - Risk Profit Short)
    rps_sl_1_percent: Optional[float] = None    # RPS со стопом 1%
    rps_sl_2_percent: Optional[float] = None    # RPS со стопом 2%
    rps_sl_3_percent: Optional[float] = None    # RPS со стопом 3%
    rps_sl_4_percent: Optional[float] = None    # RPS со стопом 4%
    rps_sl_5_percent: Optional[float] = None    # RPS со стопом 5%
    max_loss_percent: Optional[float] = None    # Максимальный убыток в %
    
    # Риск-профит для покупки (RPB - Risk Profit Buy)
    rpb_sl_1_percent: Optional[float] = None    # RPB со стопом 1%
    rpb_sl_2_percent: Optional[float] = None    # RPB со стопом 2%
    rpb_sl_3_percent: Optional[float] = None    # RPB со стопом 3%
    rpb_sl_4_percent: Optional[float] = None    # RPB со стопом 4%
    rpb_sl_5_percent: Optional[float] = None    # RPB со стопом 5%
    max_profit_percent: Optional[float] = None  # Максимальная прибыль в %
    

    def to_dict(self) -> dict:
        """Преобразует объект в словарь для CSV"""
        return {
            'ticker': self.ticker,
            'volume': self.volume,
            'time': self.time,
            'buy_short_price': self.buy_short_price if self.buy_short_price is not None else pd.NA,
            'min_price': self.min_price if self.min_price is not None else pd.NA,
            'min_price_time': self.min_price_time if self.min_price_time is not None else pd.NA,
            'max_price': self.max_price if self.max_price is not None else pd.NA,
            'max_price_time': self.max_price_time if self.max_price_time is not None else pd.NA,
            'RPS_(30%)_SL_1%': self.rps_sl_1_percent if self.rps_sl_1_percent is not None else pd.NA,
            'RPS_(30%)_SL_2%': self.rps_sl_2_percent if self.rps_sl_2_percent is not None else pd.NA,
            'RPS_(30%)_SL_3%': self.rps_sl_3_percent if self.rps_sl_3_percent is not None else pd.NA,
            'RPS_(30%)_SL_4%': self.rps_sl_4_percent if self.rps_sl_4_percent is not None else pd.NA,
            'RPS_(30%)_SL_5%': self.rps_sl_5_percent if self.rps_sl_5_percent is not None else pd.NA,
            'max_loss_%': self.max_loss_percent if self.max_loss_percent is not None else pd.NA,
            'RPB_(30%)_SL_1%': self.rpb_sl_1_percent if self.rpb_sl_1_percent is not None else pd.NA,
            'RPB_(30%)_SL_2%': self.rpb_sl_2_percent if self.rpb_sl_2_percent is not None else pd.NA,
            'RPB_(30%)_SL_3%': self.rpb_sl_3_percent if self.rpb_sl_3_percent is not None else pd.NA,
            'RPB_(30%)_SL_4%': self.rpb_sl_4_percent if self.rpb_sl_4_percent is not None else pd.NA,
            'RPB_(30%)_SL_5%': self.rpb_sl_5_percent if self.rpb_sl_5_percent is not None else pd.NA,
            'max_proffit_%': self.max_profit_percent if self.max_profit_percent is not None else pd.NA
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AlertRecord':
        """Создает объект из словаря (для загрузки из CSV)"""
        # Функция для конвертации pd.NA в None
        def clean_value(val):
            if pd.isna(val) or val is pd.NA:
                return None
            return val
        
        return cls(
            ticker=data['ticker'],
            volume=data['volume'],
            time=int(data['time']),
            buy_short_price=clean_value(data.get('buy_short_price')),
            min_price=clean_value(data.get('min_price')),
            min_price_time=clean_value(data.get('min_price_time')),
            max_price=clean_value(data.get('max_price')),
            max_price_time=clean_value(data.get('max_price_time')),
            rps_sl_1_percent=clean_value(data.get('RPS_(30%)_SL_1%')),
            rps_sl_2_percent=clean_value(data.get('RPS_(30%)_SL_2%')),
            rps_sl_3_percent=clean_value(data.get('RPS_(30%)_SL_3%')),
            rps_sl_4_percent=clean_value(data.get('RPS_(30%)_SL_4%')),
            rps_sl_5_percent=clean_value(data.get('RPS_(30%)_SL_5%')),
            max_loss_percent=clean_value(data.get('max_loss_%')),
            rpb_sl_1_percent=clean_value(data.get('RPB_(30%)_SL_1%')),
            rpb_sl_2_percent=clean_value(data.get('RPB_(30%)_SL_2%')),
            rpb_sl_3_percent=clean_value(data.get('RPB_(30%)_SL_3%')),
            rpb_sl_4_percent=clean_value(data.get('RPB_(30%)_SL_4%')),
            rpb_sl_5_percent=clean_value(data.get('RPB_(30%)_SL_5%')),
            max_profit_percent=clean_value(data.get('max_proffit_%'))
        )
    
    @classmethod
    def create_from_alert(cls, ticker: str, reason: str, current_time: int) -> 'AlertRecord':
        """Создает запись алерта с базовыми полями (остальные будут None/pd.NA)"""
        return cls(
            ticker=ticker,
            volume=reason,
            time=current_time
        )
    
    def update_prices(self, min_p: float, min_time: int, max_p: float, max_time: int) -> None:
        """Обновляет ценовые экстремумы"""
        self.min_price = min_p
        self.min_price_time = min_time
        self.max_price = max_p
        self.max_price_time = max_time
    
    def update_rps_values(self, sl_1: float, sl_2: float, sl_3: float, sl_4: float, sl_5: float, max_loss: float) -> None:
        """Обновляет RPS значения для шорта"""
        self.rps_sl_1_percent = sl_1
        self.rps_sl_2_percent = sl_2
        self.rps_sl_3_percent = sl_3
        self.rps_sl_4_percent = sl_4
        self.rps_sl_5_percent = sl_5
        self.max_loss_percent = max_loss
    
    def update_rpb_values(self, sl_1: float, sl_2: float, sl_3: float, sl_4: float, sl_5: float, max_profit: float) -> None:
        """Обновляет RPB значения для покупки"""
        self.rpb_sl_1_percent = sl_1
        self.rpb_sl_2_percent = sl_2
        self.rpb_sl_3_percent = sl_3
        self.rpb_sl_4_percent = sl_4
        self.rpb_sl_5_percent = sl_5
        self.max_profit_percent = max_profit
    
    def __str__(self) -> str:
        """Краткое строковое представление"""
        return (f"AlertRecord(ticker={self.ticker}, "
                f"time={self.time}, "
                f"reason={self.volume[:30]}...)" if len(self.volume) > 30 else f"reason={self.volume})")
    
    def __repr__(self) -> str:
        """Полное строковое представление"""
        return self.__str__()
