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
    
    # Стандартные отклонения цен
    high_std: float                   # Стандартное отклонение максимумов за период
    low_std: float                     # Стандартное отклонение минимумов за период
    
    # Объемы
    total_volume: float               # Общий объем базового актива за час
    quote_volume_1m_avg: float        # Средний минутный объем в котировочной валюте
    quote_volume_std: float           # Стандартное отклонение минутных объемов
    
    # Taker объемы
    taker_buy_base_volume_1m_avg: float   # Средний минутный объем покупок базового актива
    taker_buy_base_volume_std: float      # Стандартное отклонение минутных объемов покупок базового актива
    taker_buy_quote_volume_1m_avg: float  # Средний минутный объем покупок в котировочной валюте
    taker_buy_quote_volume_std: float     # Стандартное отклонение минутных объемов покупок в котировочной валюте
    
    # Сделки
    trades_1m_avg: float              # Среднее количество сделок в минуту
    trades_std: float                 # Стандартное отклонение количества сделок
    
    # Волатильность
    volatility_1m_avg: float          # Средняя минутная волатильность
    volatility_std: float             # Стандартное отклонение минутной волатильности
    
    # Время
    hour_start_time: int              # Время начала часа (timestamp в миллисекундах)
    
    # Опциональные поля для дополнительной информации
    hour_end_time: Optional[int] = None      # Время окончания часа (timestamp в миллисекундах)
    symbols_count: Optional[int] = None      # Количество тикеров, использованных для расчета (для агрегированных данных)

    def to_dict(self) -> dict:
        """Преобразует объект в словарь для CSV"""
        return {
            'symbol': self.symbol,
            'open': self.open,
            'close': self.close,
            'high': self.high,
            'low': self.low,
            'high_std': self.high_std,
            'low_std': self.low_std,
            'total_volume': self.total_volume,
            'quote_volume_1m_avg': self.quote_volume_1m_avg,
            'quote_volume_std': self.quote_volume_std,
            'taker_buy_base_volume_1m_avg': self.taker_buy_base_volume_1m_avg,
            'taker_buy_base_volume_std': self.taker_buy_base_volume_std,
            'taker_buy_quote_volume_1m_avg': self.taker_buy_quote_volume_1m_avg,
            'taker_buy_quote_volume_std': self.taker_buy_quote_volume_std,
            'trades_1m_avg': self.trades_1m_avg,
            'trades_std': self.trades_std,
            'volatility_1m_avg': self.volatility_1m_avg,
            'volatility_std': self.volatility_std,
            'hour_start_time': self.hour_start_time,
            'hour_end_time': self.hour_end_time if self.hour_end_time else '',
            'symbols_count': self.symbols_count if self.symbols_count else ''
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
            high_std=float(data['high_std']),
            low_std=float(data['low_std']),
            total_volume=float(data['total_volume']),
            quote_volume_1m_avg=float(data['quote_volume_1m_avg']),
            quote_volume_std=float(data['quote_volume_std']),
            taker_buy_base_volume_1m_avg=float(data['taker_buy_base_volume_1m_avg']),
            taker_buy_base_volume_std=float(data['taker_buy_base_volume_std']),
            taker_buy_quote_volume_1m_avg=float(data['taker_buy_quote_volume_1m_avg']),
            taker_buy_quote_volume_std=float(data['taker_buy_quote_volume_std']),
            trades_1m_avg=float(data['trades_1m_avg']),
            trades_std=float(data['trades_std']),
            volatility_1m_avg=float(data['volatility_1m_avg']),
            volatility_std=float(data['volatility_std']),
            hour_start_time=int(data['hour_start_time']),
            hour_end_time=int(data['hour_end_time']) if data.get('hour_end_time') else None,
            symbols_count=int(data['symbols_count']) if data.get('symbols_count') else None
        )
    
    def __str__(self) -> str:
        """Краткое строковое представление"""
        return (f"HoursRecord(symbol={self.symbol}, "
                f"hour={self.hour_start_time}, "
                f"open={self.open:.2f}, close={self.close:.2f}, "
                f"avg_volume={self.quote_volume_1m_avg:.2f})")
    
    def __repr__(self) -> str:
        """Полное строковое представление"""
        return self.__str__()
