from dataclasses import dataclass

from datetime import datetime
from datetime import timezone
from typing import List
from logger import logger
from config import *

from AnalyticsBot.ramstorage.CandleRecord import CandleRecord 
from AnalyticsBot.ramstorage.HoursRecord import HoursRecord 
from AnalyticsBot.ramstorage.AlertRecord import AlertRecord 

@dataclass
class Volume_10m:
    # Тикер
    ticker:str
    # Объёмы 10 минутных свечей
    volume: float
    # Время открытия перовой свечи
    open_time: int
    # Время открытия 10й свечи
    close_time:int

# Список всех отметок за MINUTE_CANDLES_LIMIT минут 
#                   <НОМЕР_МИНУТЫ List<МИНУТНАЯ_ЗАПИСЬ>>
candle_1m_records: dict[int, list[CandleRecord]] = {}
candle_1h_records: list[list[HoursRecord]] = []
dynamic_1h_records: list[HoursRecord] = []

alerts_records: list[list[AlertRecord]] = [[] for _ in range(MINUTE_CANDLES_LIMIT)]
alerts_calc_records: list[list[AlertRecord]] = [[] for _ in range(MINUTE_CANDLES_LIMIT)]
volume_10m_sliding_window: List[Volume_10m] = []

def is_storage_consistent(candle_dict: dict[int, List[CandleRecord]]) -> bool:
    """
    Проверяет корректность словаря минутных свечей.

    Условия:
      1. Для каждого CandleRecord поле open_time должно совпадать с ключом (номером минуты),
         под которым запись хранится.
      2. Ключи словаря (номера минут) должны образовывать непрерывную
         возрастающую последовательность без пропусков и повторов.

    Args:
        candle_dict: словарь вида {номер_минуты: список[CandleRecord]}

    Returns:
        True, если обе проверки пройдены успешно, иначе False.
    """
    # Пустой словарь считаем корректным (или можно изменить логику при необходимости)
    if not candle_dict:
        return True

    # 1. Проверка непрерывности ключей
    keys = sorted(candle_dict.keys())
    for i in range(1, len(keys)):
        if keys[i] != keys[i-1] + 60000:
            print(f"Ошибка: разрыв в последовательности минут между {keys[i-1]} и {keys[i]}")
            return False

    # 2. Проверка совпадения open_time с ключом
    for minute, records in candle_dict.items():
        for record in records:
            if record.open_time != minute:
                print(f"Ошибка: запись {record} имеет open_time={record.open_time}, "
                      f"не совпадающий с ключом {minute}")
                return False

    return True

def get_1m_candles() -> dict[int, list[CandleRecord]]:
    return candle_1m_records

def save_10m_volumes(volumes: List[Volume_10m]) -> bool:
    global volume_10m_sliding_window
    volume_10m_sliding_window = volumes
    return True

def save_1h_dynamics(dynamic_records: list[HoursRecord]):
    global dynamic_1h_records
    dynamic_1h_records = dynamic_records
 
def save_klines_to_ram(results: List[CandleRecord]):
    """
    Сохраняет свечи в оперативную память с скользящим окном.
    Ключ словаря – начало минуты (timestamp в ms).
    При добавлении проверяется:
        • дублирование минут,
        • сохранение только последних MINUTE_CANDLES_LIMIT минут,
        • непрерывность диапазона минут.
    """
    if not results:
        logger.info("Нет данных для сохранения")
        return

    global candle_1m_records

    # Определяем начало минуты (floor to 60000 мс)
    minute_start = (results[0].open_time // 60000) * 60000

    # Проверка дублирования
    if minute_start in candle_1m_records:
        logger.debug(f"Минута {minute_start} уже существует, пропускаем")
        return

    # Сортируем свечи по времени открытия
    sorted_candles = sorted(results, key=lambda x: x.open_time)

    # Добавляем в словарь
    candle_1m_records[minute_start] = sorted_candles

    # Удаляем старые записи – сохраняем только последние MINUTE_CANDLES_LIMIT минут
    if len(candle_1m_records) > MINUTE_CANDLES_LIMIT:
        keys_sorted = sorted(candle_1m_records.keys())
        for key in keys_sorted[:-MINUTE_CANDLES_LIMIT]:
            del candle_1m_records[key]

    # Проверяем непрерывность диапазона минут
    if len(candle_1m_records) > 1:
        keys_sorted = sorted(candle_1m_records.keys())
        for i in range(len(keys_sorted)-1):
            diff_ms = keys_sorted[i+1] - keys_sorted[i]
            if diff_ms != 60000:
                logger.warning(
                    f"Разрыв между минутами {keys_sorted[i]} и "
                    f"{keys_sorted[i+1]}: Δ={diff_ms}мс"
                )

    # Логируем результат
    logger.debug(f"Добавлено {len(sorted_candles)} свечей на ключ {minute_start}")

    # Дополнительные сведения о текущем состоянии памяти
    non_empty_periods = len(candle_1m_records)
    logger.debug(f"Непустых периодов: {non_empty_periods}")
    if non_empty_periods > 0:
        times = list(candle_1m_records.keys())
        logger.debug(
            f"Временной диапазон: от {min(times)} до {max(times)}"
        )


def save_alert_to_memory(ticker, reason):
    """Сохранить алерт в файл с текущей датой по UTC"""
    global alerts_records
    
    try:
        # Получаем текущее время в миллисекундах для open_time
        current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        alert_record = AlertRecord.create_from_alert(
            ticker=ticker,
            reason=reason,
            current_time=current_time_ms
        )

        # Проверяем, есть ли уже такой тикер в последних записях
        minute_exists = False
        for period in alerts_records:
            if period and any(alert.ticker == ticker for alert in period):
                minute_exists = True
                logger.warning(f"Тикер {ticker} уже присутствует в последних записях")
                return False
            
            # Определяем позицию для вставки (индекс 0 - самая новая минута)
            # Проверяем, есть ли уже записи за текущую минуту
            insert_position = 0
            
            # Если в alerts_records[0] уже есть записи, создаем новую минуту
            if alerts_records[0] and alerts_records[0][0].time == current_time_ms:
                # Добавляем в существующую минуту
                alerts_records[0].append(alert_record)
                logger.debug(f"Алерт добавлен в существующую минуту на позиции 0")
            else:
                # Создаем новую минуту со сдвигом
                # Сдвигаем все списки на одну позицию вправо
                for i in range(len(alerts_records)-1, 0, -1):
                    alerts_records[i] = alerts_records[i-1]
                
                # На первую позицию помещаем новый список с алертом
                alerts_records[0] = [alert_record]
                logger.debug(f"Алерт добавлен в новую минуту на позиции 0")
            
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении алерта в файл: {e}")
        return False

def save_calc_alert_to_ram(rows: List[AlertRecord]):
    """Записать строки в файл alerts_calc"""
    global alerts_calc_records
    try:
        alerts_calc_records.append(rows)
        logger.info(f"Записано {len(rows)} строк в alerts_calc_records")

    except Exception as e:
        logger.error(f"Ошибка при записи в alerts_calc_records: {e}")

def get_recent_1m_klines(count: int = 60) -> dict[int, list[CandleRecord]]:
    """
    Возвращает словарь с минутными свечами за последние N минут.
    Ключи словаря – номера минут (временные метки), значения – списки CandleRecord.
    Если записей меньше, чем запрошено, возвращаются все имеющиеся.
    При пустом словаре или некорректном count возвращается пустой словарь.
    """
    global candle_1m_records

    # Защита от пустого словаря или неположительного count
    if not candle_1m_records or count <= 0:
        return {}

    # Сортируем ключи (минуты) по возрастанию
    sorted_minutes = sorted(candle_1m_records.keys())

    # Если запрошено больше, чем есть, берём все
    if count > len(sorted_minutes):
        count = len(sorted_minutes)

    # Берём последние count ключей
    recent_keys = sorted_minutes[-count:]

    # Формируем результирующий словарь
    return {key: candle_1m_records[key] for key in recent_keys}

def get_recent_1h_klines(count: int = 60) -> list[list[HoursRecord]]:
    """Получить свечи за последние N минут"""
    global candle_1h_records
    
    if count > len(candle_1h_records):
        count = len(candle_1h_records)
    
    # Создаем список для результатов
    recent_klines: list[list[HoursRecord]] = []
    
    for i in range(count):
        if candle_1h_records[i]:
            recent_klines.append(candle_1h_records[i])
    
    return recent_klines

def get_recent_alerts(minutes: int = 60) -> list[AlertRecord]:
    """Получить алерты за последние N минут"""
    global alerts_records
    
    if minutes > len(alerts_records):
        minutes = len(alerts_records)
    
    recent_alerts = []
    for i in range(minutes):
        if alerts_records[i]:
            recent_alerts.extend(alerts_records[i])
    
    return recent_alerts

def check_ticker_alert_exists(ticker: str, minutes_back: int = 60) -> bool:
    """Проверить, был ли алерт по тикеру за последние N минут"""
    recent_alerts = get_recent_alerts(minutes_back)
    return any(alert.ticker == ticker for alert in recent_alerts)

def clean_old_alerts_from_memory(hours: int = 24):
    """Очистить алерты старше N часов из памяти"""
    global alerts_records
    
    current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    cutoff_time = current_time_ms - (hours * 60 * 60 * 1000)
    
    for i, period in enumerate(alerts_records):
        if period:
            # Оставляем только алерты новее cutoff_time
            alerts_records[i] = [alert for alert in period if alert.time >= cutoff_time]
    
    logger.info(f"Очищены алерты старше {hours} часов")

