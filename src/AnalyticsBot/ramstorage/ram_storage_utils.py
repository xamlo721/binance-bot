
from datetime import datetime
from datetime import timezone
from typing import List
from logger import logger
from config import *

from AnalyticsBot.ramstorage.CandleRecord import CandleRecord 
from AnalyticsBot.ramstorage.HoursRecord import HoursRecord 
from AnalyticsBot.ramstorage.AlertRecord import AlertRecord 

# Список всех отметок за 60 минут 
candle_1m_records: list[list[CandleRecord]]= [[] for _ in range(MINUTE_CANDLES_LIMIT)]
candle_1h_records: list[list[HoursRecord]]= []
dynamic_1h_records: list[HoursRecord]= []

alerts_records: list[list[AlertRecord]]= [[] for _ in range(MINUTE_CANDLES_LIMIT)]
alerts_calc_records: list[list[AlertRecord]]= [[] for _ in range(MINUTE_CANDLES_LIMIT)]
volume_10m_sliding_window: dict[str, float] = {} 


# =====================================================================================
# =====================================================================================
# =====================================================================================

def save_10m_volumes(volumes: dict[str, float]):
    global volume_10m_sliding_window
    volume_10m_sliding_window = volumes

def save_1h_dynamics(dynamic_records: list[HoursRecord]):
    global dynamic_1h_records
    dynamic_1h_records = dynamic_records

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

def get_recent_1m_klines(count: int = 60) -> list[list[CandleRecord]]:
    """Получить свечи за последние N минут"""
    global candle_1m_records
    
    if count > len(candle_1m_records):
        count = len(candle_1m_records)
    
    recent_klines: list[list[CandleRecord]] = []
    
    for i in range(count):
        if candle_1m_records[i]:
            recent_klines.append(candle_1m_records[i])
    
    return recent_klines


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

def save_klines_to_ram(results: List[CandleRecord]):
    """Сохраняет свечи в оперативную память с скользящим окном в 180 минут"""
    if not results:
        logger.info("Нет данных для сохранения")
        return
    
    # Добавляем новые свечи в начало (самая свежая минута)
    global candle_1m_records

    
    results_open_time = results[0].open_time

    # Проверяем, существует ли уже запись за это время
    minute_exists = False
    existing_index = -1
    
    for i, minute_candles in enumerate(candle_1m_records):
        if minute_candles and minute_candles[0].open_time == results_open_time:
            minute_exists = True
            existing_index = i
            break
    
    if minute_exists:
        logger.debug(f"Минута {results_open_time} уже существует на позиции {existing_index}, пропускаем")
        return
    
    results_sorted = sorted(results, key=lambda x: x.open_time)

    # Находим правильную позицию для вставки на основе времени
    insert_position = -1

    for i in range(len(candle_1m_records)):
        if not candle_1m_records[i]:  # Пустой период
            if insert_position == -1:
                insert_position = i
            continue
        
        # Получаем время свечей в текущем периоде
        period_time = candle_1m_records[i][0].open_time
        
        if results_open_time > period_time:
            # Новые свечи новее текущего периода - вставляем перед ним
            insert_position = i
            break

        # Если не нашли позицию для вставки, добавляем в конец
    if insert_position == -1:
        # Ищем последний непустой индекс
        last_non_empty = -1
        for i in range(len(candle_1m_records)):
            if candle_1m_records[i]:
                last_non_empty = i
        
        if last_non_empty < len(candle_1m_records) - 1:
            insert_position = last_non_empty + 1
        else:
            # Все позиции заняты, нужно сдвинуть и вставить в начало
            # Сдвигаем все списки на одну позицию вправо
            for i in range(len(candle_1m_records)-1, 0, -1):
                candle_1m_records[i] = candle_1m_records[i-1]
            insert_position = 0
    
    # Вставляем на найденную позицию со сдвигом вправо
    if insert_position < len(candle_1m_records):
        # Сдвигаем элементы справа от insert_position
        for i in range(len(candle_1m_records)-1, insert_position, -1):
            candle_1m_records[i] = candle_1m_records[i-1]
        
        # Вставляем новые свечи
        candle_1m_records[insert_position] = results_sorted

    # Логируем результат
    logger.debug(f"Добавлено {len(results)} свечей на позицию {insert_position}")
    
    # Проверяем, сколько непустых периодов у нас есть
    non_empty_periods = sum(1 for period in candle_1m_records if period)
    logger.debug(f"Непустых периодов: {non_empty_periods}")
    
    # Логируем временной диапазон для отладки
    if non_empty_periods > 0:
        times = [period[0].open_time for period in candle_1m_records if period]
        logger.debug(f"Временной диапазон: от {min(times)} до {max(times)}")

