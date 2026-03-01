import os

from typing import Optional
from typing import Dict
from typing import List

from concurrent.futures import ProcessPoolExecutor, as_completed

from logger import logger
from config import *

from ramstorage.CandleRecord import CandleRecord
from ramstorage.HoursRecord import HoursRecord
from ramstorage.AlertRecord import AlertRecord

from ramstorage.ram_storage_utils import save_calc_alert_to_ram
from ramstorage.ram_storage_utils import Volume_10m

current_alerts: list[AlertRecord] = []

from typing import Dict, List, Optional

def calculate_10m_volumes_sidedWindow(
    candle_dict: Dict[int, List[CandleRecord]]
) -> Optional[List[Volume_10m]]:
    """
    Возвращает список Volume_10m – один объект для каждого тикера,
    содержащий суммарный объём за последние 10 минут и временные метки начала/конца
    интервала.

    Параметры:
        candle_dict: словарь вида {номер_минуты: список[CandleRecord]}

    Возвращает:
        Список Volume_10m, если в словаре есть минимум 10 минут, и данные согласованы.
        Иначе None.
    """
    # Проверяем наличие достаточного количества минут
    if len(candle_dict) < 10:
        return None

    # Сортируем ключи (номера минут) по возрастанию
    sorted_minutes = sorted(candle_dict.keys())

    # Берём последние 10 минут
    window_minutes = sorted_minutes[-10:]

    # Собираем списки свечей для каждой минуты окна
    window_klines = [candle_dict[m] for m in window_minutes]

    # Определяем порядок тикеров по первой минуте окна (предполагаем, что он одинаков для всех)
    first_minute_candles = window_klines[0]
    symbols = [c.symbol for c in first_minute_candles]
    n_symbols = len(symbols)

    # Инициализируем суммарные объёмы
    volumes = [0.0] * n_symbols

    # Проходим по всем минутам окна и суммируем объёмы
    for minute_candles in window_klines:
        # Проверяем, что в текущей минуте столько же свечей, сколько в первой
        if len(minute_candles) != n_symbols:
            # Данные несогласованы – возвращаем None
            return None
        # Предполагаем, что тикеры идут в том же порядке (можно добавить проверку, если нужно)
        for i, candle in enumerate(minute_candles):
            volumes[i] += candle.quote_assets_volume

    # Формируем результат: для каждого тикера берём время начала первой минуты
    # и время начала последней минуты как close_time
    open_times = [c.open_time for c in first_minute_candles]
    close_times = [c.open_time for c in window_klines[-1]]

    return [
        Volume_10m(
            ticker=symbols[i],
            volume=volumes[i],
            open_time=open_times[i],
            close_time=close_times[i]
        )
        for i in range(n_symbols)
    ]

def update_current_alert(alerts: List[AlertRecord]):
    global current_alerts
    """Найти самый свежий файл в папке"""
    try:
        if alerts != current_alerts:
            logger.info(f"Обнаружен новый набор алертов!: {alerts[0].time}")
            current_alerts = alerts
            logger.info(f"Загружено {len(alerts)} строк из файла")

            process_new_rows(alerts)

    except Exception as e:
        logger.error(f"Ошибка при обновлении текущего файла: {e}")

def process_new_rows(new_alerts: List[AlertRecord]):
    """Обработать новые строки"""
    processed_alerts: List[AlertRecord] = []
    
    # Анализируем каждую новую строку
    for alert in new_alerts:

        # Проверяем, есть ли значения цен
        if alert.buy_short_price and alert.min_price and alert.max_price:
            logger.info(f"Найдены цены для {alert.ticker}")
            processed_alerts.append(alert)
        else:
            # TODO: А как они могут быть не заполнены? 
            logger.warning(f"Цены не заполнены для {alert.ticker}, пропускаем")

    save_calc_alert_to_ram(processed_alerts)

def calculate_1h_records(candle_1m_records: dict[int, list[CandleRecord]]) -> Optional[dict[int, list[HoursRecord]]]:
    """
    Вычисляет часовые агрегаты на основе минутных свечей.
    Возвращает словарь, где ключ – начало часа (timestamp в миллисекундах), 
    значение – список HoursRecord для этого часа.
    
    Час обрабатывается только если в нём присутствуют все 60 минут для всех символов.
    Минуты, свыше кратного 60, отбрасываются.
    """
    if not candle_1m_records:
        return None

    # Проверяем, что количество минут кратно 60
    total_minutes = len(candle_1m_records)
    if total_minutes < 60:
        return None
    
    # Отбрасываем минуты, свыше кратного 60
    # Берём только последние N минут, где N кратно 60
    minutes_keys = list(candle_1m_records.keys())
    # Оставляем только количество минут, кратное 60 (отбрасываем "лишние" с начала)
    valid_minutes_count = (total_minutes // 60) * 60
    if valid_minutes_count == 0:
        return None
    
    # Берём последние valid_minutes_count минут (самые свежие данные)
    minutes_keys = minutes_keys[-valid_minutes_count:]
    
    # Группируем по часам (60 минут в каждом)
    result_records: dict[int, list[HoursRecord]] = {}

    # Разбиваем на часы по 60 минут
    for hour_idx in range(0, valid_minutes_count, 60):
        hour_minutes_keys = minutes_keys[hour_idx:hour_idx + 60]
        
        # Получаем список минутных свечей для этого часа
        hour_candles_by_minute = [candle_1m_records[key] for key in hour_minutes_keys]
        
        # Определяем список символов по первой минуте часа
        # Предполагаем, что во всех минутах одинаковый набор символов в одном порядке
        first_minute_candles = hour_candles_by_minute[0]
        symbols = [c.symbol for c in first_minute_candles]
        n_symbols = len(symbols)
        
        # Инициализируем накопители для каждого символа
        opens = [0.0] * n_symbols
        closes = [0.0] * n_symbols
        highs = [float('-inf')] * n_symbols
        lows = [float('inf')] * n_symbols
        total_volumes = [0.0] * n_symbols
        
        # Устанавливаем цены открытия (с первой минуты часа)
        for i, candle in enumerate(first_minute_candles):
            opens[i] = candle.open
        
        # Проходим по всем минутам часа и агрегируем данные
        for minute_idx, minute_candles in enumerate(hour_candles_by_minute):
            # Проверяем, что количество свечей совпадает
            if len(minute_candles) != n_symbols:
                # Если данные несогласованы, пропускаем весь час
                break
            
            for i, candle in enumerate(minute_candles):
                # Обновляем максимум
                if candle.high > highs[i]:
                    highs[i] = candle.high
                
                # Обновляем минимум
                if candle.low < lows[i]:
                    lows[i] = candle.low
                
                # Суммируем объём
                total_volumes[i] += candle.quote_assets_volume
                
                # Цена закрытия обновляется на каждой минуте, но нам нужна последняя
                if minute_idx == 59:  # последняя минута часа
                    closes[i] = candle.close
        else:
            # Этот блок выполнится, если цикл не был прерван (нет break)
            # Формируем часовые записи
            hour_start = hour_minutes_keys[0]  # время начала часа (первая минута)
            hour_records = []
            
            for i in range(n_symbols):
                # Проверяем, что все значения корректны (high не -inf, low не inf)
                if highs[i] == float('-inf') or lows[i] == float('inf'):
                    continue
                    
                record = HoursRecord(
                    symbol=symbols[i],
                    open=opens[i],
                    close=closes[i],
                    high=highs[i],
                    low=lows[i],
                    total_volume=total_volumes[i]
                )
                hour_records.append(record)
            
            if hour_records:  # добавляем только если есть записи
                result_records[hour_start] = hour_records
    
    return result_records if result_records else None

def process_single_records(records: list[HoursRecord]):
    """Обрабатывает список записей и возвращает максимальные high значения"""
    try:
        max_highs: Dict[str, float] = {}
        
        for record in records:
            symbol = record.symbol
            high = record.high
            
            if symbol not in max_highs or high > max_highs[symbol]:
                max_highs[symbol] = high
        
        logger.debug(f"Обработано {len(records)} записей, найдено {len(max_highs)} символов")
        return max_highs
        
    except Exception as e:
        logger.error(f"Ошибка при обработке записей: {e}")
        return {}
    
def agregate_12h_records(hours_records: list[list[HoursRecord]]) -> Optional[Dict[str, float]]:
    """
    Обрабатывает динамический файл и агрегирует данные
    
    Args:
        hours_records: Список списков HoursRecord (11 часовых записей + динамическая запись)
    
    Returns:
        Optional[Dict[str, float]]: Словарь {symbol: max_high} с максимальными значениями, или None в случае ошибки
    """
    try:
        # Обрабатываем файлы параллельно
        max_high_values: Dict[str, float] = {}
        
        # Создаем пул процессов
        with ProcessPoolExecutor(max_workers=min(len(hours_records), os.cpu_count() or 4)) as executor:
            # Запускаем обработку всех записей
            future_to_records = {
                executor.submit(process_single_records, records): i 
                for i, records in enumerate(hours_records)
            }
            
            # Собираем результаты по мере завершения
            for future in as_completed(future_to_records):
                try:
                    result = future.result(timeout=30)
                    if result:
                        # Объединяем результаты
                        for symbol, high in result.items():
                            if symbol not in max_high_values or high > max_high_values[symbol]:
                                max_high_values[symbol] = high
                except Exception as e:
                    logger.error(f"Ошибка при обработке записи: {e}")
        
        if max_high_values:
            logger.info(f"Обработано тикеров: {len(max_high_values)}")
            return max_high_values
        else:
            logger.info("Нет данных для обработки")
            return {}
        
    except Exception as e:
        logger.error(f"Ошибка при обработке agregate_12h_records : {e}")
        return None


def aggregate_10h_volumes(latest_records: List[HoursRecord], historical_records: List[List[HoursRecord]]) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Агрегация объемов из исторических записей
    
    Args:
        latest_records: Самые свежие записи (текущий час)
        historical_records: Список списков исторических записей (10 последних часов)
    
    Returns:
        Optional[Dict[str, Dict[str, float]]]: Словарь вида
            {
                'BTCUSDT': {
                    'total_volume_1': 12345.67,  # самый старый час
                    'total_volume_2': 12345.67,
                    ...
                    'total_volume_10': 12345.67,  # самый новый час (latest_records)
                    'total_volume_11': 12345.67   # дополнительный, если есть
                },
                ...
            }
            или None при ошибке
    """
    try:
        # Объединяем все записи в один список, добавляя latest_records в конец
        all_records = historical_records + [latest_records]
        
        # Проверяем, что у нас достаточно данных
        if len(all_records) < 2:
            logger.warning(f"Недостаточно данных для обработки. Получено: {len(all_records)} периодов")
            return None
        
        # Берем последние 10 записей (или все, если меньше)
        records_to_process = all_records[-10:] if len(all_records) >= 10 else all_records
        
        logger.info(f"Обработка {len(records_to_process)} периодов для агрегации")
        
        # Словарь для хранения объемов по тикерам
        volumes_by_symbol: Dict[str, Dict[str, float]] = {}
        
        # Обрабатываем каждый период
        for period_idx, period_records in enumerate(records_to_process, 1):
            for record in period_records:
                symbol = record.symbol
                total_volume = record.total_volume
                
                if symbol not in volumes_by_symbol:
                    volumes_by_symbol[symbol] = {}
                
                # Сохраняем объем для текущего периода
                volumes_by_symbol[symbol][f'total_volume_{period_idx}'] = total_volume
        
        # Логируем результат
        logger.info(f"Агрегировано {len(volumes_by_symbol)} тикеров за {len(records_to_process)} периодов")
        
        return volumes_by_symbol
        
    except Exception as e:
        logger.error(f"Ошибка при агрегации объемов: {e}")
        return None
