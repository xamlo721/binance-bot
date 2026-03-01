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

# Предполагается, что класс Volume_10m уже определён, например:
# class Volume_10m:
#     def __init__(self, ticker: str, volume: float, open_time: int, close_time: int):
#         self.ticker = ticker
#         self.volume = volume
#         self.open_time = open_time
#         self.close_time = close_time

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

def calculate_1h_records(candle_1m_records: list[list[CandleRecord]], candle_1h_records: list[list[HoursRecord]]) -> bool:

    # Определяем время начала часа из первой свечи
    first_candle = candle_1m_records[0][0]
    hour_start_time = (first_candle.open_time // 3600000) * 3600000
    
    # Очищаем предыдущие записи за этот час
    candle_1h_records = [r for r in candle_1h_records if r.hour_start_time != hour_start_time]
    
    # Группируем свечи по символам
    symbols_data = {}
    
    for minute_data in candle_1m_records:  # Каждая минута
        for candle in minute_data:  # Каждая свеча в минуте
            if candle.symbol not in symbols_data:
                symbols_data[candle.symbol] = []
            symbols_data[candle.symbol].append(candle)
    
    # Для каждого символа вычисляем статистику
    for symbol, candles in symbols_data.items():
        if len(candles) < 2:  # Нужно хотя бы 2 свечи для std
            continue
            
        # Сортируем по времени
        candles.sort(key=lambda x: x.open_time)
        
        # Цены
        open_price = candles[0].open
        close_price = candles[-1].close
        high_price = max(c.open for c in candles)  # или max(c.high for c in candles)
        low_price = min(c.low for c in candles)
        
        # Для std вычисляем списки значений
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        volumes = [c.quote_volume for c in candles]
        taker_buy_base = [c.taker_buy_base_volume for c in candles]
        taker_buy_quote = [c.taker_buy_quote_volume for c in candles]
        trades = [c.trades for c in candles]
        
        # Волатильность
        volatilities = [(c.high - c.low) / c.open for c in candles]
        
        # Вычисляем статистику
        def mean(lst): return sum(lst) / len(lst)
        def std(lst):
            m = mean(lst)
            return (sum((x - m) ** 2 for x in lst) / len(lst)) ** 0.5
        
        # Создаем запись
        hour_record = HoursRecord(
            symbol=symbol,
            open=open_price,
            close=close_price,
            high=high_price,
            low=low_price,
            high_std=std(highs),
            low_std=std(lows),
            total_volume=sum(volumes),
            quote_volume_1m_avg=mean(volumes),
            quote_volume_std=std(volumes),
            taker_buy_base_volume_1m_avg=mean(taker_buy_base),
            taker_buy_base_volume_std=std(taker_buy_base),
            taker_buy_quote_volume_1m_avg=mean(taker_buy_quote),
            taker_buy_quote_volume_std=std(taker_buy_quote),
            trades_1m_avg=mean(trades),
            trades_std=std(trades),
            volatility_1m_avg=mean(volatilities),
            volatility_std=std(volatilities),
            hour_start_time=hour_start_time,
            hour_end_time=hour_start_time + 3600000,
            symbols_count=len(candles)
        )
        
        candle_1h_records.append(hour_record)
    
    return True


def calculate_1h_dynamic(current_candles: list[list[CandleRecord]]) -> Optional[list[HoursRecord]]:

    if not current_candles or not current_candles[0]:
        logger.info("Нет данных для обработки hdr_dynamic")
        return None
    
    logger.info(f"Обрабатываем {len(current_candles)} минут")

    # Группируем свечи по символам
    symbols_data: dict[str, list[CandleRecord]] = {}
  
    for minute_data in current_candles:
        for candle in minute_data:
            if candle.symbol not in symbols_data:
                symbols_data[candle.symbol] = []
            symbols_data[candle.symbol].append(candle)

    # Определяем время начала периода (из первой свечи)
    first_candle = current_candles[0][0]
    period_start_time = (first_candle.open_time // 60000) * 60000  # округляем до минуты

    # Вспомогательные функции для статистики
    def mean(lst): return sum(lst) / len(lst) if lst else 0
    def std(lst):
        if len(lst) < 2:
            return 0
        m = mean(lst)
        return (sum((x - m) ** 2 for x in lst) / len(lst)) ** 0.5

    # Создаем записи для каждого символа
    new_records: list[HoursRecord] = []
    for symbol, candles in symbols_data.items():
        if len(candles) < 2:  # Нужно хотя бы 2 свечи для std
            continue
        
        # Сортируем по времени
        candles.sort(key=lambda x: x.open_time)
        
        # Собираем значения для статистики
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        volumes = [c.quote_assets_volume for c in candles]
        taker_buy_base = [c.taker_buy_base_volume for c in candles]
        taker_buy_quote = [c.taker_buy_quote_volume for c in candles]
        trades = [c.num_of_trades for c in candles]
        volatilities = [(c.high - c.low) / c.open for c in candles]
        
        # Создаем словарь с данными
        record_dict = {
            'symbol': symbol,
            'open': candles[0].open,
            'close': candles[-1].close,
            'high': max(highs),
            'low': min(lows),
            'high_std': std(highs),
            'low_std': std(lows),
            'total_volume': sum(volumes),
            'quote_volume_1m_avg': mean(volumes),
            'quote_volume_std': std(volumes),
            'taker_buy_base_volume_1m_avg': mean(taker_buy_base),
            'taker_buy_base_volume_std': std(taker_buy_base),
            'taker_buy_quote_volume_1m_avg': mean(taker_buy_quote),
            'taker_buy_quote_volume_std': std(taker_buy_quote),
            'trades_1m_avg': mean(trades),
            'trades_std': std(trades),
            'volatility_1m_avg': mean(volatilities),
            'volatility_std': std(volatilities),
            'hour_start_time': period_start_time,
            'hour_end_time': period_start_time + (len(candles) * 60000),  # окончание через N минут
            'symbols_count': len(candles)
        }
        
        hour_record = HoursRecord.from_dict(record_dict)
        new_records.append(hour_record)
    
    
    return new_records


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
