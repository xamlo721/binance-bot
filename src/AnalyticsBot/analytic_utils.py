import os

from typing import Optional
from typing import Dict
from typing import List
from datetime import datetime

from AnalyticsBot.logger import logger
from AnalyticsBot.config import *
from collections import OrderedDict

from AnalyticsBot.bot_types import KlineRecord
from AnalyticsBot.bot_types import HoursRecord
from AnalyticsBot.bot_types import AlertRecord
from AnalyticsBot.bot_types import Volume_10m

current_alerts: list[AlertRecord] = []

from typing import Dict, List, Optional

def validate_ticker(candle_dict: OrderedDict[int, List[KlineRecord]]) -> OrderedDict[int, List[KlineRecord]]:
    """
    Фильтрует тикеры, оставляя только те, у которых данные полны и непротиворечивы.
    
    Критерии отбраковки:
    1. Тикер появился позже (не во всех минутах диапазона)
    2. Тикер был делистнут (пропал раньше конца диапазона)
    3. Тикер имеет пропуски данных внутри диапазона
    4. Тикер имеет нулевые объёмы во всех минутах (мёртвый тикер)
    5. Тикер имеет отрицательные объёмы (ошибка данных)
    6. Тикер с битыми/неконсистентными ценами (high < low, open/close вне диапазона)
    """
    
    if not candle_dict:
        return OrderedDict()
    
    # Получаем все минуты в диапазоне
    all_minutes = list(candle_dict.keys())
    if not all_minutes:
        return OrderedDict()
    
    # Множество всех тикеров, встречающихся в данных
    all_tickers = set()
    ticker_first_seen = {}  # минута первого появления тикера
    ticker_last_seen = {}   # минута последнего появления тикера
    ticker_minutes_count = {}  # количество минут, где тикер присутствует
    ticker_zero_volume_count = {}  # количество минут с нулевым объёмом
    ticker_invalid_price = set()   # тикеры с некорректными ценами
    
    # Проходим по всем минутам для сбора статистики
    for minute, records in candle_dict.items():
        for record in records:
            # Проверка на отрицательные объёмы
            if record.quote_assets_volume < 0 or record.volume < 0:
                logger.error(f"Ошибка: отрицательный объём у {record.symbol} в минуте {minute}")
                ticker_invalid_price.add(record.symbol)
            
            # Проверка консистентности цен
            if record.high < record.low:
                logger.error(f"Ошибка: high < low у {record.symbol} в минуте {minute}")
                ticker_invalid_price.add(record.symbol)
            
            if record.open < record.low or record.open > record.high:
                logger.warning(f"Предупреждение: open вне диапазона у {record.symbol} в минуте {minute}")
                # Можно не отбраковывать строго, но для чистоты добавим в список проблемных
                ticker_invalid_price.add(record.symbol)
            
            if record.close < record.low or record.close > record.high:
                logger.warning(f"Предупреждение: close вне диапазона у {record.symbol} в минуте {minute}")
                ticker_invalid_price.add(record.symbol)
            
            # Обновляем статистику по тикеру
            if record.symbol not in ticker_first_seen:
                ticker_first_seen[record.symbol] = minute
                ticker_last_seen[record.symbol] = minute
                ticker_minutes_count[record.symbol] = 1
                ticker_zero_volume_count[record.symbol] = 1 if record.quote_assets_volume == 0 else 0
            else:
                ticker_last_seen[record.symbol] = minute
                ticker_minutes_count[record.symbol] += 1
                if record.quote_assets_volume == 0:
                    ticker_zero_volume_count[record.symbol] += 1
            
            all_tickers.add(record.symbol)
    
    # Определяем минимальную и максимальную минуту в данных
    min_minute = min(all_minutes)
    max_minute = max(all_minutes)
    expected_minutes_count = len(all_minutes)
    
    # Множество валидных тикеров
    valid_tickers = set()
    
    for ticker in all_tickers:
        # Если тикер уже отмечен как проблемный по ценам – пропускаем
        if ticker in ticker_invalid_price:
            logger.info(f"Отбраковка {ticker}: некорректные цены")
            continue
        
        # Проверка 1: Тикер присутствует во всех минутах диапазона
        if ticker_minutes_count[ticker] != expected_minutes_count:
            logger.info(f"Отбраковка {ticker}: присутствует только в {ticker_minutes_count[ticker]} из {expected_minutes_count} минут")
            continue
        
        # Проверка 2: Тикер не появился позже и не исчез раньше
        if ticker_first_seen[ticker] != min_minute:
            logger.info(f"Отбраковка {ticker}: первое появление {ticker_first_seen[ticker]} (ожидалось {min_minute})")
            continue
            
        if ticker_last_seen[ticker] != max_minute:
            logger.info(f"Отбраковка {ticker}: последнее появление {ticker_last_seen[ticker]} (ожидалось {max_minute})")
            continue
        
        # Проверка 3: Тикер не состоит полностью из нулевых объёмов
        if ticker_zero_volume_count[ticker] == expected_minutes_count:
            logger.info(f"Отбраковка {ticker}: все объёмы нулевые")
            continue
        
        # Проверка 4: Процент нулевых объёмов не слишком высок (опционально)
        zero_volume_ratio = ticker_zero_volume_count[ticker] / expected_minutes_count
        if zero_volume_ratio > 0.5:  # больше 50% нулевых объёмов
            logger.info(f"Отбраковка {ticker}: {zero_volume_ratio:.1%} нулевых объёмов")
            continue
        
        # Если все проверки пройдены, тикер валиден
        valid_tickers.add(ticker)
    
    # Формируем результат, оставляя только валидные тикеры
    result = OrderedDict()
    
    for minute, records in candle_dict.items():
        filtered_records = [r for r in records if r.symbol in valid_tickers]
        if filtered_records:  # если после фильтрации остались записи
            result[minute] = filtered_records
    
    logger.info(f"Валидация завершена. Было тикеров: {len(all_tickers)}, стало: {len(valid_tickers)}")
    return result

def calculate_10m_volumes_slidedWindow(candle_dict: OrderedDict[int, List[KlineRecord]]) -> Optional[List[Volume_10m]]:
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

def calculate_1h_records(candle_1m_records: OrderedDict[int, list[KlineRecord]]) -> Optional[OrderedDict[int, list[HoursRecord]]]:
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
    result_records: OrderedDict[int, list[HoursRecord]] = OrderedDict()

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
    
    if result_records:
        return OrderedDict((key, result_records[key]) for key in result_records.keys())
    else:
        return None

def calculate_volumes_slidedWindow(all_records: OrderedDict[int, List[HoursRecord]], num_hours: int) -> Optional[Dict[str, dict[str, float]]]:
    """
    Агрегация объемов из исторических записей
    
    Args:
        all_records: Словарь вида {timestamp_начала_часа: List[HoursRecord]}
        num_hours: Количество последних часов для агрегации (по умолчанию 10)
    
    Returns:
        Optional[Dict[str, Dict[str, float]]]: Словарь вида
            {
                'BTCUSDT': {
                    'total_volume_1': 12345.67,  # самый старый час
                    'total_volume_2': 12345.67,
                    ...
                    'total_volume_10': 12345.67,  # самый новый час (latest_records)
                },
                ...
            }
            или None при ошибке
    """
    try:
        # Проверяем наличие данных
        if not all_records:
            logger.warning("Получен пустой словарь записей")
            return None
        
        # Проверяем, что у нас достаточно данных
        if len(all_records) < num_hours:
            logger.warning(f"Недостаточно данных для обработки. Получено: {len(all_records)} периодов")
            return None
        
        # Берем последние num_hours записей (или все, если меньше)
        records_to_process = list(all_records.keys())[-num_hours:]

        def _format_ts(ts_ms: int) -> str:
            """Преобразует timestamp в миллисекундах в строку ГГГГ-ММ-ДД ЧЧ:ММ:СС"""
            return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

        logger.info(f"Обработка {len(records_to_process)} периодов для агрегации (с {_format_ts(records_to_process[0] * 60000)} по {_format_ts(records_to_process[-1] * 60000)})")        

        # Словарь для хранения объемов по тикерам
        volumes_by_symbol: Dict[str, Dict[str, float]] = {}
        
        # Обрабатываем каждый период
        for period_idx, hour_timestamp in enumerate(records_to_process, 1):
            hour_records = all_records[hour_timestamp]

            for record in hour_records:
                symbol = record.symbol
                total_volume = record.total_volume
                
                if symbol not in volumes_by_symbol:
                    volumes_by_symbol[symbol] = {}
                
                # Сохраняем объем для текущего периода
                # period_idx=1 - самый старый час в окне, period_idx=10 - самый новый
                volumes_by_symbol[symbol][f'total_volume_{period_idx}'] = total_volume
        
        # Логируем результат
        logger.info(f"Агрегировано {len(volumes_by_symbol)} тикеров за {len(records_to_process)} периодов")
        
        return volumes_by_symbol
        
    except Exception as e:
        logger.error(f"Ошибка при агрегации объемов: {e}")
        return None

def calculate_prices_slidedWindow(hours_records: dict[int, list[HoursRecord]], num_hours: int) -> Optional[dict[str, float]]:
    """
    Обрабатывает словарь часовых записей и возвращает максимальные значения high по каждому тикеру.
    Берёт первые num_hours записей из словаря (по возрастанию ключей – самые старые часы).

    Args:
        hours_records: Словарь вида {timestamp_начала_часа: list[HoursRecord]}
        num_hours: Количество первых часов для обработки (по умолчанию 12)

    Returns:
        Optional[Dict[str, float]]: Словарь {symbol: max_high} с максимальными значениями,
        или None в случае ошибки. При отсутствии данных возвращается пустой словарь.
    """
    try:
        if not hours_records:
            logger.info("Пустой словарь часовых записей")
            return {}

        max_high_values: Dict[str, float] = {}

        # Проходим по всем часам (ключи словаря)
        for hour_timestamp, records in hours_records.items():
            # records — список HoursRecord для данного часа
            for record in records:
                symbol = record.symbol
                high = record.high
                # Обновляем максимум для символа
                if symbol not in max_high_values or high > max_high_values[symbol]:
                    max_high_values[symbol] = high

        if max_high_values:
            logger.info(f"Обработано тикеров: {len(max_high_values)}")
            return max_high_values
        else:
            logger.info("Нет данных для обработки")
            return {}

    except Exception as e:
        logger.error(f"Ошибка при обработке часовых записей: {e}")
        return None

def check_price_overlimit(klines: List[KlineRecord], aggregated_highs: dict[str, float]) -> Optional[dict[str, float]]:
    """
    Сравнивает цены закрытия минутных свечей с максимальными значениями high из агрегированных данных
    
    Args:
        klines: Список минутных свечей (CandleRecord)
        aggregated_highs: Словарь {symbol: max_high} с максимальными значениями high из агрегации
    
    Returns:
        Optional[Dict[str, float]]: Словарь {symbol: difference} для тикеров, где close > high или None при ошибке
    """
    try:
        # Группируем последние значения close по символам
        latest_closes: Dict[str, float] = {}
        
        for candle in klines:
            symbol = candle.symbol
            close = candle.close
            
            # Берем самую свежую цену закрытия для каждого символа
            if symbol not in latest_closes:
                latest_closes[symbol] = close
        
        # Находим тикеры где close > high
        tickers_up: Dict[str, float] = {}
        
        for symbol, close in latest_closes.items():
            if symbol in aggregated_highs:
                high = aggregated_highs[symbol]
                if close > high:
                    difference = close - high
                    tickers_up[symbol] = difference
        
        return tickers_up
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}")
        return None

def isWindow10mValid(volumes_10m: List[Volume_10m], expected_tickers: List[str]) -> bool:
    """
    Проверяет, что в рассчитанном 10-минутном окне объёмов присутствуют все ожидаемые тикеры.

    Args:
        volumes_10m: Список объектов Volume_10m, полученный из calculate_10m_volumes_slidedWindow.
        expected_tickers: Список тикеров, которые должны быть в окне (обычно из последней минуты).

    Returns:
        True, если все ожидаемые тикеры присутствуют в volumes_10m, иначе False.
    """
    if not volumes_10m:
        logger.error("volumes_10m пуст")
        return False

    actual_tickers = {v.ticker for v in volumes_10m}
    expected_set = set(expected_tickers)

    missing = expected_set - actual_tickers
    if missing:
        logger.error(f"В 10-минутном окне отсутствуют тикеры: {missing}")
        return False

    return True

def analyze_ticker(ticker: str, volume_10m_interval: Optional[float], volume_slided_window: List[float]) -> bool:
    """
    Анализирует один тикер по условиям:
    volume_10m_interval * 6 должно превышать каждый элемент volume_10h_list в X_MULTIPLIER раз.

    Args:
        ticker: Символ тикера
        volume_10m_interval: Значение volume_10m_interval для тикера (может быть None)
        volume_10h_list: Список из 10 значений total_volume для тикера из разных часов

    Returns:
        bool True/False
    """
    if volume_10m_interval is None:
        logger.error(f"❌ Нет данных volume_10m_interval")
        return False

    multiplied_volume = volume_10m_interval * VOLUME_LIMIT_MULTIPLIER

    if not volume_slided_window or len(volume_slided_window) < HOURS_VOLUMES_SLIDED_WINDOW_PERIOD:
        logger.error(f"❌ Недостаточно данных volume_slided_window")
        return False

    # Проверяем условие для каждого часа
    min_ratio = float('inf')
    for idx, vol_10h in enumerate(volume_slided_window, 1):
        if vol_10h <= 0:
            logger.error(f"❌ total_volume_{idx} <= 0 ({vol_10h})")
            return False

        ratio = multiplied_volume / vol_10h
        min_ratio = min(min_ratio, ratio)

        if ratio < VOLUME_MULTIPLIER:
            logger.error(f"❌ Условие не выполнено (коэффициент {ratio:.2f} для total_volume_{idx})")
            return False

    # Все условия выполнены
    return True

def check_volume_overlimit(klines: List[KlineRecord], volumes_10m: List[Volume_10m], volumes_10h: Dict[str, Dict[str, float]]) -> Optional[dict[str, float]]:
    """
    Проверяет превышение лимитов объёма для тикеров из последней минуты.

    Args:
        klines: Список свечей за последнюю минуту (каждая свеча соответствует одному тикеру).
        volumes_10m: Список объектов Volume_10m с суммарным объёмом за последние 10 минут.
        volumes_10h: Словарь, где ключ — тикер, значение — словарь с объёмами за последние 10 часов
                        вида {"total_volume_1": val1, ..., "total_volume_10": val10}.

    Returns:
        Словарь {тикер: объём_за_10_минут} для тикеров, по которым сработало условие,
        или None в случае ошибки.
    """
    try:
        # Преобразуем volumes_10m в словарь для быстрого доступа по тикеру
        volumes_10m_dict = {item.ticker: item.volume for item in volumes_10m}

        results = {}

        for candle in klines:
            ticker = candle.symbol

            # Получаем 10-минутный объём для тикера
            vol_10m = volumes_10m_dict.get(ticker)
            if vol_10m is None:
                logger.debug(f"Тикер {ticker} отсутствует в volumes_10m, пропускаем")
                continue

            # Получаем список объёмов за последние 10 часов
            hour_volumes_dict = volumes_10h.get(ticker)
            if hour_volumes_dict is None:
                logger.debug(f"Тикер {ticker} отсутствует в volumes_10h, пропускаем")
                continue

            # Преобразуем словарь в список в порядке возрастания периода (от total_volume_1 до total_volume_10)
            # Предполагаем, что ключи именуются как 'total_volume_1'...'total_volume_10'
            hour_volumes = [hour_volumes_dict[f'total_volume_{i}'] for i in range(1, 11)]

            # Анализируем тикер (функция analyze_ticker должна быть определена)
            if analyze_ticker(ticker, vol_10m, hour_volumes):
                results[ticker] = vol_10m
                logger.info(f"🚨 #{ticker}: Alert! (10m volume = {vol_10m})")

        logger.info(f"Обработано тикеров: {len(klines)}. Найдено сработавших: {len(results)}")
        return results if results else None

    except Exception as e:
        logger.error(f"Ошибка при проверке объёмов: {e}")
        return None
