from dataclasses import dataclass

from datetime import datetime
from datetime import timezone
from typing import List
from AnalyticsBot.logger import logger
from AnalyticsBot.config import *
from collections import OrderedDict

from AnalyticsBot.bot_types import KlineRecord 
from AnalyticsBot.bot_types import HoursRecord 
from AnalyticsBot.bot_types import AlertRecord 


# Список всех отметок за MINUTE_CANDLES_LIMIT минут 
#                   <НОМЕР_МИНУТЫ List<МИНУТНАЯ_ЗАПИСЬ>>
candle_1m_records: OrderedDict[int, list[KlineRecord]] = OrderedDict()
candle_1h_records: OrderedDict[int, list[HoursRecord]] = OrderedDict()

def _format_ts(ts_ms: int) -> str:
    """Преобразует timestamp в миллисекундах в строку ГГГГ-ММ-ДД ЧЧ:ММ:СС"""
    return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def is_storage_consistent(candle_dict: OrderedDict[int, List[KlineRecord]]) -> bool:
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
        if keys[i] != keys[i-1] + 1:
            print(f"Ошибка: разрыв в последовательности минут между {keys[i-1]} и {keys[i]}")
            return False

    # 2. Проверка совпадения open_time с ключом
    for minute, records in candle_dict.items():
        for record in records:
            if record.open_time != minute * 60000:
                print(f"Ошибка: запись {record} имеет open_time={_format_ts(record.open_time)}, "
                      f"не совпадающий с ключом {_format_ts(minute * 60000)}")
                return False

    return True

def get_1m_candles() -> OrderedDict[int, list[KlineRecord]]:
    return candle_1m_records

def save_1h_records(volumes:  OrderedDict[int, list[HoursRecord]]) -> bool:
    global candle_1h_records
    candle_1h_records = volumes
    return True


def save_klines_to_ram(results: OrderedDict[int, list[KlineRecord]]):
    """
    Сохраняет свечи в оперативную память с скользящим окном.
    Ключ словаря – номер минуты в штуках с 1970.
    При добавлении проверяется:
        • дублирование минут,
        • сохранение только последних MINUTE_CANDLES_LIMIT минут,
        • непрерывность диапазона минут.
    """
    if not results:
        logger.info("Нет данных для сохранения")
        return

    global candle_1m_records

    added_any = False

    for minute_key, candles in results.items():

        # Проверка дублирования
        if minute_key in candle_1m_records:
            # logger.debug(f"Минута {minute_key} уже существует, пропускаем")
            continue

        # Добавляем в глобальный кэш
        candle_1m_records[minute_key] = candles
        added_any = True
        # logger.debug(f"Добавлено {len(candles)} свечей на ключ {minute_key}")

    if not added_any:
        logger.debug("Не добавлено ни одной новой минуты (все уже существуют)")
        return

    # Удаляем старые записи – сохраняем только последние MAX_CACHED_CANDLES минут
    if len(candle_1m_records) > MAX_CACHED_CANDLES:
        # Сортируем ключи (миллисекунды) и удаляем самые старые
        keys_sorted = sorted(candle_1m_records.keys())
        for key in keys_sorted[:-MAX_CACHED_CANDLES]:
            del candle_1m_records[key]

    # Проверяем непрерывность диапазона минут (после возможного удаления)
    if len(candle_1m_records) > 1:
        keys_sorted = sorted(candle_1m_records.keys())
        for i in range(len(keys_sorted) - 1):
            if keys_sorted[i+1] != keys_sorted[i] +1:
                logger.warning(f"Разрыв между минутами {keys_sorted[i]} и {keys_sorted[i+1]}: Δ={keys_sorted[i+1] - keys_sorted[i]}с")

    # Логируем общее состояние
    non_empty_periods = len(candle_1m_records)
    logger.debug(f"Непустых периодов в памяти: {non_empty_periods}")
    if non_empty_periods > 0:
        times = list(candle_1m_records.keys())
        logger.debug(f"Временной диапазон: от {_format_ts(min(times) * 60000)} до {_format_ts(max(times) * 60000)}")


def get_recent_1m_klines(count: int = 60) -> OrderedDict[int, list[KlineRecord]]:
    """
    Возвращает словарь с минутными свечами за последние N минут.
    Ключи словаря – номера минут (временные метки), значения – списки KlineRecord.
    Если записей меньше, чем запрошено, возвращаются все имеющиеся.
    При пустом словаре или некорректном count возвращается пустой словарь.
    """
    global candle_1m_records

    # Защита от пустого словаря или неположительного count
    if not candle_1m_records or count <= 0:
        return OrderedDict()
    
    # Если запрошено больше, чем есть, берём все
    if count > len(candle_1m_records):
        count = len(candle_1m_records)

    # Берём последние count ключей (порядок сохраняется, так как candle_1m_records — OrderedDict)
    recent_keys = list(candle_1m_records.keys())[-count:]

    # Формируем новый OrderedDict с теми же ключами и значениями
    return OrderedDict((key, candle_1m_records[key]) for key in recent_keys)

def get_recent_1h_klines(count: int = 60) -> OrderedDict[int, list[HoursRecord]]:
    """Получить свечи за последние N часов."""
    global candle_1h_records

    # Защита от пустого словаря или неположительного count
    if not candle_1h_records or count <= 0:
        return OrderedDict()

    if count > len(candle_1h_records):
        count = len(candle_1h_records)

    # Берём последние count ключей (порядок сохраняется, если candle_1h_records — OrderedDict)
    recent_keys = list(candle_1h_records.keys())[-count:]

    # Формируем OrderedDict
    return OrderedDict((key, candle_1h_records[key]) for key in recent_keys)
