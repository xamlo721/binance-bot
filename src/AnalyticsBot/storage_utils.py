from dataclasses import dataclass

from datetime import datetime
from datetime import timezone
from typing import List
from logger import logger
from config import *

from bot_types import KlineRecord 
from bot_types import HoursRecord 
from bot_types import AlertRecord 


# Список всех отметок за MINUTE_CANDLES_LIMIT минут 
#                   <НОМЕР_МИНУТЫ List<МИНУТНАЯ_ЗАПИСЬ>>
candle_1m_records: dict[int, list[KlineRecord]] = {}
candle_1h_records: dict[int, list[HoursRecord]] = {}

def is_storage_consistent(candle_dict: dict[int, List[KlineRecord]]) -> bool:
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

def get_1m_candles() -> dict[int, list[KlineRecord]]:
    return candle_1m_records

def save_1h_records(volumes:  dict[int, list[HoursRecord]]) -> bool:
    global candle_1h_records
    candle_1h_records = volumes
    return True


def save_klines_to_ram(results: List[KlineRecord]):
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


def get_recent_1m_klines(count: int = 60) -> dict[int, list[KlineRecord]]:
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
    
    # Если запрошено больше, чем есть, берём все
    if count > len(candle_1m_records):
        count = len(candle_1m_records)

    # Берём последние count ключей
    recent_keys = list(candle_1m_records.keys())[-count:]

    # Формируем результирующий словарь
    return {key: candle_1m_records[key] for key in recent_keys}

def get_recent_1h_klines(count: int = 60) -> dict[int, list[HoursRecord]]:
    """Получить свечи за последние N минут"""
    global candle_1h_records
    
    # Защита от пустого словаря или неположительного count
    if not candle_1h_records or count <= 0:
        return {}
    
    if count > len(candle_1h_records):
        count = len(candle_1h_records)
    
    # Берём последние count ключей
    recent_keys = list(candle_1h_records.keys())[-count:]

    # Формируем результирующий словарь
    return {key: candle_1h_records[key] for key in recent_keys}
