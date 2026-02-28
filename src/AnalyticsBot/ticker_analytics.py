import os
import glob
from logger import logger
from config import *
from typing import Dict, List, Tuple, Optional

# Пороговое значение (X раз)
X_MULTIPLIER = 5.0

def analyze_ticker(ticker: str, volume_10m: Optional[float], volume_10h_list: List[float]) -> Tuple[bool, str]:
    """
    Анализирует один тикер по условиям:
    volume_10m * 6 должно превышать каждый элемент volume_10h_list в X_MULTIPLIER раз.

    Args:
        ticker: Символ тикера
        volume_10m: Значение volume_10m для тикера (может быть None)
        volume_10h_list: Список из 10 значений total_volume для тикера из разных часов

    Returns:
        Tuple[bool, str]: (True/False, сообщение)
    """
    if volume_10m is None:
        return False, "Нет данных volume_10m"

    # Умножаем volume_10m на 6
    multiplied_volume = volume_10m * 6

    if not volume_10h_list or len(volume_10h_list) < 10:
        return False, "Недостаточно данных volume_10h"

    # Проверяем условие для каждого часа
    min_ratio = float('inf')
    for idx, vol_10h in enumerate(volume_10h_list, 1):
        if vol_10h <= 0:
            return False, f"total_volume_{idx} <= 0 ({vol_10h})"

        ratio = multiplied_volume / vol_10h
        min_ratio = min(min_ratio, ratio)

        if ratio < X_MULTIPLIER:
            return False, f"Условие не выполнено (коэффициент {ratio:.2f} для total_volume_{idx})"

    # Все условия выполнены
    return True, f"{min_ratio:.2f}"

def process_tickers_analytics(tickers: List[str], volume_10m_dict: Dict[str, float], volume_10h_dict: Dict[str, List[float]]) -> List[Tuple[str, str]]:
    """
    Обрабатывает список тикеров и возвращает те, которые удовлетворяют условиям.

    Args:
        tickers: Список тикеров из tickers_up
        volume_10m_dict: Словарь {symbol: volume_10m}
        volume_10h_dict: Словарь {symbol: [total_volume_1, total_volume_2, ..., total_volume_10]}

    Returns:
        List[Tuple[str, str]]: Список кортежей (тикер, сообщение) для сработавших тикеров.
    """
    alerts = []
    processed_count = 0

    for ticker in tickers:
        processed_count += 1
        vol_10m = volume_10m_dict.get(ticker)
        vol_10h_list = volume_10h_dict.get(ticker, [])

        ok, message = analyze_ticker(ticker, vol_10m, vol_10h_list)

        if ok:
            alerts.append((ticker, message))
            logger.info(f"🚨 #{ticker}: {message}")
        else:
            # Для отладки можно логировать первые несколько тикеров
            if processed_count <= 10:
                logger.info(f"{ticker}: {message}")

    logger.info(f"Обработано тикеров: {processed_count}")
    logger.info(f"Найдено сработавших: {len(alerts)}")

    return alerts
