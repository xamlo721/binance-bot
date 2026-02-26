from dataclasses import dataclass, field
import os
from typing import List, Tuple
from ramstorage.AlertRecord import AlertRecord
from logger import logger
from config import *

@dataclass
class AlertMetrics:
    """
    Метрики, вычисленные по списку алертов.
    """
    # Суммы по 12 полям (именованные для ясности)
    sum_rps_sl_1: float = 0.0
    sum_rps_sl_2: float = 0.0
    sum_rps_sl_3: float = 0.0
    sum_rps_sl_4: float = 0.0
    sum_rps_sl_5: float = 0.0
    sum_max_loss: float = 0.0
    sum_rpb_sl_1: float = 0.0
    sum_rpb_sl_2: float = 0.0
    sum_rpb_sl_3: float = 0.0
    sum_rpb_sl_4: float = 0.0
    sum_rpb_sl_5: float = 0.0
    sum_max_profit: float = 0.0

    # Топ-10 убытков (список кортежей (тикер, значение))
    top_loss: List[Tuple[str, float]] = field(default_factory=list)

    # Топ-10 прибылей
    top_profit: List[Tuple[str, float]] = field(default_factory=list)

    # Общее количество записей
    tickers_count: int = 0

def calculate_alert_metrics(alerts: List[AlertRecord]) -> AlertMetrics:
    """
    Вычисляет метрики по списку алертов (AlertRecord) и возвращает объект AlertMetrics.
    """
    metrics = AlertMetrics()

    if not alerts:
        logger.warning(amm_SCRIPT_NAME + "Нет данных для вычисления метрик")
        return metrics

    # Проходим по всем алертам и накапливаем суммы
    loss_pairs = []
    profit_pairs = []

    for alert in alerts:
        # Суммы
        if alert.rps_sl_1_percent is not None:
            metrics.sum_rps_sl_1 += alert.rps_sl_1_percent
        if alert.rps_sl_2_percent is not None:
            metrics.sum_rps_sl_2 += alert.rps_sl_2_percent
        if alert.rps_sl_3_percent is not None:
            metrics.sum_rps_sl_3 += alert.rps_sl_3_percent
        if alert.rps_sl_4_percent is not None:
            metrics.sum_rps_sl_4 += alert.rps_sl_4_percent
        if alert.rps_sl_5_percent is not None:
            metrics.sum_rps_sl_5 += alert.rps_sl_5_percent
        if alert.max_loss_percent is not None:
            metrics.sum_max_loss += alert.max_loss_percent
            loss_pairs.append((alert.ticker, alert.max_loss_percent))

        if alert.rpb_sl_1_percent is not None:
            metrics.sum_rpb_sl_1 += alert.rpb_sl_1_percent
        if alert.rpb_sl_2_percent is not None:
            metrics.sum_rpb_sl_2 += alert.rpb_sl_2_percent
        if alert.rpb_sl_3_percent is not None:
            metrics.sum_rpb_sl_3 += alert.rpb_sl_3_percent
        if alert.rpb_sl_4_percent is not None:
            metrics.sum_rpb_sl_4 += alert.rpb_sl_4_percent
        if alert.rpb_sl_5_percent is not None:
            metrics.sum_rpb_sl_5 += alert.rpb_sl_5_percent
        if alert.max_profit_percent is not None:
            metrics.sum_max_profit += alert.max_profit_percent
            profit_pairs.append((alert.ticker, alert.max_profit_percent))

    # Формируем топ-10
    loss_pairs.sort(key=lambda x: x[1], reverse=True)
    profit_pairs.sort(key=lambda x: x[1], reverse=True)

    metrics.top_loss = loss_pairs[:10]
    metrics.top_profit = profit_pairs[:10]
    metrics.tickers_count = len(alerts)

    logger.info(amm_SCRIPT_NAME + f"Метрики вычислены: всего записей={metrics.tickers_count}")
    return metrics
