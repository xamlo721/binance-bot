
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import re
import traceback
from typing import Optional, List, Dict, Tuple
from logger import logger
from config import *

from ramstorage.AlertRecord import AlertRecord
from ramstorage.AlertMetrics import AlertMetrics
from ramstorage.CandleRecord import CandleRecord


def update_alert_record(
        alerts: List[AlertRecord],
        kline_records: Dict[str, CandleRecord],
        kline_time: datetime
) -> None:
    """
    Update min_price and max_price of alerts using the latest candle data.
    Only alerts newer than 48 hours are considered.
    """
    cutoff = datetime.now() - timedelta(hours=48)
    recent_alerts = [
        a for a in alerts
        if datetime.fromtimestamp(a.time / 1000) >= cutoff
    ]

    for alert in recent_alerts:
        candle = kline_records.get(alert.ticker)
        if not candle:
            continue

        # Update min_price
        if alert.min_price is None or candle.low < alert.min_price:
            alert.min_price = candle.low
            alert.min_price_time = int(kline_time.timestamp() * 1000)

        # Update max_price
        if alert.max_price is None or candle.high > alert.max_price:
            alert.max_price = candle.high
            alert.max_price_time = int(kline_time.timestamp() * 1000)


def calculate_alert_metrics(alerts: List[AlertRecord]) -> AlertMetrics:
    """
    Compute aggregate metrics from a list of alerts.
    """
    metrics = AlertMetrics()

    for alert in alerts:
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
            metrics.top_loss.append((alert.ticker, alert.max_loss_percent))

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
            metrics.top_profit.append((alert.ticker, alert.max_profit_percent))

    # Top‑10 losses and profits
    metrics.top_loss.sort(key=lambda x: x[1], reverse=True)
    metrics.top_profit.sort(key=lambda x: x[1], reverse=True)

    metrics.top_loss = metrics.top_loss[:10]
    metrics.top_profit = metrics.top_profit[:10]
    metrics.tickers_count = len(alerts)

    return metrics
