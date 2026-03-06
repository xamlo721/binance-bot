from bot_types import AlertRecord
from typing import List

interprocess_buffer: list[AlertRecord] = []

def takeAlerts() -> List[AlertRecord]:
    """
    Извлекает все алерты из буфера и очищает его.
    Возвращает список извлечённых записей.
    """
    # В реальной многопроцессной среде необходима синхронизация (например, блокировка).
    # Если interprocess_buffer является Manager().list, то операции атомарны.
    alerts_copy = list(interprocess_buffer)
    interprocess_buffer.clear()
    return alerts_copy

def saveAlerts(alerts: List[AlertRecord]) -> None:
    """
    Сохраняет новые алерты в межпроцессный буфер.
    """
    interprocess_buffer.extend(alerts)