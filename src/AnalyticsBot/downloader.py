import asyncio
from typing import List, Dict
from collections import defaultdict
from datetime import datetime

# Предполагаем, что классы UDPClient, UDPRequest, UDPResponse, MessageSerializer 
# уже определены (например, в модуле udp_client.py)
from udp_client import UDPClient, UDPRequest, UDPResponse

from bot_types import *

def minute_number_from_datetime(dt: datetime) -> int:
    """Возвращает количество минут с эпохи (Unix time в минутах)."""
    return int(dt.timestamp() // 60)

async def download_candles(
    trackable_tickers: List[str],
    minutes: int,
    end_time: datetime,
    server_addr: tuple = ('127.0.0.1', 9999),
    timeout: float = 3.0
) -> List[List[KlineRecord]]:
    """
    Асинхронная внутренняя функция, выполняющая запросы к UDP-серверу.
    Возвращает список списков KlineRecord, сгруппированных по тикерам.
    """
    end_minute = minute_number_from_datetime(end_time)
    start_minute = end_minute - minutes  # включительно, получим minutes свечей: [start_minute, end_minute-1]

    # Словарь для накопления данных по тикерам
    symbol_to_records: Dict[str, List[KlineRecord]] = defaultdict(list)

    # Создаём клиент и подключаемся
    async with UDPClient() as client:
        # Для каждой минуты из диапазона
        for minute in range(start_minute, end_minute):
            # Можно добавить повторные попытки при таймауте/ошибке
            for attempt in range(3):  # до 3 попыток
                try:
                    response = await client.request(
                        packet_number=minute,  # используем номер минуты как packet_number для простоты
                        minute_number=minute,
                        server_addr=server_addr,
                        timeout=timeout
                    )
                    # Фильтруем записи только по интересующим тикерам
                    for record in response.records:
                        if record.symbol in trackable_tickers:
                            symbol_to_records[record.symbol].append(record)
                    break  # успешно, выходим из цикла попыток
                except asyncio.TimeoutError:
                    print(f"Таймаут при запросе минуты {minute}, попытка {attempt+1}")
                    if attempt == 2:
                        print(f"Пропускаем минуту {minute} после 3 неудачных попыток")
                except Exception as e:
                    print(f"Ошибка при запросе минуты {minute}: {e}")
                    break  # другие ошибки не повторяем

    # Преобразуем словарь в список списков, сохраняя порядок тикеров из trackable_tickers?
    # Для совместимости со старой функцией вернём список списков, где каждый внутренний список
    # содержит свечи для одного тикера (порядок может быть произвольным, но обычно по тикерам)
    result = [symbol_to_records[symbol] for symbol in trackable_tickers if symbol in symbol_to_records]
    return result
