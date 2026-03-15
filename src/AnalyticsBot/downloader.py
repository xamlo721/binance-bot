import asyncio
from typing import List, Dict
from collections import defaultdict
from datetime import datetime

# Предполагаем, что классы UDPClient, UDPRequest, UDPResponse, MessageSerializer 
# уже определены (например, в модуле udp_client.py)
from udp_client import UDPClient, UDPRequest, UDPResponse

from bot_types import *
from logger import *

async def download_candles(
    trackable_tickers: List[str],
    minutes: int,
    end_time: datetime,
    server_addr: tuple = ('127.0.0.1', 58001),
    timeout: float = 3.0
) -> List[List[KlineRecord]]:
    """
    Асинхронная внутренняя функция, выполняющая запросы к UDP-серверу.
    Возвращает список списков KlineRecord, сгруппированных по тикерам.
    """
    end_minute = int(end_time.timestamp() // 60)
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
                    logger.warning(f"Таймаут при запросе минуты {minute}, попытка {attempt+1}")
                    if attempt == 2:
                        logger.warning(f"Пропускаем минуту {minute} после 3 неудачных попыток")

                except Exception as e:
                    logger.error(f"Ошибка при запросе минуты {minute}: {e}")
                    break  # другие ошибки не повторяем
            logger.debug(f"Минута {minute} принесла {len(result[minute])} тикеров")


    logger.debug(f"Всего скачано {len(result)} минут для {len(trackable_tickers)} тикеров")

    return result
