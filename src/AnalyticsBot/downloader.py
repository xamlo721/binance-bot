import asyncio
from typing import List, Dict
from collections import OrderedDict
from datetime import datetime

# Предполагаем, что классы UDPClient, UDPRequest, UDPResponse, MessageSerializer 
# уже определены (например, в модуле udp_client.py)
from udp_client import UDPClient, UDPRequest, UDPResponse

from AnalyticsBot.bot_types import *
from AnalyticsBot.logger import *

async def download_candles(
    trackable_tickers: List[str],
    minutes: int,
    end_time: datetime,
    server_addr: tuple = ('127.0.0.1', 58001),
    timeout: float = 10.0
) -> OrderedDict[int, list[KlineRecord]]:
    """
    Асинхронная внутренняя функция, выполняющая запросы к UDP-серверу.
    Возвращает список списков KlineRecord, сгруппированных по тикерам.
    """
    end_minute = int(end_time.timestamp() // 60)
    start_minute = end_minute - minutes  # включительно, получим minutes свечей: [start_minute, end_minute-1]

    # Словарь для накопления данных по тикерам
    result: OrderedDict[int, list[KlineRecord]] = OrderedDict()

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
                    filtered = [rec for rec in response.records if rec.symbol in trackable_tickers]
                    result[minute] = filtered


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
