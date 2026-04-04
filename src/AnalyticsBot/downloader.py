import asyncio
from typing import List
from collections import OrderedDict
from datetime import datetime

from AnalyticsBot.udp_client import UDPClient
from AnalyticsBot.bot_types import KlineRecord
from AnalyticsBot.logger import logger
from AnalyticsBot.config import *
from AnalyticsBot.protocol_download import KlineResponseStatus

async def download_candles(trackable_tickers: List[str], minutes: int, end_time: datetime, server_addr: tuple = (DOWNLOAD_SERVER_IP, DOWNLOAD_SERVER_PORTL), timeout: float = 10.0 ) -> OrderedDict[int, list[KlineRecord]]:
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
            success = False
            # Можно добавить повторные попытки при таймауте/ошибке
            for attempt in range(3):  # до 3 попыток
                try:
                    response = await client.request_klines(
                        minute_number=minute,
                        server_addr=server_addr,
                        timeout=timeout
                    )
                    # Обрабатываем статус ответа
                    if response.status == KlineResponseStatus.OK:
                        # Фильтруем записи только по интересующим тикерам
                        filtered = [rec for rec in response.records if rec.symbol in trackable_tickers]
                        result[minute] = filtered
                        success = True
                        break  # успешно
                    elif response.status == KlineResponseStatus.BUSY:
                        logger.warning(f"Сервер занят, попытка {attempt+1} для минуты {minute}")
                        await asyncio.sleep(10)  # ждём 10 секунду перед повтором
                        attempt = attempt - 1
                        continue
                    elif response.status == KlineResponseStatus.NOT_FOUND:
                        logger.warning(f"Минута {minute} не найдена на сервере, пропускаем")
                        # TODO: Надор промаркировать 
                        break  # не повторяем, данных нет
                    else:
                        logger.error(f"Неизвестный статус {response.status} для минуты {minute}, пропускаем")
                        break

                except asyncio.TimeoutError:
                    logger.warning(f"Таймаут при запросе минуты {minute}, попытка {attempt+1}")
                    if attempt == 2:
                        logger.warning(f"Пропускаем минуту {minute} после 3 неудачных попыток")

                except Exception as e:
                    logger.error(f"Ошибка при запросе минуты {minute}: {e}")
                    break  # другие ошибки не повторяем
                
            if success:
                logger.debug(f"Минута {minute} принесла {len(result[minute])} тикеров")
            else:
                logger.debug(f"Минута {minute} не загружена")

    logger.debug(f"Всего скачано {len(result)} минут для {len(trackable_tickers)} тикеров")

    return result
