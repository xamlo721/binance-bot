import asyncio
from typing import List
from typing import Optional
from collections import OrderedDict
from datetime import datetime

from AnalyticsBot.udp_client import UDPClient
from AnalyticsBot.bot_types import KlineRecord
from AnalyticsBot.logger import logger
from AnalyticsBot.config import *
from AnalyticsBot.protocol_download import KlineResponseStatus
from AnalyticsBot.protocol_download import SymbolsResponse
from AnalyticsBot.protocol_download import TimeResponse


def get_server_time_diff() -> Optional[int]:
    """
    Запрашивает время сервера и возвращает разницу (серверное время - локальное время клиента) в миллисекундах.
    Если запрос не удался, возвращает None.
    """
    server_addr = (DOWNLOAD_SERVER_IP, DOWNLOAD_SERVER_PORT)
    client_timestamp_ms = int(datetime.now().timestamp() * 1000)
    try:
        response = asyncio.run(_request_time_async(client_timestamp_ms, server_addr))
        if response and response.status == 0:
            diff = response.server_time_ms - client_timestamp_ms
            logger.debug(f"Разница времени с сервером: {diff} мс")
            return diff
        else:
            logger.error(f"Ошибка получения времени: статус {response.status if response else 'None'}")
            return None
    except Exception as e:
        logger.error(f"Исключение при получении времени: {e}")
        return None

async def _request_time_async(client_timestamp_ms: int, server_addr: tuple, timeout: float = 10.0) -> Optional[TimeResponse]:
    async with UDPClient() as client:
        return await client.request_time(client_timestamp_ms, server_addr, timeout)

# ========================================================================================================== #

async def _request_symbols_async(server_addr: tuple, timeout: float = 10.0) -> Optional[SymbolsResponse]:
    """Асинхронно запрашивает список символов с сервера."""
    async with UDPClient() as client:
        # Номер минуты (можно передать 0, сервер должен вернуть актуальный список)
        request_time = int(datetime.now().timestamp() // 60)
        response = await client.request_symbols(request_time, server_addr, timeout)
        return response

def get_trading_symbols_from_server() -> List[str]:
    """Синхронная обёртка для получения списка символов с UDP-сервера."""
    server_addr = (DOWNLOAD_SERVER_IP, DOWNLOAD_SERVER_PORT)
    try:
        response = asyncio.run(_request_symbols_async(server_addr))
        if response and response.status == 0:
            logger.info(f"Получено {len(response.symbols)} символов с сервера")
            return response.symbols
        else:
            logger.error(f"Ошибка получения символов: статус {response.status if response else 'None'}")
            return []
    except Exception as e:
        logger.error(f"Исключение при получении символов: {e}")
        return []
    
# ========================================================================================================== #
async def download_candles(trackable_tickers: List[str], minutes: int, end_time: datetime, server_addr: tuple = (DOWNLOAD_SERVER_IP, DOWNLOAD_SERVER_PORT), timeout: float = 10.0 ) -> OrderedDict[int, list[KlineRecord]]:
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
