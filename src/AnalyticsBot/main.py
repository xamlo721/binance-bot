import time
import asyncio
import requests
from pathlib import Path

import sys

from logger import logger
from config import *

src_path = Path(__file__).resolve().parent.parent
analytics_bot_src_path = Path(__file__).resolve().parent
sys.path.append(str(src_path))
sys.path.append(str(analytics_bot_src_path))
logger.warning(src_path)
logger.warning(analytics_bot_src_path)

from datetime import datetime

from typing import Optional
from typing import Dict
from typing import List
from collections import OrderedDict

from bot_types import AlertRecord
from bot_types import KlineRecord
from bot_types import HoursRecord
from bot_types import Volume_10m

from AnalyticsBot.storage_utils import get_recent_1m_klines
from AnalyticsBot.storage_utils import get_recent_1h_klines
from AnalyticsBot.storage_utils import get_1m_candles
from AnalyticsBot.storage_utils import get_recent_1m_klines
from AnalyticsBot.storage_utils import save_1h_records
from AnalyticsBot.storage_utils import save_klines_to_ram
from AnalyticsBot.storage_utils import is_storage_consistent

from analytic_utils import calculate_10m_volumes_slidedWindow
from analytic_utils import calculate_1h_records
from analytic_utils import calculate_volumes_slidedWindow
from analytic_utils import calculate_prices_slidedWindow
from analytic_utils import check_price_overlimit
from analytic_utils import check_volume_overlimit

from downloader import download_candles

def download_candles_reccursively(trackable_tickers: list[str], minutes: int) -> OrderedDict[int, list[KlineRecord]]:
    logger.info(f"✅ Запущено предварительное скачивание архивных данных {minutes} минутных свеч...")
    download_start_time = time.time()
    klines_1m_full: OrderedDict[int, list[KlineRecord]] = asyncio.run(download_candles(trackable_tickers, minutes, datetime.now()))
    download_stop_time = time.time()
    logger.info(f"✅ Скачивание завершено.")

    # Если скачивание шло несколько минут, то прошедшие минуты тоже надо докачать
    duration_minutes: float = (download_stop_time - download_start_time)/60   
    while int(duration_minutes) > 0:
        duration_seconds = download_stop_time - download_start_time
        logger.warning(f"⚠️ Скачивание заняло {duration_seconds:.2f} секунд.")
        logger.warning(f"⚠️ За это время уже сформировалось {int(duration_minutes) } минутных свечей.")
        logger.warning( "⚠️ Необходимо запустить скачивание оставшихся свечей.")

        # Запускаем второй этап скачивания
        logger.info(f"✅ Запущено предварительное скачивание архивных данных {int(duration_minutes)} минутных свеч...")
        download_start_time = time.time()
        sub_klines: OrderedDict[int, list[KlineRecord]] = asyncio.run(download_candles(trackable_tickers, int(duration_minutes), datetime.now()))
        download_stop_time = time.time()
        logger.info(f"✅ Скачивание завершено.")

        for minute, records in sub_klines.items():
            klines_1m_full[minute] = records

        # Пересчитываем количество минут, которые ещё нужно скачать
        duration_minutes = (download_stop_time - download_start_time) / 60
    
    return klines_1m_full

def get_trading_symbols():
    """Получение списка торгующихся тикеров"""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url)
        data = response.json()
        
        symbols = []
        for symbol_info in data['symbols']:
            if (symbol_info['status'] == 'TRADING' and symbol_info.get('contractType') == 'PERPETUAL'):
                # Исключаем тикеры с "USDC"
                symbol_name = symbol_info['symbol']
                if not symbol_name.startswith("USDC") and not symbol_name.endswith("USDC"):
                    symbols.append(symbol_name)
        
        return symbols
    except Exception as e:
        logger.error(f"Ошибка при получении списка тикеров: {str(e)}")
        return []
    
def getTrackedTickers() -> list[str]:
    symbols = get_trading_symbols()
    if not symbols:
        return []
    
    # Берём только первые 10 тикеров (для дебага)
    # symbols = symbols[:10]    

    # for s in symbols:
    #     print(s)

    return symbols#[:500] 
    # return [
    #     "BTCUSDT", 
    #     "ETHUSDT",
    #     "BCHUSDT",
    #     "XRPUSDT",
    #     "LTCUSDT",
    #     "TRXUSDT",
    #     "ETCUSDT",
    #     "XLMUSDT",
    #     "ADAUSDT",
    #     "XMRUSDT",
    #     "ZECUSDT",
    #     "XTZUSDT",
    #     "BNBUSDT"
    # ]

def doTick():
    """
    Функция ежеминутного тика
    """

    logger.info("    # ====================== doTick ========================= #")
    logger.info("    Обновляем список тикеров...")
    # TODO: Необходимо отработать моменты, когда отслеживаемые тикеры закрываются для торговли
    trackable_tickers: list[str] = getTrackedTickers()
    if len(trackable_tickers) == 0:
        logger.error("❌ Не удалось получить список тикеров")
        return
    logger.info(f"✅ Найдено торгующихся тикеров: {len(trackable_tickers)}")

    # ======================================================= # 

    recent_klines = get_recent_1m_klines(1)          # последние 1 минута
    if recent_klines:
        last_minute_number = max(recent_klines.keys())
        # -1 стоит, потому что в свеча с номером текущей минутой закрывается по мнению бинанса в 00 следующей минуты
        # и чтобы не ломать психику логике ботов, проще тут это учесть
        current_minute_number = int(datetime.now().timestamp() / 60) - 1

        if last_minute_number != current_minute_number:          # более чем 1 минута разницы
            missing_minutes = min(current_minute_number - last_minute_number, MAX_CACHED_CANDLES)
            logger.info(f"Найдены {missing_minutes} недостающих минут. Скачиваем...")

            klines_missing: OrderedDict[int, list[KlineRecord]] = download_candles_reccursively(trackable_tickers, missing_minutes)

            if klines_missing:
                save_klines_to_ram(klines_missing)
            else:
                logger.warning("❌ Не удалось загрузить недостающие минутные свечи")
    # ======================================================= # 

    logger.info(f"Запускаю проверку хранилища на консистентность...")
    storage_klines: OrderedDict[int, list[KlineRecord]] = get_1m_candles()
    if not is_storage_consistent(storage_klines):
        logger.error("Список candle_1m_records не содержит непрерывный диапазон минутных свечей.")
        return
    
    logger.info(f"✅ Проверка хранилища успешно пройдена.")
    # ======================================================= # 

    logger.info(f"Обновляю скользящие 10м объёмы...")
    klines_1m: OrderedDict[int, list[KlineRecord]] = get_recent_1m_klines(MAX_CACHED_CANDLES)
    if (len(klines_1m) < 10):
        logger.error(f"❌ Найдено только {len(klines_1m)} минутных свечей в хранилище.")
        logger.error(f"❌ Нужно хотя бы 10. Пропускаем тик.")
        return
    
    volumes_10m: Optional[List[Volume_10m]] = calculate_10m_volumes_slidedWindow(klines_1m)
    if volumes_10m is None:
        logger.error(f"❌ Ошибка вычисления 10м объёмов. Пропускаем тик.")
        return

    logger.info(f"✅ Обновление 10м интервалов объёмов успешно. Получилось {len(volumes_10m)} маркеров")


    # ======================================================= # 
    logger.info(f"Обновляю скользящую часовую статистику...")

    hours_statistic: Optional[OrderedDict[int, list[HoursRecord]]] = calculate_1h_records(storage_klines)
    if hours_statistic is None:
        logger.error(f"❌ Ошибка вычисления часовой статистики. Пропускаем тик.")
        return
    
    if not save_1h_records(hours_statistic):
        logger.error(f"❌ Ошибка сохранении часовой статистики в RAM. Пропускаем тик.")
        return

    logger.info(f"✅ Обновление часовой статистики успешно. Получилось {len(hours_statistic)} отметок")

    # ======================================================= # 
    logger.info(f"Обновляю 10ти часовое скользящее окно объёмов...")

    # Получаем самые свежие часовые файлы
    h1_records: dict[int, list[HoursRecord]] = get_recent_1h_klines(10)

    if h1_records is None or len(h1_records) != 10:
        logger.error("❌ Не найдено часовых файлов для обработки скользящего окна 10ч. Пропускаем тик.")
        return
    
    volumes_10h: Optional[Dict[str, Dict[str, float]]] = calculate_volumes_slidedWindow(h1_records, 10)

    if volumes_10h is None:
        logger.error(f"❌ Ошибка вычисления 10 часовой статистики. Пропускаем тик.")
        return

    logger.info(f"✅ Обновление 10 часовой статистики успешно. Получилось {len(volumes_10h)} отметок")
    # ======================================================= # 
    logger.info(f"Обновляю 12ти часовое скользящее окно цен...")

    h1_records: dict[int, list[HoursRecord]] = get_recent_1h_klines(12)

    if h1_records is None or len(h1_records) != 12:
        logger.error("❌ Не найдено часовых файлов для обработки скользящего окна 10ч. Пропускаем тик.")
        return
    
    max_highs: Optional[Dict[str, float]] = calculate_prices_slidedWindow(h1_records, 12)

    if max_highs is None:
        logger.error("❌ Обработка 12 часовых файлов прошла с ошибкой. Пропускаем тик.")
        return
    
    logger.info(f"✅ Обновление 12 часовой статистики успешно. Получилось {len(max_highs)} отметок")
    # ======================================================= # 
    # "level_breakout_12h.py" -  скрипт, отслеживающий пробой уровня
    # При появлении нового файла в папке "/srv/ftp/Bot_v2/Data/K_lines/1M" - сравнивает значения максимума цены для каждого тикера с максимумом значения цены в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/Agr_12h"
    # Если по какому либо тикеру цена в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/K_lines/1M"  - выше чем цена по этому тикеру в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/Agr_12h" - создаёт в папке "/srv/ftp/Bot_v2/Data/Ticker_up" - файл со списком этих тикеров
    # если файлов результата становится более чем 2 - удаляет самый старый
    # Скрипт автономный и запускается скриптом "starter.py", а далее самостоятельно отслеживает появление новых файлов в папке "/srv/ftp/Bot_v2/Data/K_lines/1M"  и при появлении свежего файла - делает обработку
    # ======================================================= # 
    logger.info(f"Проверяю превышение максимумов цен...")

    # Список тикеров, у которых превышен лимит на цены
    price_overlimit_tickers = []

    last_minute_key = max(storage_klines.keys())
    last_minute_candles = storage_klines[last_minute_key]
    price_alerts: Optional[dict[str, float]] = check_price_overlimit(last_minute_candles, max_highs)

    if price_alerts is None:
        logger.error("✅ Проверка закончена. Не зафиксировано превышений.")
    else:
        price_overlimit_tickers = [candle for candle in last_minute_candles if candle.symbol in price_alerts]
        logger.info(f"✅ Проверка закончена. Зафиксировано {len(price_overlimit_tickers)} превышений")
    # ======================================================= # 
    logger.info(f"Проверяю превышение максимумов объёмов...")

    volume_alerts: Optional[dict[str, float]] = check_volume_overlimit(price_overlimit_tickers, volumes_10m, volumes_10h)

    # Список тикеров, у которых превышен лимит и на цены и на объёмы
    overlimit_tickers = []

    if volume_alerts is None:
        logger.error("✅ Проверка закончена. Не зафиксировано превышений.")
    else:
        overlimit_tickers = [candle for candle in last_minute_candles if candle.symbol in volume_alerts]
        logger.info(f"✅ Проверка закончена. Зафиксировано {len(overlimit_tickers)} превышений")
    # ======================================================= # 

    return

def start():
    try:
        logger.info("Скрипт-стартер запущен.")
        logger.info(f"Получаю список актуальных тикеров...")

        # TODO: Необходимо отработать моменты, когда отслеживаемые тикеры закрываются для торговли
        trackable_tickers: list[str] = getTrackedTickers()
        if len(trackable_tickers) == 0:
            logger.error("❌ Не удалось получить список тикеров")
            return
        logger.info(f"✅ Найдено торгующихся тикеров: {len(trackable_tickers)}")

        
        klines_1m_full: OrderedDict[int, list[KlineRecord]] = download_candles_reccursively(trackable_tickers, MAX_CACHED_CANDLES)

        logger.info(f"✅ Актуальные архивные данные за {len(klines_1m_full)} минут получены.")

        if klines_1m_full:
            save_klines_to_ram(klines_1m_full)
        else:
            logger.warning("❌ Не удалось загрузить исторические данные")
        logger.info(f"Записываю данные в хранилище...")


        storage_klines: dict[int, list[KlineRecord]] = get_1m_candles()
        logger.info(f"Запускаю проверку хранилища на консистентность...")
        if not is_storage_consistent(storage_klines):
            logger.error("Список candle_1m_records не содержит непрерывный диапазон минутных свечей.")
            return
        logger.info(f"✅ Проверка хранилища успешно пройдена.")

        logger.info(f"Запускаю основной аналитический цикл анализа...")

        while True:
            start_time = time.time()

            doTick()

            # Вычисляем сколько осталось ждать
            elapsed = time.time() - start_time
            wait_time = max(0, 60 - elapsed)  # минимум 0 секунд

            logger.info(f"Function took {elapsed:.2f}s, waiting {wait_time:.2f}s")

            if wait_time > 0:
                time.sleep(wait_time)

    except KeyboardInterrupt:
        logger.warning("Получен сигнал прерывания...")


start()

