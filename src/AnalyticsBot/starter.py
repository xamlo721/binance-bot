import time
import asyncio
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

from AnalyticsBot.downloader import download_current_1m_Candles
from AnalyticsBot.downloader import download_more_candles

from ramstorage.AlertRecord import AlertRecord
from ramstorage.CandleRecord import CandleRecord
from ramstorage.HoursRecord import HoursRecord

from ramstorage.ram_storage_utils import get_recent_alerts
from ramstorage.ram_storage_utils import get_recent_1m_klines
from ramstorage.ram_storage_utils import get_recent_1h_klines
from analytic_utils import calculate_1h_dynamic
from analytic_utils import update_current_alert
from analytic_utils import agregate_12h_records
from analytic_utils import calculate_1h_records
from analytic_utils import aggregate_10h_volumes
from analytic_utils import calculate_10m_volumes_sidedWindow

from binance_utils.my_binance_utils import get_trading_symbols

from ramstorage.ram_storage_utils import get_recent_1m_klines
from ramstorage.ram_storage_utils import save_10m_volumes
from ramstorage.ram_storage_utils import save_klines_to_ram
from ramstorage.ram_storage_utils import is_storage_consistent

from ramstorage.ram_storage_utils import candle_1m_records
from ramstorage.ram_storage_utils import candle_1h_records


def getTrackedTickers() -> list[str]:
    symbols = get_trading_symbols()
    if not symbols:
        return []
        
    return symbols

def doTick():
    """
    Функция ежеминутного тика
    """

    logger.info("    # ====================== doTick ========================= #")
    logger.info("    Обновляем список тикеров...")
    # TODO: Необходимо отработать моменты, когда отслеживаемые тикеры закрываются для торговли
    trackable_tickers: list[str] = getTrackedTickers()

    logger.info(f"✅ Скачивание данные свеч за последнюю минуту...")

    # ======================================================= # 
    klines_1m_full = asyncio.run(download_more_candles(trackable_tickers, MINUTE_CANDLES_LIMIT, datetime.now()))
    if klines_1m_full:
        for candles in klines_1m_full:
            save_klines_to_ram(candles)
            open_time_dt = datetime.fromtimestamp(candles[0].open_time / 1000)
            logger.info(f"Сохранена минута {open_time_dt}." )
    else:
        logger.warning("❌ Не удалось загрузить исторические данные")

    # ======================================================= # 

    calculate_1h_records(candle_1m_records, candle_1h_records)
    # ======================================================= # 

    if calculate_10m_volumes_sidedWindow():
        logger.info(f"✅ Обновление 10м интервалов объёмов успешно.")

    # ======================================================= # 


    # Количество обрабатываемых файлов равно текущей минуте
    FILES_TO_WORK = datetime.now().minute
    # В 00 минут обрабатываем 1 самый новый файл
    if FILES_TO_WORK == 0:
        FILES_TO_WORK = 1

    # Получаем последние FILES_TO_WORK минутных свеч
    current_candles: list[list[CandleRecord]] = get_recent_1m_klines(FILES_TO_WORK)

    # Обновляем новые записи в глобальное хранилище
    dynamic_candles: Optional[list[HoursRecord]] = calculate_1h_dynamic(current_candles)
    if dynamic_candles is None:
        logger.error("Не удалось рассчитать динамические данные")
        return False  
    # ======================================================= # 


    count = AGR_H_COUNT
    MINUTE = datetime.now().minute
    if MINUTE == 0:
        count += 1

    # Получаем самые свежие часовые файлы
    h1_records: list[list[HoursRecord]] = get_recent_1h_klines(count)

    if not h1_records:
        logger.error("Не найдено часовых файлов для обработки")
        return
    
    if count == 11:
        h1_records.append(dynamic_candles)

    # symbol,high
    # 0GUSDT,0.6953999996185303
    # 2ZUSDT,0.07882999628782272
    # ACEUSDT,0.1712000072002411
    max_highs = agregate_12h_records(h1_records)

    if max_highs is None:
        logger.error("Обработка 12 часовых файлов прошла с ошибкой")
        return
    
    # ======================================================= # 

    # Агрегация объемов из 10 последних файлов (исключая самый свежий)
    h10_records: list[list[HoursRecord]] = get_recent_1h_klines(10)
    vol_10h: Optional[Dict[str, Dict[str, float]]] = aggregate_10h_volumes(dynamic_candles, h10_records)


    # ======================================================= # 
    # "total_volume_24h.py" -  при появлении нового файла в папке "/srv/ftp/Bot_v2/Data/K_lines/Dynamic" 
    # - берёт этот файл, + 23 часовых файла из папки "/srv/ftp/Bot_v2/Data/K_lines/1H"  
    # и суммирует объёмы торгов по каждому тикеру

    # В настоящее время данные из этого скрипта не используются в скрипте анализатора
    # (А задумка была в отсеивании тикеров, которые имеют слишком маленький объём торгов)
    # Результат сохраняет в папку "/srv/ftp/Bot_v2/Data/Total_volume_24H"
    # Скрипт автономный и запускается скриптом "starter.py", а далее самостоятельно отслеживает появление новых файлов в папке "/srv/ftp/Bot_v2/Data/K_lines/Dynamic"  и при появлении свежего файла - делает обработку



    # ======================================================= # 
    # "level_breakout_12h.py" -  скрипт, отслеживающий пробой уровня
    # При появлении нового файла в папке "/srv/ftp/Bot_v2/Data/K_lines/1M" - сравнивает значения максимума цены для каждого тикера с максимумом значения цены в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/Agr_12h"
    # Если по какому либо тикеру цена в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/K_lines/1M"  - выше чем цена по этому тикеру в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/Agr_12h" - создаёт в папке "/srv/ftp/Bot_v2/Data/Ticker_up" - файл со списком этих тикеров
    # если файлов результата становится более чем 2 - удаляет самый старый
    # Скрипт автономный и запускается скриптом "starter.py", а далее самостоятельно отслеживает появление новых файлов в папке "/srv/ftp/Bot_v2/Data/K_lines/1M"  и при появлении свежего файла - делает обработку

    # process_level_breakout_12h()


    # ======================================================= # 
    # "ticker_analytics.py" - при появлении нового файла в папке "/srv/ftp/Bot_v2/Data/Ticker_up" - по каждому тикеру из этого файла делает вычисления:
    # Берёт объём для этого тикера из самого свежего файла в папке "/srv/ftp/Bot_v2/Data/Volume_10M"
    # домножает его на 6 (мы получаем примерный объём торгов за будующий час, в случае, если такой объём торгов сохранится) (для удобства назову его тут "Z")
    # Далее сравнивает его с объёмами для этого тикера в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/Volume_10H"
    # И если "Z" - превышает более чем в 5 раз (!) каждый объём для данного тикера в самом свежем файле в папке "/srv/ftp/Bot_v2/Data/Volume_10H" - то выводим уведомление в телегу и записываем такие тикеры в папку "/srv/ftp/Bot_v2/Data/Alerts"
    # (Один файл в папке "/srv/ftp/Bot_v2/Data/Alerts" - соответствует одним суткам. При наступлении новых суток по utc - при появлении файла с сигналами - создаётся новый файл алертсов для уже новых суток и постепеноо дополняется до наступления новых суток по utc)
    # Скрипт автономный и запускается скриптом "starter.py", а далее самостоятельно отслеживает появление новых файлов в папке "/srv/ftp/Bot_v2/Data/Ticker_up"  и при появлении свежего файла - делает обработку


    # alerts copy
    alert: list[AlertRecord] = get_recent_alerts(1)
    update_current_alert(alert)

    return


def start():
    try:
        logger.info("Скрипт-стартер запущен.")
        logger.info(f"    Получаю список актуальных тикеров...")

        # TODO: Необходимо отработать моменты, когда отслеживаемые тикеры закрываются для торговли
        trackable_tickers: list[str] = getTrackedTickers()
        if len(trackable_tickers) == 0:
            logger.error("❌ Не удалось получить список тикеров")
            return
        logger.info(f"✅ Найдено торгующихся тикеров: {len(trackable_tickers)}")

        logger.info(f"✅ Запущено предварительное скачивание архивных данных {MINUTE_CANDLES_LIMIT} минутных свеч...")
        
        download_start_time = time.time()
        klines_1m_full: List[List[CandleRecord]] = asyncio.run(download_more_candles(trackable_tickers, MINUTE_CANDLES_LIMIT, datetime.now()))
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
            sub_klines: List[List[CandleRecord]] = asyncio.run(download_more_candles(trackable_tickers, int(duration_minutes), datetime.now()))
            download_stop_time = time.time()
            logger.info(f"✅ Скачивание завершено.")
            klines_1m_full.extend(sub_klines)

            # Пересчитываем количество минут, которые ещё нужно скачать
            duration_minutes = (download_stop_time - download_start_time) / 60

        logger.info(f"✅ Актуальные архивные данные за {len(klines_1m_full)} минут получены.")

        if klines_1m_full:
            for candles in klines_1m_full:
                save_klines_to_ram(candles)
                open_time_dt = datetime.fromtimestamp(candles[0].open_time / 1000)
                logger.info(f"Сохранена минута {open_time_dt}." )
        else:
            logger.warning("❌ Не удалось загрузить исторические данные")
        logger.info(f"Записываю данные в хранилище...")


        logger.info(f"Запускаю проверку хранилища на консистентность...")

        if not is_storage_consistent():
            logger.error("Список candle_1m_records не содержит непрерывный диапазон минутных свечей.")
            return
        logger.info(f"✅ Проверка хранилища успешно пройдена.")

        logger.info(f"    Запускаю основной аналитический цикл анализа...")

        while True:
            
            doTick()

            time.sleep(1)

    except KeyboardInterrupt:
        logger.warning("Получен сигнал прерывания...")


start()

