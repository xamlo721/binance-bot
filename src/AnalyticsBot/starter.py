import time
import asyncio
from datetime import datetime

from typing import Optional
from typing import Dict

from k_line_downloader import download_current_1m_Candles
from k_line_downloader import download_more_candles

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

from logger import logger
from config import *

from ramstorage.ram_storage_utils import get_recent_1m_klines
from ramstorage.ram_storage_utils import save_10m_volumes

from ramstorage.ram_storage_utils import candle_1m_records
from ramstorage.ram_storage_utils import candle_1h_records

def update_10m_volumes() -> bool:
    # Получаем 10 самых свежих файлов
    latest_1m_klines = get_recent_1m_klines(10)
    
    if len(latest_1m_klines) < 10:
        logger.warning(f"Найдено только {len(latest_1m_klines)} файлов, нужно 10. Пропускаем.")
        return False
    
    # Рассчитываем объемы
    volumes: dict[str, float] = {} 
    for candle_list in latest_1m_klines:
        for candle in candle_list:
            volumes[candle.symbol] = volumes.get(candle.symbol, 0) + candle.quote_assets_volume

    # Сохраняем результат
    save_10m_volumes(volumes)
    return True


def doTick():

    logger.info(f"✅ Запущено скачивание данных 1 минут свеч...")
    asyncio.run(download_current_1m_Candles())
    # ======================================================= # 

    calculate_1h_records(candle_1m_records, candle_1h_records)
    # ======================================================= # 

    if update_10m_volumes():
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



    # ======================================================= # 



    # ======================================================= # 



    # ======================================================= # 


    # alerts copy
    alert: list[AlertRecord] = get_recent_alerts(1)
    update_current_alert(alert)

    return


try:
    logger.info("Скрипт-стартер запущен. Фоновые процессы активны.")

    logger.info(f"Запущено скачивание данных 60 минутных свеч...")
    asyncio.run(download_more_candles(MINUTE_CANDLE_FILE_LIMIT, datetime.now()))
    logger.info(f"✅ Скачивание завершено.")

    logger.info(f"Запускаю основной цикл анализа...")
    while True:
        
        doTick()

        time.sleep(1)

except KeyboardInterrupt:
    logger.warning("Получен сигнал прерывания...")
