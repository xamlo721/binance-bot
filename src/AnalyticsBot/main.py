import time
import asyncio
from pathlib import Path

import sys

src_path = Path(__file__).resolve().parent.parent
analytics_bot_src_path = Path(__file__).resolve().parent
sys.path.insert(0, str(src_path))
sys.path.insert(0, str(analytics_bot_src_path))

from AnalyticsBot.logger import logger
from AnalyticsBot.config import *

logger.info(str(f"src_path = {src_path}"))
logger.info(str(f"analytics_bot_src_path = {analytics_bot_src_path}"))

from datetime import datetime

from typing import Optional
from typing import Dict
from typing import List
from collections import OrderedDict

from AnalyticsBot.bot_types import AlertRecord
from AnalyticsBot.bot_types import KlineRecord
from AnalyticsBot.bot_types import HoursRecord
from AnalyticsBot.bot_types import Volume_10m

from AnalyticsBot.storage_utils import get_recent_1m_klines
from AnalyticsBot.storage_utils import get_recent_1h_klines
from AnalyticsBot.storage_utils import get_1m_candles
from AnalyticsBot.storage_utils import get_recent_1m_klines
from AnalyticsBot.storage_utils import save_1h_records
from AnalyticsBot.storage_utils import save_klines_to_ram
from AnalyticsBot.storage_utils import is_storage_consistent

from AnalyticsBot.analytic_utils import validate_ticker
from AnalyticsBot.analytic_utils import calculate_10m_volumes_slidedWindow
from AnalyticsBot.analytic_utils import isWindow10mValid
from AnalyticsBot.analytic_utils import calculate_1h_records
from AnalyticsBot.analytic_utils import calculate_volumes_slidedWindow
from AnalyticsBot.analytic_utils import calculate_prices_slidedWindow
from AnalyticsBot.analytic_utils import check_price_overlimit
from AnalyticsBot.analytic_utils import check_volume_overlimit

from AnalyticsBot.downloader import download_candles
from AnalyticsBot.downloader import get_server_time_diff
from AnalyticsBot.downloader import get_trading_symbols_from_server

from AnalyticsBot.alert_server import *
from AnalyticsBot.alert_server_thread import *

alert_thread = AlertServerThread()

def download_candles_reccursively(servertime_ms: int, trackable_tickers: list[str], minutes: int) -> OrderedDict[int, list[KlineRecord]]:
    logger.info(f"✅ Запущено предварительное скачивание архивных данных {minutes} минутных свеч...")
    download_start_time = time.time()
    end_time = datetime.fromtimestamp(servertime_ms / 1000.0)
    klines_1m_full: OrderedDict[int, list[KlineRecord]] = asyncio.run(download_candles(trackable_tickers, minutes, end_time))
    download_stop_time = time.time()
    logger.info(f"✅ Скачивание завершено.")

    # Если скачивание шло несколько минут, то прошедшие минуты тоже надо докачать
    duration_minutes: float = (download_stop_time - download_start_time)/60   
    while int(duration_minutes) > 0:
        duration_seconds = download_stop_time - download_start_time
        logger.warning(f"⚠️ Скачивание заняло {duration_seconds:.2f} секунд.")
        logger.warning(f"⚠️ За это время уже сформировалось {int(duration_minutes) } минутных свечей.")
        logger.warning( "⚠️ Необходимо запустить скачивание оставшихся свечей.")

        # Обновляем серверное время, добавляя прошедшее время
        new_servertime_ms = servertime_ms + int((download_stop_time - download_start_time) * 1000)
        end_time = datetime.fromtimestamp(new_servertime_ms / 1000.0)

        # Запускаем второй этап скачивания
        logger.info(f"✅ Запущено предварительное скачивание архивных данных {int(duration_minutes)} минутных свеч...")
        download_start_time = time.time()
        sub_klines: OrderedDict[int, list[KlineRecord]] = asyncio.run(download_candles(trackable_tickers, int(duration_minutes), end_time))
        download_stop_time = time.time()
        logger.info(f"✅ Скачивание завершено.")

        for minute, records in sub_klines.items():
            klines_1m_full[minute] = records

        # Пересчитываем количество минут, которые ещё нужно скачать
        duration_minutes = (download_stop_time - download_start_time) / 60
    
    return klines_1m_full

    
def getTrackedTickers() -> list[str]:
    symbols = get_trading_symbols_from_server()
    if not symbols:
        return []
    
    # Берём только первые 10 тикеров (для дебага)
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

def doTick(servertime_ms: int):
    """
    Функция ежеминутного тика
    """

    # ====================== Step 1 ========================= #
    # Подключаемся к downloader-у и скачиваем оттуда список доступных торговых пар.
    #
    # ======================================================= # 
    logger.debug("Обновляем список тикеров...")
    # TODO: Необходимо отработать моменты, когда отслеживаемые тикеры закрываются для торговли
    binance_trackable_tickers: list[str] = getTrackedTickers()
    if len(binance_trackable_tickers) == 0:
        logger.error("❌ Не удалось получить список тикеров")
        return
    logger.debug(f"✅ Найдено торгующихся тикеров: {len(binance_trackable_tickers)}")
    # ====================== Step 2 ========================= #
    # Подключаемся к серверу и скачиваем оттуда недостающие свечи.
    # Скачивание идёт до тех пор, пока мы не получим свечи текущей закрытой минуты.
    # Т.е если сейчас 12:45:15, то мы должны получить свечу за 12:44. Свеча 12:45 ещё формируется 
    #
    # ======================================================= # 
    logger.debug("Получаем из хранилища последнюю сохранеённую минуту...")
    recent_klines = get_recent_1m_klines(1)          # последние 1 минута
    if recent_klines:
        last_minute_number = max(recent_klines.keys())
        # -1 стоит, потому что в свеча с номером текущей минутой закрывается по мнению бинанса в 00 следующей минуты
        # и чтобы не ломать психику логике ботов, проще тут это учесть
        current_minute_number = int(servertime_ms / 60) - 1
        logger.debug(f"Последняя сохранённая минута: {last_minute_number}. Текущая минута {current_minute_number}")
        logger.debug(f"Всего в хранилище содержится {len(get_1m_candles())} записей")

        if last_minute_number != current_minute_number:          # более чем 1 минута разницы
            missing_minutes = min(current_minute_number - last_minute_number, MAX_CACHED_CANDLES)
            logger.info(f"Найдены {missing_minutes} недостающих минут. Скачиваем...")

            # Как быстро ты скачаешь 60-80мб?
            klines_missing: OrderedDict[int, list[KlineRecord]] = download_candles_reccursively(servertime_ms, binance_trackable_tickers, missing_minutes)

            if klines_missing:
                save_klines_to_ram(klines_missing)
            else:
                logger.warning("❌ Не удалось загрузить недостающие минутные свечи")
        else:
            logger.debug(f"Докачивать данные не требуется.")

    else:
        logger.error(f"❌ Не удалось получить минутные свечи из хранилища для первичного анализа.")
        logger.error(f"❌ Попытка восстановить хранилище с помщью сети. Скачиваем  {MAX_CACHED_CANDLES} минут по всем тикерам.")

        # Как быстро ты скачаешь 60-80мб?
        klines_missing: OrderedDict[int, list[KlineRecord]] = download_candles_reccursively(servertime_ms, binance_trackable_tickers, MAX_CACHED_CANDLES)

        if klines_missing:
            save_klines_to_ram(klines_missing)
            logger.info(f"✅ Восстановление успешно, данные сохранены.")
        else:
            logger.warning("❌ Не удалось загрузить недостающие минутные свечи. Функционирование невозможно.")
            return
    # ====================== Step 3 ========================= #
    # Проверка хранилища на валидность.
    # На этом этапе надо отсеить все некорректные записи.
    # Некорректными считаются те записи, по которым нет валидных свечей за период MAX_CACHED_CANDLES
    # Дальнейшая аналитика проводится ботом только по валидным записям.
    # 
    # TODO: Необходим механизм перезапроса у Download сервера данных по указанным свечам, 
    # TODO: Либо пометка их как временно невалидных. 
    # TODO: (делистнули или наоброт залистили несколько часов назад)
    #
    # ======================================================= # 
    logger.info(f"Запускаю проверку хранилища на консистентность...")
    raw_klines: OrderedDict[int, list[KlineRecord]] = get_1m_candles()
    validated_klines = validate_ticker(raw_klines)    # применяем фильтрацию
    save_klines_to_ram(validated_klines)
    logger.info(f"Проведена валидация хранилища. Пригодно {len(validated_klines)} торговых пар для составления аналитики.")

    logger.debug(f"Проверяем хранили на консистентность...")
    if not is_storage_consistent(validated_klines):
        logger.error("Список candle_1m_records не содержит непрерывный диапазон минутных свечей.")
        return
    
    logger.debug(f"✅ Проверка хранилища успешно пройдена.")
    # ====================== Step 4 ========================= #
    # Расчёт 10ти минутного скользящего окна объёмов.
    # Защитный интервал не применяется.
    # После вычисления окна, проверяется, что скользящее окно посчиталось 
    # по всем тикерам, что мы туда отправили.
    #
    # ======================================================= # 
    logger.debug(f"Обновляю скользящие 10м объёмы...")
    klines_1m: OrderedDict[int, list[KlineRecord]] = get_recent_1m_klines(MAX_CACHED_CANDLES)
    if (len(klines_1m) < 10):
        logger.error(f"❌ Найдено только {len(klines_1m)} минутных свечей в хранилище.")
        logger.error(f"❌ Нужно хотя бы 10. Пропускаем тик.")
        return

    volumes_10m: Optional[List[Volume_10m]] = calculate_10m_volumes_slidedWindow(klines_1m)
    if volumes_10m is None:
        logger.error(f"❌ Ошибка вычисления 10м объёмов. Пропускаем тик.")
        return
    
    # Проверка валидности окна: должны быть данные по всем валидным тикерам
    # Последняя минута после валидации (validated_klines) содержит все актуальные тикеры
    last_minute = max(validated_klines.keys())
    expected_tickers = [c.symbol for c in validated_klines[last_minute]]

    if not isWindow10mValid(volumes_10m, expected_tickers):
        logger.error("❌ Скользящее окно 10м объёмов не содержит все тикеры. Пропускаем тик.")
        return
    
    logger.debug(f"✅ Обновление 10м интервалов объёмов успешно. Получилось {len(volumes_10m)} маркеров")
    # ====================== Step 5 ========================= #
    # Расчёт часовых свечей (записей) окон для всех валидных тикеров.
    # Защитный интервал не применяется.
    #
    # TODO: Нужно покрыть тестами этот этап
    #
    # ======================================================= # 
    logger.debug(f"Обновляю скользящую часовую статистику...")

    hours_statistic: Optional[OrderedDict[int, list[HoursRecord]]] = calculate_1h_records(validated_klines)
    if hours_statistic is None:
        logger.error(f"❌ Ошибка вычисления часовой статистики. Пропускаем тик.")
        return
    
    if not save_1h_records(hours_statistic):
        logger.error(f"❌ Ошибка сохранении часовой статистики в RAM. Пропускаем тик.")
        return

    logger.debug(f"✅ Обновление часовой статистики успешно. Получилось {len(hours_statistic)} отметок")
    # ====================== Step 6 ========================= #
    # Расчёт HOURS_VOLUMES_SLIDED_WINDOW_PERIOD часовое скользящее окона объёмов для всех валидных тикеров.
    # Защитный интервал состовляет HOURS_VOLUMES_PROTECTIVE_INTERVAL минут.
    # Для корректного расчёта необходимо иметь в хранилище:
    # HOURS_VOLUMES_SLIDED_WINDOW_PERIOD * 60 + HOURS_VOLUMES_PROTECTIVE_INTERVAL минут записей по всем валидным тикерам.
    #
    # TODO: Нужно дописать проверки количества тикеров в хранилище.
    # FIXME: Не функционирует защитный интервал
    #
    # ======================================================= # 
    logger.debug(f"Обновляю часовое скользящее окно объёмов...")

    # Получаем самые свежие часовые файлы
    h1_records: dict[int, list[HoursRecord]] = get_recent_1h_klines(HOURS_VOLUMES_SLIDED_WINDOW_PERIOD)

    if h1_records is None or len(h1_records) != HOURS_VOLUMES_SLIDED_WINDOW_PERIOD:
        logger.error("❌ Не найдено часовых файлов для обработки часового скользящего окна объёмов. Пропускаем тик.")
        return
    
    volumes_10h: Optional[Dict[str, Dict[str, float]]] = calculate_volumes_slidedWindow(h1_records, HOURS_VOLUMES_SLIDED_WINDOW_PERIOD)

    if volumes_10h is None:
        logger.error(f"❌ Ошибка вычисления часового скользящего окна объёмов. Пропускаем тик.")
        return

    logger.debug(f"✅ Обновление часового скользящего окна объёмов успешно. Получилось {len(volumes_10h)} отметок")
    # ====================== Step 7 ========================= #
    # Расчёт HOURS_PRICES_SLIDED_WINDOW_PERIOD часовых скользящих окон для всех валидных тикеров.
    # Защитный интервал состовляет HOURS_PRICES_PROTECTIVE_INTERVAL минут.
    # Для корректного расчёта необходимо иметь в хранилище:
    # HOURS_PRICES_SLIDED_WINDOW_PERIOD * 60 + HOURS_PRICES_PROTECTIVE_INTERVAL минут записей по всем валидным тикерам.
    #
    # TODO: Нужно дописать проверки количества тикеров в хранилище.
    # FIXME: Не функционирует защитный интервал
    #
    # ======================================================= # 
    logger.debug(f"Обновляю часовое скользящее окно цен...")

    h1_records: dict[int, list[HoursRecord]] = get_recent_1h_klines(HOURS_PRICES_SLIDED_WINDOW_PERIOD)

    if h1_records is None or len(h1_records) != HOURS_PRICES_SLIDED_WINDOW_PERIOD:
        logger.error("❌ Не найдено часовых файлов для обработки скользящего окна цен. Пропускаем тик.")
        return
    
    max_highs: Optional[Dict[str, float]] = calculate_prices_slidedWindow(h1_records, HOURS_PRICES_SLIDED_WINDOW_PERIOD)

    if max_highs is None:
        logger.error("❌ Обработка ценового часового окна прошла с ошибкой. Пропускаем тик.")
        return
    
    logger.debug(f"✅ Обновление часового скользящего окна успешно. Получилось {len(max_highs)} отметок")
    # ====================== Step 8 ========================= #
    # Проверка превышения текущих отметок цен над верхним порогом скользящего часового окна цен
    # 
    # TODO: Нужно проверить тестами на валидность
    #
    # ======================================================= # 
    logger.debug(f"Проверяю превышение максимумов цен...")

    # Список тикеров, у которых превышен лимит на цены
    price_overlimit_tickers = []

    last_minute_key = max(validated_klines.keys())
    last_minute_candles = validated_klines[last_minute_key]
    price_alerts: Optional[dict[str, float]] = check_price_overlimit(last_minute_candles, max_highs)

    if price_alerts is None:
        logger.info("✅ Не зафиксировано выхода за пределы ценового окна.")
        return
    else:
        price_overlimit_tickers = [candle for candle in last_minute_candles if candle.symbol in price_alerts]
        logger.debug(f"✅ Проверка закончена.")
        logger.info(f"Зафиксирован выход за пределы окна по N = {len(price_overlimit_tickers)} тикерам")

        for ticker, value in price_alerts.items():
            logger.debug(f"Тикер = {ticker} превысил цену в {value} раз.")
    # ====================== Step 9 ========================= #
    # Проверка превышения текущих отметок объёмов над верхним порогом скользящего часового окна цен
    # 
    # TODO: Нужно проверить тестами на валидность
    #
    # ======================================================= # 
    logger.debug(f"Проверяю превышение максимумов объёмов...")

    volume_alerts: Optional[dict[str, float]] = check_volume_overlimit(price_overlimit_tickers, volumes_10m, volumes_10h)

    # Список тикеров, у которых превышен лимит и на цены и на объёмы
    overlimit_tickers = []

    if volume_alerts is None:
        logger.info("✅ Проверка закончена. Не зафиксировано превышений.")
        return
    else:
        overlimit_tickers = [candle for candle in last_minute_candles if candle.symbol in volume_alerts]
        logger.info(f"✅ Проверка закончена. Зафиксировано {len(overlimit_tickers)} превышений")
    # ====================== Step 10 ======================== #
    # Оповещаю подключенных клиентов о новом сигнале и актуальной точке входа в сделку
    #
    # ======================================================= # 
    logger.debug(f"Формируем алерты для отправки...")
    alerts: List[AlertRecord] = []
    for kline in overlimit_tickers:
        alert = AlertRecord( ticker = kline.symbol, time = servertime_ms)
        logger.info(f"🔥🔥🔥 Зафиксирован алекрт по тикеру {kline.symbol}  time={alert.time} 🔥🔥🔥")
        alerts.append(alert)

    logger.debug(f"Рассылаем алерты клиентам...")

    for alert in alerts:
        alert_thread.send_alert(alert)
        logger.info(f"🌐 Отправили алерт {alert}")

    logger.debug(f"Все алерты разосланы клиентам...")

def main():

    logger.info("Скрипт-стартер запущен.")

    try:

        # Получаем разницу времени с сервером
        diff = get_server_time_diff()
        if diff is None:
            logger.error("❌ Не удалось получить время сервера. Выход.")
            return
        servertime_ms = int(time.time() * 1000) + diff
        # ======================================================= # 

        logger.info(f"Получаю список актуальных тикеров...")
        # TODO: Необходимо отработать моменты, когда отслеживаемые тикеры закрываются для торговли
        trackable_tickers: list[str] = getTrackedTickers()
        if len(trackable_tickers) == 0:
            logger.error("❌ Не удалось получить список тикеров")
            return
        logger.info(f"✅ Найдено торгующихся тикеров: {len(trackable_tickers)}")
        # ======================================================= # 

        klines_1m_full: OrderedDict[int, list[KlineRecord]] = download_candles_reccursively(servertime_ms, trackable_tickers, MAX_CACHED_CANDLES)

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
        
        # Запускаем сервер алертов в фоновом потоке
        alert_thread.start()

        while True:
            start_time = time.time()

            logger.info("# ====================== doTick ========================= #")

            server_time_diff_ms = int(time.time() * 1000) 
            diff = get_server_time_diff()
            if diff is not None:
                servertime_ms = server_time_diff_ms + diff
                logger.info(f"Текущая разница времени с сервером: {diff} мс")
            else:
                logger.warning("Не удалось получить разницу времени")
                logger.info("# ===================== End Tick ======================== #")
                logger.info("retry after 30s....")
                time.sleep(30)
                continue;

            doTick(server_time_diff_ms)

            logger.info("# ===================== End Tick ======================== #")
            logger.info("")

            # Вычисляем сколько осталось ждать
            elapsed = time.time() - start_time
            wait_time = max(0, 60 - elapsed)  # минимум 0 секунд

            logger.info(f"Function took {elapsed:.2f}s, waiting {wait_time:.2f}s")

            if wait_time > 0:
                time.sleep(wait_time)

    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания...")
        alert_thread.stop()
        alert_thread.join(timeout=2)
        logger.info("Остановлено пользователем")

main()

