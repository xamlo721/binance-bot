import time

from pathlib import Path

import sys

src_path = Path(__file__).resolve().parent.parent
trader_bot_src_path = Path(__file__).resolve().parent
sys.path.insert(0, str(src_path))
sys.path.insert(0, str(trader_bot_src_path))

from TraderBot.logger import logger

logger.debug(str(f"src_path = {src_path}"))
logger.debug(str(f"trader_bot_src_path = {trader_bot_src_path}"))

from logger import *

from TraderBot.logic import analyze_stop_loss
from TraderBot.logic import open_new_positions
from TraderBot.logic import check_available_position
from TraderBot.logic import get_position_grow
from TraderBot.logic import maximise_with_side
from TraderBot.logic import get_price_from_list

from TraderBot.binance_utils import get_binance_client
from TraderBot.binance_utils import get_binance_all_available_futures_tickers
from TraderBot.binance_utils import get_open_futures_positions
from TraderBot.binance_utils import open_futures_position
from TraderBot.binance_utils import move_stop_loss
from TraderBot.binance_utils import get_futures_sl_order

from binance.client import Client

from typing import List
from typing import Dict
from typing import Optional

from TraderBot.bot_types import AlertRecord

from TraderBot.alert_client import *

active_alerts:  List[AlertRecord] = []
incomming_alerts:  List[AlertRecord] = []

def on_alert(alert: AlertRecord, packet_number: int):
    print(f"[{packet_number}] {alert.ticker}: {alert.volume} at {alert.time}")
    # incomming_alerts.append(alert)

def print_active_futures_tickers_simple(tickers_list: list):
    logger.info(f"\nАктивные USDT-фьючерсы ({len(tickers_list)} шт.):")
    for i, ticker in enumerate(tickers_list, 1):
        logger.info(f"{i:3}. {ticker}")

def check_for_new_alerts() -> Optional[list[AlertRecord]]:
    global incomming_alerts 

    # Забираем из буфера аналитического бота новые алерты, если они есть
    alerts: list[AlertRecord] = incomming_alerts

    if not alerts or  len(alerts):
        logger.error("Новые сигналы от аналитического бота не найдены не найдены")
        return []
    
    logger.info(f"Найдено {len(alerts)} уникальных тикеров.")

    return alerts

def check_open_orders(binance_client: Client, binance_open_tickers: List[str], all_ticker_prices: Dict, new_alerts: list[AlertRecord], positions_info):
    logger.info("-" * 50)
    for i, alert in enumerate(binance_open_tickers, 1):
        # Найти данные позиции по тикеру
        pos = next((p for p in positions_info if p['symbol'] == alert), None)
        if not pos:
            logger.warning(f"--- analysing order {i}: {alert} – position info missing")
            continue
 
        order: Optional[Dict] = get_futures_sl_order(binance_client, alert)
        if order is None:
            continue

        stop_order_price = float(order.get('stopPrice') or order.get('triggerPrice', 0))
        
        entry_price = float(pos.get('entryPrice', 0))
        quantity = abs(float(pos.get('positionAmt', 0)))  # абсолютное количество
        side = 'BUY' if float(pos.get('positionAmt')) > 0 else 'SELL'
        breakEvenPrice = float(pos.get('breakEvenPrice', 0))

        # Текущая цена монеты
        current_price = get_price_from_list(alert, all_ticker_prices)

        # Процент изменения от цены открытия
        pct_change = get_position_grow(entry_price, current_price)

        logger.debug(f"--- analysing order {i}: {alert}")
        logger.debug(f"    Entry price: {entry_price:.8f} | Current price: {current_price:.8f} | Current SL: {stop_order_price:.8f}")
        logger.debug(f"    Position side: {side} | Quantity: {quantity:.6f}")
        logger.debug(f"    breakEvenPrice: {breakEvenPrice} | Quantity: {quantity:.6f}")
        logger.debug(f"    pct_change: {pct_change}")

        isMovable1 = analyze_stop_loss(pct_change, side, 3, 5)

        if isMovable1:
            logger.info("    we need to move position (3-5 signal).")

            if (maximise_with_side(breakEvenPrice, stop_order_price, side)) == stop_order_price:
                logger.info("Текущий SL лучше, не двигаемся.")
                continue
 
            move_stop_loss(binance_client, alert, side, quantity, breakEvenPrice)
            logger.info("    position moved.")
        else:
            logger.info("    No adjustment needed. (3-5 signal)")
            # return
# -------------------------------------------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("Анализ TP")

        # Ищем объект AlertRecord с таким ticker
        alert_record = next((a for a in new_alerts if a.ticker == alert), None)
        if alert_record is None or alert_record.min_price is None or alert_record.max_price is None:
            logger.info(f"По паре {alert} нет аналитики, пропускаю....")
            continue

        short_min_price = alert_record.min_price
        long_max_price = alert_record.max_price 

        # Цена, к которой мы стремимся
        max_ticker_price = long_max_price if float(pos.get('positionAmt')) > 0 else short_min_price
        #target_SL_price = maximise_with_side(max__ticker_price, breakEvenPrice, side)

        # Процент стоплоза
        sl_limit = 30
        # Отметка 30% движения, к которой мы стремимся
        new_stop_order_price = breakEvenPrice + (max_ticker_price - breakEvenPrice) * sl_limit / 100
        # Проверка на >5%
        isMovable2 = analyze_stop_loss(pct_change, side, 5, 9999)
        if isMovable2:
            logger.info(f"Максимальная отметка движения: {max_ticker_price}")
            logger.info(f"Цена 30% движения: {new_stop_order_price}")

            if (maximise_with_side(current_price, new_stop_order_price, side)) == new_stop_order_price:
                logger.info("Текущая цена меньше этой отметки, SL не двигается.")
                continue
 
            if (maximise_with_side(stop_order_price, new_stop_order_price, side)) != new_stop_order_price:
                logger.info("Текущий SL лучше, не двигаемся.")
                continue
 
            logger.info("Ну тут можно двигать SL")

            logger.info(f"    Moving stop‑loss to {new_stop_order_price:.8f}")
            move_stop_loss(
                binance_client,
                alert,
                side,
                quantity,
                new_stop_order_price
            )
        else:
            logger.info("    No adjustment needed.")
            
        logger.info("Анализ TP закончен")
        logger.info("-" * 50)

def do_loop(binance_client):
    global active_alerts
    global binance_open_tickers

    logger.info("# ====================== doTick ========================= #")
    binance_all_available_tickers = get_binance_all_available_futures_tickers(binance_client)
    binance_open_tickers = get_open_futures_positions(binance_client)
    # Получаем информацию о всех открытых позициях (должна включать entryPrice)
    positions_info = binance_client.futures_position_information()
    logger.info("Данные binance обновлены.")

    # all_ticker_prices = binance_client.futures_symbol_ticker()

    # if (len(incomming_alerts) == 0):
    #     logger.info("Новых алертов не фиксировано.")
    # else:
    #     logger.info("Получены новые алерты:")
    #     for alert in incomming_alerts:
    #         logger.info(f"Alert = {alert}")

    #     available_poss: list[AlertRecord] = check_available_position(active_alerts, incomming_alerts, binance_open_tickers)
    #     active_alerts.extend(available_poss)

    #     open_new_positions(binance_client = binance_client, alerts = available_poss, side = 'BUY', amount_usdt = 10, leverage = 10, stop_lose_pct = 5)

    # # Выполняем функцию
    # check_open_orders(binance_client, binance_open_tickers, all_ticker_prices, incomming_alerts, positions_info)

    logger.info("# ===================== End Tick ======================== #")
    logger.info("")

async def periodic_task(binance_client):
    """Периодически выполняет синхронную do_loop в отдельном потоке."""
    loop = asyncio.get_running_loop()

    while True:

        start_time = time.time()

        # Выполняем синхронную do_loop в потоке, чтобы не блокировать цикл событий
        await loop.run_in_executor(None, do_loop, binance_client)

        # Вычисляем сколько осталось ждать
        elapsed = time.time() - start_time
        wait_time = max(0, 60 - elapsed)

        logger.info(f"Function took {elapsed:.2f}s, waiting {wait_time:.2f}s")
        await asyncio.sleep(wait_time)

async def async_init():
    # Инициализация клиента Binance (синхронная)
    binance_client = get_binance_client()

    # Создаём и подключаем клиент алертов (не через контекстный менеджер,
    # чтобы он жил всё время работы программы)
    alert_client = AlertClient(alert_callback=on_alert)
    await alert_client.connect()
    logger.info("AlertClient подключён и ожидает алерты")

    # Запускаем периодическую задачу
    periodic_task_obj = asyncio.create_task(periodic_task(binance_client))

    # Бесконечно ждём, пока не будет прервано (Ctrl+C)
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания...")
    finally:
        # Корректно завершаем задачи
        periodic_task_obj.cancel()
        await alert_client.close()
        logger.info("Клиент отключён")

def main():

    try:
        asyncio.run(async_init())

    except ValueError as e:
        logger.error(f"Ошибка: {e}")
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")

        
if __name__ == "__main__":
    main()