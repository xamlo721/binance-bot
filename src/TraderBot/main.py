import time
import os

from logic import analyze_stop_loss, open_new_positions, check_available_position, get_position_grow, maximise_with_side, get_price_from_list
from binance_utils import get_binance_client, get_binance_all_available_futures_tickers, get_open_futures_positions, open_futures_position, move_stop_loss, get_futures_sl_order
from binance.client import Client
from typing import List
from typing import Dict
from typing import Optional

from bot_types import AlertRecord
from network_utils import takeAlerts

active_alerts:  List[AlertRecord] = []

binance_all_available_tickers: List = []
binance_open_tickers: List[str] = []

all_ticker_prices: Dict

def print_active_futures_tickers_simple(tickers_list: list):
    print(f"\nАктивные USDT-фьючерсы ({len(tickers_list)} шт.):")
    for i, ticker in enumerate(tickers_list, 1):
        print(f"{i:3}. {ticker}")

def update_binance_data() -> bool:
    global binance_all_available_tickers 
    global binance_open_tickers 
        
    binance_all_available_tickers = get_binance_all_available_futures_tickers(binance_client)
    binance_open_tickers = get_open_futures_positions(binance_client)

    return True

def check_for_new_alerts() -> Optional[list[AlertRecord]]:

    # Забираем из буфера аналитического бота новые алерты, если они есть
    alerts: list[AlertRecord] = takeAlerts()

    if not alerts or  len(alerts):
        print("Новые сигналы от аналитического бота не найдены не найдены")
        return []
    
    print(f"Найдено {len(alerts)} уникальных тикеров.")

    return alerts

# -----------------------------------------------------------------------------------------------------------------------------------------
def check_open_orders(
    binance_client: Client, 
    binance_open_tickers: List[str], 
    all_ticker_prices: Dict, 
    new_alerts: list[AlertRecord], 
    positions_info
):
    print("-" * 50)
    for i, alert in enumerate(binance_open_tickers, 1):
        # Найти данные позиции по тикеру
        pos = next((p for p in positions_info if p['symbol'] == alert), None)
        if not pos:
            print(f"--- analysing order {i}: {alert} – position info missing")
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

        print(f"--- analysing order {i}: {alert}")
        print(f"    Entry price: {entry_price:.8f} | Current price: {current_price:.8f} | Current SL: {stop_order_price:.8f}")
        print(f"    Position side: {side} | Quantity: {quantity:.6f}")
        print(f"    breakEvenPrice: {breakEvenPrice} | Quantity: {quantity:.6f}")
        print(f"    pct_change: {pct_change}")

        isMovable1 = analyze_stop_loss(pct_change, side, 3, 5)

        if isMovable1:
            print("    we need to move position (3-5 signal).")

            if (maximise_with_side(breakEvenPrice, stop_order_price, side)) == stop_order_price:
                print("Текущий SL лучше, не двигаемся.")
                continue
 
            move_stop_loss(binance_client, alert, side, quantity, breakEvenPrice)
            print("    position moved.")
        else:
            print("    No adjustment needed. (3-5 signal)")
            # return
# -------------------------------------------------------------------------------------------------
        print("-" * 50)
        print("Анализ TP")

        # Ищем объект AlertRecord с таким ticker
        alert_record = next((a for a in new_alerts if a.ticker == alert), None)
        if alert_record is None or alert_record.min_price is None or alert_record.max_price is None:
            print(f"По паре {alert} нет аналитики, пропускаю....")
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
            print(f"Максимальная отметка движения: {max_ticker_price}")
            print(f"Цена 30% движения: {new_stop_order_price}")

            if (maximise_with_side(current_price, new_stop_order_price, side)) == new_stop_order_price:
                print("Текущая цена меньше этой отметки, SL не двигается.")
                continue
 
            if (maximise_with_side(stop_order_price, new_stop_order_price, side)) != new_stop_order_price:
                print("Текущий SL лучше, не двигаемся.")
                continue
 
            print("Ну тут можно двигать SL")

            print(f"    Moving stop‑loss to {new_stop_order_price:.8f}")
            move_stop_loss(
                binance_client,
                alert,
                side,
                quantity,
                new_stop_order_price
            )
        else:
            print("    No adjustment needed.")
            
        print("Анализ TP закончен")
        print("-" * 50)


def do_loop(binance_client):
    global active_alerts
    global binance_open_tickers
    global all_ticker_prices

    if not update_binance_data():
        print("❌   Problem with files. Stopping logic...")
        return
    else:
        print("  binance data updated")

    all_ticker_prices = binance_client.futures_symbol_ticker()

    new_alerts: Optional[list[AlertRecord]] = check_for_new_alerts()
    if new_alerts is None:
        print("❌   Problem with files. Stopping logic...")
        return
    else:
        print("  tickers updated")

    available_poss: list[AlertRecord] = check_available_position(active_alerts, new_alerts, binance_open_tickers)

    open_new_positions(
        binance_client = binance_client, 
        alerts = available_poss,
        side = 'BUY',
        amount_usdt = 10,
        leverage = 10,
        stop_lose_pct = 5
    )
    
    # Получаем информацию о всех открытых позициях (должна включать entryPrice)
    positions_info = binance_client.futures_position_information()

    # Выполняем функцию
    check_open_orders(
        binance_client, 
        binance_open_tickers, 
        all_ticker_prices, 
        new_alerts,
        positions_info
    )

    active_alerts.extend(available_poss)


if __name__ == "__main__":

    try:
        binance_client = get_binance_client()

        while True:
            start_time = time.time()

            do_loop(binance_client)

            # Вычисляем сколько осталось ждать
            elapsed = time.time() - start_time
            wait_time = max(0, 60 - elapsed)  # минимум 0 секунд
            
            print(f"Function took {elapsed:.2f}s, waiting {wait_time:.2f}s")
            
            if wait_time > 0:
                time.sleep(wait_time)
        

    except ValueError as e:
        print(f"Ошибка: {e}")