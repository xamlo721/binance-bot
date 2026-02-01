import time
import os

from AlertFileSearcher import get_latest_alert_file, get_latest_alert_calc_file
from csv_utills import get_tickers_from_csv_pandas, get_min_prices_from_csv_pandas, get_max_prices_from_csv_pandas
from ticker_utils import compare_tickers, print_comparison_results
from logic import analyze_stop_loss, open_new_positions, check_available_position, get_position_grow, maximise_with_side, get_price_from_list
from binance_utils import get_binance_client, get_binance_all_available_futures_tickers, get_open_futures_positions, open_futures_position, move_stop_loss, get_futures_sl_order
from binance.client import Client
from typing import List
from typing import Dict
from typing import Optional

tickers:  List[str] = []
new_tickers:  List[str] = []

# Укажите путь к вашей директории
alerts_directory = "C:/workspace/data/alerts/"
alerts_calc_directory = "C:/workspace/data/alerts_calc"

alerts_file = ""
alerts_calc_file = ""

binance_all_available_tickers: List = []
binance_open_tickers: List[str] = []

all_ticker_prices: Dict


def print_active_futures_tickers_simple(tickers_list: list):
    print(f"\nАктивные USDT-фьючерсы ({len(tickers_list)} шт.):")
    for i, ticker in enumerate(tickers_list, 1):
        print(f"{i:3}. {ticker}")

def update_files() -> bool:
    global alerts_directory 
    global alerts_calc_directory 
    global alerts_file 
    global alerts_calc_file 
    global tickers 
        
    # print(f"Самый новый файл: {alerts_file}")

    # Debug. Выставляет на один прогон тикеры
    # old_alert_file = "C:/workspace/data/alerts/alerts_2026-01-28_test.csv"
    # tickers = get_tickers_from_csv_pandas(old_alert_file)

    alerts_file = get_latest_alert_file(alerts_directory)
    alerts_calc_file = get_latest_alert_calc_file(alerts_calc_directory)

    if not alerts_file:
        print("Файлы alerts_yyyy_mm_dd.csv не найдены")
        return False
    
    if not alerts_calc_file:
        print("Файлы alerts_calc_yyyy_mm_dd.csv не найдены")
        return False
    
    return True

def update_binance_data() -> bool:
    global binance_all_available_tickers 
    global binance_open_tickers 
        
    binance_all_available_tickers = get_binance_all_available_futures_tickers(binance_client)
    binance_open_tickers = get_open_futures_positions(binance_client)

    return True

def update_tickers() -> bool:
    global tickers 
    global new_tickers 
    global all_ticker_prices

    new_tickers = get_tickers_from_csv_pandas(alerts_file)

    if not new_tickers:
        print("Новые тикеры не найдены")
        return False
    
    print(f"Найдено {len(new_tickers)} уникальных тикеров:")

    if not tickers:
        print("Не обнаружены предыдущие значения тикеров, обновляю данные...")
        tickers = new_tickers
        return False
    
    all_ticker_prices = binance_client.futures_symbol_ticker()
    
    return True

# -----------------------------------------------------------------------------------------------------------------------------------------
def check_open_orders(
    binance_client: Client, 
    binance_open_tickers: List[str], 
    all_ticker_prices: Dict, 
    new_tickers: List[str],
    short_position_prices,
    long_position_prices, 
    positions_info
):
    print("-" * 50)
    for i, ticker in enumerate(binance_open_tickers, 1):
        # Найти данные позиции по тикеру
        pos = next((p for p in positions_info if p['symbol'] == ticker), None)
        if not pos:
            print(f"--- analysing order {i}: {ticker} – position info missing")
            continue
 
        order: Optional[Dict] = get_futures_sl_order(binance_client, ticker)
        if order is None:
            continue

        stop_order_price = float(order.get('stopPrice') or order.get('triggerPrice', 0))
        
        entry_price = float(pos.get('entryPrice', 0))
        quantity = abs(float(pos.get('positionAmt', 0)))  # абсолютное количество
        side = 'BUY' if float(pos.get('positionAmt')) > 0 else 'SELL'
        breakEvenPrice = float(pos.get('breakEvenPrice', 0))

        # Текущая цена монеты
        current_price = get_price_from_list(ticker, all_ticker_prices)

        # Процент изменения от цены открытия
        pct_change = get_position_grow(entry_price, current_price)

        
        print(f"--- analysing order {i}: {ticker}")
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
 
            move_stop_loss(binance_client, ticker, side, quantity, breakEvenPrice)
            print("    position moved.")
        else:
            print("    No adjustment needed. (3-5 signal)")
            # return
# -------------------------------------------------------------------------------------------------
        print("-" * 50)
        print("Анализ TP")

        if not ticker in new_tickers:
            print(f"По паре {ticker} нет аналитики, пропускаю....")
            continue

        if len(new_tickers) != len(short_position_prices):
            print(f"Кажется аналитика для {ticker} ещё не готова, пропускаю....")
            continue

        ticker_index: int
        # Получаем индекс тикера в списке `new_tickers`
        try:
            ticker_index = new_tickers.index(ticker)
        except ValueError:
            # Тикер не найден – можно вернуть None или -1
            ticker_index = 0
            print(f"Тикер {ticker} отсутствует в списке")
            continue

        short_min_price = float(short_position_prices[ticker_index])
        long_max_price = float(long_position_prices[ticker_index])

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
                ticker,
                side,
                quantity,
                new_stop_order_price
            )
        else:
            print("    No adjustment needed.")
            
        print("Анализ TP закончен")
        print("-" * 50)


def do_loop(binance_client):
    global tickers
    global new_tickers
    global binance_open_tickers
    global all_ticker_prices

    if not update_files():
        print("❌   Problem with files. Stopping logic...")
        return
    else:
        print("  files updated")

    if not update_binance_data():
        print("❌   Problem with files. Stopping logic...")
        return
    else:
        print("  binance data updated")

    if not update_tickers():
        print("❌   Problem with files. Stopping logic...")
        return
    else:
        print("  tickers updated")

    available_poss:  List[str] = check_available_position(
        tickers= tickers,
        new_tickers = new_tickers,
        binance_open_tickers = binance_open_tickers
    )

    open_new_positions(
        binance_client = binance_client, 
        ticker_positions = available_poss,
        side = 'BUY',
        amount_usdt= 10,
        leverage= 10,
        stop_lose_pct = 5
    )
    
    # Сейчас как подвину
    short_position_prices = get_min_prices_from_csv_pandas(alerts_calc_file)
    long_position_prices = get_max_prices_from_csv_pandas(alerts_calc_file)

    # Получаем информацию о всех открытых позициях (должна включать entryPrice)
    positions_info = binance_client.futures_position_information()

    # Выполняем функцию
    check_open_orders(
        binance_client, 
        binance_open_tickers, 
        all_ticker_prices, 
        new_tickers, 
        short_position_prices, 
        long_position_prices,
        positions_info
    )

    tickers = new_tickers


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