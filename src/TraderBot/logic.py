from binance_utils import open_futures_position
from binance.client import Client

from ticker_utils import compare_tickers, print_comparison_results

from typing import List

def get_price_from_list(ticker: str, all_ticker_prices) -> float:

    current_price_str = next(
        (item['price'] for item in all_ticker_prices
        if item['symbol'] == ticker),
        None  # если тикер не найден
    )

    if current_price_str is None:
        print(f" we not found price for {ticker}")
        return 0

    # Текущая цена монеты
    return float(current_price_str)


def maximise_with_side(value1, value2, side) -> float:
    if (side == 'BUY'):
        return max(value1, value2)
    else:
        return min(value1, value2)


def get_position_grow(entry_price: float, current_price: float) -> float:
    diff_percent = (current_price - entry_price) / entry_price * 100
    return diff_percent

def analyze_stop_loss(pct_change,  side, low_level, high_level) -> bool:
    
    if (side == 'BUY') and (low_level < pct_change < high_level):
            return True
    
    elif (-low_level > pct_change > -high_level):
            return True
    
    else:
        return False

def check_available_position(
    tickers:  List[str], 
    new_tickers:  List[str], 
    binance_open_tickers: List[str]
) -> List[str]:

    tickers_diff = compare_tickers(tickers, new_tickers)
    print_comparison_results(tickers_diff)

    # print_active_futures_tickers_simple(binance_active_tickers)
    print("-" * 50)
    # print_active_futures_tickers_simple(binance_open_tickers)

    print("Ищу неоткрытые позиции среди новых тикеров")
    added_file_tickers = tickers_diff['added']
    potencial_tickers_diff = compare_tickers(binance_open_tickers, added_file_tickers)
    # print_comparison_results(potencial_tickers_diff)

    new_order_ticker = potencial_tickers_diff['added']

    if not len(new_order_ticker):
        print("❌ Данные обновлены, торговать нечем.")
        return []

    print("⚠️ we have new tickers for open position!:")
    for i, ticker in enumerate(new_order_ticker, 1):
        print(f"{i:3d}. {ticker}")

    return new_order_ticker

def get_max_leverage(binance_client: Client, ticker: str) -> int:

    max_leverage: int = 0

    try:
        brackets = binance_client.futures_leverage_bracket(symbol = ticker)

        if brackets and brackets[0]['brackets']:
            max_leverage = int(brackets[0]['brackets'][0]['initialLeverage'])

        return max_leverage
    
    except Exception as bracket_error:
        print(f"Не удалось получить брекеты плеча: {bracket_error}")
        return 0
    

def open_new_positions(binance_client: Client, ticker_positions:  List[str], side, amount_usdt: int, leverage: int, stop_lose_pct: float):
    print("⚠️ Открываем позиции!")

    for i, ticker in enumerate(ticker_positions, 1):
        max_leverage: int = get_max_leverage(binance_client, ticker)

        if leverage > max_leverage:
            print(f"⚠️ Плечо {leverage}x > максимального {max_leverage}x для {ticker}")
            leverage = max_leverage
            print(f"Допустимое плечо для {ticker}: до {max_leverage}x")

        open_futures_position(binance_client, ticker, side, amount_usdt, leverage, stop_lose_pct)

    print("Complete!")


