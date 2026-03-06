from binance_utils import open_futures_position
from binance.client import Client

from typing import List
from bot_types import AlertRecord

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

def check_available_position(active_alerts: list[AlertRecord], new_alerts: list[AlertRecord], binance_open_tickers: List[str]) -> list[AlertRecord]:
    """
    Определяет, какие из новых алертов ещё не были обработаны и не имеют открытых позиций.
    
    Args:
        active_alerts: список уже обработанных алертов
        new_alerts: список свежих алертов
        binance_open_tickers: список тикеров с открытыми позициями на бирже
    
    Returns:
        Список алертов (AlertRecord), подходящих для открытия новых позиций.
    """
    result: list[AlertRecord] = []
    print("Ищу неоткрытые позиции среди новых тикеров")

    # Новые сигналы, которые мы раньше не обрабатывали
    alerts_diff: list[AlertRecord] = list(set(new_alerts) - set(active_alerts))

    # Вычислить все те, по которым уже открыты позиции
    new_alerts_for_open = [alert for alert in alerts_diff if alert.ticker not in binance_open_tickers]

    if not len(new_alerts_for_open):
        print("❌ Данные обновлены, торговать нечем.")
        return []

    print("⚠️ we have new tickers for open position!:")
    for i, ticker in enumerate(new_alerts_for_open, 1):
        print(f"{i:3d}. {ticker}")

    return result

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

def open_new_positions(binance_client: Client, alerts: list[AlertRecord], side, amount_usdt: int, leverage: int, stop_lose_pct: float):
    print("⚠️ Открываем позиции!")

    for i, alert in enumerate(alerts, 1):
        max_leverage: int = get_max_leverage(binance_client, alert.ticker)

        if leverage > max_leverage:
            print(f"⚠️ Плечо {leverage}x > максимального {max_leverage}x для {alert}")
            leverage = max_leverage
            print(f"⚠️ Допустимое плечо для {alert}: до {max_leverage}x")

        open_futures_position(binance_client, alert.ticker, side, amount_usdt, leverage, stop_lose_pct)

    print("Complete!")


