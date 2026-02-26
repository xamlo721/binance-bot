import os
import time

from binance.client import Client
from dotenv import load_dotenv

from typing import List, Optional
from typing import Dict



def get_binance_client():

    load_dotenv()

    API_KEY = os.getenv('BINANCE_API_KEY')
    API_SECRET = os.getenv('BINANCE_API_SECRET')

    client = Client(API_KEY, API_SECRET, demo=True)

    server_time = client.get_server_time()
    print(f"Время на сервере Binance: {server_time}")

    account_info = client.get_account()
    print(f"Статус аккаунта: {account_info['canTrade']}") 

    balance = client.get_asset_balance(asset='USDT')
    print(f"Баланс USDT: {balance}")

    ticker = client.get_symbol_ticker(symbol="BTCUSDT")
    print(f"Текущая цена BTC/USDT: {ticker['price']}")

    return client

def setup_isolated_margin_type(binance_client: Client, symbol):
    response = binance_client.futures_change_margin_type(
        symbol=symbol,
        marginType='ISOLATED',
        recvWindow=30000, 
        timestamp=int(time.time() * 1000)
    )

def setup_cross_margin_type(binance_client: Client, symbol):
    response = binance_client.futures_change_margin_type(
        symbol=symbol,
        marginType='CROSSED',
        recvWindow=30000, 
        timestamp=int(time.time() * 1000)
    )



def get_binance_all_available_futures_tickers(binance_client: Client) -> list:
    """
    Возвращает список активных тикеров USDT-фьючерсов на Binance.
    """
    try:
        # Получаем информацию по всем фьючерсным символам
        exchange_info = binance_client.futures_exchange_info()
        
        # Фильтруем: активные USDT-фьючерсы (котируются против USDT)
        active_tickers = [
            symbol['symbol']
            for symbol in exchange_info['symbols']
            if (
                symbol['status'] == 'TRADING' and  # активный для торговли
                symbol['quoteAsset'] == 'USDT' and  # USDT-пара
                symbol['contractType'] == 'PERPETUAL'  # бессрочные контракты
            )
        ]
        
        return sorted(active_tickers)  # возвращаем отсортированный список
        
    except Exception as e:
        print(f"Ошибка при получении тикеров фьючерсов: {e}")
        return []
    
def get_open_futures_positions(binance_client: Client) -> list:
    """
    Возвращает список тикеров USDT-фьючерсов, по которым есть открытые позиции.
    """
    try:
        # Получаем информацию об открытых позициях
        positions = binance_client.futures_position_information()
        
        # Фильтруем позиции с ненулевой позицией (длинной или короткой)
        open_positions = [
            pos['symbol']
            for pos in positions
            if float(pos['positionAmt']) != 0.0  # есть открытая позиция
        ]
        
        return sorted(open_positions)  # возвращаем отсортированный список
        
    except Exception as e:
        print(f"Ошибка при получении открытых позиций: {e}")
        return []


def get_futures_step_size(binance_client: Client, symbol: str) -> float:
    """
    Возвращает step_size (шаг количества) для указанного фьючерсного символа.
    """
    try:
        exchange_info = binance_client.futures_exchange_info()
        
        for sym_info in exchange_info['symbols']:
            if sym_info['symbol'] == symbol:
                for filt in sym_info['filters']:
                    if filt['filterType'] == 'LOT_SIZE':
                        return float(filt['stepSize'])
        
        print(f"Не найден фильтр LOT_SIZE для {symbol}")
        return 0.0
        
    except Exception as e:
        print(f"Ошибка при получении step_size для {symbol}: {e}")
        return 0.0
    



def calculate_quantity(binance_client: Client, ticker: str, price: float, amount_usd: float, leverage: int) -> float:
    """Рассчитываем количество токенов для позиции"""

    position_value = amount_usd * leverage
    quantity = position_value / price

    step_size = get_futures_step_size(binance_client, ticker)

    precision = len(str(step_size).split('.')[1]) if '.' in str(step_size) else 0

    return round_quantity(quantity, precision)


def round_quantity(quantity: float, step_size: float):
    step_str = str(step_size).rstrip('0')
    if '.' in step_str:
        precision = len(step_str.split('.')[1])
    else:
        precision = 0
    
    rounded = round(quantity, precision)
    
    if step_size >= 1:
        rounded = int(rounded // step_size) * step_size
    else:
        multiplier = 1 / step_size
        rounded = int(rounded * multiplier) / multiplier
    
    return rounded

def set_futures_leverage(binance_client: Client, symbol: str, leverage: int) -> bool:
    """
    Устанавливает плечо для фьючерсной пары.
    Возвращает True при успехе, False при ошибке.
    """
    try:

        params = {
            'symbol': symbol.upper(),
            'leverage': leverage,
            'recvWindow': 60000,
            'timestamp': int(time.time() * 1000)
        }
        response = binance_client.futures_change_leverage(**params)
        return response == None
    
    except Exception as e:
        print(f"❌ Ошибка при установке плеча для {symbol}: {e}")
        return False
        

def open_futures_position(
    binance_client: Client,
    symbol: str,
    side: str,  # 'BUY' или 'SELL'
    amount_usdt: float,
    leverage: int = 10,
    stop_lose: float = 5
) -> dict:
    """
    Открывает фьючерсную позицию с указанным плечом.
    """
    quantity: float

    try:
        print("Starting openning order.")

        # 1. Устанавливаем плечо (не работает на демо счете)
        if not binance_client.demo:
            print(f"* setup leverage: {leverage}")

            binance_client.futures_change_leverage(
                symbol=symbol,
                leverage=leverage
            )

            result = set_futures_leverage(binance_client, symbol, leverage)
        
        # 2. Получаем текущую цену
        currentTicker = binance_client.futures_symbol_ticker(symbol=symbol)
        current_price: float = float(currentTicker['price'])
        if not current_price:
            print("Не удалось получить текущую цену")
            return {}

        print(f"* get price: {current_price}")

        quantity = calculate_quantity(binance_client, symbol, current_price, amount_usdt, leverage)

        quantity_precision = get_quantity_precision(binance_client, symbol)
        price_precision = get_price_precision(binance_client, symbol)

        precigion_quantity = round(quantity, quantity_precision)

        print(f"Вычилили quantity {precigion_quantity}.")

        sell_price: float = 0
        take_profit: float = 0
        if side == 'SELL':
            sell_price = current_price * (1 + stop_lose/100)
            take_profit = current_price * (1 - stop_lose/100)
        else:
            sell_price = current_price * (1 - stop_lose/100)
            take_profit = current_price * (1 + stop_lose/100)

        # Округляем цены до нужной точности
        sell_price = round(sell_price, price_precision)
        take_profit = round(take_profit, price_precision)

        # 2. Создаем рыночный ордер на открытие позиции
        order = binance_client.futures_create_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity= precigion_quantity
        )
        print(f"Фьючерс открыт: {symbol} {side} {precigion_quantity} @ {amount_usdt}")

        # раскомментить если нужны тейк-профиты
        # set_futures_TP(binance_client, symbol, side, precigion_quantity, take_profit)
        set_futures_SL(binance_client, symbol, side, precigion_quantity, sell_price)

        return order
    
    except Exception as e:
        print(f"Ошибка при открытии позиции {symbol}: {e}")
        return {}
    
def set_futures_TP(
    binance_client: Client,
    symbol: str,
    side: str,  # 'BUY' или 'SELL'
    precigion_quantity: float,
    take_profit: float
):
    try:
        print(f"* take_profit price: {take_profit}")
        take_order = binance_client.futures_create_order(
            symbol =  symbol,
            side = 'SELL' if side == 'BUY' else 'BUY',
            type = 'TAKE_PROFIT_MARKET',
            stopPrice = take_profit,
            quantity = precigion_quantity,
            # reduceOnly=True,
            closePosition = 'true'
        )
        print(f"TP установлен: {symbol} {side} {precigion_quantity} @ {take_profit}")

    except Exception as e:
        print(f"Ошибка при установке TP для {symbol} {side} {precigion_quantity} @ {take_profit}: {e}")
        return {}
    
def set_futures_SL(
    binance_client: Client,
    symbol: str,
    side: str,  # 'BUY' или 'SELL'
    precigion_quantity: float,
    stop_price: float
):
    try:
        print(f"* stop_lose price: {stop_price}")
        #print(f"Вычилили stop_lose quantity {stop_precigion_quantity}.")

        # 3. Размещаем стоп-лосс ордер
        sell_order = binance_client.futures_create_order(
            symbol=symbol,
            side='SELL' if side == 'BUY' else 'BUY',
            type='STOP_MARKET',
            quantity=precigion_quantity,
            stopPrice=stop_price,
            # reduceOnly=True,
            closePosition = 'true'
        )
        print(f"SL установлен: {symbol} {side} {precigion_quantity} @ {stop_price}")
            
    except Exception as e:
        print(f"Ошибка при установке SL для {symbol} {side} {precigion_quantity} @ {stop_price}: {e}")
        return {}
        
def get_price_precision(
    binance_client: Client,
    symbol: str
) -> int:
    exchange_info = binance_client.futures_exchange_info()
    symbol_info = None
    for s in exchange_info['symbols']:
        if s['symbol'] == symbol:
            symbol_info = s
            break
        
    if not symbol_info:
        print(f"Не удалось получить информацию о символе {symbol}")
        return 0
        
    price_precision = int(symbol_info['pricePrecision'])
    return price_precision

def get_quantity_precision(    
    binance_client: Client,
    symbol: str
) -> int:
    exchange_info = binance_client.futures_exchange_info()
    symbol_info = None
    for s in exchange_info['symbols']:
        if s['symbol'] == symbol:
            symbol_info = s
            break
        
    if not symbol_info:
        print(f"Не удалось получить информацию о символе {symbol}")
        return 0
        
    quantity_precision = int(symbol_info['quantityPrecision'])
    return quantity_precision


def get_futures_sl_order(
    binance_client: "Client",
    symbol: str,
) -> Optional[Dict]:
    """
    Возвращает первый активный (NEW) SL‑ордер для заданного символа.
    Если такой ордера нет – возвращается None.
    """
    open_orders = binance_client.futures_get_all_algo_orders(
        symbol=symbol, timestamp=int(time.time() * 1000)
    )

    print(f"    * we have {len(open_orders)} orders in: {symbol}")

    for i, order in enumerate(open_orders, start=1):
        if order.get("algoStatus") == "CANCELED":
            continue

        if (
            order.get("symbol") == symbol
            and order.get("algoType") in ("CONDITIONAL", "STOP_MARKET")
            and order.get("algoStatus") == "NEW"
        ):
            print(f"   * found SL order #{i} for {symbol}")

            return order

    return None


def move_stop_loss(
    binance_client: Client,
    symbol: str,
    side: str,          # 'BUY' or 'SELL'
    quantity: float,
    new_stop_price: float
) -> dict:
    """
    Перемещает стоп‑лосс на указанную позицию.
    Перед созданием ордера округляет цену и количество до допустимых точностей.
    """

    price_precision = get_price_precision(binance_client, symbol)
    quantity_precision = get_quantity_precision(binance_client, symbol)

    new_stop_price_rounded = round(new_stop_price, price_precision)
    rounded_qty = round(quantity, quantity_precision)
    print(f"    getting all opened algo order!")

    # Если найден существующий стоп‑лосс – отменяем его
    open_orders = binance_client.futures_get_all_algo_orders(
        symbol=symbol, 
        timestamp=int(time.time() * 1000)
    )
    stop_order_id = None
    stop_order_price = None
    print(f"    * we have {len(open_orders)} orders in: {symbol}")

    for i, order in enumerate(open_orders, 1):

        if order['algoStatus'] == "CANCELED":
            continue

        # print(f"    scanning order {i}: {order}")

        if (
            order['symbol'] == symbol and
            (order['algoType'] == 'CONDITIONAL' or order['algoType'] == 'STOP_MARKET' )and
            order['algoStatus'] == 'NEW'
        ):
            print(f"   * we found exist SL order {len(open_orders)} orders in: {symbol}")

            stop_order_id = order['algoId']
            # Получаем текущую цену стоп-лосса
            stop_order_price = float(order.get('stopPrice') or order.get('triggerPrice', 0))
            # Если у ордера нет явной стоп-цены, берем активационную цену
            if stop_order_price == 0:
                stop_order_price = float(order.get('activatePrice', 0))
            break

    if stop_order_id:
        print(f"    canceling old stop order: {stop_order_id}")

        # Округляем текущую цену стоп-лосса для сравнения
        if stop_order_price:
            stop_order_price_rounded = round(stop_order_price, price_precision)
        else:
            stop_order_price_rounded = None
            
        print(f"    Current stop price: {stop_order_price_rounded}")
        print(f"    New stop price: {new_stop_price_rounded}")
        
        # Проверяем, совпадают ли цены (с учетом округления)
        if stop_order_price_rounded is not None and stop_order_price_rounded == new_stop_price_rounded:
            print(f"    ✓ Stop price is already at {new_stop_price_rounded}. No changes needed.")
            return {"status": "unchanged", "reason": "price_already_set"}
        
        # Если цены не совпадают, отменяем старый ордер
        print(f"    Prices differ. Cancelling old SL order {stop_order_id}, name = {symbol}")
        
        try:
            binance_client.futures_cancel_algo_order(
                sumbol=symbol, 
                algoid=stop_order_id, 
                timestamp=int(time.time() * 1000)
            )
            print(f"    Cancelled old SL order {stop_order_id}")


        except Exception as e:
            print(f"❌ Ошибка перемещения SL для {symbol}: {e}")
            return {}
        
    else:
        print(f"    failed to remove old SL order {stop_order_id}")
        #return {}
                
    print(f"    order id {stop_order_id}")
    set_futures_SL(binance_client, symbol, side, rounded_qty, new_stop_price_rounded)    


    return {}
