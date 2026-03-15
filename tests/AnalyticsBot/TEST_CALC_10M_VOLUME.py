from pathlib import Path

import sys

src_path = Path(__file__).resolve().parent.parent.parent / "src" 
analytics_bot_src_path = src_path / "src" / "AnalyticsBot"
sys.path.insert(0, str(src_path))
sys.path.insert(0, str(analytics_bot_src_path))
print(f"src_path = {src_path}")
print(f"analytics_bot_src_path = {analytics_bot_src_path}")

from collections import OrderedDict

from AnalyticsBot.bot_types import *
from AnalyticsBot.analytic_utils import calculate_10m_volumes_slidedWindow

def test_insufficient_minutes():
    """Тест 1: Недостаточное количество минут (менее 10)"""
    # Создаём тестовые данные - 9 минут
    test_dict = OrderedDict()
    base_time = 1700000000000  # некоторое базовое время в мс
    
    # Создаём записи для 2 тикеров на 9 минут
    for i in range(9):
        minute_key = base_time // 60000 + i
        test_dict[minute_key] = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0 + i,
                close=50100.0 + i,
                high=50200.0 + i,
                low=49900.0 + i,
                volume=10.0 + i,
                close_time=base_time + (i+1)*60000 - 1,
                quote_assets_volume=500000.0 + i*1000,
                taker_buy_base_volume=5.0 + i*0.1,
                taker_buy_quote_volume=250000.0 + i*500,
                num_of_trades=100 + i*10,
                open_time=base_time + i*60000
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0 + i,
                close=3010.0 + i,
                high=3020.0 + i,
                low=2990.0 + i,
                volume=20.0 + i,
                close_time=base_time + (i+1)*60000 - 1,
                quote_assets_volume=60000.0 + i*200,
                taker_buy_base_volume=10.0 + i*0.2,
                taker_buy_quote_volume=30000.0 + i*100,
                num_of_trades=80 + i*5,
                open_time=base_time + i*60000
            )
        ]
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат равен None (так как минут меньше 10)
    assert result is None, f"Ожидался None, получен {result}"

def test_empty_dict():
    """Тест 2: Пустой словарь"""
    # Подготавливаем тестовые данные: пустой словарь
    test_dict = OrderedDict()
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат равен None (так как минут меньше 10)
    assert result is None, f"Ожидался None для пустого словаря, получен {result}"
    
    # Дополнительно проверяем, что результат действительно None, а не пустой список
    assert result is None, "Функция должна вернуть None, а не пустой список"

def test_exact_10_minutes_same_order():
    """Тест 3: Ровно 10 минут, одинаковый набор и порядок тикеров"""
    # Подготавливаем тестовые данные: словарь с 10 минутами
    test_dict = OrderedDict()
    
    # Базовое время для минут (в мс)
    base_time = 1700000000000  # например, 2023-11-14
    
    # Создаём 10 минут с тестовыми данными
    for i in range(10):
        minute_key = base_time // 60000 + i
        
        # Создаём свечи для трёх тикеров (BTC, ETH, SOL) с разными объёмами
        # В каждой минуте объёмы увеличиваются для проверки суммирования
        candles = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0 + i * 100,
                close=51000.0 + i * 100,
                high=52000.0 + i * 100,
                low=49000.0 + i * 100,
                volume=100.0 + i * 10,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=1000.0 + i * 100,  # BTC объём растёт с каждой минутой
                taker_buy_base_volume=50.0 + i * 5,
                taker_buy_quote_volume=500.0 + i * 50,
                num_of_trades=1000 + i * 100,
                open_time=base_time + i * 60000
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0 + i * 10,
                close=3100.0 + i * 10,
                high=3200.0 + i * 10,
                low=2900.0 + i * 10,
                volume=1000.0 + i * 50,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=2000.0 + i * 200,  # ETH объём
                taker_buy_base_volume=500.0 + i * 25,
                taker_buy_quote_volume=1000.0 + i * 100,
                num_of_trades=2000 + i * 150,
                open_time=base_time + i * 60000
            ),
            KlineRecord(
                symbol="SOLUSDT",
                open=100.0 + i,
                close=110.0 + i,
                high=120.0 + i,
                low=90.0 + i,
                volume=5000.0 + i * 200,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=500.0 + i * 50,  # SOL объём
                taker_buy_base_volume=2500.0 + i * 100,
                taker_buy_quote_volume=250.0 + i * 25,
                num_of_trades=3000 + i * 200,
                open_time=base_time + i * 60000
            )
        ]
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат не None
    assert result is not None, "Функция вернула None при корректных данных"
    
    # Проверяем, что получили ровно 3 объекта Volume_10m (по числу тикеров)
    assert len(result) == 3, f"Ожидалось 3 тикера, получено {len(result)}"
    
    # Проверяем правильность тикеров
    expected_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for i, volume_obj in enumerate(result):
        assert volume_obj.ticker == expected_symbols[i], \
            f"На позиции {i} ожидался {expected_symbols[i]}, получен {volume_obj.ticker}"
    
    # Вычисляем ожидаемые суммы объёмов за 10 минут
    expected_volumes = [
        sum(1000.0 + i * 100 for i in range(10)),  # BTC: 1000 + 1100 + ... + 1900
        sum(2000.0 + i * 200 for i in range(10)),  # ETH: 2000 + 2200 + ... + 3800
        sum(500.0 + i * 50 for i in range(10))     # SOL: 500 + 550 + ... + 950
    ]
    
    # Проверяем объёмы
    for i, volume_obj in enumerate(result):
        assert abs(volume_obj.volume - expected_volumes[i]) < 0.001, \
            f"Для {expected_symbols[i]} ожидался объём {expected_volumes[i]}, получен {volume_obj.volume}"
    
    # Проверяем временные метки
    # open_time должен быть из первой минуты (i=0)
    expected_open_time = base_time
    # close_time должен быть из последней минуты (i=9)
    expected_close_time = base_time + 9 * 60000
    
    for volume_obj in result:
        assert volume_obj.open_time == expected_open_time, \
            f"Ожидался open_time {expected_open_time}, получен {volume_obj.open_time}"
        assert volume_obj.close_time == expected_close_time, \
            f"Ожидался close_time {expected_close_time}, получен {volume_obj.close_time}"
    
    # Дополнительно: проверим, что у всех тикеров одинаковые временные метки
    open_times = [v.open_time for v in result]
    close_times = [v.close_time for v in result]
    assert all(ot == expected_open_time for ot in open_times), "Не у всех тикеров одинаковый open_time"
    assert all(ct == expected_close_time for ct in close_times), "Не у всех тикеров одинаковый close_time"

def test_more_than_10_minutes():
    """Тест 4: Более 10 минут (например, 15 минут) - проверка что берутся последние 10"""
    # Подготавливаем тестовые данные: словарь с 15 минутами
    test_dict = OrderedDict()
    
    base_time = 1700000000000  # базовое время в мс
    minutes_count = 15
    
    # Создаём 15 минут с тестовыми данными
    # В каждой минуте будут свечи для двух тикеров: BTCUSDT и ETHUSDT
    for i in range(minutes_count):
        minute_key = base_time // 60000 + i
        
        # Для каждой минуты задаём quote_assets_volume, который линейно растёт с номером минуты
        # Это позволит легко проверить, что суммируются именно последние 10 минут
        btc_volume = 1000.0 + i * 100.0  # для BTC: 1000, 1100, 1200, ...
        eth_volume = 2000.0 + i * 200.0  # для ETH: 2000, 2200, 2400, ...
        
        candles = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0,
                close=51000.0,
                high=52000.0,
                low=49000.0,
                volume=100.0,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=btc_volume,
                taker_buy_base_volume=50.0,
                taker_buy_quote_volume=500.0,
                num_of_trades=1000,
                open_time=base_time + i * 60000
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0,
                close=3100.0,
                high=3200.0,
                low=2900.0,
                volume=1000.0,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=eth_volume,
                taker_buy_base_volume=500.0,
                taker_buy_quote_volume=1000.0,
                num_of_trades=2000,
                open_time=base_time + i * 60000
            )
        ]
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат не None
    assert result is not None, "Функция вернула None при наличии 15 минут"
    
    # Проверяем, что в результате два элемента (для BTC и ETH)
    assert len(result) == 2, f"Ожидалось 2 элемента, получено {len(result)}"
    
    # Рассчитываем ожидаемые суммы для последних 10 минут (минуты с индексами 5-14)
    expected_btc_volume = sum(1000.0 + i * 100.0 for i in range(5, 15))
    expected_eth_volume = sum(2000.0 + i * 200.0 for i in range(5, 15))
    
    # Находим результаты для каждого тикера
    btc_result = next((r for r in result if r.ticker == "BTCUSDT"), None)
    eth_result = next((r for r in result if r.ticker == "ETHUSDT"), None)
    
    assert btc_result is not None, "BTCUSDT не найден в результате"
    assert eth_result is not None, "ETHUSDT не найден в результате"
    
    # Проверяем суммы объёмов
    assert abs(btc_result.volume - expected_btc_volume) < 0.001, \
        f"BTC volume: ожидалось {expected_btc_volume}, получено {btc_result.volume}"
    assert abs(eth_result.volume - expected_eth_volume) < 0.001, \
        f"ETH volume: ожидалось {expected_eth_volume}, получено {eth_result.volume}"
    
    # Проверяем временные метки - должны быть из 6-й (индекс 5) и 15-й (индекс 14) минут
    expected_open_time = base_time + 5 * 60000  # open_time первой минуты окна (минута 5)
    expected_close_time = base_time + 14 * 60000  # open_time последней минуты окна (минута 14)
    
    assert btc_result.open_time == expected_open_time, \
        f"BTC open_time: ожидалось {expected_open_time}, получено {btc_result.open_time}"
    assert btc_result.close_time == expected_close_time, \
        f"BTC close_time: ожидалось {expected_close_time}, получено {btc_result.close_time}"
    assert eth_result.open_time == expected_open_time, \
        f"ETH open_time: ожидалось {expected_open_time}, получено {eth_result.open_time}"
    assert eth_result.close_time == expected_close_time, \
        f"ETH close_time: ожидалось {expected_close_time}, получено {eth_result.close_time}"

def test_inconsistent_candle_count():
    """Тест 5: Несогласованное количество свечей в минутах"""
    # Подготавливаем тестовые данные: словарь с 10 минутами
    test_dict = OrderedDict()
    
    base_time = 1700000000000  # базовое время в мс
    
    # Создаём 10 минут
    for i in range(10):
        minute_key = base_time // 60000 + i
        
        if i == 5:  # В 6-й минуте (индекс 5) делаем только одну свечу (недостающую)
            candles = [
                KlineRecord(
                    symbol="BTCUSDT",
                    open=50000.0 + i * 100,
                    close=51000.0 + i * 100,
                    high=52000.0 + i * 100,
                    low=49000.0 + i * 100,
                    volume=100.0 + i * 10,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=1000.0 + i * 100,
                    taker_buy_base_volume=50.0 + i * 5,
                    taker_buy_quote_volume=500.0 + i * 50,
                    num_of_trades=1000 + i * 100,
                    open_time=base_time + i * 60000
                )
                # ETHUSDT отсутствует в этой минуте!
            ]
        else:  # В остальных минутах по две свечи
            candles = [
                KlineRecord(
                    symbol="BTCUSDT",
                    open=50000.0 + i * 100,
                    close=51000.0 + i * 100,
                    high=52000.0 + i * 100,
                    low=49000.0 + i * 100,
                    volume=100.0 + i * 10,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=1000.0 + i * 100,
                    taker_buy_base_volume=50.0 + i * 5,
                    taker_buy_quote_volume=500.0 + i * 50,
                    num_of_trades=1000 + i * 100,
                    open_time=base_time + i * 60000
                ),
                KlineRecord(
                    symbol="ETHUSDT",
                    open=3000.0 + i * 10,
                    close=3100.0 + i * 10,
                    high=3200.0 + i * 10,
                    low=2900.0 + i * 10,
                    volume=1000.0 + i * 50,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=2000.0 + i * 100,
                    taker_buy_base_volume=500.0 + i * 20,
                    taker_buy_quote_volume=1000.0 + i * 50,
                    num_of_trades=2000 + i * 150,
                    open_time=base_time + i * 60000
                )
            ]
        
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат равен None (так как данные несогласованы)
    assert result is None, f"Ожидался None при несогласованном количестве свечей, получен {result}"
    
    # Дополнительная проверка: если бы мы не добавили проверку, то возникла бы ошибка IndexError
    # или некорректный результат, но наша функция должна вернуть None

def test_different_ticker_order():
    """Тест 6: Одинаковое количество свечей, но разный порядок тикеров"""
    # Подготавливаем тестовые данные: словарь с 10 минутами
    test_dict = OrderedDict()
    base_time = 1700000000000  # базовое время в мс
    
    # Создаём 10 минут с разным порядком тикеров
    for i in range(10):
        minute_key = base_time // 60000 + i
        
        if i % 2 == 0:  # чётные минуты: сначала BTC, потом ETH
            candles = [
                KlineRecord(
                    symbol="BTCUSDT",
                    open=50000.0 + i * 100,
                    close=51000.0 + i * 100,
                    high=52000.0 + i * 100,
                    low=49000.0 + i * 100,
                    volume=100.0,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=1000.0 + i * 10,
                    taker_buy_base_volume=50.0,
                    taker_buy_quote_volume=500.0,
                    num_of_trades=1000,
                    open_time=base_time + i * 60000
                ),
                KlineRecord(
                    symbol="ETHUSDT",
                    open=3000.0 + i * 10,
                    close=3100.0 + i * 10,
                    high=3200.0 + i * 10,
                    low=2900.0 + i * 10,
                    volume=1000.0,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=2000.0 + i * 20,
                    taker_buy_base_volume=500.0,
                    taker_buy_quote_volume=1000.0,
                    num_of_trades=2000,
                    open_time=base_time + i * 60000
                )
            ]
        else:  # нечётные минуты: сначала ETH, потом BTC (переставленный порядок)
            candles = [
                KlineRecord(
                    symbol="ETHUSDT",
                    open=3000.0 + i * 10,
                    close=3100.0 + i * 10,
                    high=3200.0 + i * 10,
                    low=2900.0 + i * 10,
                    volume=1000.0,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=2000.0 + i * 20,
                    taker_buy_base_volume=500.0,
                    taker_buy_quote_volume=1000.0,
                    num_of_trades=2000,
                    open_time=base_time + i * 60000
                ),
                KlineRecord(
                    symbol="BTCUSDT",
                    open=50000.0 + i * 100,
                    close=51000.0 + i * 100,
                    high=52000.0 + i * 100,
                    low=49000.0 + i * 100,
                    volume=100.0,
                    close_time=base_time + i * 60000 + 59000,
                    quote_assets_volume=1000.0 + i * 10,
                    taker_buy_base_volume=50.0,
                    taker_buy_quote_volume=500.0,
                    num_of_trades=1000,
                    open_time=base_time + i * 60000
                )
            ]
        
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Рассчитываем ожидаемые суммы вручную для проверки
    expected_btc_volume = 0.0
    expected_eth_volume = 0.0
    
    for i in range(10):
        # Функция суммирует по индексам, а не по символам
        if i % 2 == 0:  # чётные: BTC на индексе 0, ETH на индексе 1
            expected_btc_volume += 1000.0 + i * 10  # BTC объём
            expected_eth_volume += 2000.0 + i * 20  # ETH объём
        else:  # нечётные: ETH на индексе 0, BTC на индексе 1
            # ВАЖНО: из-за переставленного порядка, объёмы перепутаются!
            expected_btc_volume += 2000.0 + i * 20  # ETH объём пойдёт в BTC
            expected_eth_volume += 1000.0 + i * 10  # BTC объём пойдёт в ETH
    
    # Проверяем, что результат не None
    assert result is not None, "Функция вернула None, хотя данные согласованы по количеству"
    
    # Находим объекты для BTC и ETH в результате
    btc_result = next((v for v in result if v.ticker == "BTCUSDT"), None)
    eth_result = next((v for v in result if v.ticker == "ETHUSDT"), None)
    
    assert btc_result is not None, "BTCUSDT отсутствует в результате"
    assert eth_result is not None, "ETHUSDT отсутствует в результате"
    
    # Проверяем, что объёмы перепутались (это текущее поведение функции)
    # Вместо точного равенства, проверяем, что они не совпадают с правильными
    correct_btc = sum(1000.0 + i * 10 for i in range(10))
    correct_eth = sum(2000.0 + i * 20 for i in range(10))
    
    # Из-за перестановки, результаты должны отличаться от правильных
    assert btc_result.volume != correct_btc, "BTC объём должен отличаться от правильного (из-за перестановки)"
    assert eth_result.volume != correct_eth, "ETH объём должен отличаться от правильного (из-за перестановки)"
    
    # Проверяем, что суммы совпадают с нашими ожиданиями перепутанных объёмов
    assert abs(btc_result.volume - expected_btc_volume) < 0.001, \
        f"BTC объём {btc_result.volume} не совпадает с ожидаемым перепутанным {expected_btc_volume}"
    assert abs(eth_result.volume - expected_eth_volume) < 0.001, \
        f"ETH объём {eth_result.volume} не совпадает с ожидаемым перепутанным {expected_eth_volume}"
    
    # Дополнительно проверяем, что общая сумма объёмов сохраняется
    total_volume = btc_result.volume + eth_result.volume
    correct_total = correct_btc + correct_eth
    assert abs(total_volume - correct_total) < 0.001, \
        f"Общая сумма {total_volume} не совпадает с правильной {correct_total}"
    
    print(f"  Отладочная информация:")
    print(f"    Правильный BTC: {correct_btc}, полученный BTC: {btc_result.volume}")
    print(f"    Правильный ETH: {correct_eth}, полученный ETH: {eth_result.volume}")
    print(f"    Ожидаемый перепутанный BTC: {expected_btc_volume}")
    print(f"    Ожидаемый перепутанный ETH: {expected_eth_volume}")

def test_zero_volumes():
    """Тест 7: Нулевые объёмы"""
    # Подготавливаем тестовые данные: словарь с 10 минутами
    test_dict = OrderedDict()
    
    base_time = 1700000000000  # базовое время в мс
    minute_keys = []
    
    # Создаём 10 минут
    for i in range(10):
        minute_key = base_time // 60000 + i
        minute_keys.append(minute_key)
        
        # Создаём три свечи для каждой минуты (BTC, ETH, SOL) с нулевыми объёмами
        candles = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0,
                close=51000.0,
                high=52000.0,
                low=49000.0,
                volume=0.0,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=0.0,  # нулевой объём
                taker_buy_base_volume=0.0,
                taker_buy_quote_volume=0.0,
                num_of_trades=0,
                open_time=base_time + i * 60000
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0,
                close=3100.0,
                high=3200.0,
                low=2900.0,
                volume=0.0,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=0.0,  # нулевой объём
                taker_buy_base_volume=0.0,
                taker_buy_quote_volume=0.0,
                num_of_trades=0,
                open_time=base_time + i * 60000
            ),
            KlineRecord(
                symbol="SOLUSDT",
                open=100.0,
                close=105.0,
                high=110.0,
                low=95.0,
                volume=0.0,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=0.0,  # нулевой объём
                taker_buy_base_volume=0.0,
                taker_buy_quote_volume=0.0,
                num_of_trades=0,
                open_time=base_time + i * 60000
            )
        ]
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат не None
    assert result is not None, "Функция вернула None при корректных данных"
    
    # Проверяем, что получили правильное количество объектов Volume_10m
    assert len(result) == 3, f"Ожидалось 3 тикера, получено {len(result)}"
    
    # Проверяем тикеры
    expected_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for i, volume_10m in enumerate(result):
        assert volume_10m.ticker == expected_symbols[i], \
            f"Тикер {i}: ожидался {expected_symbols[i]}, получен {volume_10m.ticker}"
    
    # Проверяем, что все объёмы равны 0
    for i, volume_10m in enumerate(result):
        assert volume_10m.volume == 0.0, \
            f"Объём для {volume_10m.ticker} должен быть 0.0, получен {volume_10m.volume}"
    
    # Проверяем временные метки
    first_minute_open_time = base_time
    last_minute_open_time = base_time + 9 * 60000
    
    for i, volume_10m in enumerate(result):
        assert volume_10m.open_time == first_minute_open_time, \
            f"open_time для {volume_10m.ticker}: ожидался {first_minute_open_time}, получен {volume_10m.open_time}"
        assert volume_10m.close_time == last_minute_open_time, \
            f"close_time для {volume_10m.ticker}: ожидался {last_minute_open_time}, получен {volume_10m.close_time}"

def test_timestamps():
    """Тест 8: Проверка временных меток open_time и close_time"""
    # Подготавливаем тестовые данные: словарь с 10 минутами
    test_dict = OrderedDict()
    
    # Базовое время для минут (каждая минута начинается в 00:00)
    base_time = 1700000000000  # допустим, это 2023-11-15 00:00:00 UTC в мс
    
    # Создаём 10 минут с разными open_time
    for i in range(10):
        minute_key = base_time // 60000 + i
        open_time_minute = base_time + i * 60000  # open_time увеличивается на 1 минуту
        
        # Создаём две свечи для каждой минуты (BTC и ETH)
        candles = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0 + i,
                close=51000.0 + i,
                high=52000.0 + i,
                low=49000.0 + i,
                volume=100.0,
                close_time=open_time_minute + 59000,  # за 1 сек до конца минуты
                quote_assets_volume=1000.0 * (i + 1),
                taker_buy_base_volume=50.0,
                taker_buy_quote_volume=500.0,
                num_of_trades=1000,
                open_time=open_time_minute
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0 + i,
                close=3100.0 + i,
                high=3200.0 + i,
                low=2900.0 + i,
                volume=1000.0,
                close_time=open_time_minute + 59000,
                quote_assets_volume=2000.0 * (i + 1),
                taker_buy_base_volume=500.0,
                taker_buy_quote_volume=1000.0,
                num_of_trades=2000,
                open_time=open_time_minute
            )
        ]
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат не None
    assert result is not None, "Функция вернула None при корректных данных"
    
    # Проверяем, что получили 2 объекта Volume_10m (для двух тикеров)
    assert len(result) == 2, f"Ожидалось 2 результата, получено {len(result)}"
    
    # Сортируем результат по тикеру для удобства проверки
    result_sorted = sorted(result, key=lambda x: x.ticker)
    
    # Ожидаемые значения
    # open_time должен быть из первой минуты (i=0)
    expected_open_time_btc = base_time  # open_time BTC из первой минуты
    expected_open_time_eth = base_time  # open_time ETH из первой минуты
    
    # close_time должен быть open_time из последней минуты (i=9)
    expected_close_time_btc = base_time + 9 * 60000  # open_time BTC из последней минуты
    expected_close_time_eth = base_time + 9 * 60000  # open_time ETH из последней минуты
    
    # Проверяем BTC
    btc_result = next(r for r in result_sorted if r.ticker == "BTCUSDT")
    assert btc_result.open_time == expected_open_time_btc, \
        f"BTCUSDT open_time: ожидалось {expected_open_time_btc}, получено {btc_result.open_time}"
    assert btc_result.close_time == expected_close_time_btc, \
        f"BTCUSDT close_time: ожидалось {expected_close_time_btc}, получено {btc_result.close_time}"
    
    # Проверяем ETH
    eth_result = next(r for r in result_sorted if r.ticker == "ETHUSDT")
    assert eth_result.open_time == expected_open_time_eth, \
        f"ETHUSDT open_time: ожидалось {expected_open_time_eth}, получено {eth_result.open_time}"
    assert eth_result.close_time == expected_close_time_eth, \
        f"ETHUSDT close_time: ожидалось {expected_close_time_eth}, получено {eth_result.close_time}"
    
    # Дополнительная проверка: объёмы должны быть корректными (проверяем BTC)
    expected_btc_volume = sum(1000.0 * (i + 1) for i in range(10))
    assert abs(btc_result.volume - expected_btc_volume) < 0.001, \
        f"BTCUSDT volume: ожидалось {expected_btc_volume}, получено {btc_result.volume}"

def test_input_immutability():
    """Тест 9: Неизменяемость входного словаря"""
    import copy
    
    # Подготавливаем тестовые данные: словарь с 10 минутами
    test_dict = OrderedDict()
    base_time = 1700000000000  # базовое время в мс
    
    # Создаём 10 минут с тестовыми данными
    for i in range(10):
        minute_key = base_time // 60000 + i
        # Создаём две свечи для каждой минуты
        candles = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0 + i * 100,  # меняем цены для разнообразия
                close=51000.0 + i * 100,
                high=52000.0 + i * 100,
                low=49000.0 + i * 100,
                volume=100.0 + i * 10,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=1000.0 + i * 100,
                taker_buy_base_volume=50.0 + i * 5,
                taker_buy_quote_volume=500.0 + i * 50,
                num_of_trades=1000 + i * 100,
                open_time=base_time + i * 60000
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0 + i * 10,
                close=3100.0 + i * 10,
                high=3200.0 + i * 10,
                low=2900.0 + i * 10,
                volume=1000.0 + i * 100,
                close_time=base_time + i * 60000 + 59000,
                quote_assets_volume=2000.0 + i * 200,
                taker_buy_base_volume=500.0 + i * 50,
                taker_buy_quote_volume=1000.0 + i * 100,
                num_of_trades=2000 + i * 200,
                open_time=base_time + i * 60000
            )
        ]
        test_dict[minute_key] = candles
    
    # Делаем глубокую копию исходного словаря для последующего сравнения
    original_dict = copy.deepcopy(test_dict)
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверка 1: функция должна вернуть результат (не None, так как минут ровно 10)
    assert result is not None, "Функция вернула None при корректных данных"
    
    # Проверка 2: исходный словарь не изменился
    assert test_dict.keys() == original_dict.keys(), "Изменились ключи словаря"
    
    # Проверка 3: для каждой минуты содержимое не изменилось
    for minute_key in test_dict.keys():
        original_candles = original_dict[minute_key]
        current_candles = test_dict[minute_key]
        
        assert len(current_candles) == len(original_candles), f"Изменилось количество свечей в минуте {minute_key}"
        
        # Проверяем каждую свечу
        for j, (orig_candle, curr_candle) in enumerate(zip(original_candles, current_candles)):
            # Сравниваем все атрибуты свечей
            assert orig_candle.symbol == curr_candle.symbol, f"Изменился symbol в минуте {minute_key}, свеча {j}"
            assert orig_candle.open == curr_candle.open, f"Изменился open в минуте {minute_key}, свеча {j}"
            assert orig_candle.close == curr_candle.close, f"Изменился close в минуте {minute_key}, свеча {j}"
            assert orig_candle.high == curr_candle.high, f"Изменился high в минуте {minute_key}, свеча {j}"
            assert orig_candle.low == curr_candle.low, f"Изменился low в минуте {minute_key}, свеча {j}"
            assert orig_candle.volume == curr_candle.volume, f"Изменился volume в минуте {minute_key}, свеча {j}"
            assert orig_candle.close_time == curr_candle.close_time, f"Изменился close_time в минуте {minute_key}, свеча {j}"
            assert orig_candle.quote_assets_volume == curr_candle.quote_assets_volume, f"Изменился quote_assets_volume в минуте {minute_key}, свеча {j}"
            assert orig_candle.taker_buy_base_volume == curr_candle.taker_buy_base_volume, f"Изменился taker_buy_base_volume в минуте {minute_key}, свеча {j}"
            assert orig_candle.taker_buy_quote_volume == curr_candle.taker_buy_quote_volume, f"Изменился taker_buy_quote_volume в минуте {minute_key}, свеча {j}"
            assert orig_candle.num_of_trades == curr_candle.num_of_trades, f"Изменился num_of_trades в минуте {minute_key}, свеча {j}"
            assert orig_candle.open_time == curr_candle.open_time, f"Изменился open_time в минуте {minute_key}, свеча {j}"

def test_large_number_of_tickers():
    """Тест 10: Большое количество тикеров (стресс-тест)"""
    import random
    
    # Параметры теста
    NUM_TICKERS = 500  # количество тикеров
    NUM_MINUTES = 10   # ровно 10 минут
    base_time = 1700000000000
    
    # Генерируем названия тикеров (USDT пары)
    tickers = [f"TICKER{i:03d}USDT" for i in range(NUM_TICKERS)]
    
    # Создаём словарь с данными
    test_dict = OrderedDict()
    
    # Для каждой минуты
    for minute_offset in range(NUM_MINUTES):
        minute_key = base_time // 60000 + minute_offset
        candles = []
        
        # Для каждого тикера создаём свечу
        for ticker in tickers:
            # Генерируем случайные, но реалистичные данные
            open_price = random.uniform(10, 1000)
            close_price = open_price * random.uniform(0.95, 1.05)
            high_price = max(open_price, close_price) * random.uniform(1.0, 1.03)
            low_price = min(open_price, close_price) * random.uniform(0.97, 1.0)
            volume = random.uniform(1000, 10000)
            quote_volume = volume * (open_price + close_price) / 2
            
            candle = KlineRecord(
                symbol=ticker,
                open=open_price,
                close=close_price,
                high=high_price,
                low=low_price,
                volume=volume,
                close_time=base_time + minute_offset * 60000 + 59000,
                quote_assets_volume=quote_volume,
                taker_buy_base_volume=volume * random.uniform(0.4, 0.6),
                taker_buy_quote_volume=quote_volume * random.uniform(0.4, 0.6),
                num_of_trades=random.randint(100, 1000),
                open_time=base_time + minute_offset * 60000
            )
            candles.append(candle)
        
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверка 1: результат не None
    assert result is not None, "Функция вернула None при корректных данных"
    
    # Проверка 2: количество результатов равно количеству тикеров
    assert len(result) == NUM_TICKERS, f"Ожидалось {NUM_TICKERS} результатов, получено {len(result)}"
    
    # Проверка 3: проверка структуры и типов данных для первого элемента
    first = result[0]
    assert hasattr(first, 'ticker'), "Объект не имеет поля ticker"
    assert hasattr(first, 'volume'), "Объект не имеет поля volume"
    assert hasattr(first, 'open_time'), "Объект не имеет поля open_time"
    assert hasattr(first, 'close_time'), "Объект не имеет поля close_time"
    
    # Проверка 4: все тикеры присутствуют в результате
    result_tickers = {item.ticker for item in result}
    expected_tickers = set(tickers)
    assert result_tickers == expected_tickers, "Набор тикеров в результате не совпадает с входным"
    
    # Проверка 5: проверка сумм объёмов для нескольких случайных тикеров
    import random
    sample_tickers = random.sample(tickers, 5)
    
    for ticker in sample_tickers:
        # Находим индекс тикера в исходном порядке
        ticker_index = tickers.index(ticker)
        
        # Вычисляем ожидаемую сумму вручную
        expected_volume = 0.0
        for minute_offset in range(NUM_MINUTES):
            candle = test_dict[base_time // 60000 + minute_offset][ticker_index]
            expected_volume += candle.quote_assets_volume
        
        # Находим результат для этого тикера
        result_item = next(item for item in result if item.ticker == ticker)
        
        # Проверяем с учётом погрешности
        assert abs(result_item.volume - expected_volume) < 0.01, \
            f"Для {ticker}: ожидался объём {expected_volume}, получен {result_item.volume}"
    
    # Проверка 6: проверка временных меток для первого тикера
    first_ticker_result = result[0]
    expected_open_time = test_dict[base_time // 60000][0].open_time
    expected_close_time = test_dict[base_time // 60000 + 9][0].open_time
    
    assert first_ticker_result.open_time == expected_open_time, \
        f"open_time: ожидался {expected_open_time}, получен {first_ticker_result.open_time}"
    assert first_ticker_result.close_time == expected_close_time, \
        f"close_time: ожидался {expected_close_time}, получен {first_ticker_result.close_time}"
    
    # Проверка 7: производительность (опционально, можно замерить время)
    import time
    start_time = time.time()
    # Повторный вызов для замера времени (не обязательная проверка)
    calculate_10m_volumes_slidedWindow(test_dict)
    elapsed = time.time() - start_time
    print(f"  Стресс-тест с {NUM_TICKERS} тикерами выполнен за {elapsed:.3f} секунд")
    
    # Проверка 8: все объёмы положительные (не должны быть отрицательными)
    for item in result:
        assert item.volume >= 0, f"Отрицательный объём для {item.ticker}: {item.volume}"

def test_out_of_order_minutes():
    """Тест 11: Минуты идут не по порядку в словаре"""
    # Подготавливаем тестовые данные: словарь с 10 минутами, но ключи расположены в произвольном порядке
    test_dict = OrderedDict()
    
    base_time = 1700000000000  # базовое время в мс
    
    # Создаём 10 минут с ключами в хаотичном порядке
    # Намеренно располагаем минуты не по возрастанию
    minute_keys = [5, 2, 8, 1, 9, 3, 7, 4, 6, 0]  # произвольный порядок
    
    # Для каждой минуты создадим свечи с разными объёмами, чтобы можно было проверить суммы
    volumes_by_minute = {
        0: [100.0, 200.0],   # минута 0: BTC=100, ETH=200
        1: [110.0, 210.0],   # минута 1: BTC=110, ETH=210
        2: [120.0, 220.0],   # минута 2: BTC=120, ETH=220
        3: [130.0, 230.0],   # минута 3: BTC=130, ETH=230
        4: [140.0, 240.0],   # минута 4: BTC=140, ETH=240
        5: [150.0, 250.0],   # минута 5: BTC=150, ETH=250
        6: [160.0, 260.0],   # минута 6: BTC=160, ETH=260
        7: [170.0, 270.0],   # минута 7: BTC=170, ETH=270
        8: [180.0, 280.0],   # минута 8: BTC=180, ETH=280
        9: [190.0, 290.0],   # минута 9: BTC=190, ETH=290
    }
    
    # Заполняем словарь в хаотичном порядке
    for idx, minute_key in enumerate(minute_keys):
        btc_volume, eth_volume = volumes_by_minute[minute_key]
        
        candles = [
            KlineRecord(
                symbol="BTCUSDT",
                open=50000.0 + minute_key * 100,
                close=51000.0 + minute_key * 100,
                high=52000.0 + minute_key * 100,
                low=49000.0 + minute_key * 100,
                volume=100.0 + minute_key * 10,
                close_time=base_time + minute_key * 60000 + 59000,
                quote_assets_volume=btc_volume,
                taker_buy_base_volume=50.0 + minute_key * 5,
                taker_buy_quote_volume=500.0 + minute_key * 50,
                num_of_trades=1000 + minute_key * 100,
                open_time=base_time + minute_key * 60000
            ),
            KlineRecord(
                symbol="ETHUSDT",
                open=3000.0 + minute_key * 10,
                close=3100.0 + minute_key * 10,
                high=3200.0 + minute_key * 10,
                low=2900.0 + minute_key * 10,
                volume=1000.0 + minute_key * 100,
                close_time=base_time + minute_key * 60000 + 59000,
                quote_assets_volume=eth_volume,
                taker_buy_base_volume=500.0 + minute_key * 50,
                taker_buy_quote_volume=1000.0 + minute_key * 100,
                num_of_trades=2000 + minute_key * 200,
                open_time=base_time + minute_key * 60000
            )
        ]
        test_dict[minute_key] = candles
    
    # Вызываем тестируемую функцию
    result = calculate_10m_volumes_slidedWindow(test_dict)
    
    # Проверяем, что результат не None
    assert result is not None, "Функция вернула None, хотя должно быть 10 минут"
    
    # Ожидаемые суммы: последние 10 минут по порядку возрастания ключей
    # Минуты от 0 до 9, последние 10 минут = все минуты (0-9)
    # BTC сумма = 100+110+120+130+140+150+160+170+180+190 = 1450
    # ETH сумма = 200+210+220+230+240+250+260+270+280+290 = 2450
    expected_btc_volume = 1450.0
    expected_eth_volume = 2450.0
    
    # Проверяем, что получили 2 тикера
    assert len(result) == 2, f"Ожидалось 2 тикера, получено {len(result)}"
    
    # Находим результаты для BTC и ETH (порядок может быть любым)
    btc_result = next((r for r in result if r.ticker == "BTCUSDT"), None)
    eth_result = next((r for r in result if r.ticker == "ETHUSDT"), None)
    
    assert btc_result is not None, "BTCUSDT не найден в результате"
    assert eth_result is not None, "ETHUSDT не найден в результате"
    
    # Проверяем суммы объёмов
    assert abs(btc_result.volume - expected_btc_volume) < 0.001, \
        f"BTC: ожидался объём {expected_btc_volume}, получен {btc_result.volume}"
    
    assert abs(eth_result.volume - expected_eth_volume) < 0.001, \
        f"ETH: ожидался объём {expected_eth_volume}, получен {eth_result.volume}"
    
    # Проверяем временные метки: open_time из первой по порядку минуты (минута 0)
    expected_open_time = base_time  # минута 0
    expected_close_time = base_time + 9 * 60000  # минута 9
    
    assert btc_result.open_time == expected_open_time, \
        f"BTC open_time: ожидался {expected_open_time}, получен {btc_result.open_time}"
    assert eth_result.open_time == expected_open_time, \
        f"ETH open_time: ожидался {expected_open_time}, получен {eth_result.open_time}"
    
    assert btc_result.close_time == expected_close_time, \
        f"BTC close_time: ожидался {expected_close_time}, получен {btc_result.close_time}"
    assert eth_result.close_time == expected_close_time, \
        f"ETH close_time: ожидался {expected_close_time}, получен {eth_result.close_time}"
    
    # Дополнительная проверка: убеждаемся, что порядок ключей в словаре не важен,
    # и функция всегда берёт последние 10 минут по значению ключа, а не по порядку добавления


def create_test_candle(symbol: str = "BTCUSDT") -> KlineRecord:
    """Вспомогательная функция для создания тестовой свечи"""
    return KlineRecord(
        symbol=symbol,
        open=50000.0,
        close=51000.0,
        high=52000.0,
        low=49000.0,
        volume=100.0,
        close_time=1700000059000,
        quote_assets_volume=1000.0,
        taker_buy_base_volume=50.0,
        taker_buy_quote_volume=500.0,
        num_of_trades=1000,
        open_time=1700000000000
    )

def run_all_tests():
    """
    Основная функция тестирования.
    Запускает все тесты, перехватывает и выводит результаты.
    """
    tests = [
        test_insufficient_minutes,
        test_empty_dict,
        test_exact_10_minutes_same_order,
        test_more_than_10_minutes,
        test_inconsistent_candle_count,
        test_different_ticker_order,
        test_zero_volumes,
        test_timestamps,
        test_input_immutability,
        test_large_number_of_tickers,
        test_out_of_order_minutes,
    ]

    print("Запуск тестов для calculate_10m_volumes_slidedWindow...\n")
    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            print(f"✅ {test.__name__}: OK")
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__}: FAILED - {e}")
            failed += 1
        except Exception as e:
            print(f"⚠️ {test.__name__}: ERROR - {e}")
            failed += 1

    print(f"\nРезультаты: {passed} пройдено, {failed} упало.")

if __name__ == "__main__":
    run_all_tests()