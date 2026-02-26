import os

from binance.client import Client
from dotenv import load_dotenv

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