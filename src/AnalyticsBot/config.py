import os

# Количество свечей, хранящихся в кеше
MAX_CACHED_CANDLES: int = 1500  # 
# Лимит Binance для klines endpoint
MAX_CANDLES_PER_REQUEST: int = 1500
# Binance API limit per minute = 1200 
BINANCE_API_LIMIT: int = 800 
# Количество пото
THREAD_POOL_SIZE: int = 30 # 30
