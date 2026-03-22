
# Количество свечей, хранящихся в кеше
MAX_CACHED_CANDLES: int = 2880 # 2880
# Лимит Binance для klines endpoint
MAX_CANDLES_PER_REQUEST: int = 1500
# Binance API limit requests per minute = 1200 
BINANCE_API_REQUEST_LIMIT: int = 800 
# Binance API limit wight per minute = 2400
BINANCE_API_WEIGHT_LIMIT: int = 2000
# Количество потоков, участвующих в запросе сервера
THREAD_POOL_SIZE: int = 12 # 30
# UDP IP, PORT
DOWNLOADER_UDP_IP: str = "127.0.0.1"
DOWNLOADER_UDP_PORT: int = 58001
