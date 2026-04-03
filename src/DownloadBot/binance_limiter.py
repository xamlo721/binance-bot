import asyncio
import time

from collections import deque

from logger import logger

# ============== Rate Limiter для Binance API ==============

def get_kline_weight(limit: int) -> int:
    if limit <= 100:
        return 1
    elif limit <= 500:
        return 2
    elif limit <= 1000:
        return 5
    else:
        return 10
    
class BinanceRateLimiter:
    """Ограничитель запросов для Binance API"""
    
    def __init__(self, requests_per_minute: int, requests_weight_per_minute: int):
        """
        Args:
            requests_per_minute: Максимальное количество запросов в минуту (Binance: 1200)
        """
        self.weight_limit = requests_weight_per_minute
        self.requests_limit = requests_per_minute
        self.requests = deque()
        self._lock = asyncio.Lock()
        
    async def wait_if_needed(self, weight: int = 1):
        """Ожидает, если превышен лимит запросов"""
        async with self._lock:
            now = time.time()
            
            # Удаляем запросы старше 1 минуты
            while self.requests and self.requests[0][0] < now - 60:
                self.requests.popleft()
            
            # Текущий вес запросов
            total_weight = sum(w for _, w in self.requests)
            
            # Если достигнут лимит, ждем
            while total_weight + weight > self.weight_limit or len(self.requests) >= self.requests_limit:
                # Ждём, пока освободится достаточно веса
                # Для простоты ждём до истечения самого старого запроса

                if not self.requests:
                    # Очередь пуста, но лимит формально превышен (маловероятно) – просто ждём 1 сек
                    await asyncio.sleep(1)
                    now = time.time()
                    continue

                # Ждём, пока истечёт самый старый запрос
                oldest = self.requests[0][0]
                wait_time = 60 - (now - oldest) 

                if wait_time > 0:
                    # Много потоков спамят в консоль, когда стукаются об лимит.
                    # Обычно первый поток стукается в лимитер и остальные его догоняют
                    # У первого задержка в 40-55 секунд, а у остальных 0,0ХХ секунд (зависит от камня)
                    if wait_time > 1:
                        logger.warning(f"Достигнут лимит запросов Binance. Ожидание {wait_time:.2f} секунд...")
                    await asyncio.sleep(wait_time + 1)
                    
                # После ожидания очищаем старые записи
                now = time.time()
                while self.requests and self.requests[0][0] <= now - 60:
                    self.requests.popleft()
                        
                # Пересчитываем общий вес для следующей итерации цикла
                total_weight = sum(w for _, w in self.requests)
            
            # Добавляем текущий запрос
            self.requests.append((time.time(), weight))