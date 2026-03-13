import asyncio
import struct
from dataclasses import dataclass
from typing import List, Optional, Tuple

# ========== Копия классов сериализации (или импорт) ==========
# Эти классы должны быть идентичны тем, что используются на сервере.
# Предположим, они находятся в модуле `serializer`.
# Если нет – скопируйте их сюда или импортируйте.
from serializer import UDPRequest, UDPResponse, MessageSerializer

# ========== Клиент ==========
class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, serializer: MessageSerializer):
        self.serializer = serializer
        self.transport = None
        self.response_future: Optional[asyncio.Future] = None
        self.timeout_handle = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        """Получен ответ от сервера"""
        if self.response_future and not self.response_future.done():
            response = self.serializer.deserialize_response(data)
            if response:
                self.response_future.set_result(response)
            else:
                self.response_future.set_exception(
                    ValueError("Некорректный ответ от сервера")
                )
        if self.timeout_handle:
            self.timeout_handle.cancel()

    def error_received(self, exc):
        if self.response_future and not self.response_future.done():
            self.response_future.set_exception(exc)

    async def send_request(
        self, request: UDPRequest, addr: Tuple[str, int], timeout: float = 5.0
    ) -> UDPResponse:
        """Отправить запрос и дождаться ответа"""
        # Убедимся, что транспорт инициализирован
        if self.transport is None:
            raise RuntimeError("Транспорт не инициализирован. Сначала создайте соединение через create_datagram_endpoint.")
        
        loop = asyncio.get_running_loop()
        self.response_future = loop.create_future()

        # Сериализуем запрос и отправляем
        data = self.serializer.serialize_request(request)
        self.transport.sendto(data, addr)

        # Таймаут
        self.timeout_handle = loop.call_later(timeout, self._on_timeout)

        try:
            return await self.response_future
        finally:
            if self.timeout_handle:
                self.timeout_handle.cancel()

    def _on_timeout(self):
        if self.response_future and not self.response_future.done():
            self.response_future.set_exception(asyncio.TimeoutError("Таймаут ожидания ответа"))


class UDPClient:
    """Удобный клиент для отправки запросов к UDP-серверу."""
    def __init__(self, serializer: Optional[MessageSerializer] = None):
        self.serializer = serializer or MessageSerializer()
        self.transport = None
        self.protocol = None

    async def __aenter__(self):
        """Поддержка контекстного менеджера async with."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def connect(self):
        """Создать UDP-соединение (один раз для всех запросов)."""
        loop = asyncio.get_running_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(self.serializer),
            local_addr=('0.0.0.0', 0)  # любой свободный порт
        )

    async def request(
        self,
        packet_number: int,
        minute_number: int,
        server_addr: Tuple[str, int],
        timeout: float = 5.0
    ) -> UDPResponse:
        """
        Отправить запрос на сервер и получить ответ.
        """
        if not self.protocol:
            raise RuntimeError("Клиент не подключён. Вызовите connect() или используйте async with.")
        request = UDPRequest(
            packet_number=packet_number,
            minute_number=minute_number
        )
        return await self.protocol.send_request(request, server_addr, timeout)

    def close(self):
        """Закрыть транспорт."""
        if self.transport:
            self.transport.close()
            self.transport = None
            self.protocol = None