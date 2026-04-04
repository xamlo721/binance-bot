import asyncio

from typing import Optional, Tuple, Dict, Any

from AnalyticsBot.logger import logger
from AnalyticsBot.config import *

from AnalyticsBot.protocol_download import *
from AnalyticsBot.protocol_download_serializer import ProtocolSerializer

# ========== Клиент ==========
class UDPClientProtocol(asyncio.DatagramProtocol):

    def __init__(self, serializer: ProtocolSerializer):
        self.serializer = serializer
        self.transport = None
        self.pending_futures: Dict[int, asyncio.Future] = {}
        self.timeout_handles: Dict[int, asyncio.TimerHandle] = {}

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        parsed = self.serializer.deserialize_packet(data)
        if not parsed:
            logger.error("Некорректный заголовок пакета")
            return
        ptype, packet_number, payload = parsed

        if ptype not in (PacketType.KLINES_RESPONSE, PacketType.SYMBOLS_RESPONSE):
            logger.warning(f"Получен пакет не-ответ: {ptype}")
            return

        future = self.pending_futures.pop(packet_number, None)
        if future is None:
            logger.warning(f"Ответ с неизвестным packet_number={packet_number}")
            return

        handle = self.timeout_handles.pop(packet_number, None)
        if handle:
            handle.cancel()

        try:
            if ptype == PacketType.KLINES_RESPONSE:
                response = self.serializer.deserialize_kline_response(payload)
            else:
                response = self.serializer.deserialize_symbols_response(payload)
            if response is None:
                future.set_exception(ValueError("Ошибка десериализации ответа"))
            else:
                future.set_result(response)
        except Exception as e:
            future.set_exception(e)

    def error_received(self, exc):
        for future in self.pending_futures.values():
            if not future.done():
                future.set_exception(exc)
        self.pending_futures.clear()
        for handle in self.timeout_handles.values():
            handle.cancel()
        self.timeout_handles.clear()

    async def send_request(self, data: bytes, addr: Tuple[str, int], packet_number: int, timeout: float) -> Any:
        if self.transport is None:
            raise RuntimeError("Транспорт не инициализирован")
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_futures[packet_number] = future

        def on_timeout():
            if packet_number in self.pending_futures:
                self.pending_futures.pop(packet_number)
                if not future.done():
                    future.set_exception(asyncio.TimeoutError(f"Таймаут {timeout}с для пакета {packet_number}"))

        handle = loop.call_later(timeout, on_timeout)
        self.timeout_handles[packet_number] = handle

        self.transport.sendto(data, addr)
        return await future


class UDPClient:
    def __init__(self, serializer: Optional[ProtocolSerializer] = None):
        self.serializer = serializer or ProtocolSerializer()
        self.transport = None
        self.protocol = None
        self._packet_counter = 0

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def connect(self):
        loop = asyncio.get_running_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(self.serializer),
            local_addr=(ALERT_SERVER_IP, 0)
        )

    def _next_packet_number(self) -> int:
        self._packet_counter += 1
        return self._packet_counter

    async def request_klines(self, minute_number: int, server_addr: Tuple[str, int], timeout: float = 10.0, packet_number: Optional[int] = None) -> KlineResponse:
        if not self.protocol:
            raise RuntimeError("Клиент не подключён. Вызовите connect() или используйте async with.")
        pnum = packet_number if packet_number is not None else self._next_packet_number()
        req = KlineRequest(minute_number=minute_number)
        data = self.serializer.serialize_kline_request(req, pnum)
        response = await self.protocol.send_request(data, server_addr, pnum, timeout)
        if not isinstance(response, KlineResponse):
            raise TypeError(f"Ожидался KlineResponse, получен {type(response)}")
        return response

    async def request_symbols(self, request_time: int, server_addr: Tuple[str, int], timeout: float = 10.0, packet_number: Optional[int] = None) -> SymbolsResponse:
        if not self.protocol:
            raise RuntimeError("Клиент не подключён.")
        pnum = packet_number if packet_number is not None else self._next_packet_number()
        req = SymbolsRequest(request_time=request_time)
        data = self.serializer.serialize_symbols_request(req, pnum)
        response = await self.protocol.send_request(data, server_addr, pnum, timeout)
        if not isinstance(response, SymbolsResponse):
            raise TypeError(f"Ожидался SymbolsResponse, получен {type(response)}")
        return response

    def close(self):
        if self.transport:
            self.transport.close()
            self.transport = None
            self.protocol = None