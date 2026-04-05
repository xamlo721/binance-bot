import asyncio
import time
from collections import OrderedDict

from config import *
from logger import *
from DownloadBot.protocol_download_serializer import *
from DownloadBot.protocol_download import *

class UDPMarketDataServer:
    
    def __init__(self, host: str = DOWNLOADER_UDP_IP, port: int = DOWNLOADER_UDP_PORT):
        self.host = host
        self.port = port
        self.global_data: OrderedDict[int, list[KlineRecord]] = OrderedDict()
        self.symbols: List[str] = []                    # ← храним список символов
        self.serializer = ProtocolSerializer()
        self.transport = None
        self.is_busy = False   # флаг занятости
        self.time_offset_ms: int = 0   # смещение относительно Binance
        
    async def start(self):
        loop = asyncio.get_running_loop()
        
        # Создаем UDP endpoint
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPServerProtocol(self),
            local_addr=(self.host, self.port)
        )
        
        self.transport = transport
        logger.info(f"UDP сервер запущен на {self.host}:{self.port}")
        
    def stop(self):
        if self.transport:
            self.transport.close()
            
    def set_time_offset(self, offset_ms: int) -> None:
        """Устанавливает смещение времени (Binance - локальное) в миллисекундах."""
        self.time_offset_ms = offset_ms

    def get_adjusted_now_ms(self) -> int:
        """Возвращает текущее время с учётом смещения."""
        return int(time.time() * 1000) + self.time_offset_ms
    
    def update_data(self, new_data: OrderedDict[int, list[KlineRecord]]):
        """Обновление данных (вызывается при поступлении новых данных)"""
        self.global_data = new_data

    def update_symbols(self, new_symbols: List[str]):
        self.symbols = new_symbols
        
    def set_busy(self, busy: bool):
        """Установка флага занятости сервера"""
        self.is_busy = busy

class UDPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: UDPMarketDataServer):
        self.server = server
        self.transport = None
        self.packet_counter = 0
        
    def connection_made(self, transport):
        self.transport = transport

    def _send_response(self, data: bytes, addr):
        if self.transport:
            self.transport.sendto(data, addr)
        
    def datagram_received(self, data: bytes, addr):
        try:
            # Разбираем заголовок
            parsed = self.server.serializer.deserialize_packet(data)
            if not parsed:
                logger.error(f"Неверный формат пакета от {addr}")
                return
            ptype, packet_number, payload = parsed

            # Обработка в зависимости от типа
            if ptype == PacketType.KLINES_REQUEST:
                self._handle_kline_request(packet_number, payload, addr)
            elif ptype == PacketType.SYMBOLS_REQUEST:
                self._handle_symbols_request(packet_number, payload, addr)
            elif ptype == PacketType.TIME_REQUEST:
                self._handle_time_request(packet_number, payload, addr)
            else:
                logger.warning(f"Неизвестный тип пакета {ptype} от {addr}")

        except Exception as e:
            logger.error(f"Ошибка обработки запроса от {addr}: {e}")

    def _handle_time_request(self, packet_number: int, payload: bytes, addr):
        req = self.server.serializer.deserialize_time_request(payload)
        if req is None:
            logger.error(f"Некорректный TIME_REQUEST от {addr}")
            return

        logger.debug(f"Time запрос от {addr}: packet={packet_number}, client_ts={req.client_timestamp_ms}")

        # Формируем ответ: всегда успех (0), серверное время
        server_time = self.server.get_adjusted_now_ms()
        resp = TimeResponse(status=0, server_time_ms=server_time)
        response_data = self.server.serializer.serialize_time_response(resp, packet_number)
        self._send_response(response_data, addr)
        logger.debug(f"Отправлен Time ответ для {addr}: server_time={server_time}")

    def _handle_kline_request(self, packet_number: int, payload: bytes, addr):
        # Десериализуем запрос на свечи
        req = self.server.serializer.deserialize_kline_request(payload)
        if req is None:
            logger.error(f"Некорректный KLINES_REQUEST от {addr}")
            return

        logger.info(f"Kline запрос от {addr}: packet={packet_number}, minute={req.minute_number}")

        # Проверка занятости
        if self.server.is_busy:
            resp = KlineResponse(
                minute_number=req.minute_number,
                status=KlineResponseStatus.BUSY,
                records=[]
            )
            response_data = self.server.serializer.serialize_kline_response(resp, packet_number)
            self._send_response(response_data, addr)
            return

        # Поиск минуты
        records = self.server.global_data.get(req.minute_number, [])
        if not records:
            status = KlineResponseStatus.NOT_FOUND
        else:
            status = KlineResponseStatus.OK

        resp = KlineResponse(
            minute_number=req.minute_number,
            status=status,
            records=records
        )
        response_data = self.server.serializer.serialize_kline_response(resp, packet_number)
        self._send_response(response_data, addr)
        logger.debug(f"Отправлен Kline ответ для {addr}: minute={req.minute_number}, records={len(records)}")

    def _handle_symbols_request(self, packet_number: int, payload: bytes, addr):
        # Десериализуем запрос (пустой)
        req = self.server.serializer.deserialize_symbols_request(payload)
        if req is None:
            logger.error(f"Некорректный SYMBOLS_REQUEST от {addr}")
            return

        # TODO: Учитывать запрошеное время

        logger.info(f"Symbols запрос от {addr}: packet={packet_number}")

        # Формируем ответ
        resp = SymbolsResponse(
            status=0 if not self.server.is_busy else 1,   # 1 - сервер занят (можно расширить)
            symbols=self.server.symbols if not self.server.is_busy else []
        )
        response_data = self.server.serializer.serialize_symbols_response(resp, packet_number)
        self._send_response(response_data, addr)
        logger.debug(f"Отправлен Symbols ответ для {addr}: {len(resp.symbols)} символов")