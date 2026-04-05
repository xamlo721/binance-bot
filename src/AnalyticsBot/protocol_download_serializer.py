import struct
import zlib

from typing import Optional

from AnalyticsBot.bot_types import *
from AnalyticsBot.protocol_download import *
from AnalyticsBot.bot_types_serializer import *

class ProtocolSerializer:
    """Класс для бинарной сериализации сообщений"""
    
    # Заголовок: тип (B), номер пакета (I), длина данных (I) -> 1+4+4=9 байт
    # ! означает сетевой порядок байт (big-endian)
    HEADER_FORMAT = '!BII'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @staticmethod
    def _build_header(packet_type: PacketType, packet_number: int, data_length: int) -> bytes:
        return struct.pack(ProtocolSerializer.HEADER_FORMAT,
                           packet_type.value, packet_number, data_length)

    @staticmethod
    def _parse_header(data: bytes) -> Optional[tuple[PacketType, int, int]]:
        if len(data) < ProtocolSerializer.HEADER_SIZE:
            return None
        ptype_val, pnum, dlen = struct.unpack(ProtocolSerializer.HEADER_FORMAT,
                                              data[:ProtocolSerializer.HEADER_SIZE])
        try:
            ptype = PacketType(ptype_val)
        except ValueError:
            return None
        return ptype, pnum, dlen

    # ---------- Сериализация запросов/ответов ----------
    @staticmethod
    def serialize_kline_request(req: KlineRequest, packet_number: int) -> bytes:
        """Формирует пакет KLINES_REQUEST."""
        # Данные: minute_number (I)
        data = struct.pack('!I', req.minute_number)
        header = ProtocolSerializer._build_header(PacketType.KLINES_REQUEST,
                                                  packet_number, len(data))
        return header + data

    @staticmethod
    def serialize_kline_response(resp: KlineResponse, packet_number: int) -> bytes:
        """Формирует пакет KLINES_RESPONSE."""
        # Сериализуем records через KlineRecordSerializer
        records_data = KlineRecordSerializer.serialize_records(resp.records)
        compressed = zlib.compress(records_data, level=6)
        # Формат: minute_number (I), status (I), compressed_len (I), compressed_data
        data = struct.pack('!II', resp.minute_number, resp.status) + \
               struct.pack('!I', len(compressed)) + compressed
        header = ProtocolSerializer._build_header(PacketType.KLINES_RESPONSE,
                                                  packet_number, len(data))
        return header + data

    @staticmethod
    def serialize_symbols_request(req: SymbolsRequest, packet_number: int) -> bytes:
        """Формирует пакет SYMBOLS_REQUEST."""
        # payload: timestamp_minutes (4 байта, big-endian)
        data = struct.pack('!I', req.request_time)
        header = ProtocolSerializer._build_header(PacketType.SYMBOLS_REQUEST,
                                                  packet_number, len(data))
        return header + data

    @staticmethod
    def serialize_symbols_response(resp: SymbolsResponse, packet_number: int) -> bytes:
        """Формирует пакет SYMBOLS_RESPONSE."""
        # Формат: status (I), количество строк (I), затем для каждой строки: длина (H) + UTF-8        
        data = struct.pack('!II', resp.status, len(resp.symbols))
        for sym in resp.symbols:
            sym_bytes = sym.encode('utf-8')
            data += struct.pack('!H', len(sym_bytes)) + sym_bytes
        header = ProtocolSerializer._build_header(PacketType.SYMBOLS_RESPONSE,
                                                  packet_number, len(data))
        return header + data

    @staticmethod
    def serialize_time_request(req: TimeRequest, packet_number: int) -> bytes:
        """Формирует пакет TIME_REQUEST."""
        data = struct.pack('!q', req.client_timestamp_ms)  # 8 байт, signed
        header = ProtocolSerializer._build_header(PacketType.TIME_REQUEST,
                                                  packet_number, len(data))
        return header + data

    @staticmethod
    def serialize_time_response(resp: TimeResponse, packet_number: int) -> bytes:
        """Формирует пакет TIME_RESPONSE."""
        data = struct.pack('!iq', resp.status, resp.server_time_ms)  # status: int, time: long long
        header = ProtocolSerializer._build_header(PacketType.TIME_RESPONSE,
                                                  packet_number, len(data))
        return header + data
    
    # ---------- Десериализация ----------
    @staticmethod
    def deserialize_packet(data: bytes) -> Optional[tuple[PacketType, int, bytes]]:
        """Разбирает заголовок и возвращает (тип, номер_пакета, payload)."""
        parsed = ProtocolSerializer._parse_header(data)
        if not parsed:
            return None
        ptype, pnum, dlen = parsed
        if len(data) < ProtocolSerializer.HEADER_SIZE + dlen:
            return None
        payload = data[ProtocolSerializer.HEADER_SIZE:ProtocolSerializer.HEADER_SIZE + dlen]
        return ptype, pnum, payload

    @staticmethod
    def deserialize_kline_request(payload: bytes) -> Optional[KlineRequest]:
        if len(payload) < 4:
            return None
        minute_number = struct.unpack('!I', payload[:4])[0]
        return KlineRequest(minute_number=minute_number)

    @staticmethod
    def deserialize_kline_response(payload: bytes) -> Optional[KlineResponse]:
        # Формат: minute (I), status (I), compressed_len (I), compressed_data
        if len(payload) < 12:
            return None
        minute, status, comp_len = struct.unpack('!III', payload[:12])
        if len(payload) < 12 + comp_len:
            return None
        compressed = payload[12:12+comp_len]
        try:
            records_data = zlib.decompress(compressed)
            records = KlineRecordSerializer.deserialize_records(records_data)
        except Exception:
            return None
        return KlineResponse(minute_number=minute, status=status, records=records)

    @staticmethod
    def deserialize_symbols_request(payload: bytes) -> Optional[SymbolsRequest]:
        """Извлекает timestamp_minutes из payload."""
        if len(payload) < 4:
            return None
        ts_min = struct.unpack('!I', payload[:4])[0]
        return SymbolsRequest(request_time=ts_min)

    @staticmethod
    def deserialize_symbols_response(payload: bytes) -> Optional[SymbolsResponse]:
        if len(payload) < 8:   # минимум status + count
            return None
        status, num_symbols = struct.unpack('!II', payload[:8])
        symbols = []
        pos = 8
        for _ in range(num_symbols):
            if pos + 2 > len(payload):
                return None
            sym_len = struct.unpack('!H', payload[pos:pos+2])[0]
            pos += 2
            if pos + sym_len > len(payload):
                return None
            sym = payload[pos:pos+sym_len].decode('utf-8')
            symbols.append(sym)
            pos += sym_len
        return SymbolsResponse(status=status, symbols=symbols)  

    @staticmethod
    def deserialize_time_request(payload: bytes) -> Optional[TimeRequest]:
        if len(payload) < 8:
            return None
        client_ts = struct.unpack('!q', payload[:8])[0]
        return TimeRequest(client_timestamp_ms=client_ts)

    @staticmethod
    def deserialize_time_response(payload: bytes) -> Optional[TimeResponse]:
        if len(payload) < 12:  # 4 (status) + 8 (time)
            return None
        status, server_time = struct.unpack('!iq', payload[:12])
        return TimeResponse(status=status, server_time_ms=server_time)
    


    