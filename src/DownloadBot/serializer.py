import struct
from typing import Optional

from bot_types import *

class MessageSerializer:
    """Класс для бинарной сериализации сообщений"""
    
    # Формат заголовка: packet_number (I), minute_number (I)
    # ! означает сетевой порядок байт (big-endian)
    # 12 байт
    REQUEST_HEADER_FORMAT = '!II'
    REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_HEADER_FORMAT)

    # Формат ответа: packet_number (I), minute_number (I), status (I) — 12 байт
    RESPONSE_HEADER_FORMAT = '!III'
    RESPONSE_HEADER_SIZE = struct.calcsize(RESPONSE_HEADER_FORMAT)
    
    @staticmethod
    def serialize_request(request: UDPRequest) -> bytes:
        """Сериализация запроса в бинарный формат"""
        return struct.pack(
            MessageSerializer.REQUEST_HEADER_FORMAT,
            request.packet_number,
            request.minute_number
        )
    
    @staticmethod
    def deserialize_request(data: bytes) -> Optional[UDPRequest]:
        """Десериализация запроса из бинарных данных"""
        try:
            if len(data) < MessageSerializer.REQUEST_HEADER_SIZE:
                return None
                
            packet_number, minute_number = struct.unpack(
                MessageSerializer.REQUEST_HEADER_FORMAT,
                data[:MessageSerializer.REQUEST_HEADER_SIZE]
            )
            
            return UDPRequest(
                packet_number=packet_number,
                minute_number=minute_number
            )
        except struct.error:
            return None
    
    @staticmethod
    def serialize_response(response: UDPResponse) -> bytes:
        """Сериализация ответа в бинарный формат"""
        # Сериализуем заголовок
        header = struct.pack(
            MessageSerializer.RESPONSE_HEADER_FORMAT,
            response.packet_number,
            response.minute_number,
            response.status
        )

        # Сериализуем записи компактно
        records_data = KlineRecordSerializer.serialize_records(response.records)

        # Сжимаем данные (уровень сжатия 6 — компромисс)
        compressed_data = zlib.compress(records_data, level=6)

        # Длина сжатых данных (4 байта)
        compressed_len = struct.pack('!I', len(compressed_data))

        return header + compressed_len + compressed_data
    
    @staticmethod
    def deserialize_response(data: bytes) -> Optional[UDPResponse]:
        """Десериализация ответа из бинарных данных"""
        if len(data) < MessageSerializer.RESPONSE_HEADER_SIZE + 4:
            return None

        # Заголовок (packet, minute, status)
        packet_number, minute_number, status = struct.unpack(
            MessageSerializer.RESPONSE_HEADER_FORMAT,
            data[:MessageSerializer.RESPONSE_HEADER_SIZE]
        )

        # Длина сжатых данных
        compressed_len = struct.unpack('!I', data[MessageSerializer.RESPONSE_HEADER_SIZE:MessageSerializer.RESPONSE_HEADER_SIZE+4])[0]

        # Проверяем, что данных достаточно
        if len(data) < MessageSerializer.RESPONSE_HEADER_SIZE + 4 + compressed_len:
            return None

        # Извлекаем сжатые данные
        compressed_data = data[MessageSerializer.RESPONSE_HEADER_SIZE+4:MessageSerializer.RESPONSE_HEADER_SIZE+4+compressed_len]

        try:
            # Распаковываем
            records_data = zlib.decompress(compressed_data)
            # Десериализуем записи
            records = KlineRecordSerializer.deserialize_records(records_data)
        except (zlib.error, struct.error) as e:
            print(f"Ошибка распаковки/десериализации: {e}")
            return None

        return UDPResponse(
            packet_number=packet_number,
            minute_number=minute_number,
            status=status,
            records=records
        )

import struct
import zlib
from typing import List, Tuple

class KlineRecordSerializer:
    """
    Компактная бинарная сериализация для KlineRecord.
    Формат записи (все поля фиксированной длины):
        symbol (10 байт) — UTF-8, обрезается/дополняется нулями
        open, close, high, low, volume, quote_assets_volume,
        taker_buy_base_volume, taker_buy_quote_volume — 8 double (64 bit)
        close_time, open_time — 2 long long (64 bit)
        num_of_trades — 1 int (32 bit)
    Итого: 10 + 8*8 + 2*8 + 4 = 94 байта на запись.
    """
    RECORD_FORMAT = '!16s8d2qI'  # 'q' для signed long long, 'I' для unsigned int
    RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

    @staticmethod
    def serialize_records(records: List[KlineRecord]) -> bytes:
        """Сериализует список записей в бинарный блок."""
        data = bytearray()
        for rec in records:
            # symbol обрезаем до 10 байт и дополняем нулями
            symbol_bytes = rec.symbol.encode('utf-8')[:16].ljust(10, b'\x00')
            packed = struct.pack(
                KlineRecordSerializer.RECORD_FORMAT,
                symbol_bytes,
                rec.open,
                rec.close,
                rec.high,
                rec.low,
                rec.volume,
                rec.quote_assets_volume,
                rec.taker_buy_base_volume,
                rec.taker_buy_quote_volume,
                rec.close_time,
                rec.open_time,
                rec.num_of_trades
            )
            data.extend(packed)
        return bytes(data)

    @staticmethod
    def deserialize_records(data: bytes) -> List[KlineRecord]:
        """Десериализует бинарный блок в список записей."""
        records = []
        offset = 0
        while offset + KlineRecordSerializer.RECORD_SIZE <= len(data):
            record_data = data[offset:offset + KlineRecordSerializer.RECORD_SIZE]
            (symbol_bytes,
             open_,
             close_,
             high_,
             low_,
             volume_,
             quote_assets_volume_,
             taker_buy_base_volume_,
             taker_buy_quote_volume_,
             close_time_,
             open_time_,
             num_of_trades_) = struct.unpack(
                KlineRecordSerializer.RECORD_FORMAT, record_data
            )
            symbol = symbol_bytes.decode('utf-8').rstrip('\x00')
            record = KlineRecord(
                symbol=symbol,
                open=open_,
                close=close_,
                high=high_,
                low=low_,
                volume=volume_,
                close_time=close_time_,
                quote_assets_volume=quote_assets_volume_,
                taker_buy_base_volume=taker_buy_base_volume_,
                taker_buy_quote_volume=taker_buy_quote_volume_,
                num_of_trades=num_of_trades_,
                open_time=open_time_
            )
            records.append(record)
            offset += KlineRecordSerializer.RECORD_SIZE
        return records