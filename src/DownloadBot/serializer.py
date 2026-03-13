import struct
import pickle
from typing import Tuple, Optional

from bot_types import *

class MessageSerializer:
    """Класс для бинарной сериализации сообщений"""
    
    # Формат заголовка: packet_number (I), minute_number (I)
    HEADER_FORMAT = '!II'  # ! означает сетевой порядок байт (big-endian)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    
    @staticmethod
    def serialize_request(request: UDPRequest) -> bytes:
        """Сериализация запроса в бинарный формат"""
        return struct.pack(
            MessageSerializer.HEADER_FORMAT,
            request.packet_number,
            request.minute_number
        )
    
    @staticmethod
    def deserialize_request(data: bytes) -> Optional[UDPRequest]:
        """Десериализация запроса из бинарных данных"""
        try:
            if len(data) < MessageSerializer.HEADER_SIZE:
                return None
                
            packet_number, minute_number = struct.unpack(
                MessageSerializer.HEADER_FORMAT,
                data[:MessageSerializer.HEADER_SIZE]
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
            MessageSerializer.HEADER_FORMAT,
            response.packet_number,
            response.minute_number
        )
        
        # Сериализуем список записей с помощью pickle
        # Альтернатива: можно реализовать кастомную сериализацию для KlineRecord
        records_data = pickle.dumps(response.records)
        
        # Добавляем длину данных записей для возможности чтения
        records_len = struct.pack('!I', len(records_data))
        
        return header + records_len + records_data
    
    @staticmethod
    def deserialize_response(data: bytes) -> Optional[UDPResponse]:
        """Десериализация ответа из бинарных данных"""
        try:
            if len(data) < MessageSerializer.HEADER_SIZE + 4:  # +4 для длины данных
                return None
            
            # Читаем заголовок
            packet_number, minute_number = struct.unpack(
                MessageSerializer.HEADER_FORMAT,
                data[:MessageSerializer.HEADER_SIZE]
            )
            
            # Читаем длину данных записей
            records_len = struct.unpack('!I', data[MessageSerializer.HEADER_SIZE:MessageSerializer.HEADER_SIZE+4])[0]
            
            # Проверяем, что данных достаточно
            if len(data) < MessageSerializer.HEADER_SIZE + 4 + records_len:
                return None
            
            # Десериализуем записи
            records_data = data[MessageSerializer.HEADER_SIZE+4:MessageSerializer.HEADER_SIZE+4+records_len]
            records = pickle.loads(records_data)
            
            return UDPResponse(
                packet_number=packet_number,
                minute_number=minute_number,
                records=records
            )
        except (struct.error, pickle.PickleError, EOFError):
            return None