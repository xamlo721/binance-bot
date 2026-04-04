import struct
from typing import Union

from protocol_alert import *
from bot_types_serializer import *

class AlertProtocolSerializer:
    """Сериализация/десериализация сообщений алерт-системы.

    Формат сообщения:
        [тип сообщения (1 байт)] [packet_number (4 байта)] [данные (опционально)]
    """

    # Формат: тип (B) + длина данных (I) + данные
    HEADER_FORMAT = '!BI'  # unsigned char, unsigned int
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @staticmethod
    def serialize(msg: Union[AlertRegister, AlertUnregister, AlertData]) -> bytes:
        """Сериализует сообщение в байты."""
        if isinstance(msg, AlertRegister):
            msg_type = AlertMessageType.REGISTER
            payload = b''
        elif isinstance(msg, AlertUnregister):
            msg_type = AlertMessageType.UNREGISTER
            payload = b''
        elif isinstance(msg, AlertData):
            msg_type = AlertMessageType.ALERT
            payload = AlertRecordSerializer.serialize(msg.alert) 
        else:
            raise TypeError(f"Неподдерживаемый тип сообщения: {type(msg)}")

        header = struct.pack(AlertProtocolSerializer.HEADER_FORMAT, msg_type.value, msg.packet_number)
        return header + payload

    @staticmethod
    def deserialize(data: bytes) -> Union[AlertRegister, AlertUnregister, AlertData]:
        """Десериализует байты в объект сообщения."""
        if len(data) < AlertProtocolSerializer.HEADER_SIZE:
            raise ValueError("Недостаточно данных для заголовка")

        msg_type_val, packet_number = struct.unpack(AlertProtocolSerializer.HEADER_FORMAT, data[:AlertProtocolSerializer.HEADER_SIZE])
        msg_type = AlertMessageType(msg_type_val)
        payload = data[AlertProtocolSerializer.HEADER_SIZE:]

        if msg_type == AlertMessageType.REGISTER:
            if payload:
                raise ValueError("Регистрационное сообщение не должно содержать данных")
            return AlertRegister(packet_number=packet_number)

        elif msg_type == AlertMessageType.UNREGISTER:
            if payload:
                raise ValueError("Сообщение отписки не должно содержать данных")
            return AlertUnregister(packet_number=packet_number)

        elif msg_type == AlertMessageType.ALERT:
            try:
                alert = AlertRecordSerializer.deserialize(payload)
            except Exception as e:
                raise ValueError(f"Ошибка десериализации AlertRecord: {e}")
            if not isinstance(alert, AlertRecord):
                raise TypeError("Ожидался объект AlertRecord")
            return AlertData(packet_number=packet_number, alert=alert)

        else:
            raise ValueError(f"Неизвестный тип сообщения: {msg_type_val}")
        
        