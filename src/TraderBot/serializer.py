import struct
import pickle
from typing import Optional
from typing import Tuple
from typing import Union

from TraderBot.bot_types import *
from TraderBot.bot_types import *


class AlertRecordSerializer:
    # Максимальные длины строк
    TICKER_MAX_BYTES = 16

    @staticmethod
    def serialize(alert: AlertRecord) -> bytes:
        # Тикер (фиксированная длина, дополняется нулями)
        ticker_bytes = alert.ticker.encode('utf-8')[:AlertRecordSerializer.TICKER_MAX_BYTES].ljust(AlertRecordSerializer.TICKER_MAX_BYTES, b'\x00')

        # Объём (теперь float -> double, 8 байт)
        volume_bytes = struct.pack('!d', alert.volume)

        # Время события (unsigned long long, 8 байт)
        time_bytes = struct.pack('!Q', alert.time)

        # Битовая маска наличия опциональных полей
        flags = 0
        if alert.buy_short_price is not None:
            flags |= 1 << 0
        if alert.min_price is not None:
            flags |= 1 << 1
        if alert.min_price_time is not None:
            flags |= 1 << 2
        if alert.max_price is not None:
            flags |= 1 << 3
        if alert.max_price_time is not None:
            flags |= 1 << 4
        flags_byte = struct.pack('!B', flags)

        # Собираем обязательную часть
        data = ticker_bytes + volume_bytes + time_bytes + flags_byte

        # Опциональные поля в порядке битов
        if alert.buy_short_price is not None:
            data += struct.pack('!d', alert.buy_short_price)
        if alert.min_price is not None:
            data += struct.pack('!d', alert.min_price)
        if alert.min_price_time is not None:
            data += struct.pack('!Q', alert.min_price_time)
        if alert.max_price is not None:
            data += struct.pack('!d', alert.max_price)
        if alert.max_price_time is not None:
            data += struct.pack('!Q', alert.max_price_time)

        return data

    @staticmethod
    def deserialize(data: bytes) -> AlertRecord:
        offset = 0

        # Тикер
        ticker_bytes = data[offset:offset + AlertRecordSerializer.TICKER_MAX_BYTES]
        ticker = ticker_bytes.decode('utf-8').rstrip('\x00')
        offset += AlertRecordSerializer.TICKER_MAX_BYTES

        # Объём (double)
        volume = struct.unpack('!d', data[offset:offset + 8])[0]
        offset += 8

        # Время
        time = struct.unpack('!Q', data[offset:offset + 8])[0]
        offset += 8

        # Флаги
        flags = struct.unpack('!B', data[offset:offset + 1])[0]
        offset += 1

        # Создаём базовый объект
        alert = AlertRecord(ticker=ticker, volume=volume, time=time)

        # Опциональные поля
        if flags & (1 << 0):
            alert.buy_short_price = struct.unpack('!d', data[offset:offset + 8])[0]
            offset += 8
        if flags & (1 << 1):
            alert.min_price = struct.unpack('!d', data[offset:offset + 8])[0]
            offset += 8
        if flags & (1 << 2):
            alert.min_price_time = struct.unpack('!Q', data[offset:offset + 8])[0]
            offset += 8
        if flags & (1 << 3):
            alert.max_price = struct.unpack('!d', data[offset:offset + 8])[0]
            offset += 8
        if flags & (1 << 4):
            alert.max_price_time = struct.unpack('!Q', data[offset:offset + 8])[0]
            offset += 8

        return alert
 
    

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
        
        
        
