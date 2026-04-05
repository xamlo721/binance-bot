from dataclasses import dataclass

from bot_types import KlineRecord

from enum import IntEnum

class PacketType(IntEnum):

    # Запрос/ответ для получения свечей за минуту
    KLINES_REQUEST = 1
    KLINES_RESPONSE = 128 + KLINES_REQUEST

    # Запрос/ответ для получения списка доступных символов
    SYMBOLS_REQUEST = 2
    SYMBOLS_RESPONSE = 128 + SYMBOLS_REQUEST

    # Запрос/ответ для получения текущего времени binance
    TIME_REQUEST = 3
    TIME_RESPONSE = 128 + TIME_REQUEST


@dataclass
class Packet:
    # тип пакета (1 байт)
    packet_type: PacketType
    # номер пакета (4 байта)
    packet_number: int
    # размер поля данных пакета (4 байта)
    packet_lenght: int

class ServerResponseStatus:
    # успех
    OK = 0
    # данные не найдены
    NOT_FOUND = 1
    # сервер занят
    BUSY = 2

# ============================== Symbols requests ==================================================== #

@dataclass
class SymbolsRequest:
    # номер минуты, начиная с 1970г, для которой нужно вернуть список торгуемых тикеров (4 байта)
    request_time: int

@dataclass
class SymbolsResponse:
    # код статуса (0=успех, иначе ошибка)
    status: int
    # список тикеров
    symbols: list[str]

# ============================== Kline requests ==================================================== #

@dataclass
class KlineRequest:
    # номер минуты (4 байта)
    minute_number: int


@dataclass 
class KlineResponse:
    # номер минуты (4 байта)
    minute_number: int
    # код статуса
    status: int
    # список записей              
    records: list[KlineRecord]

# ============================== Kline requests ==================================================== #

@dataclass
class TimeRequest:
    """Запрос времени сервера. Тело может быть пустым или содержать метку времени клиента."""
    # время клиента на момент отправки
    client_timestamp_ms: int = 0

@dataclass
class TimeResponse:
    # 0 - успех, иначе ошибка
    status: int
     # текущее время сервера (скорректированное) в миллисекундах
    server_time_ms: int

    