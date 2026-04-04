from dataclasses import dataclass

from bot_types import KlineRecord

from enum import IntEnum

class PacketType(IntEnum):

    # Запрос/ответ для получения свечей за минуту
    KLINES_REQUEST = 1
    KLINES_RESPONSE = 128 + KLINES_REQUEST

    # Запрос/ответ для получения списка доступных символов
    SYMBOLS_REQUEST = 2
    SYMBOLS_RESPONSE = 128 + KLINES_REQUEST


@dataclass
class Packet:
    # тип пакета (1 байт)
    packet_type: PacketType
    # номер пакета (4 байта)
    packet_number: int
    # размер поля данных пакета (4 байта)
    packet_lenght: int

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

class KlineResponseStatus:
    # успех
    OK = 0
    # минута не найдена
    NOT_FOUND = 1
    # сервер занят
    BUSY = 2

@dataclass 
class KlineResponse:
    # номер минуты (4 байта)
    minute_number: int
    # код статуса
    status: int
    # список записей              
    records: list[KlineRecord]

