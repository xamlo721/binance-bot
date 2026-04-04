import struct
import zlib
from typing import List, Tuple
from bot_types import KlineRecord

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