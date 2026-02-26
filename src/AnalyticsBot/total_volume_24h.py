from datetime import datetime
from logger import logger
from config import *

from ramstorage.ram_storage_utils import get_recent_1h_klines
from ramstorage.HoursRecord import HoursRecord

def process_single_hour(hour_records: list[HoursRecord]) -> dict[str, float]:
    """
    Принимает список записей за один час и возвращает словарь
    {symbol: суммарный total_volume} для этого часа.
    """
    result = {}
    for record in hour_records:
        sym = record.symbol
        vol = record.total_volume
        result[sym] = result.get(sym, 0.0) + vol
    return result

def process_new_record(new_hour_record: list[HoursRecord]) -> dict[str, float]:
    """
    Основная функция обработки. Получает свежую часовую запись,
    запрашивает необходимое количество предыдущих часов из хранилища,
    агрегирует total_volume по всем символам и возвращает итоговый словарь.
    """
    try:
        # Определяем, сколько предыдущих часов нужно запросить
        current_minute = datetime.now().minute
        if current_minute == 0:
            request_count = TOTAL_V_H_COUNT + 1
        else:
            request_count = TOTAL_V_H_COUNT

        logger.info(total_v_SCRIPT_NAME + f"Будет обработано {request_count} предыдущих часовых записей")

        # Получаем предыдущие записи из RAM-хранилища
        previous_hours = get_recent_1h_klines(request_count)

        if not previous_hours:
            logger.warning(total_v_SCRIPT_NAME + "Не найдено предыдущих часовых записей")
            return {}

        if len(previous_hours) != request_count:
            logger.warning(total_v_SCRIPT_NAME + "Найдено недостаточно предыдущих записей")
            return {}

        # Объединяем все записи: текущий час + предыдущие
        all_hours = [new_hour_record] + previous_hours

        # Агрегируем суммы по всем часам
        total_volumes = {}
        for hour_data in all_hours:
            hour_sums = process_single_hour(hour_data)
            for symbol, volume in hour_sums.items():
                total_volumes[symbol] = total_volumes.get(symbol, 0.0) + volume

        logger.info(total_v_SCRIPT_NAME + f"Обработано уникальных тикеров: {len(total_volumes)}")
        return total_volumes

    except Exception as e:
        logger.error(total_v_SCRIPT_NAME + f"Ошибка при обработке: {e}")
        return {}

# TOTAL_V_K_LINES_DIN_DIR
