import logging

# Создаём логгер
logger = logging.getLogger("download_bot")

# Добавляем обработчик только если его ещё нет
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    # Предотвращаем дублирование через корневой логгер (опционально)
    logger.propagate = False