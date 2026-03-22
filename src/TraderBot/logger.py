import logging

# Создаём логгер
logger = logging.getLogger("trader_bot")

# Если у логгера ещё нет обработчиков, добавляем их
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # предотвращает передачу сообщений корневому логгеру, если не нужно