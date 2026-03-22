# alert_server.py
import asyncio
from typing import Set, Tuple

from AnalyticsBot.bot_types import AlertData, AlertRecord
from AnalyticsBot.bot_types import AlertRegister
from AnalyticsBot.bot_types import AlertUnregister
from AnalyticsBot.serializer import AlertProtocolSerializer
from AnalyticsBot.logger import logger

class AlertServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: 'AlertServer'):
        self.packet_numbers = 0
        self.server = server
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        sockname = transport.get_extra_info('sockname')
        logger.info(f"AlertServer запущен на {sockname}")

    def datagram_received(self, data: bytes, addr):
        """Обрабатывает входящие сообщения (регистрация/отписка)."""
        try:
            msg = AlertProtocolSerializer.deserialize(data)
        except Exception as e:
            logger.error(f"Ошибка десериализации от {addr}: {e}")
            return

        if isinstance(msg, AlertRegister):
            self.server.clients.add(addr)
            logger.info(f"Клиент {addr} зарегистрирован (packet={msg.packet_number}). "
                        f"Всего клиентов: {len(self.server.clients)}")
        elif isinstance(msg, AlertUnregister):
            self.server.clients.discard(addr)
            logger.info(f"Клиент {addr} отписался (packet={msg.packet_number}). "
                        f"Осталось: {len(self.server.clients)}")
        else:
            logger.warning(f"Получено неподдерживаемое сообщение от {addr}: {type(msg)}")

    def error_received(self, exc):
        logger.error(f"Ошибка сокета: {exc}")

class AlertServer:
    def __init__(self, ):
        self.clients: Set[Tuple[str, int]] = set()
        self.transport = None
        self.protocol = None

    async def start(self, host: str = '127.0.0.1', port: int = 8888):
        """Запускает UDP сервер."""
        loop = asyncio.get_running_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: AlertServerProtocol(self),
            local_addr=(host, port)
        )
        logger.info(f"AlertServer слушает {host}:{port}")

    def stop(self):
        """Останавливает сервер."""
        if self.transport:
            self.transport.close()
            logger.info("AlertServer остановлен")

    async def send_alert(self, alert: AlertRecord, packet_number: int = 0):
        """
        Отправляет алерт всем зарегистрированным клиентам.
        packet_number можно использовать для идентификации (например, порядковый номер).
        """
        if not self.clients:
            logger.debug("Нет подписанных клиентов, алерт не отправлен")
            return

        if self.transport is None:
            logger.warning("Транспорт не инициализирован, отписка невозможна")
            return
        
        msg = AlertData(packet_number=packet_number, alert=alert)
        data = AlertProtocolSerializer.serialize(msg)

        for addr in list(self.clients):  # итерируем по копии, т.к. множество может измениться
            try:
                self.transport.sendto(data, addr)
                logger.info(f"Алерт отправлен {addr}: {alert.ticker}")
            except Exception as e:
                logger.error(f"Ошибка отправки клиенту {addr}: {e}")
                # Возможно, клиент недоступен — можно удалить, но осторожно
                # self.clients.discard(addr)