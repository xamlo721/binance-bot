# alert_client.py
import asyncio
import logging
import random
from typing import Callable, Optional, Tuple

from AnalyticsBot.bot_types import AlertRecord, AlertRegister, AlertUnregister, AlertData
from AnalyticsBot.serializer import *

logger = logging.getLogger(__name__)

class AlertClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, client: 'AlertClient'):
        self.client = client
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        """Обрабатывает входящие алерты от сервера."""
        try:
            msg = AlertProtocolSerializer.deserialize(data)
        except Exception as e:
            logger.error(f"Ошибка десериализации алерта: {e}")
            return

        if isinstance(msg, AlertData):
            if self.client.alert_callback:
                self.client.alert_callback(msg.alert, msg.packet_number)
            else:
                logger.debug(f"Получен алерт (packet={msg.packet_number}): {msg.alert}")
        else:
            logger.warning(f"Получено неожиданное сообщение: {type(msg)}")

    def error_received(self, exc):
        logger.error(f"Ошибка сокета клиента: {exc}")

class AlertClient:
    """UDP клиент для подписки и получения алертов."""

    def __init__(self, server_addr: Tuple[str, int] = ('127.0.0.1', 8888),
                 alert_callback: Optional[Callable[[AlertRecord, int], None]] = None):
        self.server_addr = server_addr
        self.alert_callback = alert_callback
        self.transport = None
        self.protocol = None
        self._packet_number = random.randint(1, 1_000_000)  # случайный начальный номер

    async def connect(self):
        """Создаёт UDP сокет и отправляет регистрацию."""
        loop = asyncio.get_running_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: AlertClientProtocol(self),
            local_addr=('0.0.0.0', 0)  # любой свободный порт
        )
        await self._send_register()
        logger.info(f"AlertClient подключён к серверу {self.server_addr}")

    async def _send_register(self):
        """Отправляет сообщение регистрации."""
        if self.transport is None:
            logger.warning("Транспорт не инициализирован, регистрация невозможна")
            return
        msg = AlertRegister(packet_number=self._packet_number)
        data = AlertProtocolSerializer.serialize(msg)
        self.transport.sendto(data, self.server_addr)

    async def _send_unregister(self):
        """Отправляет сообщение отписки."""
        if self.transport is None:
            logger.warning("Транспорт не инициализирован, отписка невозможна")
            return
        msg = AlertUnregister(packet_number=self._packet_number)
        data = AlertProtocolSerializer.serialize(msg)
        self.transport.sendto(data, self.server_addr)

    async def close(self):
        """Отписывается и закрывает соединение."""
        if self.transport:
            try:
                await self._send_unregister()
            except Exception as e:
                logger.error(f"Ошибка при отписке: {e}")
            self.transport.close()
            logger.info("AlertClient отключён")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()