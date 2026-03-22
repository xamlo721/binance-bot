import threading
import asyncio
import time
from alert_server import AlertServer

class AlertServerThread(threading.Thread):
    def __init__(self, host='127.0.0.1', port=8888):
        super().__init__(daemon=True)  # поток-демон завершится при выходе из main
        self.host = host
        self.port = port
        self.server = AlertServer()
        self.loop = None
        self._stop_event = threading.Event()

    def run(self):
        """Запускает asyncio-сервер в этом потоке."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        async def start_server():
            await self.server.start()
            # Ждём сигнала остановки
            await self._async_wait_for_stop()

        try:
            self.loop.run_until_complete(start_server())
        finally:
            self.loop.close()

    async def _async_wait_for_stop(self):
        """Ожидает установки события остановки."""
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)

    def stop(self):
        """Сигнал остановки сервера."""
        self._stop_event.set()
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)

    def send_alert(self, alert):
        """Потокобезопасная отправка алерта."""
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self.server.send_alert(alert),
                self.loop
            )