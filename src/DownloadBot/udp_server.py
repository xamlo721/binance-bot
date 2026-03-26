import asyncio
from collections import OrderedDict
from typing import Dict

from config import *
from logger import *
from serializer import *

class UDPMarketDataServer:
    
    def __init__(self, host: str = DOWNLOADER_UDP_IP, port: int = DOWNLOADER_UDP_PORT):
        self.host = host
        self.port = port
        self.global_data: OrderedDict[int, list[KlineRecord]] = OrderedDict()
        self.serializer = MessageSerializer()
        self.transport = None
        
    async def start(self):
        """Запуск UDP сервера"""
        loop = asyncio.get_running_loop()
        
        # Создаем UDP endpoint
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPServerProtocol(self),
            local_addr=(self.host, self.port)
        )
        
        self.transport = transport
        logger.info(f"UDP сервер запущен на {self.host}:{self.port}")
        
    def stop(self):
        """Остановка сервера"""
        if self.transport:
            self.transport.close()
            
    def update_data(self, new_data: OrderedDict[int, list[KlineRecord]]):
        """Обновление данных (вызывается при поступлении новых данных)"""
        self.global_data = new_data

class UDPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: UDPMarketDataServer):
        self.server = server
        self.transport = None
        
    def connection_made(self, transport):
        self.transport = transport
        
    def datagram_received(self, data: bytes, addr):
        """Обработка входящего датаграмма"""
        try:
            # Проверяем, что транспорт инициализирован
            if self.transport is None:
                logger.error(f"Ошибка: транспорт не инициализирован для запроса от {addr}")
                return
                
            # Десериализуем запрос
            request = self.server.serializer.deserialize_request(data)
            
            if request is None:
                logger.error(f"Получен некорректный запрос от {addr}")
                return
                
            logger.info(f"Запрос от {addr}: packet={request.packet_number}, minute={request.minute_number}")
            
            if (request.minute_number not in self.server.global_data.keys()):
                logger.warning(f"Запрос от {addr}: packet={request.packet_number}, minute={request.minute_number}")
                logger.warning(f"Запрошеной минуты в хранилище не обнаружено.")

                # TODO: вернуть клиенту код отсутствия 
                return

            # Получаем данные за запрошенную минуту
            records: list[KlineRecord] = self.server.global_data.get(request.minute_number, [])
            
            # Формируем и отправляем ответ
            response = UDPResponse(
                packet_number=request.packet_number,
                minute_number=request.minute_number,
                records=records
            )
            
            response_data = self.server.serializer.serialize_response(response)
            
            # Отправляем ответ
            try:
                self.transport.sendto(response_data, addr)
                logger.debug(f"Отправлен ответ для {addr}: packet={response.packet_number}, minute={response.minute_number}, tickers={len(records)}.")
                logger.debug(f"Размер ответа для минуты {request.minute_number}: {len(response_data)} байт")

            except AttributeError as e:
                logger.error(f"Ошибка отправки: транспорт не поддерживает sendto - {e}")
            except Exception as e:
                logger.error(f"Ошибка отправки данных: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка обработки запроса от {addr}: {e}")