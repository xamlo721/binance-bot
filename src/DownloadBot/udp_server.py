import asyncio
from collections import OrderedDict
from typing import Dict

from config import *
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
        print(f"UDP сервер запущен на {self.host}:{self.port}")
        
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
                print(f"Ошибка: транспорт не инициализирован для запроса от {addr}")
                return
                
            # Десериализуем запрос
            request = self.server.serializer.deserialize_request(data)
            
            if request is None:
                print(f"Получен некорректный запрос от {addr}")
                return
                
            print(f"Запрос от {addr}: packet={request.packet_number}, minute={request.minute_number}")
            
            # Получаем данные за запрошенную минуту
            records = self.server.global_data.get(request.minute_number, [])
            
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
            except AttributeError as e:
                print(f"Ошибка отправки: транспорт не поддерживает sendto - {e}")
            except Exception as e:
                print(f"Ошибка отправки данных: {e}")
            
        except Exception as e:
            print(f"Ошибка обработки запроса от {addr}: {e}")