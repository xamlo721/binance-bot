import schedule
import time
import subprocess
import datetime
import os
import sys

from logger import logger
from config import *

src_path = os.path.dirname(os.path.abspath(__file__))

# Папка с KLD
KLD_DIR = os.path.join(src_path, "k_line_downloader.py")    

# Папка с HDP_1H
HDP_1H_DIR = os.path.join(src_path, "hdp_1h.py")                

class ScriptStarter:
    def __init__(self):
        self.background_processes = []
        self.last_hour_check = datetime.datetime.now().hour  # Запоминаем текущий час при инициализации
        
    def start_background_scripts(self):
        """Запускает все фоновые скрипты""" 
        background_config = [
            {
                "name": "Agregator_12h",
                "script": os.path.join(src_path, "agregator_12h.py"),
            },
            {
                "name": "Level_breakout_12h", 
                "script": os.path.join(src_path, "level_breakout_12h.py"),
            },
            {
                "name": "total_volume_24h",
                "script": os.path.join(src_path, "total_volume_24h.py")
            },
            {
                "name": "volume_10h",
                "script": os.path.join(src_path, "volume_10h.py")
            },
            {
                "name": "volume_10m",
                "script": os.path.join(src_path, "volume_10m.py")
            },
            {
                "name": "ticker_analytics",
                "script": os.path.join(src_path, "ticker_analytics.py")
            },
            {
                "name": "buy_price_writer",
                "script": os.path.join(src_path, "buy_price_writer.py")
            },
            {
                "name": "alerts_copy",
                "script": os.path.join(src_path, "alerts_copy.py")
            },
            {
                "name": "calculator",
                "script": os.path.join(src_path, "calculator.py")
            },
            {
                "name": "alert_metrics_monitor",
                "script": os.path.join(src_path, "alert_metrics_monitor.py")
            }
        ]
        
        for config in background_config:

            try:
                # Проверяем существование файла
                if not os.path.exists(config["script"]):
                    logger.error(MAIN_SCRIPT_NAME + f"❌ Файл не найден: {config['script']}")
                    continue

                # Формируем команду для Windows (используем python вместо python3)
                python_exe = sys.executable  # Используем тот же Python, что запустил starter.py

                # Запускаем процесс
                process = subprocess.Popen(
                    [python_exe, config["script"]],
                    cwd=src_path,
                    #stdout=subprocess.DEVNULL, #Если вывод данных в консоль не нужен
                    #stderr=subprocess.DEVNULL, #Если вывод ошибок в консоль не нужен
                    stdout=None, #Вывод данных в консоль
                    stderr=None, #Вывод ошибок в консоль

                    start_new_session=True
                )
                
                self.background_processes.append({
                    "name": config["name"],
                    "process": process,
                    "script": config["script"]
                })
 
                logger.info(MAIN_SCRIPT_NAME + f"✅ Запущен фоновый скрипт: {config['name']} (PID: {process.pid})")
                
                # Даем время на инициализацию
                time.sleep(1)
                
            except Exception as e:
                logger.error(MAIN_SCRIPT_NAME + f"❌ Ошибка при запуске {config['name']}: {e}")
    
    def run_data_collector(self):
        """Запускает основной скрипт сбора данных (KLD)"""
        try:
            logger.info("            :  ======================================")
            logger.info( MAIN_SCRIPT_NAME + "Запуск KLD_1M...")
            subprocess.run(["python3", KLD_DIR], check=True)
            logger.info(MAIN_SCRIPT_NAME + "KLD_1M завершил работу.")
            
            # После завершения KLD проверяем, не наступил ли новый час
            self.check_and_run_hourly_script()
            
        except subprocess.CalledProcessError as e:
            logger.error(MAIN_SCRIPT_NAME + f"Ошибка при запуске KLD_1M: {e}")
    
    def check_and_run_hourly_script(self):
        """Проверяет, наступил ли новый час, и если да - запускает часовой скрипт"""
        current_hour = datetime.datetime.now().hour
        current_time = datetime.datetime.now().strftime("%H:%M:%S")
        
        if current_hour != self.last_hour_check:
            logger.info(MAIN_SCRIPT_NAME + f"Запуск HDP_1H...")
            
            try:
                # Запускаем часовой скрипт
                result = subprocess.run(
                    ["python3", HDP_1H_DIR], 
                    check=True,
                    capture_output=True,
                    text=True
                )
                #print(SCRIPT_NAME + f"✅ Часовой скрипт hdp_1h.py успешно выполнен")
                if result.stdout:
                    #Вывод данных с hdp_1h.py
                    logger.info(result.stdout)
                
            except subprocess.CalledProcessError as e:
                logger.error(MAIN_SCRIPT_NAME + f"❌ Ошибка при запуске hdp_1h.py: {e}")
                if e.stderr:
                    logger.error(MAIN_SCRIPT_NAME + f"Ошибка скрипта: {e.stderr}")
            except Exception as e:
                logger.error(MAIN_SCRIPT_NAME + f"❌ Неожиданная ошибка при запуске hdp_1h.py: {e}")
            
            # Обновляем время последней проверки
            self.last_hour_check = current_hour
    
    def cleanup(self):
        """Останавливает все фоновые процессы при завершении"""
        for process in self.background_processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()
        logger.info(MAIN_SCRIPT_NAME + "Все фоновые процессы остановлены")

# Создаем экземпляр и запускаем
starter = ScriptStarter()

# Запускаем фоновые скрипты
starter.start_background_scripts()

# Планируем периодические задачи
schedule.every().minute.at(":00").do(starter.run_data_collector)

logger.info(MAIN_SCRIPT_NAME + "Скрипт-стартер запущен. Фоновые процессы активны.")

try:
    while True:
        schedule.run_pending()
        time.sleep(0.1)
except KeyboardInterrupt:
    logger.warning(MAIN_SCRIPT_NAME + "Получен сигнал прерывания...")
finally:
    starter.cleanup()