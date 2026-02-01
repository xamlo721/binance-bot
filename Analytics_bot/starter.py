import schedule
import time
import subprocess
import datetime
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
MAIN_SCRIPT_NAME = "STARTER     :  "                                                                       # Имя этого скрипта для вывода в консоль
KLD_DIR = "Modules/K_line_downloader/k_line_downloader.py"                                               # Папка с KLD
HDP_1H_DIR = "/srv/ftp/Bot_v2/Modules/Historical_data_processor/hdp_1h.py"                               # Папка с HDP_1H

class ScriptStarter:
    def __init__(self):
        self.background_processes = []
        self.last_hour_check = datetime.datetime.now().hour  # Запоминаем текущий час при инициализации
        
    def start_background_scripts(self):
        """Запускает все фоновые скрипты"""
        background_config = [
            {
                "name": "Agregator_12h",
                "command": ["python3", "Modules/Historical_data_processor/agregator_12h.py"],
            },
            {
                "name": "Level_breakout_12h", 
                "command": ["python3", "Modules/Level_breakout/level_breakout_12h.py"],
            },
            {
                "name": "total_volume_24h",
                "command": ["python3", "Modules/Historical_data_processor/total_volume_24h.py"]
            },
            {
                "name": "volume_10h",
                "command": ["python3", "Modules/Historical_data_processor/volume_10h.py"]
            },
            {
                "name": "volume_10m",
                "command": ["python3", "Modules/Historical_data_processor/volume_10m.py"]
            },
            {
                "name": "ticker_analytics",
                "command": ["python3", "Modules/Ticker_analytics/ticker_analytics.py"]
            },
            {
                "name": "buy_price_writer",
                "command": ["python3", "Modules/Strategy_tester/buy_price_writer.py"]
            },
            {
                "name": "alerts_copy",
                "command": ["python3", "Modules/Strategy_tester/alerts_copy.py"]
            },
            {
                "name": "calculator",
                "command": ["python3", "Modules/Strategy_tester/calculator.py"]
            },
            {
                "name": "alert_metrics_monitor",
                "command": ["python3", "Modules/Metrics/alert_metrics_monitor.py"]
            }
        ]
        
        for config in background_config:
            try:
                # Создаем окружение
                env = os.environ.copy()
                if "env" in config:
                    env.update(config["env"])
                
                # Запускаем процесс
                process = subprocess.Popen(
                    config["command"],
                    #stdout=subprocess.DEVNULL, #Если вывод данных в консоль не нужен
                    #stderr=subprocess.DEVNULL, #Если вывод ошибок в консоль не нужен
                    stdout=None, #Вывод данных в консоль
                    stderr=None, #Вывод ошибок в консоль

                    start_new_session=True,
                    env=env
                )
                
                self.background_processes.append(process)
                print(MAIN_SCRIPT_NAME + f"✅ Запущен фоновый скрипт: {config['name']} (PID: {process.pid})")
                
                # Даем время на инициализацию
                time.sleep(1)
                
            except Exception as e:
                print(MAIN_SCRIPT_NAME + f"❌ Ошибка при запуске {config['name']}: {e}")
    
    def run_data_collector(self):
        """Запускает основной скрипт сбора данных (KLD)"""
        try:
            print("\n======================================\n" + MAIN_SCRIPT_NAME + "Запуск KLD_1M...")
            subprocess.run(["python3", KLD_DIR], check=True)
            print(MAIN_SCRIPT_NAME + "KLD_1M завершил работу.")
            
            # После завершения KLD проверяем, не наступил ли новый час
            self.check_and_run_hourly_script()
            
        except subprocess.CalledProcessError as e:
            print(MAIN_SCRIPT_NAME + f"Ошибка при запуске KLD_1M: {e}")
    
    def check_and_run_hourly_script(self):
        """Проверяет, наступил ли новый час, и если да - запускает часовой скрипт"""
        current_hour = datetime.datetime.now().hour
        current_time = datetime.datetime.now().strftime("%H:%M:%S")
        
        if current_hour != self.last_hour_check:
            print(MAIN_SCRIPT_NAME + f"Запуск HDP_1H...")
            
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
                    print(result.stdout)
                
            except subprocess.CalledProcessError as e:
                print(MAIN_SCRIPT_NAME + f"❌ Ошибка при запуске hdp_1h.py: {e}")
                if e.stderr:
                    print(MAIN_SCRIPT_NAME + f"Ошибка скрипта: {e.stderr}")
            except Exception as e:
                print(MAIN_SCRIPT_NAME + f"❌ Неожиданная ошибка при запуске hdp_1h.py: {e}")
            
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
        print(MAIN_SCRIPT_NAME + "Все фоновые процессы остановлены")

# Создаем экземпляр и запускаем
starter = ScriptStarter()

# Запускаем фоновые скрипты
starter.start_background_scripts()

# Планируем периодические задачи
schedule.every().minute.at(":00").do(starter.run_data_collector)

print(MAIN_SCRIPT_NAME + "Скрипт-стартер запущен. Фоновые процессы активны.")

try:
    while True:
        schedule.run_pending()
        time.sleep(0.1)
except KeyboardInterrupt:
    print(MAIN_SCRIPT_NAME + "Получен сигнал прерывания...")
finally:
    starter.cleanup()