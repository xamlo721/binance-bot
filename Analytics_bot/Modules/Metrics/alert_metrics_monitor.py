import os
import pandas as pd
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import threading
from pathlib import Path


SCRIPT_NAME = "A_M_M       :  "                           #Имя скрипта для вывода в консоль

class AlertMetricsProcessor:
    def __init__(self):
        self.source_dir = "/srv/ftp/Bot_v2/Data/Alerts_calc"
        self.metrics_dir = "/srv/ftp/Bot_v2/Data/Alerts_calc_metrics"
        
        # Создаем директорию для метрик, если не существует
        if not os.path.exists(self.metrics_dir):
            os.makedirs(self.metrics_dir)
            
        # Словарь для отслеживания времени последнего изменения файлов
        self.last_modified = {}
        # Блокировка для потокобезопасности
        self.lock = threading.Lock()
        
    def get_three_newest_files(self):
        """Получить три самых новых файла в директории"""
        try:
            files = []
            for f in os.listdir(self.source_dir):
                if f.startswith("alerts_calc_") and f.endswith(".csv"):
                    filepath = os.path.join(self.source_dir, f)
                    mtime = os.path.getmtime(filepath)
                    files.append((f, mtime))
            
            # Сортируем по времени изменения (новые в начале)
            files.sort(key=lambda x: x[1], reverse=True)
            return [f[0] for f in files[:3]]
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при получении списка файлов: {e}")
            return []
    
    def wait_for_file_stability(self, filename, timeout=30, check_interval=1):
        """Ожидание стабилизации файла (прекращения изменений)"""
        filepath = os.path.join(self.source_dir, filename)
        start_time = time.time()
        last_size = -1
        
        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(filepath)
                if current_size == last_size:
                    # Размер не изменился в течение 2 проверок
                    time.sleep(check_interval)
                    current_size2 = os.path.getsize(filepath)
                    if current_size2 == current_size:
                        return True
                last_size = current_size
            except Exception as e:
                print(SCRIPT_NAME + f"Ошибка при проверке файла {filename}: {e}")
                return False
            time.sleep(check_interval)
        
        print(SCRIPT_NAME + f"Таймаут ожидания стабилизации файла {filename}")
        return False
    
    def process_file(self, filename):
        """Обработка одного файла и создание метрик"""
        source_path = os.path.join(self.source_dir, filename)
        
        # Определяем имя файла метрик
        date_part = filename.replace("alerts_calc_", "").replace(".csv", "")
        metrics_filename = f"alerts_calc_metrics_{date_part}.csv"
        metrics_path = os.path.join(self.metrics_dir, metrics_filename)
        
        try:
            print(SCRIPT_NAME + f"Обработка файла: {filename}")
            
            # Читаем CSV файл
            df = pd.read_csv(source_path)
            
            # 1-12: Суммируем все требуемые столбцы
            sum_columns = [
                'RPS_(30%)_SL_1%', 'RPS_(30%)_SL_2%', 'RPS_(30%)_SL_3%',
                'RPS_(30%)_SL_4%', 'RPS_(30%)_SL_5%', 'max_loss_%',
                'RPB_(30%)_SL_1%', 'RPB_(30%)_SL_2%', 'RPB_(30%)_SL_3%',
                'RPB_(30%)_SL_4%', 'RPB_(30%)_SL_5%', 'max_proffit_%'
            ]
            
            # Проверяем наличие всех столбцов
            for col in sum_columns:
                if col not in df.columns:
                    print(SCRIPT_NAME + f"Предупреждение: столбец {col} не найден в {filename}")
            
            # Вычисляем суммы
            sums = []
            for col in sum_columns:
                if col in df.columns:
                    sums.append(df[col].sum())
                else:
                    sums.append(0.0)
            
            # 13: Топ-10 тикеров по max_loss_%
            if 'max_loss_%' in df.columns and 'ticker' in df.columns:
                top_loss = df.nlargest(10, 'max_loss_%')[['ticker', 'max_loss_%']]
            else:
                top_loss = pd.DataFrame(columns=['ticker', 'max_loss_%'])
            
            # 14: Топ-10 тикеров по max_proffit_%
            if 'max_proffit_%' in df.columns and 'ticker' in df.columns:
                top_profit = df.nlargest(10, 'max_proffit_%')[['ticker', 'max_proffit_%']]
            else:
                top_profit = pd.DataFrame(columns=['ticker', 'max_proffit_%'])
            
            # 15: Количество тикеров
            tickers_count = len(df)
            
            # Формируем данные для записи
            metrics_data = []
            
            # 1-12: Суммы
            for s in sums:
                metrics_data.append(str(s))
            
            # 13: Топ-10 убытков
            for i in range(10):
                if i < len(top_loss):
                    metrics_data.append(str(top_loss.iloc[i]['ticker']))
                    metrics_data.append(str(top_loss.iloc[i]['max_loss_%']))
                else:
                    metrics_data.append("")  # Пустой тикер
                    metrics_data.append("")  # Пустое значение
            
            # 14: Топ-10 прибылей
            for i in range(10):
                if i < len(top_profit):
                    metrics_data.append(str(top_profit.iloc[i]['ticker']))
                    metrics_data.append(str(top_profit.iloc[i]['max_proffit_%']))
                else:
                    metrics_data.append("")  # Пустой тикер
                    metrics_data.append("")  # Пустое значение
            
            # 15: Количество тикеров
            metrics_data.append(str(tickers_count))
            
            # Формируем заголовки
            headers = [
                'S_RPS_(30%)_SL_1%', 'S_RPS_(30%)_SL_2%', 'S_RPS_(30%)_SL_3%',
                'S_RPS_(30%)_SL_4%', 'S_RPS_(30%)_SL_5%', 'S_max_loss_%',
                'S_RPB_(30%)_SL_1%', 'S_RPB_(30%)_SL_2%', 'S_RPB_(30%)_SL_3%',
                'S_RPB_(30%)_SL_4%', 'S_RPB_(30%)_SL_5%', 'S_max_proffit_%'
            ]
            
            # Добавляем заголовки для топ-10 убытков
            for i in range(1, 11):
                headers.append(f'Top{i}_Loss_Ticker')
                headers.append(f'Top{i}_Loss_Value')
            
            # Добавляем заголовки для топ-10 прибылей
            for i in range(1, 11):
                headers.append(f'Top{i}_Proffit_Ticker')
                headers.append(f'Top{i}_Proffit_Value')
            
            # Добавляем заголовок для количества тикеров
            headers.append('Tickers_count')
            
            # Создаем DataFrame для метрик
            metrics_df = pd.DataFrame([metrics_data], columns=headers)
            
            # Сохраняем в CSV
            metrics_df.to_csv(metrics_path, index=False)
            print(SCRIPT_NAME + f"Метрики сохранены в: {metrics_filename}")
            
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при обработке файла {filename}: {e}")
    
    def process_all_missing_files(self):
        """Обработка всех файлов, для которых отсутствуют метрики"""
        try:
            source_files = [f for f in os.listdir(self.source_dir) 
                          if f.startswith("alerts_calc_") and f.endswith(".csv")]
            
            existing_metrics = [f for f in os.listdir(self.metrics_dir) 
                              if f.startswith("alerts_calc_metrics_") and f.endswith(".csv")]
            
            for source_file in source_files:
                date_part = source_file.replace("alerts_calc_", "").replace(".csv", "")
                metrics_file = f"alerts_calc_metrics_{date_part}.csv"
                
                if metrics_file not in existing_metrics:
                    print(SCRIPT_NAME + "Создание метрик для: {source_file}")
                    self.process_file(source_file)
                else:
                    # Проверяем, нужно ли обновить файл (источник новее метрик)
                    source_path = os.path.join(self.source_dir, source_file)
                    metrics_path = os.path.join(self.metrics_dir, metrics_file)
                    
                    if os.path.exists(source_path) and os.path.exists(metrics_path):
                        source_mtime = os.path.getmtime(source_path)
                        metrics_mtime = os.path.getmtime(metrics_path)
                        
                        if source_mtime > metrics_mtime:
                            print(SCRIPT_NAME + f"Обновление метрик для: {source_file}")
                            self.process_file(source_file)
        
        except Exception as e:
            print(SCRIPT_NAME + f"Ошибка при обработке отсутствующих файлов: {e}")
    
    def is_file_new_enough(self, filename):
        """Проверяем, не старше ли файл 3-х дней"""
        try:
            # Извлекаем дату из имени файла
            date_str = filename.replace("alerts_calc_", "").replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Проверяем, что файл не старше 3-х дней
            three_days_ago = datetime.now() - timedelta(days=3)
            return file_date >= three_days_ago
        except:
            return False
    
    def check_and_process_newest_files(self):
        """Проверка и обработка трех самых новых файлов при изменении"""
        with self.lock:
            newest_files = self.get_three_newest_files()
            
            for filename in newest_files:
                if not self.is_file_new_enough(filename):
                    continue
                    
                filepath = os.path.join(self.source_dir, filename)
                current_mtime = os.path.getmtime(filepath)
                
                if filename in self.last_modified:
                    if current_mtime != self.last_modified[filename]:
                        # Файл изменился
                        print(SCRIPT_NAME + f"Файл изменен: {filename}")
                        if self.wait_for_file_stability(filename):
                            self.process_file(filename)
                            self.last_modified[filename] = current_mtime
                else:
                    # Новый файл в списке самых новых
                    self.last_modified[filename] = current_mtime
                    print(SCRIPT_NAME + f"Новый файл в списке самых новых: {filename}")
                    if self.wait_for_file_stability(filename):
                        self.process_file(filename)


class AlertFileHandler(FileSystemEventHandler):
    def __init__(self, processor):
        self.processor = processor
        self.debounce_time = 2  # Задержка перед обработкой (секунды)
        self.last_event_time = 0
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            # Извлекаем имя файла из пути
            filename = os.path.basename(event.src_path)
            if filename.startswith("alerts_calc_"):
                current_time = time.time()
                if current_time - self.last_event_time > self.debounce_time:
                    print(SCRIPT_NAME + f"Создан новый файл: {filename}")
                    time.sleep(1)  # Короткая задержка для записи файла
                    self.processor.check_and_process_newest_files()
                    self.last_event_time = current_time
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            filename = os.path.basename(event.src_path)
            if filename.startswith("alerts_calc_"):
                current_time = time.time()
                if current_time - self.last_event_time > self.debounce_time:
                    # Проверяем, является ли файл одним из трех самых новых
                    newest_files = self.processor.get_three_newest_files()
                    if filename in newest_files and self.processor.is_file_new_enough(filename):
                        print(SCRIPT_NAME + f"Изменен файл из списка самых новых: {filename}")
                        self.processor.check_and_process_newest_files()
                        self.last_event_time = current_time


def main():
    print(SCRIPT_NAME + "Запуск мониторинга папки Alerts_calc...")
    
    # Инициализируем процессор
    processor = AlertMetricsProcessor()
    
    # Обрабатываем все отсутствующие файлы при запуске
    print(SCRIPT_NAME + "Проверка отсутствующих метрик...")
    processor.process_all_missing_files()
    
    # Настраиваем наблюдатель
    event_handler = AlertFileHandler(processor)
    observer = Observer()
    observer.schedule(event_handler, processor.source_dir, recursive=False)
    observer.start()
    
    print(SCRIPT_NAME + f"Мониторинг запущен для папки: {processor.source_dir}")
    #print(SCRIPT_NAME + "Нажмите Ctrl+C для остановки...")
    
    try:
        while True:
            # Периодическая проверка (на случай пропущенных событий)
            time.sleep(60)
            processor.check_and_process_newest_files()
            
    except KeyboardInterrupt:
        print(SCRIPT_NAME + "\nОстановка мониторинга...")
        observer.stop()
    
    observer.join()
    print(SCRIPT_NAME + "Мониторинг остановлен.")


if __name__ == "__main__":
    main()