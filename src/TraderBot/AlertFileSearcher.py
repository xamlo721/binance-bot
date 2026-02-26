import os
import re

from datetime import datetime

def get_latest_alert_file(directory_path):

    # alerts_2026-01-21.csv
    pattern = r'alerts_(\d{4})-(\d{2})-(\d{2})\.csv'
    latest_file = None
    latest_date = None
    
    # Проверка существование директории
    if not os.path.isdir(directory_path):
        raise ValueError(f"Директория не существует: {directory_path}")
    
    # Проход по всем файлам в директории
    for filename in os.listdir(directory_path):
        match = re.match(pattern, filename)
        if match:
            try:
                # Дата из имени файла
                year, month, day = map(int, match.groups())
                file_date = datetime(year, month, day)
                
                # Сравнение с текущей самой поздней датой
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = os.path.join(directory_path, filename)
            except ValueError:
                continue
    
    return latest_file


def get_latest_alert_calc_file(directory_path):

    # alerts_2026-01-21.csv
    pattern = r'alerts_calc_(\d{4})-(\d{2})-(\d{2})\.csv'
    latest_file = None
    latest_date = None
    
    # Проверка существование директории
    if not os.path.isdir(directory_path):
        raise ValueError(f"Директория не существует: {directory_path}")
    
    # Проход по всем файлам в директории
    for filename in os.listdir(directory_path):
        match = re.match(pattern, filename)
        if match:
            try:
                # Дата из имени файла
                year, month, day = map(int, match.groups())
                file_date = datetime(year, month, day)
                
                # Сравнение с текущей самой поздней датой
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = os.path.join(directory_path, filename)
            except ValueError:
                continue
    
    return latest_file

