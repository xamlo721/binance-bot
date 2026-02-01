
import os
import re

import pandas as pd
from typing import List

def get_tickers_from_csv_pandas(file_path) -> List[str]:

    try:
        # Читаем CSV-файл
        df = pd.read_csv(file_path)
        
        # Проверяем наличие столбца 'ticker'
        if 'ticker' not in df.columns:
            # Попробуем найти столбец с похожим названием (регистронезависимо)
            matching_columns = [col for col in df.columns if col.lower() == 'ticker']
            if matching_columns:
                ticker_column = matching_columns[0]
                print(f"Найден столбец '{ticker_column}' (регистр отличается)")
            else:
                print(f"Доступные столбцы: {list(df.columns)}")
                raise ValueError(f"Столбец 'ticker' не найден в файле")
        else:
            ticker_column = 'ticker'
        
        # Извлекаем тикеры, удаляем NaN значения и преобразуем в список
        tickers = df[ticker_column].dropna().astype(str).tolist()
        
        return tickers
        
    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
        return []
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return []

def get_min_prices_from_csv_pandas(file_path) -> List[str]:

    try:
        # Читаем CSV-файл
        df = pd.read_csv(file_path)
        
        # Проверяем наличие столбца 'ticker'
        if 'min_price' not in df.columns:
            # Попробуем найти столбец с похожим названием (регистронезависимо)
            matching_columns = [col for col in df.columns if col.lower() == 'min_price']
            if matching_columns:
                ticker_column = matching_columns[0]
                print(f"Найден столбец '{ticker_column}' (регистр отличается)")
            else:
                print(f"Доступные столбцы: {list(df.columns)}")
                raise ValueError(f"Столбец 'min_price' не найден в файле")
        else:
            ticker_column = 'min_price'
        
        # Извлекаем тикеры, удаляем NaN значения и преобразуем в список
        tickers = df[ticker_column].dropna().astype(str).tolist()
        
        return tickers
        
    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
        return []
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return []

def get_max_prices_from_csv_pandas(file_path) -> List[str]:

    try:
        # Читаем CSV-файл
        df = pd.read_csv(file_path)
        
        # Проверяем наличие столбца 'ticker'
        if 'max_price' not in df.columns:
            # Попробуем найти столбец с похожим названием (регистронезависимо)
            matching_columns = [col for col in df.columns if col.lower() == 'max_price']
            if matching_columns:
                ticker_column = matching_columns[0]
                print(f"Найден столбец '{ticker_column}' (регистр отличается)")
            else:
                print(f"Доступные столбцы: {list(df.columns)}")
                raise ValueError(f"Столбец 'max_price' не найден в файле")
        else:
            ticker_column = 'max_price'
        
        # Извлекаем тикеры, удаляем NaN значения и преобразуем в список
        tickers = df[ticker_column].dropna().astype(str).tolist()
        
        return tickers
        
    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
        return []
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return []
