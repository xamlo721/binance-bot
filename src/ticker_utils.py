
import os
import re
from typing import List, Dict, Any

def compare_tickers(old_tickers: List[str], new_tickers: List[str]) -> Dict[str, List[str]]:
    """
    Сравнивает два списка тикеров и возвращает изменения.
    
    Args:
        old_tickers: Список тикеров до изменений
        new_tickers: Список тикеров после изменений
        
    Returns:
        Словарь с ключами:
        - 'added': тикеры, которые появились в new_tickers
        - 'removed': тикеры, которые исчезли из old_tickers
        - 'unchanged': тикеры, которые остались без изменений
    """
    # Приводим к множествам для удобства сравнения (игнорируем дубликаты)
    old_set = set(old_tickers)
    new_set = set(new_tickers)
    
    # Находим различия
    added = list(new_set - old_set)
    removed = list(old_set - new_set)
    unchanged = list(old_set & new_set)
    
    # Сортируем для удобства чтения
    added.sort()
    removed.sort()
    unchanged.sort()
    
    # Подсчитываем изменения
    total_changes = len(added) + len(removed)
    
    return {
        'added': added,
        'removed': removed,
        'unchanged': unchanged,
    }


def print_comparison_results(comparison: Dict) -> None:
    """
    Красиво выводит результаты сравнения.
    
    Args:
        comparison: Результат работы compare_tickers или compare_tickers_detailed
        verbose: Если True, выводит подробную информацию
    """
    if comparison['added']:
        print("\n" + "=" * 50)
        print("Added tickers:")
        for i, ticker in enumerate(comparison['added'], 1):
            print(f"{i:3d}. {ticker}")
        
    if comparison['removed']:
        print("\n" + "=" * 50)
        print("Removed tickers:")
        for i, ticker in enumerate(comparison['removed'], 1):
            print(f"{i:3d}. {ticker}")

    if comparison['unchanged']:
        print("\n" + "=" * 50)
        print("Unchanged tickers:")
        for i, ticker in enumerate(comparison['unchanged'], 1):
            print(f"{i:3d}. {ticker}")
