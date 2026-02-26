
from csv_utills import get_tickers_from_csv_pandas
from ticker_utils import compare_tickers, print_comparison_results

if __name__ == "__main__":

    directory = "C:/workspace/data/alerts/"

    try:
        old_alert_file = "C:/workspace/data/alerts/alerts_2026-01-20.csv"
        new_alert_file = "C:/workspace/data/alerts/alerts_2026-01-21.csv"

        if old_alert_file and new_alert_file:
            old_tickers = get_tickers_from_csv_pandas(old_alert_file)
            new_tickers = get_tickers_from_csv_pandas(new_alert_file)

            if old_tickers and new_tickers:

                print(f"Раньше было  {len(old_tickers)} уникальных тикеров.")
                # for i, ticker in enumerate(old_tickers[:len(old_tickers)]):
                #     print(f"  {i+1}. {ticker}")

                print(f"Сейчас стало {len(new_tickers)} уникальных тикеров.")
                # for i, ticker in enumerate(new_tickers[:len(new_tickers)]):
                #     print(f"  {i+1}. {ticker}")

                tickers_diff = compare_tickers(old_tickers, new_tickers)
                print_comparison_results(tickers_diff)

            else:
                print("Тикеры не найдены")
        else:
            print("Файлы alerts_yyyy_mm_dd.csv не найдены")
    except ValueError as e:
        print(f"Ошибка: {e}")