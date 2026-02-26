from ticker_utils import compare_tickers, print_comparison_results


if __name__ == "__main__":

    old_tickers_list = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "FB", "NVDA", "NFLX"]
    new_tickers_list = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "AMD", "INTC"]

    print("-" * 50)
    result1 = compare_tickers(old_tickers_list, new_tickers_list)
    print_comparison_results(result1)
