from AlertFileSearcher import get_latest_alert_file

if __name__ == "__main__":

    directory = "C:/workspace/data/alerts/"
    
    try:
        alert_file = get_latest_alert_file(directory)
        if alert_file:
            print(f"Самый новый файл: {alert_file}")

        else:
            print("Файлы alerts_yyyy_mm_dd.csv не найдены")

    except ValueError as e:
        print(f"Ошибка: {e}")