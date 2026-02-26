from pathlib import Path
import os


data_path = Path('Data')

src_path = os.path.dirname(os.path.abspath(__file__))

# Папка с KLD
KLD_DIR = os.path.join(src_path, "k_line_downloader.py")    
            

ALERTS_FOLDER = "Data/Alerts"
ALERTS_CALC_FOLDER = "Data/Alerts_calc"
# Папка с минутными свечами
MINUTES_KLINE_FOLDER = "Data/K_lines/1M" 
# Max файлов в папке
MINUTE_CANDLE_FILE_LIMIT = 60                                 
#Папка с часовыми свечами
HOURS_KLINE_FOLDER = "Data/K_lines/1H" 
# Max файлов в папке
HOURS_CANDLE_FILE_LIMIT = 60        


# Agregator_12h
AGR_H_COUNT = 11                                                        # Количество файлов обработки
AGR_MAX_RESULT_FILES = 2                                                # Максимум файлов результата

# Amm
AMM_metrics_dir = "Data/Alerts_calc_metrics"

# k line
k_line_SCRIPT_NAME = "KLD_1M      :  "                      # Имя скрипта для вывода в консоль

# hdp_1h
hdp_1h_SCRIPT_NAME = "HDP_1H      :  "
hdp_1h_MAX_RESULT_FILES = 720                                  # Максимум файлов результата

# leveling break 
lb_SCRIPT_NAME = "LB_12H      :  "                                                             # Имя скрипта для вывода в консоль
lb_12H_FOLDER = "Data/Agr_12h"                                             # Папка с историческими агрегированными данными
lb_OUTPUT_FOLDER = "Data/Ticker_up"                                            # Папка с результатом
lb_MAX_RESULT_FILES = 2                                                                        # Максимум файлов результата
lb_HDP_SCRIPT_PATH = "hdp_dynamic.py"        # Путь к скрипту hdp_dynamic.py

hdr_dyn_SCRIPT_NAME = "HDP_DYN     :  " 
agr_SCRIPT_NAME = "AGR_12H     :  "
amm_SCRIPT_NAME = "A_M_M       :  "
ALLERTS_COPY_SCRIPT_NAME = "ALERTS_COPY :  "
BPW_SCRIPT_NAME = "BP_WRITER   :  "
CALC_SCRIPT_NAME = "CALC        :  "

# t anal
T_ANAL_SCRIPT_NAME = "T_ANAL      :  " #Имя скрипта для вывода в консоль
T_ANAL_TICKER_UP_FOLDER = "Data/Ticker_up"
T_ANAL_VOLUME_10M_FOLDER = "Data/Volume_10M"
T_ANAL_VOLUME_10H_FOLDER = "Data/Volume_10H"

# total V
total_v_SCRIPT_NAME = "T_VOL_24H   :  "                                     # Имя скрипта
TOTAL_V_K_LINES_DIN_DIR = "Data/K_lines/Dynamic"            # Папка с динамическим файлом
TOTAL_V_OUTPUT_FOLDER = "Data/Total_volume_24H"             # Папка с результатом
TOTAL_V_H_COUNT = 23                                                        # Количество файлов обработки
TOTAL_V_MAX_RESULT_FILES = 2                                                # Максимум файлов результата

#val 10h
VAL_10H_SCRIPT_NAME = "VOL_10H     :  "
VAL_10H_MAX_RESULT_FILES = 1                                            # Максимум файлов результата
VAL_10H_hdr_1h_RESULTS_DIR = "Data/K_lines/Volume_10H"