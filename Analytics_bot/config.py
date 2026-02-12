from pathlib import Path
import os


data_path = Path('Data')

src_path = os.path.dirname(os.path.abspath(__file__))

# Папка с KLD
KLD_DIR = os.path.join(src_path, "k_line_downloader.py")    

# Папка с HDP_1H
HDP_1H_DIR = os.path.join(src_path, "hdp_1h.py")                



# Agregator_12h
aggr_K_LINES_1H_DIR = "Data/K_lines/1H"                  # Папка с часовыми свечами
aggr_K_LINES_DIN_DIR = "Data/K_lines/Dynamic"            # Папка с динамическим файлом
aggr_OUTPUT_FOLDER = "Data/Agr_12h"                      # Папка с результатом
agr_H_COUNT = 11                                                        # Количество файлов обработки
agr_MAX_RESULT_FILES = 2                                                # Максимум файлов результата

# Amm
AMM_source_dir = "Data/Alerts_calc"
AMM_metrics_dir = "Data/Alerts_calc_metrics"

# Allerts copy
AC_alerts_folder = "Data/Alerts"
AC_calc_folder = "Data/Alerts_calc"

# buy price writer
BPW_alerts_folder = "Data/Alerts"
BPW_klines_folder = "Data/K_lines/1M"

# calculator
calc_k_lines_path = "Data/K_lines/1M"
calc_alerts_calc_path = "Data/Alerts_calc"

# k line

k_line_SCRIPT_NAME = "KLD_1M      :  "                       # Имя скрипта для вывода в консоль
k_K_LINES_DIR = "Data/K_lines/1M"     # Папка с минутными свечами
k_line_CLEAN_OLD_FILES = 180                               # Max файлов в папке



# hdp_1h
hdr_1h_K_LINES_DIR = "Data/K_lines/1M"         # Папка с минутными свечами
hdr_1h_RESULTS_DIR = "Data/K_lines/1H"         # Папка с результатом
hdp_1h_SCRIPT_NAME = "HDP_1H      :  "
hdp_1h_FILES_TO_WORK = 60                                      # Количество файлов обработки
hdp_1h_MAX_RESULT_FILES = 720                                  # Максимум файлов результата

# leveling break 
lb_SCRIPT_NAME = "LB_12H      :  "                                                             # Имя скрипта для вывода в консоль
lb_12H_FOLDER = "Data/Agr_12h"                                             # Папка с историческими агрегированными данными
lb_K_LINES_DIR = "Data/K_lines/1M"                                             # Папка с минутными свечами
lb_OUTPUT_FOLDER = "Data/Ticker_up"                                            # Папка с результатом
lb_MAX_RESULT_FILES = 2                                                                        # Максимум файлов результата
lb_HDP_SCRIPT_PATH = "hdp_dynamic.py"        # Путь к скрипту hdp_dynamic.py


MAIN_SCRIPT_NAME = "STARTER     :  "            
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
T_ANAL_ALERTS_FOLDER = "Data/Alerts"  # Новая папка для хранения алертов

# total V
total_v_SCRIPT_NAME = "T_VOL_24H   :  "                                     # Имя скрипта
total_V_K_LINES_1H_DIR = "Data/K_lines/1H"                  # Папка с часовыми свечами
TOTAL_V_K_LINES_DIN_DIR = "Data/K_lines/Dynamic"            # Папка с динамическим файлом
TOTAL_V_OUTPUT_FOLDER = "Data/Total_volume_24H"             # Папка с результатом
TOTAL_V_H_COUNT = 23                                                        # Количество файлов обработки
TOTAL_V_MAX_RESULT_FILES = 2                                                # Максимум файлов результата

#val 10h
VAL_10H_SCRIPT_NAME = "VOL_10H     :  "
VAL_10H_MAX_RESULT_FILES = 1                                            # Максимум файлов результата
VAL_10H_input_dir = "Data/K_lines/1H" 
VAL_10H_hdr_1h_RESULTS_DIR = "Data/K_lines/Volume_10H"

# vol 10m
vol_10m_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Или, если скрипт находится глубже в структуре:
# BASE_DIR = Path(__file__).parent.parent.parent  # если нужно подняться на 3 уровня вверх

vol_10m_SCRIPT_NAME = "VOL_10M     :  "
hdr_1h_K_LINES_DIR = os.path.join(vol_10m_BASE_DIR, "Data", "K_lines", "1M")                 # Папка с минутными свечами
hdr_1h_RESULTS_DIR = os.path.join(vol_10m_BASE_DIR, "Data", "Volume_10M")                 # Папка с результатом
vol_10m_MAX_RESULT_FILES = 2                                            # Максимум файлов результата
