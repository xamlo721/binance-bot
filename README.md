# Binance Trading Bot Suite

Торговый бот для Binance, состоящий из трех взаимосвязанных модулей для загрузки данных, аналитики и автоматической торговли.

## 🏗 Архитектура проекта

Проект состоит из трех основных компонентов:

1. **DownloadBot** - Загрузка исторических и рыночных данных с Binance
2. **AnalyticsBot** - Анализ данных, расчет индикаторов и генерация сигналов
3. **TraderBot** - Автоматическая торговля на основе аналитических сигналов

## 📋 Системные требования

- Linux (Ubuntu/Debian рекомендованы)
- Python 3.8 или выше
- Минимум 2GB RAM
- Стабильное интернет-соединение

## 🚀 Быстрая установка

### 1. Обновление системы и установка зависимостей

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv

# Проверка версий
python3 --version
pip3 --version
```

### 2. Клонирование репозитория

```bash
git clone https://github.com/xamlo721/binance-bot.git
cd binance-bot
```

### 3. Создание и активация виртуального окружения

```bash
python3 -m venv .binance_env
source .binance_env/bin/activate
```

### 4. Установка Python зависимостей

```bash
pip install aiohttp
pip install aiodns
pip install numpy
pip install python-binance
pip install websocket-client
```

Или установить все сразу:

```bash
pip install aiohttp aiodns numpy python-binance websocket-client
```

## 📦 Структура проекта

```
binance-bot/
├── src/
│   ├── DownloadBot/
│   │   ├── main.py
│   │   ├── downloader.py(допишу)
│   │   └── config.py
│   ├── AnalyticsBot/
│   │   ├── main.py
│   │   ├── analyzer.py (допишу)
│   └── TraderBot/
│       ├── main.py
│       ├── trader.py(допишу)
├── data/
│   ├── market_data/
│   └── analytics/
├── logs/
├── config/
│   └── settings.yaml
├── requirements.txt
└── README.md
```

## 🔧 Настройка конфигурации

Создайте файл `.env` в корне проекта:

```bash
nano .env
```

Добавьте ваши API ключи Binance:

```env
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here
BINANCE_TESTNET_API_KEY=your_testnet_key
BINANCE_TESTNET_API_SECRET=your_testnet_secret

# Настройки ботов (допишу)
DOWNLOAD_BOT_INTERVAL=60
ANALYTICS_BOT_INTERVAL=300
TRADER_BOT_INTERVAL=60
```

## 🎯 Запуск компонентов

### Запуск DownloadBot (загрузчик данных)

```bash
cd /path/to/binance-bot
source .binance_env/bin/activate
python3 src/DownloadBot/main.py
```

**Фоновый запуск:**
```bash
nohup python3 src/DownloadBot/main.py > logs/download_bot.log 2>&1 &
```

### Запуск AnalyticsBot (аналитический модуль)

```bash
cd /path/to/binance-bot
source .binance_env/bin/activate
python3 src/AnalyticsBot/main.py
```

**Фоновый запуск:**
```bash
nohup python3 src/AnalyticsBot/main.py > logs/analytics_bot.log 2>&1 &
```

### Запуск TraderBot (торговый модуль)

```bash
cd /path/to/binance-bot
source .binance_env/bin/activate
python3 src/TraderBot/main.py
```

**Фоновый запуск:**
```bash
nohup python3 src/TraderBot/main.py > logs/trader_bot.log 2>&1 &
```

## 📜 Скрипт для одновременного запуска всех ботов

Создайте скрипт `start_all_bots.sh`:

```bash
nano start_all_bots.sh
```

Добавьте содержимое:

```bash
#!/bin/bash

cd /path/to/binance-bot
source .binance_env/bin/activate

# Создание папки для логов
mkdir -p logs

# Запуск ботов
echo "Starting DownloadBot..."
nohup python3 src/DownloadBot/main.py > logs/download_bot.log 2>&1 &
echo $! > logs/download_bot.pid

echo "Starting AnalyticsBot..."
nohup python3 src/AnalyticsBot/main.py > logs/analytics_bot.log 2>&1 &
echo $! > logs/analytics_bot.pid

echo "Starting TraderBot..."
nohup python3 src/TraderBot/main.py > logs/trader_bot.log 2>&1 &
echo $! > logs/trader_bot.pid

echo "All bots started successfully!"
echo "Check logs in ./logs directory"
```

Дайте права на выполнение:

```bash
chmod +x start_all_bots.sh
./start_all_bots.sh
```

## 🛑 Остановка всех ботов

Скрипт `stop_all_bots.sh`:

```bash
#!/bin/bash

cd /path/to/binance-bot

if [ -f logs/download_bot.pid ]; then
    kill $(cat logs/download_bot.pid)
    echo "DownloadBot stopped"
fi

if [ -f logs/analytics_bot.pid ]; then
    kill $(cat logs/analytics_bot.pid)
    echo "AnalyticsBot stopped"
fi

if [ -f logs/trader_bot.pid ]; then
    kill $(cat logs/trader_bot.pid)
    echo "TraderBot stopped"
fi

echo "All bots stopped"
```

## 📊 Мониторинг работы ботов

### Просмотр логов в реальном времени

```bash
# Логи DownloadBot
tail -f logs/download_bot.log

# Логи AnalyticsBot
tail -f logs/analytics_bot.log

# Логи TraderBot
tail -f logs/trader_bot.log

# Все логи вместе
tail -f logs/*.log
```

### Проверка запущенных процессов

```bash
ps aux | grep "DownloadBot\|AnalyticsBot\|TraderBot" | grep -v grep
```

## 🔄 Настройка автоматического запуска (systemd)

Создайте сервисные файлы для каждого бота:

**/etc/systemd/system/downloadbot.service**
```ini
[Unit]
Description=Binance Download Bot
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/binance-bot
ExecStart=/bin/bash -c 'source /path/to/binance-bot/.binance_env/bin/activate && python3 /path/to/binance-bot/src/DownloadBot/main.py'
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Запуск через systemd:

```bash
sudo systemctl daemon-reload
sudo systemctl enable downloadbot
sudo systemctl start downloadbot
sudo systemctl status downloadbot
```

## 🧪 Тестовый режим

Для тестирования используйте Binance Testnet:

```python
# В конфигурации ботов установите
TESTNET_MODE = True
```

## 📝 Файл requirements.txt

Создайте для удобной установки всех зависимостей:

```txt
aiohttp>=3.8.0
aiodns>=3.0.0
pandas>=1.5.0
numpy>=1.23.0
python-binance>=1.0.16
websocket-client>=1.4.0
```

Установка через requirements:

```bash
pip install -r requirements.txt
```

## ⚠️ Важные замечания

1. **Тестируйте на Testnet** перед использованием реальных средств
2. **Регулярно проверяйте логи** для выявления проблем
3. **Делайте резервные копии** конфигурационных файлов
4. **Не запускайте несколько копий** одного бота одновременно

## 🐛 Устранение неполадок

### Проблема: Permission denied при запуске
```bash
chmod +x start_all_bots.sh
```

### Проблема: Модуль не найден
```bash
pip install --upgrade pip
pip install missing_module_name
```

### Проблема: Бот не видит API ключи
```bash
# Проверьте наличие .env файла
ls -la .env
# Перезагрузите переменные окружения
source .binance_env/bin/activate
```

## ⚖️ Лицензия

MIT License

---

**⚠️ Дисклеймер**: Торговля криптовалютами сопряжена с высоким риском. Автор не несет ответственности за финансовые потери при использовании данного бота. Используйте на свой страх и риск.
