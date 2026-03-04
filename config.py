from __future__ import annotations

from pathlib import Path

# Базовая директория проекта (папка, где лежит этот файл).
BASE_DIR = Path(__file__).resolve().parent

# URL источника CSV с MOEX ISS.
# Значение: строка URL.
# По умолчанию: актуальная ссылка на rates.csv с нужными sec_type.
SOURCE_CSV_URL = (
    "https://iss.moex.com/iss/apps/infogrid/stock/rates.csv?q=&sec_type="
    "stock_ofz_bond,stock_subfederal_bond,stock_municipal_bond,"
    "stock_corporate_bond,stock_exchange_bond,stock_cb_bond&bg=&iss.dp=comma&"
    "iss.df=%25d.%25m.%25Y&iss.tf=%25H:%25M:%25S&iss.dtf=%25d.%25m.%25Y%20%25H:%25M:%25S&"
    "iss.only=rates&limit=unlimited&lang=ru"
)

# Сколько часов считается «свежим» кэш.
# Значение: целое число > 0.
# По умолчанию: 12 часов (повторные запросы в сеть не выполняются).
CACHE_TTL_HOURS = 12

# Таймаут HTTP-запроса в секундах.
# Значение: число (int/float).
# По умолчанию: 120 сек.
REQUEST_TIMEOUT_SECONDS = 120

# Папка для «сырого» файла, полученного из сети.
# По умолчанию: <проект>/raw
RAW_DIR = BASE_DIR / "raw"

# Папка для кэш-файла (копия последней загрузки).
# По умолчанию: <проект>/cache
CACHE_DIR = BASE_DIR / "cache"

# Папка для базы данных SQLite.
# По умолчанию: <проект>/DB
DB_DIR = BASE_DIR / "DB"

# Папка для Excel-снапшотов проверки наполненности.
# По умолчанию: <проект>/BaseSnapshots
BASE_SNAPSHOTS_DIR = BASE_DIR / "BaseSnapshots"

# Папка логов (техническая информация).
# По умолчанию: <проект>/logs
LOGS_DIR = BASE_DIR / "logs"

# Папка документации.
# По умолчанию: <проект>/docs
DOCS_DIR = BASE_DIR / "docs"

# Имя raw-файла в папке /raw.
RAW_FILENAME = "moex_rates.csv"

# Имя кэш-файла в папке /cache.
CACHE_FILENAME = "moex_rates_cache.csv"

# Имя файла базы данных SQLite.
DB_FILENAME = "moex_rates.sqlite3"

# Имя лог-файла (перезаписывается на каждом запуске).
LOG_FILENAME = "main.log"

# Имя Excel-файла со срезом 5 случайных уникальных SECID.
SNAPSHOT_FILENAME = "rates_snapshot.xlsx"

# Имя таблицы с данными котировок.
RATES_TABLE_NAME = "rates"

# Имя таблицы метаданных кэша.
META_TABLE_NAME = "meta"
