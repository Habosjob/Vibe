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

# Имя таблицы уникальных эмитентов (витрина по INN).
# Хранит EMITENTNAME, INN, а также пользовательские поля Scoring и DateScoring.
EMITENTS_TABLE_NAME = "emitents"

# Имя Excel-файла витрины эмитентов в корне проекта.
# Пользователь вносит ручные оценки в колонку Scoring.
EMITENTS_XLSX_FILENAME = "Emitents.xlsx"

# Цвет заливки заголовков в Excel-витрине (HEX ARGB).
# По умолчанию: светло-голубой.
EMITENTS_HEADER_FILL_COLOR = "FFD9E1F2"

# Допустимые значения для колонки Scoring в витрине Emitents.xlsx.
# Значение: кортеж строк, которые можно выбрать из выпадающего списка.
# По умолчанию: ("Red", "Yellow", "Green").
SCORING_ALLOWED_VALUES = ("Red", "Yellow", "Green")

# Формат даты для автоматического заполнения DateScoring.
# По умолчанию: ISO-формат YYYY-MM-DD.
DATE_SCORING_FORMAT = "%Y-%m-%d"

# Имя Excel-файла снапшота по таблице эмитентов (5 случайных строк).
EMITENTS_SNAPSHOT_FILENAME = "emitents_snapshot.xlsx"

# URL страницы НРА «Рейтинги», где находится кнопка выгрузки Excel.
# Значение: строка URL.
# По умолчанию: публичная страница НРА.
NRA_RATINGS_PAGE_URL = "https://www.ra-national.ru/ratings/"

# Имя raw-файла НРА в папке /raw.
# Это оригинальный файл, скачанный по кнопке выгрузки Excel.
NRA_RAW_FILENAME = "nra_ratings.xlsx"

# Имя файла общей базы SQLite с рейтингами агентств (отдельная БД от основной).
# По умолчанию: raitings.sqlite3 (единая БД под несколько рейтинговых агентств).
RAITINGS_DB_FILENAME = "raitings.sqlite3"

# Имя таблицы НРА c сырыми записями из выгрузки.
NRA_TABLE_NAME = "nra_ratings"

# Имя таблицы последних уникальных рейтингов НРА по ИНН.
NRA_LATEST_TABLE_NAME = "nra_latest_by_inn"

# Имя Excel-файла снапшота по последним рейтингам НРА (5 случайных строк).
NRA_SNAPSHOT_FILENAME = "nra_snapshot.xlsx"

# TTL для повторной загрузки Excel НРА.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
NRA_CACHE_TTL_HOURS = 12

# HTTP User-Agent для запросов к странице/файлу НРА.
# По умолчанию: нейтральный desktop User-Agent.
NRA_REQUEST_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
