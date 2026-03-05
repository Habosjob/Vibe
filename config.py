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
RAW_FILENAME = "moex_bonds.csv"

# Имя кэш-файла в папке /cache.
CACHE_FILENAME = "moex_bonds_cache.csv"

# Имя файла базы данных SQLite.
DB_FILENAME = "bonds.sqlite3"

# Имя лог-файла (перезаписывается на каждом запуске).
LOG_FILENAME = "main.log"

# Имя Excel-файла со срезом 5 случайных уникальных SECID.
SNAPSHOT_FILENAME = "moex_bonds_snapshot.xlsx"

# Имя таблицы с данными котировок.
RATES_TABLE_NAME = "moex_bonds"

# Легаси-имя таблицы MOEX для миграции существующей БД.
# Если таблица с таким именем найдена, она будет автоматически переименована.
LEGACY_RATES_TABLE_NAME = "rates"

# Легаси-имя файла БД для миграции со старого проекта.
# Если найден DB/moex_rates.sqlite3 и отсутствует DB/bonds.sqlite3, файл будет переименован.
LEGACY_DB_FILENAME = "moex_rates.sqlite3"

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

# Имя Excel-файла снапшота по последним рейтингам НРА (5 самых свежих строк по дате).
NRA_SNAPSHOT_FILENAME = "nra_snapshot.xlsx"

# Имя Excel-файла снапшота по рейтингам АКРА (5 самых свежих строк по дате).
ACRA_SNAPSHOT_FILENAME = "acra_snapshot.xlsx"

# Имя Excel-файла снапшота по рейтингам НКР (5 самых свежих строк по дате).
NKR_SNAPSHOT_FILENAME = "nkr_snapshot.xlsx"

# URL страницы поиска RAEX (Эксперт РА).
RAEX_SEARCH_URL = "https://raexpert.ru/search/"

# Имя таблицы RAEX с инкрементальной историей рейтингов.
# Ключ уникальности: (inn, rating_date, rating, forecast).
RAEX_TABLE_NAME = "raex_ratings"

# Имя таблицы последних актуальных рейтингов RAEX по ИНН.
RAEX_LATEST_TABLE_NAME = "raex_latest_by_inn"

# Имя Excel-файла снапшота по рейтингам RAEX (5 самых свежих строк по дате).
RAEX_SNAPSHOT_FILENAME = "raex_snapshot.xlsx"

# TTL для повторного обновления RAEX.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
RAEX_CACHE_TTL_HOURS = 12

# Количество потоков для параллельного парсинга RAEX.
# Значение: целое число > 0.
# По умолчанию: 12 (ускоряет первичный массовый прогон по ИНН).
RAEX_MAX_WORKERS = 12

# Фиксированная ширина колонок с рейтингами в витрине эмитентов (в символах Excel).
# Значение: число > 0.
# По умолчанию: 22.09 (~250 px, применяется только к колонкам *_Rate).
EMITENTS_RATINGS_COLUMN_WIDTH = 22.09

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

# URL списка рейтингов АКРА (эмитенты).
# Значение: строка URL.
# По умолчанию: страница со всеми эмитентами (count=1000).
ACRA_RATINGS_LIST_URL = (
    "https://www.acra-ratings.ru/ratings/issuers/"
    "?text=&sectors[]=&activities[]=&countries[]=&forecasts[]=&on_revision=0"
    "&rating_scale=0&rate_from=0&rate_to=0&page=1&sort=&count=1000&"
)

# Имя таблицы АКРА с инкрементальной историей рейтингов.
# Ключ уникальности: (issuer_url, rating_date, rating).
ACRA_TABLE_NAME = "acra_ratings"

# TTL для первого этапа АКРА (парсинг списка рейтингов).
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
ACRA_CACHE_TTL_HOURS = 12

# Папка persistent-профиля браузера для АКРА.
# Позволяет сохранять cookies/состояние между запусками.
# По умолчанию: <проект>/cache/acra_profile
ACRA_PROFILE_DIR = CACHE_DIR / "acra_profile"

# Канал браузера Playwright для АКРА.
# Значение: строка (например "chrome", "msedge") или None для bundled Chromium.
# По умолчанию: "chrome".
ACRA_BROWSER_CHANNEL = "chrome"

# Headless-режим для АКРА.
# Значения: True/False. Для сложной антибот-защиты обычно лучше False.
# По умолчанию: False.
ACRA_HEADLESS = False

# Количество попыток открыть страницу списка АКРА.
# Значение: целое число > 0.
# По умолчанию: 6.
ACRA_LIST_GOTO_ATTEMPTS = 6

# Количество попыток открыть карточку эмитента АКРА.
# Значение: целое число > 0.
# По умолчанию: 5.
ACRA_CARD_GOTO_ATTEMPTS = 5

# Папка для дампов АКРА (список, карточки, прогресс-лог).
# По умолчанию: <проект>/raw/acra_dump
ACRA_DUMP_DIR = RAW_DIR / "acra_dump"

# Подпапка HTML-дампов карточек эмитентов АКРА.
# По умолчанию: <проект>/acra_dump/issuers
ACRA_ISSUERS_DUMP_DIR = ACRA_DUMP_DIR / "issuers"

# Имя JSONL-файла лога прогресса обработки карточек АКРА.
# По умолчанию: progress.jsonl
ACRA_PROGRESS_LOG_FILENAME = "progress.jsonl"

# Имя HTML-дампа страницы списка эмитентов АКРА.
# По умолчанию: issuers_list.html
ACRA_LIST_HTML_FILENAME = "issuers_list.html"

# Имя MHTML-дампа страницы списка эмитентов АКРА.
# По умолчанию: issuers_list.mhtml
ACRA_LIST_MHTML_FILENAME = "issuers_list.mhtml"

# URL страницы НКР «Список эмитентов».
NKR_RATINGS_PAGE_URL = "https://ratings.ru/ratings/issuers/"

# Имя raw-файла НКР в папке /raw.
NKR_RAW_FILENAME = "nkr_ratings.xlsx"

# Имя таблицы НКР с инкрементальной историей рейтингов.
# Ключ уникальности: (tin, rating_date, rating, outlook).
NKR_TABLE_NAME = "nkr_ratings"

# Имя таблицы последних уникальных рейтингов НКР по ИНН.
NKR_LATEST_TABLE_NAME = "nkr_latest_by_tin"

# TTL для повторной загрузки Excel НКР.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
NKR_CACHE_TTL_HOURS = 12

# Headless-режим для НКР.
# Значения: True/False.
# По умолчанию: True.
NKR_HEADLESS = True

# Канал браузера Playwright для НКР.
# Значение: строка (например "chrome", "msedge") или None для bundled Chromium.
# По умолчанию: "chrome".
NKR_BROWSER_CHANNEL = "chrome"

# CSS-селектор кнопки/ссылки выгрузки НКР.
# Значение: строка CSS-селектора.
# По умолчанию: ".Excel-btn".
NKR_EXPORT_BUTTON_SELECTOR = ".Excel-btn"

# Количество попыток скачивания выгрузки НКР.
# Значение: целое число > 0.
# По умолчанию: 3.
NKR_DOWNLOAD_ATTEMPTS = 3

# URL страницы Доходъ с таблицей облигаций и кнопкой выгрузки Excel.
DOHOD_BONDS_PAGE_URL = "https://www.dohod.ru/analytic/bonds"

# Имя raw-файла выгрузки Доходъ в папке /raw.
DOHOD_RAW_FILENAME = "dohod_bonds.xlsx"

# TTL для повторной загрузки Excel Доходъ.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
DOHOD_CACHE_TTL_HOURS = 12

# Имя таблицы Доходъ в основной БД bonds.sqlite3.
DOHOD_TABLE_NAME = "Dohod_Bonds"

# Имя Excel-файла снапшота по таблице Доходъ (5 случайных строк по уникальному ISIN).
DOHOD_SNAPSHOT_FILENAME = "dohod_bonds_snapshot.xlsx"

# Headless-режим для Доходъ.
# Значения: True/False.
# По умолчанию: True.
DOHOD_HEADLESS = True

# Канал браузера Playwright для Доходъ.
# Значение: строка (например "chrome", "msedge") или None для bundled Chromium.
# По умолчанию: "chrome".
DOHOD_BROWSER_CHANNEL = "chrome"

# Имя merge-таблицы для облигаций эмитентов со Scoring = Green.
# По умолчанию: MergeGreenBonds.
MERGE_GREEN_TABLE_NAME = "MergeGreenBonds"

# Имя merge-таблицы для облигаций эмитентов со Scoring = Yellow.
# По умолчанию: MergeYellowBonds.
MERGE_YELLOW_TABLE_NAME = "MergeYellowBonds"

# Имя Excel-файла снапшота merge-таблицы Green (5 случайных строк).
# По умолчанию: merge_green_bonds_snapshot.xlsx.
MERGE_GREEN_SNAPSHOT_FILENAME = "merge_green_bonds_snapshot.xlsx"

# Имя Excel-файла снапшота merge-таблицы Yellow (5 случайных строк).
# По умолчанию: merge_yellow_bonds_snapshot.xlsx.
MERGE_YELLOW_SNAPSHOT_FILENAME = "merge_yellow_bonds_snapshot.xlsx"

# Имя таблицы единого скринера (объединение MergeGreenBonds + MergeYellowBonds).
# По умолчанию: Screener.
SCREENER_TABLE_NAME = "Screener"

# Имя Excel-витрины скринера (перезаписывается на каждом запуске).
# Внутри два листа: Green и Yellow.
# По умолчанию: Screener.xlsx.
SCREENER_XLSX_FILENAME = "Screener.xlsx"

# Сортировать ли строки в Excel-витрине Screener по AmortStarrtDate.
# Если True — в листах Green/Yellow сначала идут бумаги с ближайшей датой
# амортизации (по возрастанию), пустые/невалидные даты — в конце; при
# одинаковой дате добавляется вторичная сортировка по Названию и ISIN.
# Если False — сохраняется исходный порядок строк из SQL-таблицы Screener.
# Значения: True/False.
# По умолчанию: True.
SCREENER_SORT_BY_AMORT_START_DATE = True

# Сколько знаков после запятой сохранять в колонке YTM в таблице Screener.
# Значение: целое число >= 0.
# По умолчанию: 4.
YTM_OUTPUT_PRECISION = 4

# Включать ли встроенный self-check расчета YTM на этапе Screener.
# Проверки не пишут в консоль, только в лог.
# Значения: True/False.
# По умолчанию: True.
YTM_SELFCHECK_ENABLED = True

# Режим strict для self-check YTM.
# Если True и проверка не пройдена — скрипт завершится ошибкой.
# Если False — ошибки self-check попадут в лог как warning.
# Значения: True/False.
# По умолчанию: False.
YTM_SELFCHECK_STRICT = True

# Режим склейки Merge-таблиц по ISIN между MOEX и Доходъ.
# Если True, в Merge попадают только бумаги, которые есть в обеих таблицах
# (INNER JOIN по ISIN). Если False — используются все бумаги из MOEX и
# колонки Доходъ добавляются при наличии совпадения (LEFT JOIN).
# Значения: True/False.
# По умолчанию: True.
MERGE_REQUIRE_DOHOD_ISIN_MATCH = True

# Минимальный остаточный срок до погашения/оферты для этапа Presorter (в днях).
# Если до даты меньше этого значения, бумага исключается из Merge*.
# Значение: целое число > 0.
# По умолчанию: 365.
PRESORTER_MIN_DAYS_TO_EVENT = 365

# Значение поля BOND_TYPE, которое исключается на этапе Presorter.
# Сравнение выполняется после trim и без учета регистра.
# Значение: строка.
# По умолчанию: "Структурная облигация".
PRESORTER_EXCLUDED_BOND_TYPE = "Структурная облигация"

# Использовать ли дату из Доходъ ("Ближайшая дата погашения/оферты (Дата)")
# в правиле по минимальному количеству дней до события.
# Значения: True/False.
# По умолчанию: True (правило по дате из Доходъ включено как отдельный фильтр).
PRESORTER_USE_DOHOD_NEAREST_DATE = True

# Базовый URL карточки облигации на Corpbonds.
# Для запроса используется SECID: <CORPBONDS_BOND_URL_PREFIX><SECID>
# По умолчанию: https://corpbonds.ru/bond/
CORPBONDS_BOND_URL_PREFIX = "https://corpbonds.ru/bond/"

# Имя таблицы с обогащением Merge* из Corpbonds.
# По умолчанию: CorpbondsBonds.
CORPBONDS_TABLE_NAME = "CorpbondsBonds"

# TTL для повторного запроса карточки Corpbonds по SECID.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
CORPBONDS_CACHE_TTL_HOURS = 12

# Количество потоков для параллельной загрузки Corpbonds.
# Значение: целое число > 0.
# По умолчанию: 16.
CORPBONDS_MAX_WORKERS = 16

# Таймаут HTTP-запроса к Corpbonds (секунды).
# Значение: число (int/float).
# По умолчанию: 30.
CORPBONDS_REQUEST_TIMEOUT_SECONDS = 30

# Имя Excel-файла снапшота по Corpbonds-обогащению (5 случайных SECID).
# По умолчанию: corpbonds_snapshot.xlsx.
CORPBONDS_SNAPSHOT_FILENAME = "corpbonds_snapshot.xlsx"

# Режим объединения Merge* c Corpbonds по SECID.
# Если True, после этапа Corpbonds в Merge* остаются только бумаги,
# для которых найден SECID в Corpbonds (INNER JOIN), и поля Corpbonds
# записываются прямо в Merge-таблицы в колонки с префиксом "Corpbonds_".
# Значения: True/False.
# По умолчанию: True.
MERGE_REQUIRE_CORPBONDS_SECID_MATCH = True

# Базовый URL карточки облигации на Smart-Lab.
# Для запроса используется SECID: <SMARTLAB_BOND_URL_PREFIX><SECID>/
# По умолчанию: https://smart-lab.ru/q/bonds/
SMARTLAB_BOND_URL_PREFIX = "https://smart-lab.ru/q/bonds/"

# Имя таблицы с обогащением Merge* из Smart-Lab.
# По умолчанию: SmartlabBonds.
SMARTLAB_TABLE_NAME = "SmartlabBonds"

# TTL для повторного запроса карточки Smart-Lab по SECID.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
SMARTLAB_CACHE_TTL_HOURS = 12

# Количество потоков для параллельной загрузки Smart-Lab.
# Значение: целое число > 0.
# По умолчанию: 32.
SMARTLAB_MAX_WORKERS = 32

# Таймаут HTTP-запроса к Smart-Lab (секунды).
# Значение: число (int/float).
# По умолчанию: 30.
SMARTLAB_REQUEST_TIMEOUT_SECONDS = 30

# Имя Excel-файла снапшота по Smart-Lab-обогащению (5 случайных SECID).
# По умолчанию: smartlab_snapshot.xlsx.
SMARTLAB_SNAPSHOT_FILENAME = "smartlab_snapshot.xlsx"

# Режим объединения Merge* c Smart-Lab по SECID.
# Если True, после этапа Smart-Lab в Merge* остаются только бумаги,
# для которых найден SECID в Smart-Lab (INNER JOIN), и поля Smart-Lab
# записываются прямо в Merge-таблицы в колонки с префиксом "Smartlab_".
# Значения: True/False.
# По умолчанию: True.
MERGE_REQUIRE_SMARTLAB_SECID_MATCH = True

# URL-шаблон MOEX ISS для амортизаций по облигации.
# Формат: строка с {secid}, подставляется тикер SECID.
# По умолчанию: публичный endpoint MOEX с блоком amortizations.
MOEX_AMORTIZATION_URL_TEMPLATE = (
    "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/"
    "{secid}.json?iss.only=amortizations&iss.meta=off"
)

# Имя таблицы с амортизациями MOEX.
# По умолчанию: MoexAmortizations.
MOEX_AMORTIZATION_TABLE_NAME = "MoexAmortizations"

# TTL для повторного запроса амортизаций MOEX по SECID.
# Значение: целое число > 0 (в часах).
# По умолчанию: 12 часов.
MOEX_AMORTIZATION_CACHE_TTL_HOURS = 12

# Количество потоков для параллельной загрузки амортизаций MOEX.
# Значение: целое число > 0.
# По умолчанию: 24.
MOEX_AMORTIZATION_MAX_WORKERS = 24

# Таймаут HTTP-запроса к MOEX амортизациям (секунды).
# Значение: число (int/float).
# По умолчанию: 20.
MOEX_AMORTIZATION_REQUEST_TIMEOUT_SECONDS = 20

# Имя Excel-файла снапшота по амортизациям MOEX.
# В файл выгружаются все строки по 5 случайным SECID.
# По умолчанию: moex_amortizations_snapshot.xlsx.
MOEX_AMORTIZATION_SNAPSHOT_FILENAME = "moex_amortizations_snapshot.xlsx"

# Значение в Corpbonds_Наличие амортизации, при котором бумага идет в запрос MOEX.
# Сравнение выполняется без учета регистра и лишних пробелов.
# По умолчанию: "Да".
MOEX_AMORTIZATION_REQUIRED_FLAG = "Да"
