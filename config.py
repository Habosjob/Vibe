from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Пути к рабочим папкам проекта.
# Все пути задаются через pathlib.Path для корректной работы на Windows.
# При запуске скрипта эти директории будут созданы автоматически.
DOCS_DIR = BASE_DIR / "docs"
CACHE_DIR = BASE_DIR / "cache"
DB_DIR = BASE_DIR / "db"
OUTPUT_DIR = BASE_DIR / "output"
RAW_DIR = BASE_DIR / "raw"
LOGS_DIR = BASE_DIR / "logs"

# Имя SQLite-базы данных.
# В базе хранится снимок прошлых цен по SECID, чтобы считать динамику между выгрузками.
DB_FILE_NAME = "bonds.sqlite3"

# Имя файла состояния (чекпоинтов).
# Здесь сохраняется прогресс обработки; при сбое можно продолжить с последнего этапа.
STATE_FILE_NAME = "state.json"

# Имя файла кэша.
# В текущей реализации файл зарезервирован под расширение кэша API-ответов.
CACHE_FILE_NAME = "bonds_cache.json"

# Режим отладки выгрузки Excel.
# True  — всегда использовать одни и те же имена файлов (перезапись).
# False — использовать альтернативные имена/логику версионирования в будущем.
DEBUG_SINGLE_EXCEL_FILE = True

# Флаги включения/выключения сохранения Excel.
# True  — файл формируется.
# False — этап сохранения конкретного провайдера пропускается.
EXPORT_MOEX_TO_EXCEL = True
EXPORT_CORPBONDS_TO_EXCEL = True

# Имя итогового Excel-файла MOEX в режиме отладки.
MOEX_OUTPUT_FILE_NAME_DEBUG = "MoexBonds.xlsx"

# Имя итогового Excel-файла CorpBonds в режиме отладки.
CORPBONDS_OUTPUT_FILE_NAME_DEBUG = "CorpBonds.xlsx"

# Имена итоговых Excel-файлов в неотладочном режиме.
MOEX_OUTPUT_FILE_NAME_RELEASE = "MoexBonds_latest.xlsx"
CORPBONDS_OUTPUT_FILE_NAME_RELEASE = "CorpBonds_latest.xlsx"

# Имя итогового объединенного Excel-файла (MOEX + CorpBonds) в режиме отладки.
MERGED_OUTPUT_FILE_NAME_DEBUG = "MergeBonds.xlsx"

# Имя итогового объединенного Excel-файла (MOEX + CorpBonds) в неотладочном режиме.
MERGED_OUTPUT_FILE_NAME_RELEASE = "MergeBonds_latest.xlsx"

# Имя Excel-файла со справочником эмитентов.
# Файл создается в output и используется для ручной расстановки ScoreList.
EMITENTS_OUTPUT_FILE_NAME = "Emitents.xlsx"

# Имя итогового Excel-файла скринера.
# В файле будут листы Green/Yellow/Red/Unsorted.
SCREENER_OUTPUT_FILE_NAME = "Screener.xlsx"

# Настройки логирования.
# LOG_FILE_NAME      — основной файл логов.
# LOG_LEVEL          — уровень логов: DEBUG, INFO, WARNING, ERROR.
# LOG_MAX_BYTES      — максимальный размер одного файла перед ротацией.
# LOG_BACKUP_COUNT   — количество архивных файлов логов.
LOG_FILE_NAME = "app.log"
LOG_LEVEL = "INFO"
LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5

# Настройки HTTP-запросов к MOEX ISS API.
# REQUEST_CONNECT_TIMEOUT_SEC — таймаут на установку соединения (сек).
# REQUEST_READ_TIMEOUT_SEC    — таймаут чтения ответа (сек).
# REQUEST_RETRIES             — количество попыток при временных ошибках.
# REQUEST_BACKOFF_SEC         — базовая задержка между повторами (экспоненциальный рост).
REQUEST_CONNECT_TIMEOUT_SEC = 7
REQUEST_READ_TIMEOUT_SEC = 35
REQUEST_RETRIES = 4
REQUEST_BACKOFF_SEC = 1.2

# Ограничение параллельности.
# Позволяет ускорить сбор данных, но не перегружать API MOEX.
MAX_CONCURRENT_TASKS = 12

# Отдельное ограничение параллельности для загрузки карточек CorpBonds.
# Обычно этот этап самый долгий, поэтому лимит можно держать выше,
# чем общий MAX_CONCURRENT_TASKS, чтобы ускорить полный прогон.
CORPBONDS_MAX_CONCURRENT_TASKS = 120

# TTL кэша в секундах.
# Если кэш старше этого времени, он должен считаться устаревшим.
CACHE_TTL_SEC = 60 * 60 * 24

# Размер батча для потенциальных пакетных операций записи в БД.
BATCH_SIZE = 200

# Отдельный TTL кэша для CorpBonds в секундах.
# Карточки CorpBonds меняются редко, поэтому держим кэш дольше,
# чтобы повторные запуски не перекачивали тысячи страниц.
CORPBONDS_CACHE_TTL_SEC = 60 * 60 * 24 * 7

# Название листа в Excel MOEX.
EXCEL_SHEET_NAME = "MoexBonds"

# Название листа в Excel CorpBonds.
CORPBONDS_EXCEL_SHEET_NAME = "CorpBonds"

# Название листа в Excel MergeBonds.
MERGED_EXCEL_SHEET_NAME = "MergeBonds"

# Название листа в Excel справочника эмитентов.
EMITENTS_EXCEL_SHEET_NAME = "Emitents"

# Разрешенные значения для ручного скоринга эмитентов.
# Значения используются как выпадающий список в Emitents.xlsx.
SCORE_LIST_ALLOWED_VALUES = ["GreenList", "YellowList", "RedList"]

# Названия колонок итогового Screener по умолчанию.
# Если список пустой, используются все колонки из MergeBonds + ScoreList + DateScoreList.
SCREENER_INCLUDE_COLUMNS: list[str] = []

# Колонки, которые нужно удалить из итогового Screener.
# Применяются после SCREENER_INCLUDE_COLUMNS.
SCREENER_EXCLUDE_COLUMNS: list[str] = []

# Настройки фильтров сортера перед формированием Screener.
# enabled=True  — фильтр включен.
# enabled=False — фильтр отключен.
# days — порог в днях (для датных фильтров).
SCREENER_FILTERS = {
    "exclude_amortization_started_or_soon": {"enabled": True, "days": 365},
    "exclude_structural_bonds": {"enabled": True},
    "exclude_defaults": {"enabled": True},
    "exclude_maturity_soon": {"enabled": True, "days": 365},
    "exclude_offer_soon": {"enabled": True, "days": 365},
    "exclude_qualified_investors": {"enabled": True},
}

# Базовый URL карточки облигации на CorpBonds (работает по SECID).
CORPBONDS_BOND_URL_TEMPLATE = "https://corpbonds.ru/bond/{secid}"

# User-Agent для загрузки страниц CorpBonds.
CORPBONDS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

# Настройки прогнозных значений индексов для расчета YTM по флоатерам и линкерам.
# Year0 — текущий год, Year1 — +1 год, Year2 — +2 года.
# Если срок бумаги дальше Year2, используется значение Year2.
YTM_KEY_RATE_FORECAST = {
    0: 14.0,
    1: 8.5,
    2: 8.0,
}

# Прогноз инфляции по годам для линкеров (например, ОФЗ 50*).
YTM_INFLATION_FORECAST = {
    0: 5.3,
    1: 4.0,
    2: 4.0,
}


def get_db_path() -> Path:
    return DB_DIR / DB_FILE_NAME


def get_state_file_path() -> Path:
    return CACHE_DIR / STATE_FILE_NAME


def get_cache_file_path() -> Path:
    return CACHE_DIR / CACHE_FILE_NAME


def get_moex_output_file_path() -> Path:
    file_name = MOEX_OUTPUT_FILE_NAME_DEBUG if DEBUG_SINGLE_EXCEL_FILE else MOEX_OUTPUT_FILE_NAME_RELEASE
    return OUTPUT_DIR / file_name


def get_corpbonds_output_file_path() -> Path:
    file_name = CORPBONDS_OUTPUT_FILE_NAME_DEBUG if DEBUG_SINGLE_EXCEL_FILE else CORPBONDS_OUTPUT_FILE_NAME_RELEASE
    return OUTPUT_DIR / file_name


def get_log_file_path() -> Path:
    return LOGS_DIR / LOG_FILE_NAME


def get_merged_output_file_path() -> Path:
    file_name = MERGED_OUTPUT_FILE_NAME_DEBUG if DEBUG_SINGLE_EXCEL_FILE else MERGED_OUTPUT_FILE_NAME_RELEASE
    return OUTPUT_DIR / file_name


def get_emitents_output_file_path() -> Path:
    return OUTPUT_DIR / EMITENTS_OUTPUT_FILE_NAME


def get_screener_output_file_path() -> Path:
    return BASE_DIR / SCREENER_OUTPUT_FILE_NAME
