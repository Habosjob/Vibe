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
CORPBONDS_OUTPUT_FILE_NAME_DEBUG = "CorBonds.xlsx"

# Имена итоговых Excel-файлов в неотладочном режиме.
MOEX_OUTPUT_FILE_NAME_RELEASE = "MoexBonds_latest.xlsx"
CORPBONDS_OUTPUT_FILE_NAME_RELEASE = "CorBonds_latest.xlsx"

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

# TTL кэша в секундах.
# Если кэш старше этого времени, он должен считаться устаревшим.
CACHE_TTL_SEC = 60 * 60 * 24

# Размер батча для потенциальных пакетных операций записи в БД.
BATCH_SIZE = 200

# Название листа в Excel MOEX.
EXCEL_SHEET_NAME = "MoexBonds"

# Название листа в Excel CorpBonds.
CORPBONDS_EXCEL_SHEET_NAME = "CorBonds"

# Базовый URL карточки облигации на CorpBonds (работает по SECID).
CORPBONDS_BOND_URL_TEMPLATE = "https://corpbonds.ru/bond/{secid}"

# User-Agent для загрузки страниц CorpBonds.
CORPBONDS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


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
