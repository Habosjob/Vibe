from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Пути к рабочим папкам проекта.
# Все папки создаются автоматически при запуске.
# При желании можно поменять пути на абсолютные или относительные.
DOCS_DIR = BASE_DIR / "docs"
CACHE_DIR = BASE_DIR / "cache"
DB_DIR = BASE_DIR / "db"
OUTPUT_DIR = BASE_DIR / "output"
RAW_DIR = BASE_DIR / "raw"
LOGS_DIR = BASE_DIR / "logs"

# Имя SQLite-базы с рабочими данными по облигациям.
# Файл будет создан внутри папки DB_DIR.
DB_FILE_NAME = "bonds.sqlite3"

# Имя файла состояния (чекпоинтов).
# В этот файл сохраняется прогресс, чтобы можно было продолжить после сбоя.
STATE_FILE_NAME = "state.json"

# Имя файла кэша метаданных.
# Здесь можно хранить промежуточные ответы API или результаты парсинга.
CACHE_FILE_NAME = "bonds_cache.json"

# Имя итогового Excel-файла.
# В текущем скелете сохраняется демонстрационный отчет.
OUTPUT_FILE_NAME = "bonds_screening_result.xlsx"

# Настройки логирования.
# LOG_FILE_NAME — файл логов внутри папки LOGS_DIR.
# LOG_LEVEL — уровень детализации: DEBUG, INFO, WARNING, ERROR.
# LOG_MAX_BYTES — размер одного лог-файла до ротации (в байтах).
# LOG_BACKUP_COUNT — сколько архивных логов хранить.
LOG_FILE_NAME = "app.log"
LOG_LEVEL = "INFO"
LOG_MAX_BYTES = 1_000_000
LOG_BACKUP_COUNT = 5

# Настройки сетевых операций.
# REQUEST_CONNECT_TIMEOUT_SEC — таймаут подключения к серверу.
# REQUEST_READ_TIMEOUT_SEC — таймаут чтения ответа.
# REQUEST_RETRIES — число повторных попыток при временных сбоях.
# REQUEST_BACKOFF_SEC — базовая задержка перед повтором; далее применяется экспоненциальный рост.
REQUEST_CONNECT_TIMEOUT_SEC = 5
REQUEST_READ_TIMEOUT_SEC = 20
REQUEST_RETRIES = 3
REQUEST_BACKOFF_SEC = 1.0

# Параллельность для I/O-операций.
# MAX_CONCURRENT_TASKS ограничивает число одновременных задач,
# чтобы не перегружать сеть/диск.
MAX_CONCURRENT_TASKS = 5

# TTL кэша в секундах.
# Если кэш старше этого значения, он считается устаревшим и перезаписывается.
CACHE_TTL_SEC = 60 * 60 * 24

# Размер пачки записей в БД.
# Чем больше BATCH_SIZE, тем реже коммиты и выше скорость,
# но больше нагрузка на память.
BATCH_SIZE = 100

# Демонстрационные параметры отбора облигаций.
# Это заглушка под будущие реальные правила скрининга.
MIN_COUPON_RATE = 0.0
MAX_MATURITY_YEARS = 30
MIN_RATING = "B-"

# Имя листа в Excel.
EXCEL_SHEET_NAME = "Screening"


def get_db_path() -> Path:
    return DB_DIR / DB_FILE_NAME


def get_state_file_path() -> Path:
    return CACHE_DIR / STATE_FILE_NAME


def get_cache_file_path() -> Path:
    return CACHE_DIR / CACHE_FILE_NAME


def get_output_file_path() -> Path:
    return OUTPUT_DIR / OUTPUT_FILE_NAME


def get_log_file_path() -> Path:
    return LOGS_DIR / LOG_FILE_NAME
