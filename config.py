from pathlib import Path

# Корневая папка проекта. Определяется автоматически как директория, где лежит config.py.
PROJECT_ROOT: Path = Path(__file__).resolve().parent

# Папка для итогового Excel-файла и других пользовательских выгрузок.
OUTPUT_DIR: Path = PROJECT_ROOT / "output"

# Папка для хранения сырых ответов/вспомогательных выгрузок при необходимости отладки.
RAW_DIR: Path = PROJECT_ROOT / "raw"

# Папка для кэша и чекпоинтов выполнения. Здесь хранится state.json.
CACHE_DIR: Path = PROJECT_ROOT / "cache"

# Папка для логов приложения.
LOG_DIR: Path = PROJECT_ROOT / "logs"

# Путь к SQLite-базе данных.
DB_PATH: Path = PROJECT_ROOT / "db" / "bonds.sqlite3"

# Включить/выключить запись итоговой витрины в Excel.
# True  - формировать /output/MoexBonds.xlsx при каждом запуске.
# False - не создавать Excel, сохранять данные только в БД.
EXPORT_EXCEL: bool = True

# Имя основного Excel-файла в папке OUTPUT_DIR.
EXCEL_FILE_NAME: str = "MoexBonds.xlsx"

# Резервное имя Excel-файла, если основной открыт в Excel и заблокирован для перезаписи.
EXCEL_LOCKED_FILE_NAME: str = "MoexBonds.locked.xlsx"

# Максимальное число одновременных сетевых запросов к ISS API.
MAX_CONCURRENCY: int = 8

# HTTP-таймауты в секундах: (connect_timeout, read_timeout).
HTTP_TIMEOUTS: tuple[float, float] = (10.0, 30.0)

# Количество повторных попыток при временных сетевых ошибках/таймаутах/5xx.
RETRY_COUNT: int = 4

# Базовая задержка для экспоненциального backoff между повторами.
# Пример: 0.7, 1.4, 2.8, 5.6 сек.
RETRY_BACKOFF: float = 0.7

# TTL кэша в секундах. Если кэш старше, данные считаются устаревшими.
CACHE_TTL_SECONDS: int = 24 * 3600

# Количество торговых дней, по которым рассчитывается агрегат ликвидности.
LOOKBACK_TRADING_DAYS: int = 10

# Количество календарных дней, за которые берётся история,
# чтобы гарантированно покрыть нужные торговые дни.
LIQUIDITY_LOOKBACK_CALENDAR_DAYS: int = 20

# Исключать ли из финальной витрины погашенные бумаги (matdate < today).
SKIP_INACTIVE_MATURED: bool = True

# Размер батча для коммитов в БД (для инкрементальной записи по мере обработки).
DB_COMMIT_BATCH_SIZE: int = 50

# Путь к файлу checkpoint-состояния.
STATE_PATH: Path = CACHE_DIR / "state.json"

# Путь к файлу логов приложения.
LOG_FILE_PATH: Path = LOG_DIR / "app.log"

# Уровень логирования: DEBUG, INFO, WARNING, ERROR.
LOG_LEVEL: str = "INFO"

# Показывать динамический progress-bar в терминале.
# Если запуск идёт в среде без полноценного TTY (например, некоторые режимы VS Code Debug),
# скрипт автоматически переключится на текстовый прогресс с ETA.
SHOW_PROGRESS_BAR: bool = True

# Интервал (в секундах) между текстовыми сообщениями о прогрессе,
# когда графический progress-bar недоступен.
PROGRESS_FALLBACK_INTERVAL_SEC: float = 1.5

# Ограничения ротации логов.
LOG_MAX_BYTES: int = 2_000_000
LOG_BACKUP_COUNT: int = 5

# Базовый URL MOEX ISS API.
MOEX_BASE_URL: str = "https://iss.moex.com"

# Обязательные директории, которые должны существовать до старта.
REQUIRED_DIRS: tuple[Path, ...] = (
    OUTPUT_DIR,
    RAW_DIR,
    CACHE_DIR,
    LOG_DIR,
    DB_PATH.parent,
)
