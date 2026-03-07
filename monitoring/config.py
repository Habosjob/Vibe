from __future__ import annotations

from pathlib import Path

# Базовая директория автономного контура monitoring.
# По умолчанию: папка monitoring рядом с корневым main.py.
BASE_DIR = Path(__file__).resolve().parent

# Путь к корневой директории проекта (read-only входы: Emitents.xlsx и т.д.).
# По умолчанию: родитель BASE_DIR.
ROOT_DIR = BASE_DIR.parent

# Путь к корневому файлу с эмитентами (read-only вход).
# Значение: pathlib.Path.
# По умолчанию: <root>/Emitents.xlsx.
EMITENTS_SOURCE_FILE = ROOT_DIR / "Emitents.xlsx"

# Опциональный явный путь к пользовательскому файлу портфеля.
# Значения: строка пути или None.
# По умолчанию: None (поиск последнего файла по маскам).
PORTFOLIO_SOURCE_FILE: str | None = None

# Маски для поиска портфеля в рабочей директории.
# По умолчанию: варианты в разном регистре для Windows-окружений.
PORTFOLIO_GLOBS = ("*портфель*.xlsx", "*портфель*.xls", "*Портфель*.xlsx", "*Портфель*.xls")

# Папка кэша контура monitoring.
CACHE_DIR = BASE_DIR / "cache"

# Папка сырых артефактов (html/json/xlsx).
RAW_DIR = BASE_DIR / "raw"

# Папка БД SQLite.
DB_DIR = BASE_DIR / "DB"

# Папка логов (перезаписываемых).
LOGS_DIR = BASE_DIR / "logs"

# Папка локальных snapshot-файлов.
BASE_SNAPSHOTS_DIR = BASE_DIR / "BaseSnapshots"

# Основная SQLite БД monitoring.
DB_FILE = DB_DIR / "monitoring.sqlite3"

# Лог-файл monitoring (перезаписывается при каждом запуске).
LOG_FILE = LOGS_DIR / "monitoring.log"

# Выходная витрина ленты событий.
REPORTS_XLSX = BASE_DIR / "Reports_monitoring.xlsx"

# Выходная витрина портфеля.
PORTFOLIO_XLSX = BASE_DIR / "Portfolio.xlsx"

# Snapshot эмитентов.
EMITENTS_SNAPSHOT_XLSX = BASE_SNAPSHOTS_DIR / "emitents_snapshot.xlsx"

# Snapshot портфеля.
PORTFOLIO_SNAPSHOT_XLSX = BASE_SNAPSHOTS_DIR / "portfolio_snapshot.xlsx"

# Таймаут установки TCP соединения для e-disclosure (секунды).
# Значения: int/float > 0. По умолчанию: 4.
CONNECT_TIMEOUT_SECONDS = 4

# Таймаут чтения тела ответа для e-disclosure (секунды).
# Значения: int/float > 0. По умолчанию: 8.
READ_TIMEOUT_SECONDS = 8

# Количество повторов сетевых запросов (только для retryable ошибок).
# Значения: int >= 0. По умолчанию: 1.
HTTP_RETRIES = 1

# Максимальная пауза backoff между попытками (секунды).
# Значения: int/float > 0. По умолчанию: 20.
HTTP_MAX_BACKOFF_SECONDS = 20

# Верхняя граница Retry-After при 429/503 (секунды).
# Значения: int/float > 0. По умолчанию: 30.
HTTP_RETRY_AFTER_MAX_SECONDS = 30

# База backoff при повторах.
# Задержка = BACKOFF_BASE_SECONDS * (2 ** attempt).
BACKOFF_BASE_SECONDS = 0.4

# TTL маппинга INN->company_id в днях.
COMPANY_MAP_TTL_DAYS = 30

# TTL кэша карточки компании (часы).
EDISCLOSURE_CARD_TTL_HOURS = 24

# Режим обхода e-disclosure.
# Значения: "incremental" (быстрый режим по умолчанию) или "full_sync".
EDISCLOSURE_MODE = "incremental"

# Запускать полный scan раз в N прогонов (когда режим incremental).
EDISCLOSURE_FULL_SCAN_EVERY_N_RUNS = 20

# Если последний full scan старше N дней, делаем полный обход.
EDISCLOSURE_FULL_SCAN_MAX_AGE_DAYS = 7

# Максимальное число worker-потоков для сбора e-disclosure.
EDISCLOSURE_MAX_WORKERS = 12

# Минимальное число worker-потоков при деградации/троттлинге.
EDISCLOSURE_MIN_WORKERS = 4

# Включить авто-регулирование уровня параллелизма.
EDISCLOSURE_ADAPTIVE_CONCURRENCY = True

# Максимум кандидатов компании, для которых проверяется карточка при неоднозначном поиске.
EDISCLOSURE_MAX_CARD_CHECKS = 2

# Сколько первых строк читать в cheap-check перед полным парсингом страницы отчетности.
EDISCLOSURE_PREVIEW_ROWS = 1

# Максимум новых строк, которые парсим сверху на один тип отчета в incremental режиме.
EDISCLOSURE_PARSE_MAX_NEW_ROWS_PER_TYPE = 3

# Символический fast-path jitter в миллисекундах (обычный успешный запрос).
EDISCLOSURE_FAST_JITTER_MIN_MS = 0
EDISCLOSURE_FAST_JITTER_MAX_MS = 10

# Случайная задержка при инициализации thread-local клиента (мс).
# Нужна для сглаживания всплеска параллельных warmup-запросов к e-disclosure.
# Значения: int >= 0. По умолчанию: 250.
EDISCLOSURE_INIT_STAGGER_MAX_MS = 250

# Выполнять warmup-запросы к e-disclosure при создании клиента.
# Значения: bool. По умолчанию: True.
EDISCLOSURE_WARMUP_ENABLED = True

# Считать warmup обязательным.
# Значения: bool. По умолчанию: False.
# False: при 429/503 на warmup логируем предупреждение и продолжаем работу.
# True: при провале warmup поднимаем исключение.
EDISCLOSURE_WARMUP_STRICT = False

# Jitter только для retry-path (429/403/5xx/timeout).
EDISCLOSURE_RETRY_JITTER_MIN_MS = 150
EDISCLOSURE_RETRY_JITTER_MAX_MS = 500

# TTL кэша событий компании (часы).
EDISCLOSURE_EVENTS_TTL_HOURS = 6

# TTL кэша отчетов/files (часы).
EDISCLOSURE_REPORTS_TTL_HOURS = 12

# Горизонт новостей в днях.
NEWS_DAYS_BACK = 30

# Пауза между запросами к Smartlab (секунды).
NEWS_REQUEST_PAUSE_SECONDS = 1.0

# Порог stale-alert по отчетности (дней).
REPORT_STALE_DAYS = 220

# Максимальная ширина колонки в Excel.
MAX_EXCEL_COL_WIDTH = 60

# Формат отображения дат в Excel.
EXCEL_DATE_FORMAT = "yyyy-mm-dd"

# Подсветка новых событий/новостей.
NEW_ITEM_FILL_COLOR = "FFFFF2CC"

# Browser-like User-Agent для web-flow запросов.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
