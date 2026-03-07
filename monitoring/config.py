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

# Soft TTL маппинга INN->company_id в днях (можно обновлять в фоне, но не ломать hot path).
# Значения: int >= 1. По умолчанию: 30.
COMPANY_MAP_SOFT_TTL_DAYS = 30

# Hard TTL маппинга INN->company_id в днях (после истечения нужен новый company search).
# Значения: int >= 1. По умолчанию: 90.
COMPANY_MAP_HARD_TTL_DAYS = 90

# TTL кэша карточки компании (часы).
EDISCLOSURE_CARD_TTL_HOURS = 24

# Принудительный full scan всех эмитентов (override scheduler + event gate).
# Значения: bool. По умолчанию: False.
EDISCLOSURE_FORCE_FULL_SCAN = False

# Интервалы scheduler для stage_reports (часы).
# RECENT_CHANGE: если изменения были в последние 30 дней.
# ACTIVE: если изменения были в последние 90 дней.
# STABLE: давно стабильный эмитент.
EDISCLOSURE_RECENT_CHANGE_RECHECK_HOURS = 6
EDISCLOSURE_ACTIVE_RECHECK_HOURS = 24
EDISCLOSURE_STABLE_RECHECK_HOURS = 72

# Через сколько дней без full scan принудительно выполнять deep pass.
# Значения: int >= 1. По умолчанию: 14.
EDISCLOSURE_FULL_SCAN_MAX_AGE_DAYS = 14

# Пределы и дефолты для worker pool e-disclosure.
# На старте используется DEFAULT, затем AUTOTUNE может выбрать значение в [MIN, MAX].
EDISCLOSURE_FETCH_WORKERS_MIN = 8
EDISCLOSURE_FETCH_WORKERS_DEFAULT = 12
EDISCLOSURE_FETCH_WORKERS_MAX = 24

# Пределы и дефолты semaphore для тяжелого endpoint files.aspx.
# На старте используется DEFAULT, затем AUTOTUNE может выбрать значение в [MIN, MAX].
EDISCLOSURE_FILES_SEMAPHORE_MIN = 4
EDISCLOSURE_FILES_SEMAPHORE_DEFAULT = 6
EDISCLOSURE_FILES_SEMAPHORE_MAX = 10

# Автотюнинг параллельности e-disclosure.
# True: подбираем стабильные workers/files_semaphore по телеметрии и сохраняем в БД meta.
EDISCLOSURE_AUTOTUNE_ENABLED = True

# Порог «массовых» 429/timeout (доля от общего числа запросов), выше которого autotune снижает параллельность.
EDISCLOSURE_AUTOTUNE_ERROR_RATE_THRESHOLD = 0.06

# Максимум кандидатов компании, для которых проверяется карточка при неоднозначном поиске.
EDISCLOSURE_MAX_CARD_CHECKS = 2

# Максимум новых строк, которые парсим сверху на один тип отчета в incremental режиме.
EDISCLOSURE_PARSE_MAX_NEW_ROWS_PER_TYPE = 3

# Символический fast-path jitter в миллисекундах (обычный успешный запрос).
EDISCLOSURE_FAST_JITTER_MIN_MS = 0
EDISCLOSURE_FAST_JITTER_MAX_MS = 0

# Deprecated: историческая настройка stagger при инициализации клиента.
# Не участвует в runtime execution path.
EDISCLOSURE_INIT_STAGGER_MAX_MS = 0

# Warmup path intentionally removed from execution flow.

# Jitter только для retry-path (429/403/5xx/timeout).
EDISCLOSURE_RETRY_JITTER_MIN_MS = 100
EDISCLOSURE_RETRY_JITTER_MAX_MS = 300

# -----------------------------
# Deprecated runtime-throttling knobs
# -----------------------------
# Сохраняем для обратной совместимости конфиг-файла,
# но эти параметры больше не участвуют в stage_reports execution path.
EDISCLOSURE_MAX_INFLIGHT_REQUESTS = 0
EDISCLOSURE_MAX_INFLIGHT_FILES_REQUESTS = 0
EDISCLOSURE_MIN_REQUEST_INTERVAL_MS = 0
EDISCLOSURE_BURST_SIZE = 0
EDISCLOSURE_ADAPTIVE_WINDOW = 0
EDISCLOSURE_DECAY_ON_429 = 0
EDISCLOSURE_GROWTH_STEP = 0
EDISCLOSURE_STABLE_WINDOWS_TO_GROW = 0
EDISCLOSURE_MIN_WORKERS = 0
EDISCLOSURE_MAX_WORKERS = 0
EDISCLOSURE_ADAPTIVE_CONCURRENCY = False

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
