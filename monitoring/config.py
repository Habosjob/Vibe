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

# Таймаут HTTP-запроса (секунды).
# Значения: int/float > 0. По умолчанию: 25.
REQUEST_TIMEOUT_SECONDS = 25

# Количество повторов сетевых запросов.
# Значения: int >= 0. По умолчанию: 3.
HTTP_RETRIES = 3

# Множитель backoff при повторах.
# Задержка = BACKOFF_BASE_SECONDS * (attempt + 1).
BACKOFF_BASE_SECONDS = 1.2

# TTL маппинга INN->company_id в днях.
COMPANY_MAP_TTL_DAYS = 30

# TTL кэша карточки компании (часы).
EDISCLOSURE_CARD_TTL_HOURS = 24

# Максимальное число worker-потоков для сбора e-disclosure.
# Значения: int, рекомендуемый диапазон 6..12. По умолчанию: 8.
EDISCLOSURE_MAX_WORKERS = 8

# Максимум кандидатов компании, для которых проверяется карточка при неоднозначном поиске.
# Значения: int >= 1. По умолчанию: 3.
EDISCLOSURE_MAX_CARD_CHECKS = 3

# Сколько первых строк читать в cheap-check перед полным парсингом страницы отчетности.
# Значения: int >= 1. По умолчанию: 2.
EDISCLOSURE_PREVIEW_ROWS = 2

# Случайная задержка (джиттер) перед HTTP запросом к e-disclosure в миллисекундах.
# Значения: int >= 0. По умолчанию: 50..150.
EDISCLOSURE_REQUEST_JITTER_MIN_MS = 50
EDISCLOSURE_REQUEST_JITTER_MAX_MS = 150

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
