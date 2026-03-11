from __future__ import annotations

from pathlib import Path

# =============================
# Базовые пути проекта Scalp
# =============================
# ROOT_DIR: абсолютный путь к подпроекту Scalp.
# Значение формируется автоматически на базе расположения текущего файла.
ROOT_DIR = Path(__file__).resolve().parent

# PROJECT_ROOT: путь к корню основного репозитория (на уровень выше папки Scalp).
PROJECT_ROOT = ROOT_DIR.parent

# INPUT_SCREENER_PATH: путь к read-only источнику Screener.xlsx.
# Дефолт: <корень проекта>/Screener.xlsx.
INPUT_SCREENER_PATH = PROJECT_ROOT / "Screener.xlsx"

# INPUT_EMITENTS_PATH: путь к read-only источнику Emitents.xlsx.
# Используется для обогащения полями эмитента при наличии.
INPUT_EMITENTS_PATH = PROJECT_ROOT / "Emitents.xlsx"

# OUTPUT_EXCEL_PATH: целевой файл витрины сигналов.
OUTPUT_EXCEL_PATH = ROOT_DIR / "Scalp.xlsx"

# SNAPSHOT_EXCEL_PATH: файл с примерами raw/snapshot для ручной проверки.
SNAPSHOT_EXCEL_PATH = ROOT_DIR / "BaseSnapshots" / "scalp_snapshot.xlsx"

# SQLITE_PATH: отдельная автономная БД только для контура Scalp.
SQLITE_PATH = ROOT_DIR / "DB" / "scalp.sqlite3"

# LOG_PATH: перезаписываемый лог Scalp процесса.
LOG_PATH = ROOT_DIR / "logs" / "scalp.log"

# Служебные папки автономного контура.
CACHE_DIR = ROOT_DIR / "cache"
RAW_DIR = ROOT_DIR / "raw"
DB_DIR = ROOT_DIR / "DB"
LOG_DIR = ROOT_DIR / "logs"
BASE_SNAPSHOTS_DIR = ROOT_DIR / "BaseSnapshots"

# =============================
# Источники данных рынка
# =============================
# MOEX_ISS_SECURITY_ENDPOINT: endpoint MOEX ISS по конкретному SECID.
# Параметр {secid} подставляется автоматически.
MOEX_ISS_SECURITY_ENDPOINT = (
    "https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{secid}.json"
)

# REQUEST_TIMEOUT_SECONDS: таймаут одного HTTP-запроса.
# Допустимые значения: float/int > 0.
# Дефолт: 12.
REQUEST_TIMEOUT_SECONDS = 12

# REQUEST_RETRIES: число повторных попыток HTTP при ошибках сети/5xx.
# Допустимые значения: int >= 0.
# Дефолт: 3.
REQUEST_RETRIES = 3

# REQUEST_BACKOFF_SECONDS: базовая пауза между ретраями.
# Допустимые значения: float/int >= 0.
# Дефолт: 1.2.
REQUEST_BACKOFF_SECONDS = 1.2

# MARKET_TTL_SECONDS: TTL для raw-cache MOEX ответа.
# Если не истек, используется локальный cache/*.json без повторного запроса.
# Допустимые значения: int >= 0.
# Дефолт: 45.
MARKET_TTL_SECONDS = 45

# SNAPSHOT_INTERVAL_SECONDS: рекомендуемый интервал между запусками скрипта,
# при котором сравнение snapshot'ов дает наилучший эффект.
# Используется как метапараметр в логике и документации.
# Допустимые значения: int >= 1.
# Дефолт: 120.
SNAPSHOT_INTERVAL_SECONDS = 120

# =============================
# Фильтры сигналов и ликвидности
# =============================
# SIGNAL_MIN_SCORE: минимальный итоговый скоринг сигнала для вывода в витрину.
# Допустимые значения: 0..100.
# Дефолт: 35.
SIGNAL_MIN_SCORE = 35

# MIN_NUM_TRADES: минимальное число сделок для валидного сигнала.
# Допустимые значения: int >= 0.
# Дефолт: 8.
MIN_NUM_TRADES = 8

# MIN_TURNOVER_RUB: минимальный оборот (руб.) для валидного сигнала.
# Допустимые значения: float/int >= 0.
# Дефолт: 400000.
MIN_TURNOVER_RUB = 400_000

# MIN_VOLUME_PIECES: минимальный объем (шт.) для валидного сигнала.
# Допустимые значения: float/int >= 0.
# Дефолт: 1000.
MIN_VOLUME_PIECES = 1_000

# MIN_DAYS_TO_EVENT: минимальное расстояние до ближайшего события (купон/оферта/амортизация),
# при меньшем значении сигнал подавляется или сильно ослабляется.
# Допустимые значения: int >= 0.
# Дефолт: 10.
MIN_DAYS_TO_EVENT = 10

# EVENT_HARD_BLOCK_DAYS: жесткая блокировка сигналов при слишком близком событии.
# Допустимые значения: int >= 0.
# Дефолт: 4.
EVENT_HARD_BLOCK_DAYS = 4

# =============================
# Пороги по типам сигналов
# =============================
# GAPDOWN_PCT_THRESHOLD: порог (%) падения открытия к prev close для GapDown.
GAPDOWN_PCT_THRESHOLD = -0.7

# INTRADAY_DUMP_PCT_THRESHOLD: порог (%) падения текущей цены к open для IntradayDump.
INTRADAY_DUMP_PCT_THRESHOLD = -0.9

# DIRTY_DROP_PCT_THRESHOLD: порог (%) изменения dirty к prev close.
DIRTY_DROP_PCT_THRESHOLD = -1.0

# REBOUND_MIN_PCT: минимальный отскок (%) от day low для ReboundCandidate.
REBOUND_MIN_PCT = 0.35

# REBOUND_REQUIRED_DUMP_PCT: минимальная внутренняя просадка от open до low,
# чтобы отскок считался meaningful.
REBOUND_REQUIRED_DUMP_PCT = -1.1

# PEER_DISLOCATION_DELTA_PCT: отклонение от медианы peer-группы по эмитенту.
PEER_DISLOCATION_DELTA_PCT = -0.9

# =============================
# Параметры исполнения
# =============================
# MAX_WORKERS: число потоков для параллельного запроса MOEX.
MAX_WORKERS = 8

# SNAPSHOT_SAMPLE_ROWS: число строк для листа BaseSnapshot в Excel.
SNAPSHOT_SAMPLE_ROWS = 20

# USER_AGENT: User-Agent для HTTP запросов.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
