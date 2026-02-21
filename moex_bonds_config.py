"""Конфиг выгрузки облигаций MOEX.

Файл специально сделан простым и с комментариями,
чтобы его можно было редактировать вручную в VS Code без аргументов запуска.
"""

from __future__ import annotations

from pathlib import Path


# Базовая папка проекта (обычно менять не нужно).
BASE_DIR = Path(__file__).resolve().parent

# Папки проекта.
LOGS_DIR = BASE_DIR / "logs"
RAW_DIR = BASE_DIR / "raw"
CACHE_DIR = BASE_DIR / "cache" / "moex"
OUTPUT_DIR = BASE_DIR / "output"

# Основной файл (теперь «облегченный»):
# содержит только торгуемые облигации + читабельное описание выпусков.
CORE_OUTPUT_FILE = OUTPUT_DIR / "moex_bonds_full_export.xlsx"

# Отдельные справочники, чтобы не раздувать основной Excel.
EMITENTS_OUTPUT_FILE = OUTPUT_DIR / "moex_emitents.xlsx"
COUPONS_OUTPUT_FILE = OUTPUT_DIR / "moex_bond_coupons.xlsx"
AMORTIZATIONS_OUTPUT_FILE = OUTPUT_DIR / "moex_bond_amortizations.xlsx"
OFFERS_OUTPUT_FILE = OUTPUT_DIR / "moex_bond_offers.xlsx"

# Файл лога (перезаписывается на каждом запуске).
LOG_FILE = LOGS_DIR / "moex_bonds_export.log"

# Сетевые настройки.
REQUEST_TIMEOUT_SECONDS = 30  # Таймаут одного HTTP-запроса к MOEX.
RETRY_COUNT = 3  # Сколько раз повторять запрос при временной ошибке.

# Производительность.
MAX_WORKERS = 10  # Количество потоков для параллельной загрузки.
CHUNK_COUNT = 10  # На сколько частей делить список облигаций.

# Ограничитель для отладки: None = брать все облигации.
MAX_BONDS_TO_PROCESS: int | None = None

# Срок жизни кэша (вынесено в отдельный блок, как требовалось).
# Каждый пункт отвечает за свой тип данных.
CACHE_TTL_HOURS = {
    # Карточка бумаги + bondization (купоны/амортизации/оферты).
    "security_details": 24,
    # Точечный справочник /securities?q=<SECID>.
    "reference_rows": 24,
    # Чекпоинт загрузки расширенных данных (для возобновления после падения сети).
    "checkpoint": 72,
}

# Служебные пути checkpoint-механизма.
CHECKPOINT_DIR = CACHE_DIR / "checkpoints"
CHECKPOINT_STATE_FILE = CHECKPOINT_DIR / "extended_data_checkpoint.json"
CHECKPOINT_DESCRIPTIONS_FILE = CHECKPOINT_DIR / "descriptions.json"
CHECKPOINT_COUPONS_FILE = CHECKPOINT_DIR / "coupons.json"
CHECKPOINT_AMORTIZATIONS_FILE = CHECKPOINT_DIR / "amortizations.json"
CHECKPOINT_OFFERS_FILE = CHECKPOINT_DIR / "offers.json"

# URL API MOEX ISS.
MOEX_BASE_URL = "https://iss.moex.com/iss"
