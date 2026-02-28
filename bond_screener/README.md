# bond_screener

Самообъясняемый каркас проекта для поэтапной реализации Stage0..Stage5.

## Быстрый старт

1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Запустите проект (без CLI-аргументов):
   ```bash
   python main.py
   ```

## Порядок запуска

`main.py` запускает стадии последовательно:
1. `Stage0` — инфраструктурные проверки и reset-инструменты.
2. `Stage1` — сбор эмитентов/облигаций MOEX и пересборка `Emitents.xlsx`.

## Где что лежит

- `config/config.yaml` — все основные настройки приложения.
- `config/reset.yaml` — одноразовые флаги сброса данных на следующий запуск.
- `db/bonds.db` — SQLite, главный источник правды.
- `logs/*.log` — перезаписываемые логи по одному файлу на каждый скрипт.
- `source/xlsx/*.xlsx` — ручной UI и debug-выгрузки.
- `cache/http` — HTTP cache c TTL.
- `cache/checkpoints` — директория под файлы чекпоинтов (резерв), текущая реализация чекпоинтов хранится в SQLite (`job_items`).

## Конфигурация (`config/config.yaml`)

Ключевые параметры:

- `excel_debug: true|false` — включить/выключить Excel выгрузки.
- `excel_debug_exports: [stage1, ...]` — какие витрины выгружать (по имени).
- `stage1.ttl_hours` — TTL в часах для сетевого обновления Stage1.
- `paths.*` — относительные пути директорий.
- `net.timeout` — явные сетевые таймауты (`connect/read/write/pool`).
- `net.retry` — retry с exponential backoff + jitter.
- `net.cache_ttl_s_default` — TTL кэша по умолчанию.
- `db.filename` — путь к SQLite.

## Stage1 (кратко)

Stage1 создаёт и поддерживает:
- `emitents_raw`
- `securities_raw`
- `emitents_manual`
- `emitents_effective` (VIEW)
- `source/xlsx/Emitents.xlsx` — ручной UI с валидацией `scoring_flag`.

Правила ручных полей:
- `scoring_flag` допускает только `Greenlist | Yellowlist | Redlist | ""`.
- `scoring_date` хранится в Excel как дата с форматом `DD.MM.YYYY`, в SQLite — строкой `DD.MM.YYYY`.
- Если `scoring_flag` пустой — `scoring_date` очищается.

Подробно: `scripts/stage1/README.md`.

## Логи и интерактивность

- Stage0:
  - `logs/stage0_env_check.log`
  - `logs/stage0_reset_tool.log`
  - `logs/stage0_run_registry.log`
- Stage1:
  - `logs/stage1_run.log`
  - `logs/stage1_moex_emitents_collector.log`

В консоли показываются прогрессбары (`tqdm`) и статусы выполнения.
