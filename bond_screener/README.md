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
3. `Stage2` — отбор `candidate_bonds` по `Greenlist` + применение ручных исключений `Dropped_bonds.xlsx`.

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
- `excel_debug_exports: [stage2, ...]` — какие debug-витрины выгружать (по имени stage).
- `stage1.ttl_hours` — TTL в часах для сетевого обновления Stage1.
- `stage1.emitents_page_size` — размер страницы для справочника эмитентов MOEX (`/iss/securities.json`).
- `stage1.emitents_max_pages` — защитный лимит страниц справочника эмитентов, чтобы исключить бесконечный цикл при проблемах API.
- `stage2.dropped_ui_filename` — имя ручного файла исключений (по умолчанию `Dropped_bonds.xlsx`).
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

## Stage2 (кратко)

Stage2 создаёт и поддерживает:
- `candidate_bonds`
- `dropped_manual`
- `dropped_auto`
- `dropped_effective` (VIEW с приоритетом `manual` над `auto`)
- `source/xlsx/Dropped_bonds.xlsx` — ручной UI для исключений бумаг

Правила Stage2:
- `ScoringSelector` читает `emitents_effective` и `securities_raw` из БД, берёт только `Greenlist` и полностью пересобирает `candidate_bonds` на каждом запуске.
- `DroppedManager` читает `Dropped_bonds.xlsx`, синхронизирует его в `dropped_manual` (UPSERT), затем удаляет из `candidate_bonds` бумаги из `dropped_effective`.
- TTL для dropped записей: если `until < today`, запись считается истекшей и автоматически не применяется во view `dropped_effective` (история остаётся в Excel/SQLite).

Подробно: `scripts/stage2/README.md`.

## Логи и интерактивность

- Stage0:
  - `logs/stage0_env_check.log`
  - `logs/stage0_reset_tool.log`
  - `logs/stage0_run_registry.log`
- Stage1:
  - `logs/stage1_run.log`
  - `logs/stage1_moex_emitents_collector.log`
- Stage2:
  - `logs/stage2_run.log`
  - `logs/stage2_scoring_selector.log`
  - `logs/stage2_dropped_manager.log`

В консоли показываются прогрессбары (`tqdm`) и статусы выполнения.
