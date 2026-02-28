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

## Где что лежит

- `config/config.yaml` — все основные настройки приложения.
- `config/reset.yaml` — одноразовые флаги сброса данных на следующий запуск.
- `db/bonds.db` — SQLite, главный источник правды.
- `logs/*.log` — перезаписываемые логи по одному файлу на каждый скрипт.
- `source/xlsx/*.xlsx` — debug-выгрузки в Excel.
- `cache/http` — HTTP cache c TTL.
- `cache/checkpoints` — директория под файлы чекпоинтов (резерв), текущая реализация чекпоинтов хранится в SQLite (`job_items`).

## Конфигурация (`config/config.yaml`)

Ключевые параметры:

- `excel_debug: true|false` — включить/выключить Excel выгрузки.
- `excel_debug_exports: [stage0, ...]` — какие витрины выгружать (по имени).
- `paths.*` — относительные пути директорий.
- `net.timeout` — явные сетевые таймауты (`connect/read/write/pool`).
- `net.retry` — retry с exponential backoff + jitter.
- `net.cache_ttl_s_default` — TTL кэша по умолчанию.
- `db.filename` — путь к SQLite.

## Reset-процедура (`config/reset.yaml`)

Файл используется скриптом `scripts/stage0/reset_tool.py`.

Пример:

```yaml
reset_mode: ["cache", "checkpoints"]
cache:
  clear_all: true
checkpoints:
  clear_all: true
```

После применения reset tool автоматически возвращает `reset.yaml` к безопасному состоянию:

- `reset_mode: []`
- все флаги `false`

Это защищает от случайного повторного удаления данных на каждом запуске.

## Логи и интерактивность

- Каждый Stage0-скрипт пишет отдельный лог:
  - `logs/stage0_env_check.log`
  - `logs/stage0_reset_tool.log`
  - `logs/stage0_run_registry.log`
- Формат логов:
  - `timestamp | level | module | message`
- В консоли показывается прогрессбар (`tqdm`) и статусы:
  - `[STAGE0][env_check] OK | 0.42s | msg...`
  - `[STAGE0][env_check] FAIL | 0.10s | error=...`

## Что реализовано в Stage0

- Базовая структура проекта.
- Загрузка YAML-конфигов.
- Инициализация SQLite и таблиц `runs`, `job_items`.
- Run registry (`open_run`, `close_run_success`, `close_run_fail`).
- HTTP cache с TTL и API `get/set/is_expired/clear`.
- Async HTTP client (`httpx`) с retry (`tenacity`) и cache.
- Checkpoint интерфейс на SQLite:
  - `start_job(job_name, items)`
  - `mark_done(item)`
  - `mark_failed(item, error)`
  - `resume_pending(job_name)`

Подробнее: `scripts/stage0/README.md`.
