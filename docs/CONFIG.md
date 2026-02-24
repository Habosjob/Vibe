# Конфигурация проекта

Все конфиги лежат в `config/` и создаются автоматически при первом запуске `python scripts/run.py`.

## `config/config.yml`

Главный конфиг приложения.

Поля по умолчанию:

- `app.name` — имя приложения.
- `app.timezone` — часовой пояс (по умолчанию `Europe/Moscow`).
- `logging.level` — уровень логирования (`INFO`).
- `logging.file` — путь к логу (`logs/latest.log`).
- `raw.enabled` — включение raw-дампов (безопасный дефолт: `true`).
- `raw.ttl_days` — TTL очистки папки `raw/` в днях (по умолчанию `7`).
- `output.excel_file` — путь к Excel-результату общего запуска (`out/bond_screener.xlsx`).
- `output.screen_basic_excel` — путь к Excel-результату базового скрининга (`out/screen_basic.xlsx`).
- `database.path` — путь к SQLite базе (по умолчанию `data/bond_screener.sqlite`).
- `providers.moex_iss.limit` — размер страницы ISS (по умолчанию `100`).
- `providers.moex_iss.q` — поисковая строка ISS (`null` = без фильтра).
- `providers.moex_iss.cache_ttl_seconds` — TTL HTTP-кэша для ISS-запросов.
- `providers.moex_iss.cashflows_cache_ttl_seconds` — TTL кэша запросов расписания выплат (по умолчанию `86400`, 24 часа).
- `providers.moex_iss.cashflows_concurrency` — параллельность загрузки расписаний выплат.
- `providers.moex_iss.rate_limit_per_sec` — ограничение частоты запросов к `iss.moex.com`.

## `config/scenarios.yml`

Сценарии отбора бумаг. Сейчас используется только дефолтный шаблон с параметрами-заглушками.

## `config/allowlist.yml`

Белый список инструментов/эмитентов:

- `isins` — список ISIN.
- `emitents` — список эмитентов.

## `config/issuer_links.yml`

Справочник связей по эмитентам (`issuers`). Сейчас шаблон пустой.

## `config/portfolio.yml`

Текущий портфель (`positions`). Сейчас шаблон пустой.

## Секреты

Секреты (логины/токены) храните только в `config/secrets.yml` или в ENV-переменных.
Файл `config/secrets.yml` добавлен в `.gitignore` и не должен попадать в git.

## SQLite БД (текущий безопасный дефолт)

- По умолчанию инспектор БД использует файл `data/bond_screener.sqlite`.
- Если файла нет, таблицы создаются автоматически при первом вызове `init_db(...)` или `python scripts/db_inspect.py`.
- Для смены пути БД измените путь в вызывающем коде (или константу `DEFAULT_DB_PATH` в `scripts/db_inspect.py`).
