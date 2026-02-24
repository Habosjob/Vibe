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
- `output.excel_file` — путь к Excel-результату (пока заглушка: `out/bond_screener.xlsx`).

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
