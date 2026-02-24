# bond_screener

Локальный проект для скрининга облигаций MOEX, ориентированный на запуск из VS Code и работу без аргументов.

## Быстрый старт

1. Установите Python 3.11+.
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```
3. Запустите проект:
   ```bash
   python run.py
   ```

## Что происходит при запуске без аргументов

- автоматически создаются папки `config/`, `out/`, `logs/`, `raw/`;
- если нет конфигов, создаются:
  - `config/config.yml`
  - `config/scenarios.yml`
  - `config/allowlist.yml`
  - `config/issuer_links.yml`
  - `config/portfolio.yml`
- выполняется полный пайплайн: `sync_moex_universe` → `sync_moex_cashflows` → `screen_basic`;
- на каждом этапе печатается прогресс и итог;
- перезаписывается `logs/latest.log`;
- печатается итоговая сводка и время выполнения.

## Структура

- `bond_screener/` — пакет с основной логикой запуска.
- `scripts/` — исполняемые скрипты (`run.py`, `sync_moex_universe.py`, `sync_moex_cashflows.py`, `screen_basic.py`).
- `docs/` — документация по конфигам, источникам, скорингу и мониторингу.
- `tests/` — pytest-тесты.
- `config/`, `out/`, `logs/`, `raw/` — рабочие папки, создаются автоматически.

## Тесты

```bash
pytest
```


## SQLite БД

- Схема БД описана в `bond_screener/db.py` (SQLAlchemy, SQLite).
- Инициализация таблиц: используйте `init_db(...)` или `make_session_factory(...)`.
- Быстрая проверка наполнения таблиц:
  ```bash
  python scripts/db_inspect.py
  ```
- Подробности в `docs/DB.md`.


## Базовый скрининг (Excel)

```bash
python scripts/screen_basic.py
```

Скрипт читает derived-поля из SQLite (`instrument_fields`), записывает минимальную классификацию `bond_class` и формирует `out/screen_basic.xlsx` с листами `screen_pass` и `screen_drop`.
