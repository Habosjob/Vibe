# Локальная БД SQLite

## Назначение

Модуль `bond_screener/db.py` описывает схему SQLite БД для хранения инструментов, эмитентов, рейтингов, кэшируемых полей и снапшотов расчёта.

## Что на входе

- путь к файлу БД (`Path` или `str`) для функций:
  - `init_db(db_path)` — создать таблицы;
  - `make_session_factory(db_path)` — вернуть `sessionmaker` для чтения/записи.

## Что на выходе

- создаётся файл SQLite (например `data/bond_screener.sqlite`);
- при `init_db(...)` выполняется безопасная авто-миграция старых БД (например, добавляются недостающие колонки `instruments.shortname`, `instruments.primary_boardid`, `instruments.board`);
- в нём создаются таблицы:
  - `instruments`
  - `instrument_fields`
  - `issuers`
  - `issuer_fields`
  - `cashflows`
  - `offers`
  - `ratings`
  - `publications`
  - `snapshots`

## Скрипт `scripts/db_inspect.py`

Короткое назначение: печатает статистику по количеству строк в каждой таблице.

- Вход: файл БД `data/bond_screener.sqlite` (создаётся автоматически, если отсутствует).
- Выход: печать статистики в консоль вида `- table_name: N rows`.
- Как менять конфиг: поменяйте константу `DEFAULT_DB_PATH` внутри `scripts/db_inspect.py`.
