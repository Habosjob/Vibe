# Модули и скрипты

## `run.py`
- Назначение: запуск полного сценария без аргументов.
- Вход: `config.yml` (опционально), сеть.
- Выход: `output/moex_bonds.xlsx`, `logs/latest.log`, опционально `raw/*.json`.
- Как менять конфиг: правьте `config.yml` (см. `docs/CONFIG.md`).

## `moex_bond_screener/config.py`
- Назначение: загрузка конфигурации и дефолтов.
- Вход: `config.yml`.
- Выход: объект `AppConfig`.

## `moex_bond_screener/moex_client.py`
- Назначение: постраничный запрос облигаций с ретраями и задержками; парсит все поля `securities`, которые вернула MOEX.
- Вход: `AppConfig`, logger.
- Выход: список облигаций + число ошибок.

## `moex_bond_screener/raw_store.py`
- Назначение: сохранять raw JSON и чистить папку `raw/` по TTL/размеру.
- Вход: payload строкой, настройки TTL/лимита.
- Выход: файлы `raw/*.json`.

## `moex_bond_screener/writer.py`
- Назначение: запись итогового Excel (`.xlsx`) или CSV (`.csv`) с улучшенной читабельностью Excel (шапка, фильтр, автоширина, freeze panes) и динамическим набором колонок по данным MOEX.
- Вход: список словарей с облигациями.
- Выход: Excel/CSV-файл.

## `moex_bond_screener/logging_utils.py`
- Назначение: настраивает логирование в файл и консоль.
- Вход: нет.
- Выход: объект `Logger`.
