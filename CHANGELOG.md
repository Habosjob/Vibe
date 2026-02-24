# CHANGELOG

## 2026-02-24
- Ускорен `sync_moex_cashflows`: запись в `cashflows` и `instrument_fields` переведена на batch-операции (меньше транзакций и запросов).
- Повышены безопасные дефолты производительности для cashflows sync: `cashflows_concurrency=12`, `rate_limit_per_sec=15`.
- Добавлен sync cashflows из MOEX bondization: сохранение `coupon`/`amort`/`redemption` в таблицу `cashflows`.
- Добавлен расчет и сохранение derived-полей (`maturity_date`, `next_coupon_date`, `amort_start_date`, `has_amortization`) в `instrument_fields`.
- Добавлен скрипт `scripts/sync_moex_cashflows.py` с параллелизмом из конфига, кэшем 24 часа и выгрузкой sample-файлов `out/cashflows_sample.xlsx`, `out/derived_sample.xlsx`.
- Добавлены pytest-тесты на парсинг cashflows, расчет derived-полей и запись в БД.
- Инициализирована структура проекта `bond_screener` с запуском через `scripts/run.py`.
- Добавлено автосоздание конфигов и рабочих директорий (`config/`, `out/`, `logs/`, `raw/`).
- Добавлена очистка `raw/` по TTL и перезапись `logs/latest.log` на каждом запуске.
- Добавлен pytest-тест на автосоздание конфигов в временной директории.
- Добавлена базовая документация на русском в `docs/` и `README.md`.
- Добавлена SQLite-схема на SQLAlchemy (`instruments`, `issuers`, `cashflows`, `offers`, `ratings`, `publications`, `snapshots` и поля-расширения).
- Добавлен скрипт `scripts/db_inspect.py` для печати статистики по таблицам БД.
- Добавлен pytest-тест на создание БД, вставку и чтение данных.

